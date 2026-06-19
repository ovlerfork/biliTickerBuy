use anyhow::{anyhow, Result};
use reqwest::Client;
use serde_json::{json, Value};
use sntpc;
use std::net::UdpSocket;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub const DEFAULT_TIME_SERVER: &str = "ntp.aliyun.com";
pub const TIME_SYNC_ATTEMPTS: usize = 3;
pub const TIME_SYNC_WARMUP_ATTEMPTS: usize = 1;
pub const MIN_SUCCESSFUL_TIME_SYNC_SAMPLES: usize = 2;
pub const TIME_SYNC_SAMPLE_INTERVAL_MS: u64 = 80;

#[derive(Clone, serde::Serialize)]
pub struct TimeSyncSample {
    pub offset: i64,
    pub server: i64,
    pub local_before: i64,
    pub local_after: i64,
    pub round_trip: i64,
}

#[derive(serde::Serialize)]
pub struct TimeSyncQuality {
    pub label: &'static str,
    pub trustworthy: bool,
    pub spread: i64,
    pub best_round_trip: i64,
    pub sample_count: usize,
    pub failed_sample_count: usize,
}

#[derive(serde::Serialize)]
pub struct TimeSyncResult {
    pub diff: i64,
    pub server: i64,
    pub local: i64,
    pub round_trip: i64,
    pub spread: i64,
    pub quality: TimeSyncQuality,
    pub samples: Vec<TimeSyncSample>,
}

fn score_time_sample(sample: &TimeSyncSample, median_offset: i64) -> i64 {
    sample.round_trip + (sample.offset - median_offset).abs()
}

pub fn build_time_sync_result(
    mut samples: Vec<TimeSyncSample>,
    failed_sample_count: usize,
    current_local: i64,
) -> Result<TimeSyncResult> {
    if samples.len() < MIN_SUCCESSFUL_TIME_SYNC_SAMPLES {
        return Err(anyhow!(
            "Too few time samples collected: {}/{} succeeded",
            samples.len(),
            samples.len() + failed_sample_count
        ));
    }

    let mut offsets: Vec<i64> = samples.iter().map(|sample| sample.offset).collect();
    offsets.sort_unstable();
    let median_offset = offsets[offsets.len() / 2];
    let min_offset = *offsets.first().unwrap();
    let max_offset = *offsets.last().unwrap();
    let spread = max_offset - min_offset;

    samples.sort_by_key(|sample| score_time_sample(sample, median_offset));
    let best = samples[0].clone();
    let trustworthy = samples.len() >= 2 && spread <= 200 && best.round_trip <= 1500;
    let label = if trustworthy {
        "good"
    } else if spread <= 500 && best.round_trip <= 3000 {
        "ok"
    } else {
        "poor"
    };

    Ok(TimeSyncResult {
        diff: best.offset,
        server: current_local + best.offset,
        local: current_local,
        round_trip: best.round_trip,
        spread,
        quality: TimeSyncQuality {
            label,
            trustworthy,
            spread,
            best_round_trip: best.round_trip,
            sample_count: samples.len(),
            failed_sample_count,
        },
        samples,
    })
}

async fn read_time_once(client: &Client, url: &str) -> Result<i64> {
    if url.starts_with("http") {
        get_server_time(client, Some(url.to_string())).await
    } else {
        let ntp_url = url.to_string();
        tokio::task::spawn_blocking(move || get_ntp_time(&ntp_url).map(|t| t as i64))
            .await
            .map_err(|e| anyhow!("Task join error: {}", e))?
    }
}

pub async fn sample_time(client: &Client, url: &str) -> Result<TimeSyncResult> {
    let mut samples = Vec::new();
    let mut failed_sample_count = 0;

    for attempt in 0..TIME_SYNC_ATTEMPTS {
        let local_before = get_local_time();
        match read_time_once(client, url).await {
            Ok(server_time) => {
                let local_after = get_local_time();
                let round_trip = local_after.saturating_sub(local_before);
                let local_midpoint = local_before.saturating_add(round_trip / 2);

                // ponytail: first request warms DNS/TCP/NTP path; use later samples for offset.
                if attempt >= TIME_SYNC_WARMUP_ATTEMPTS {
                    samples.push(TimeSyncSample {
                        offset: server_time.saturating_sub(local_midpoint),
                        server: server_time,
                        local_before,
                        local_after,
                        round_trip,
                    });
                }
            }
            Err(_) => {
                failed_sample_count += 1;
            }
        }

        if attempt + 1 < TIME_SYNC_ATTEMPTS {
            tokio::time::sleep(Duration::from_millis(TIME_SYNC_SAMPLE_INTERVAL_MS)).await;
        }
    }

    build_time_sync_result(samples, failed_sample_count, get_local_time())
}

pub fn build_shared_client() -> Result<Client> {
    Client::builder()
        .connect_timeout(Duration::from_secs(3))
        .tcp_keepalive(Duration::from_secs(60))
        .build()
        .map_err(|e| anyhow!("Failed to build shared HTTP client: {}", e))
}

async fn fetch_new_project_info(client: &Client, id: &str) -> Result<Value> {
    let items_id = id
        .parse::<u64>()
        .map(|n| json!(n))
        .unwrap_or_else(|_| json!(id));
    let mut res: Value = client.post("https://mall.bilibili.com/mall-search-items/items_detail/info")
        .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")
        .header("Origin", "https://mall.bilibili.com")
        .header("Referer", format!("https://mall.bilibili.com/neul-next/ticket-renovation/detail.html?id={}&from=pc_ticketlist&noTitleBar=1", id))
        .json(&json!({
            "itemsId": items_id,
            "itemsDetailPageType": 3
        }))
        .send()
        .await?
        .json()
        .await?;

    if res["success"].as_bool() == Some(false) {
        return Err(anyhow!("new project detail API returned success=false"));
    }
    if let Some(errno) = res["errno"].as_i64().or_else(|| res["code"].as_i64()) {
        if errno != 0 {
            return Err(anyhow!("new project detail API returned {}", errno));
        }
    }

    normalize_project_data(&mut res, id);
    if res["data"]["screen_list"]
        .as_array()
        .map(|list| !list.is_empty())
        .unwrap_or(false)
    {
        if let Some(root) = res.as_object_mut() {
            root.entry("code".to_string()).or_insert(json!(0));
            root.entry("errno".to_string()).or_insert(json!(0));
        }
        Ok(res)
    } else {
        Err(anyhow!(
            "new project detail API returned no usable screen_list"
        ))
    }
}

fn normalize_project_data(res: &mut Value, id: &str) {
    let Some(data) = res.get_mut("data").and_then(|v| v.as_object_mut()) else {
        return;
    };

    let project_id = data
        .get("id")
        .or_else(|| data.get("projectId"))
        .or_else(|| data.get("itemsId"))
        .cloned()
        .unwrap_or_else(|| json!(id));
    data.insert("id".to_string(), project_id.clone());
    data.entry("project_id".to_string()).or_insert(project_id);

    if !data.contains_key("name") {
        if let Some(name) = data.get("projectName").cloned() {
            data.insert("name".to_string(), name);
        }
    }

    let hot_project = data
        .get("hotProject")
        .or_else(|| data.get("hot_project"))
        .and_then(|v| {
            v.as_bool()
                .or_else(|| v.as_i64().map(|n| n != 0))
                .or_else(|| v.as_str().map(|s| s == "true" || s == "1"))
        })
        .unwrap_or(false);
    data.insert("hotProject".to_string(), json!(hot_project));
    data.insert("hot_project".to_string(), json!(hot_project));

    let screens = data
        .get("screen_list")
        .or_else(|| data.get("screenList"))
        .cloned();
    if data.get("screen_list").and_then(|v| v.as_array()).is_none() {
        if let Some(screens) = screens.clone() {
            data.insert("screen_list".to_string(), screens);
        }
    }
    if !data.contains_key("screenList") {
        if let Some(screens) = screens.clone() {
            data.insert("screenList".to_string(), screens);
        }
    }

    if !data.contains_key("venue_info") {
        if let Some(venue_info) = data.get("skuVenueInfo").cloned() {
            data.insert("venue_info".to_string(), venue_info);
        }
    }
    if !data.contains_key("sales_dates") {
        if let Some(sales_dates) = data.get("salesDates").cloned() {
            data.insert("sales_dates".to_string(), sales_dates);
        }
    }

    let screen_list = data
        .get("screen_list")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    if !data.contains_key("has_eticket") {
        let has_eticket = !screen_list.iter().any(|screen| {
            screen
                .get("express_fee")
                .and_then(|v| {
                    v.as_i64()
                        .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
                })
                .unwrap_or(0)
                > 0
        });
        data.insert("has_eticket".to_string(), json!(has_eticket));
    }

    let start_times: Vec<i64> = screen_list
        .iter()
        .filter_map(|screen| {
            screen.get("start_time").and_then(|v| {
                v.as_i64()
                    .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
            })
        })
        .collect();
    if !data.contains_key("start_time") {
        if let Some(start_time) = start_times.iter().min() {
            data.insert("start_time".to_string(), json!(start_time));
        }
    }
    if !data.contains_key("end_time") {
        if let Some(end_time) = data.get("endTime").cloned() {
            data.insert("end_time".to_string(), end_time);
        } else if let Some(end_time) = start_times.iter().max() {
            data.insert("end_time".to_string(), json!(end_time));
        }
    }
}

pub async fn fetch_project_info(client: &Client, id: String) -> Result<Value> {
    let mut res = match fetch_new_project_info(client, &id).await {
        Ok(res) => res,
        Err(_) => {
            let url = format!("https://show.bilibili.com/api/ticket/project/getV2?version=134&id={}&project_id={}", id, id);
            client.get(&url)
                .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")
                .send()
                .await?
                .json()
                .await?
        }
    };

    normalize_project_data(&mut res, &id);

    // Check for linked goods (场贩/周边)
    let link_url = format!(
        "https://show.bilibili.com/api/ticket/linkgoods/list?project_id={}&page_type=0",
        id
    );
    let link_res_result = client.get(&link_url)
        .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")
        .send()
        .await;

    if let Ok(link_resp) = link_res_result {
        if let Ok(link_res) = link_resp.json::<Value>().await {
            if let Some(list) = link_res["data"]["list"].as_array() {
                if !list.is_empty() {
                    // Ensure screen_list exists in original response
                    if res["data"]["screen_list"].as_array().is_none() {
                        if let Some(data) = res["data"].as_object_mut() {
                            data.insert("screen_list".to_string(), serde_json::json!([]));
                        }
                    }

                    // Parallelize detail fetching
                    let mut tasks = Vec::new();

                    for item in list {
                        // Handle id as string or number
                        let link_id_opt = item["id"]
                            .as_str()
                            .map(|s| s.to_string())
                            .or_else(|| item["id"].as_i64().map(|i| i.to_string()));

                        if let Some(link_id) = link_id_opt {
                            let client_clone = client.clone();

                            tasks.push(tokio::spawn(async move {
                                 let detail_url = format!("https://show.bilibili.com/api/ticket/linkgoods/detail?link_id={}", link_id);
                                 if let Ok(detail_resp) = client_clone.get(&detail_url)
                                    .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")
                                    .send()
                                    .await
                                 {
                                    if let Ok(detail_res) = detail_resp.json::<Value>().await {
                                        return Some((detail_res, link_id));
                                    }
                                 }
                                 None
                             }));
                        }
                    }

                    // Collect results
                    for task in tasks {
                        if let Ok(Some((detail_res, link_id))) = task.await {
                            if let Some(specs) = detail_res["data"]["specs_list"].as_array() {
                                if let Some(screen_list) = res["data"]["screen_list"].as_array_mut()
                                {
                                    for spec in specs {
                                        let mut spec_obj = spec.clone();
                                        if let Some(obj) = spec_obj.as_object_mut() {
                                            obj.insert(
                                                "project_id".to_string(),
                                                detail_res["data"]["item_id"].clone(),
                                            ); // Use actual item_id from detail
                                            obj.insert(
                                                "link_id".to_string(),
                                                serde_json::json!(link_id),
                                            );
                                        }
                                        screen_list.push(spec_obj);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    // Apply express_fee logic (Match Python TicketService.py)
    if let Some(data) = res["data"].as_object_mut() {
        let has_eticket = data
            .get("has_eticket")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        if let Some(screen_list) = data.get_mut("screen_list").and_then(|v| v.as_array_mut()) {
            for screen in screen_list {
                let mut express_fee = 0;
                if !has_eticket {
                    if let Some(fee) = screen.get("express_fee").and_then(|v| v.as_i64()) {
                        if fee >= 0 {
                            express_fee = fee;
                        }
                    }
                }

                if let Some(ticket_list) =
                    screen.get_mut("ticket_list").and_then(|v| v.as_array_mut())
                {
                    for ticket in ticket_list {
                        if let Some(price) = ticket.get("price").and_then(|v| v.as_i64()) {
                            ticket["price"] = serde_json::json!(price + express_fee);
                        }
                    }
                }
            }
        }
    }

    Ok(res)
}

pub async fn fetch_buyers(
    client: &Client,
    project_id: String,
    cookies: Vec<String>,
) -> Result<Value> {
    let url = format!(
        "https://show.bilibili.com/api/ticket/buyer/list?is_default&projectId={}",
        project_id
    );

    let mut req = client.get(&url)
        .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36");

    // Add cookies
    let cookie_str = cookies.join("; ");
    req = req.header("Cookie", cookie_str);

    let res: Value = req.send().await?.json().await?;
    Ok(res)
}

pub async fn fetch_user_info(client: &Client, cookies: Vec<String>) -> Result<Value> {
    let url = "https://api.bilibili.com/x/web-interface/nav";

    let mut req = client.get(url)
        .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36");

    let cookie_str = cookies.join("; ");
    req = req.header("Cookie", cookie_str);

    let res: Value = req.send().await?.json().await?;
    Ok(res)
}

pub async fn fetch_address_list(client: &Client, cookies: Vec<String>) -> Result<Value> {
    let url = "https://show.bilibili.com/api/ticket/addr/list";

    let mut req = client.get(url)
        .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36");

    let cookie_str = cookies.join("; ");
    req = req.header("Cookie", cookie_str);

    let res: Value = req.send().await?.json().await?;
    Ok(res)
}

pub async fn get_server_time(client: &Client, url_opt: Option<String>) -> Result<i64> {
    let url = url_opt.ok_or_else(|| anyhow!("HTTP time API URL is required"))?;

    let res: Value = client.get(&url)
        .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")
        .timeout(Duration::from_secs(3))
        .send()
        .await?
        .json()
        .await?;

    // 1. Generic nested time: {"data": {"now": 169...}} (seconds or millis by magnitude)
    if let Some(t) = parse_data_now_to_millis(&res["data"]["now"]) {
        return Ok(t);
    }

    // 2. Taobao Format: {"data": {"t": "169..."}} (Millis String)
    if let Some(t) = parse_epoch_millis(&res["data"]["t"]) {
        return Ok(t);
    }

    // 3. JD Format: {"serverTime": 169...} (Millis)
    if let Some(t) = parse_epoch_millis(&res["serverTime"]) {
        return Ok(t);
    }

    // 4. Pinduoduo/Other: {"server_time": 169...} (Seconds or Millis?)
    // Guess generic time fields by magnitude.
    for field in ["server_time", "time", "timestamp"] {
        if let Some(t) = parse_epoch_millis(&res[field]) {
            return Ok(t);
        }
    }

    Err(anyhow!("Failed to parse server time from response"))
}

fn parse_epoch_millis(value: &Value) -> Option<i64> {
    let raw = match value {
        Value::Number(n) => n.to_string(),
        Value::String(s) => s.trim().to_string(),
        _ => return None,
    };
    let t = raw.parse::<i64>().ok()?;

    if t > 100_000_000_000 {
        Some(t)
    } else {
        t.checked_mul(1000)
    }
}

fn parse_data_now_to_millis(value: &Value) -> Option<i64> {
    let raw = match value {
        Value::Number(n) => n.to_string(),
        Value::String(s) => s.trim().to_string(),
        _ => return None,
    };

    if let Some((seconds, fraction)) = raw.split_once('.') {
        let seconds = seconds.parse::<i64>().ok()?;
        let millis = parse_millis_fraction(fraction)?;
        return seconds.checked_mul(1000)?.checked_add(millis);
    }

    parse_epoch_millis(value)
}

fn parse_millis_fraction(fraction: &str) -> Option<i64> {
    if fraction.is_empty() || !fraction.chars().all(|c| c.is_ascii_digit()) {
        return None;
    }

    let mut millis = 0;
    for digit in fraction.chars().take(3) {
        millis = millis * 10 + digit.to_digit(10)? as i64;
    }
    for _ in fraction.len()..3 {
        millis *= 10;
    }

    Some(millis)
}

pub fn get_local_time() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

pub fn get_ntp_time(server: &str) -> Result<u64> {
    let address = if server.contains(':') {
        server.to_string()
    } else {
        format!("{}:123", server)
    };

    let socket =
        UdpSocket::bind("0.0.0.0:0").map_err(|e| anyhow::anyhow!("UDP Bind Error: {:?}", e))?;
    socket
        .set_read_timeout(Some(Duration::from_secs(2)))
        .map_err(|e| anyhow::anyhow!("UDP Timeout Error: {:?}", e))?;

    let result = sntpc::simple_get_time(&address, &socket)
        .map_err(|e| anyhow::anyhow!("NTP Error: {:?}", e))?;

    // Calculate milliseconds: seconds * 1000 + nanoseconds / 1_000_000
    let millis = (result.seconds as u64 * 1000) + ((result.seconds_fraction as u64 * 1000) >> 32);
    Ok(millis)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn time_sync_discards_first_attempt_as_warmup() {
        assert_eq!(TIME_SYNC_ATTEMPTS, 3);
        assert_eq!(TIME_SYNC_WARMUP_ATTEMPTS, 1);
        assert_eq!(
            TIME_SYNC_ATTEMPTS - TIME_SYNC_WARMUP_ATTEMPTS,
            MIN_SUCCESSFUL_TIME_SYNC_SAMPLES
        );
    }

    #[test]
    fn parses_fractional_seconds_as_millis() {
        let value: Value = serde_json::from_str(r#"1690000000.123"#).unwrap();

        assert_eq!(parse_data_now_to_millis(&value), Some(1_690_000_000_123));
    }

    #[test]
    fn parses_integer_seconds_as_millis() {
        assert_eq!(
            parse_data_now_to_millis(&json!(1_690_000_000)),
            Some(1_690_000_000_000)
        );
    }

    #[test]
    fn keeps_integer_millis() {
        assert_eq!(
            parse_data_now_to_millis(&json!(1_690_000_000_123i64)),
            Some(1_690_000_000_123)
        );
    }

    #[test]
    fn parses_generic_seconds_and_millis() {
        assert_eq!(
            parse_epoch_millis(&json!(1_690_000_000)),
            Some(1_690_000_000_000)
        );
        assert_eq!(
            parse_epoch_millis(&json!("1690000000123")),
            Some(1_690_000_000_123)
        );
    }

    #[test]
    fn time_sync_result_prefers_low_latency_consistent_sample() {
        let result = build_time_sync_result(
            vec![
                TimeSyncSample {
                    offset: 100,
                    server: 1_100,
                    local_before: 900,
                    local_after: 1_000,
                    round_trip: 100,
                },
                TimeSyncSample {
                    offset: 103,
                    server: 1_103,
                    local_before: 900,
                    local_after: 1_020,
                    round_trip: 120,
                },
                TimeSyncSample {
                    offset: 210,
                    server: 1_210,
                    local_before: 900,
                    local_after: 950,
                    round_trip: 50,
                },
            ],
            0,
            2_000,
        )
        .unwrap();

        assert_eq!(result.diff, 100);
        assert_eq!(result.spread, 110);
        assert!(result.quality.trustworthy);
        assert_eq!(result.quality.sample_count, 3);
        assert_eq!(result.quality.failed_sample_count, 0);
    }

    #[test]
    fn time_sync_result_allows_failed_samples_when_successes_are_enough() {
        let result = build_time_sync_result(
            vec![
                TimeSyncSample {
                    offset: 100,
                    server: 1_100,
                    local_before: 900,
                    local_after: 1_000,
                    round_trip: 100,
                },
                TimeSyncSample {
                    offset: 105,
                    server: 1_105,
                    local_before: 900,
                    local_after: 1_010,
                    round_trip: 110,
                },
            ],
            1,
            2_000,
        )
        .unwrap();

        assert_eq!(result.quality.sample_count, 2);
        assert_eq!(result.quality.failed_sample_count, 1);
        assert!(result.quality.trustworthy);
    }
}
