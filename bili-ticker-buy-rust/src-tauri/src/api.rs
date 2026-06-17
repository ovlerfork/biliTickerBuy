use reqwest::Client;
use serde_json::{json, Value};
use anyhow::{Result, anyhow};
use std::time::{SystemTime, UNIX_EPOCH, Duration};
use std::net::UdpSocket;
use sntpc;

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

pub async fn fetch_project_info(id: String) -> Result<Value> {
    let client = Client::new();
    let mut res = match fetch_new_project_info(&client, &id).await {
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
    let link_url = format!("https://show.bilibili.com/api/ticket/linkgoods/list?project_id={}&page_type=0", id);
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
                        let link_id_opt = item["id"].as_str().map(|s| s.to_string())
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
                                if let Some(screen_list) = res["data"]["screen_list"].as_array_mut() {
                                    for spec in specs {
                                        let mut spec_obj = spec.clone();
                                        if let Some(obj) = spec_obj.as_object_mut() {
                                            obj.insert("project_id".to_string(), detail_res["data"]["item_id"].clone()); // Use actual item_id from detail
                                            obj.insert("link_id".to_string(), serde_json::json!(link_id));
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
        let has_eticket = data.get("has_eticket").and_then(|v| v.as_bool()).unwrap_or(false);
        
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
                
                if let Some(ticket_list) = screen.get_mut("ticket_list").and_then(|v| v.as_array_mut()) {
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

pub async fn fetch_buyers(project_id: String, cookies: Vec<String>) -> Result<Value> {
    let client = Client::new();
    let url = format!("https://show.bilibili.com/api/ticket/buyer/list?is_default&projectId={}", project_id);
    
    let mut req = client.get(&url)
        .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36");

    // Add cookies
    let cookie_str = cookies.join("; ");
    req = req.header("Cookie", cookie_str);

    let res: Value = req.send().await?.json().await?;
    Ok(res)
}

pub async fn fetch_user_info(cookies: Vec<String>) -> Result<Value> {
    let client = Client::new();
    let url = "https://api.bilibili.com/x/web-interface/nav";
    
    let mut req = client.get(url)
        .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36");

    let cookie_str = cookies.join("; ");
    req = req.header("Cookie", cookie_str);

    let res: Value = req.send().await?.json().await?;
    Ok(res)
}

pub async fn fetch_address_list(cookies: Vec<String>) -> Result<Value> {
    let client = Client::new();
    let url = "https://show.bilibili.com/api/ticket/addr/list";
    
    let mut req = client.get(url)
        .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36");

    let cookie_str = cookies.join("; ");
    req = req.header("Cookie", cookie_str);

    let res: Value = req.send().await?.json().await?;
    Ok(res)
}

pub async fn get_server_time(url_opt: Option<String>) -> Result<i64> {
    let client = Client::builder()
        .timeout(Duration::from_secs(3))
        .build()
        .map_err(|e| anyhow!("Failed to build client: {}", e))?;

    // Default to Bilibili if no URL provided
    let url = url_opt.unwrap_or_else(|| "https://api.bilibili.com/x/report/click/now".to_string());
    
    let res: Value = client.get(&url)
        .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")
        .send()
        .await?
        .json()
        .await?;
    
    // 1. Bilibili Format: {"data": {"now": 169...}} (Seconds)
    if let Some(now) = res["data"]["now"].as_i64() {
        return Ok(now * 1000);
    }

    // 2. Taobao Format: {"data": {"t": "169..."}} (Millis String)
    if let Some(t_str) = res["data"]["t"].as_str() {
        if let Ok(t) = t_str.parse::<i64>() {
            return Ok(t);
        }
    }

    // 3. JD Format: {"serverTime": 169...} (Millis)
    if let Some(t) = res["serverTime"].as_i64() {
        return Ok(t);
    }

    // 4. Pinduoduo/Other: {"server_time": 169...} (Seconds or Millis?)
    // Let's assume generic "time" or "timestamp" fields if found
    if let Some(t) = res["time"].as_i64() {
        // Guess if seconds or millis based on magnitude
        // 2023 is ~1.7e9 seconds, ~1.7e12 millis
        if t > 100_000_000_000 {
            return Ok(t);
        } else {
            return Ok(t * 1000);
        }
    }

    Err(anyhow!("Failed to parse server time from response"))
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

    let socket = UdpSocket::bind("0.0.0.0:0").map_err(|e| anyhow::anyhow!("UDP Bind Error: {:?}", e))?;
    socket.set_read_timeout(Some(Duration::from_secs(2))).map_err(|e| anyhow::anyhow!("UDP Timeout Error: {:?}", e))?;

    let result = sntpc::simple_get_time(&address, &socket).map_err(|e| anyhow::anyhow!("NTP Error: {:?}", e))?;
    
    // Calculate milliseconds: seconds * 1000 + nanoseconds / 1_000_000
    let millis = (result.seconds as u64 * 1000) + ((result.seconds_fraction as u64 * 1000) >> 32);
    Ok(millis)
}
