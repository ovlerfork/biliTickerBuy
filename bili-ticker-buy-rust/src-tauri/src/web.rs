#[path = "api.rs"]
mod api;
#[path = "auth.rs"]
mod auth;
#[path = "buy.rs"]
mod buy;
#[path = "storage.rs"]
mod storage;
#[path = "util.rs"]
mod util;

use buy::{BuyTaskOutcome, TaskEmitter, TicketInfo};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use tiny_http::{Header, Method, Response, Server, StatusCode};
use uuid::Uuid;

#[derive(Clone, Serialize)]
struct WebEvent {
    event: String,
    payload: Value,
}

#[derive(Clone)]
struct WebEmitter {
    events: Arc<Mutex<Vec<WebEvent>>>,
}

impl TaskEmitter for WebEmitter {
    fn emit_log(&self, task_id: &str, message: &str) {
        self.push("log", json!({ "task_id": task_id, "message": message }));
    }

    fn emit_payment(&self, task_id: &str, url: &str) {
        self.push("payment_qrcode", json!({ "task_id": task_id, "url": url }));
    }

    fn emit_task_result(&self, task_id: &str, success: bool, message: &str) {
        self.push(
            "task_result",
            json!({ "task_id": task_id, "success": success, "message": message }),
        );
    }
}

impl WebEmitter {
    fn push(&self, event: &str, payload: Value) {
        let mut events = self.events.lock().unwrap();
        events.push(WebEvent {
            event: event.to_string(),
            payload,
        });
        // ponytail: polling buffer only; add per-client cursors if multi-user web use matters.
        if events.len() > 1000 {
            let drain_to = events.len() - 1000;
            events.drain(..drain_to);
        }
    }
}

struct AppState {
    base_dir: PathBuf,
    dist_dir: PathBuf,
    tasks: Arc<Mutex<HashMap<String, Arc<AtomicBool>>>>,
    http_client: Client,
    events: Arc<Mutex<Vec<WebEvent>>>,
}

#[derive(Deserialize)]
struct InvokeRequest {
    cmd: String,
    #[serde(default)]
    args: Value,
}

fn main() {
    env_logger::init();

    let port = std::env::var("PORT").unwrap_or_else(|_| "8080".to_string());
    let addr = format!("0.0.0.0:{port}");
    let dist_dir = std::env::var("WEB_DIST_DIR").unwrap_or_else(|_| "../dist".to_string());
    let base_dir = std::env::var("APP_DATA_DIR").unwrap_or_else(|_| "./data".to_string());
    let password = std::env::var("WEB_PASSWORD").ok();
    let state = Arc::new(AppState {
        base_dir: PathBuf::from(base_dir),
        dist_dir: PathBuf::from(dist_dir),
        tasks: Arc::new(Mutex::new(HashMap::new())),
        http_client: api::build_shared_client().expect("failed to build shared HTTP client"),
        events: Arc::new(Mutex::new(Vec::new())),
    });

    fs::create_dir_all(&state.base_dir).expect("failed to create data directory");
    let server = Server::http(&addr).expect("failed to bind web server");
    println!("listening on http://{addr}");

    let runtime = tokio::runtime::Runtime::new().expect("failed to create tokio runtime");
    for mut request in server.incoming_requests() {
        let state = state.clone();
        let authorized = password
            .as_ref()
            .map(|password| has_basic_auth(&request, password))
            .unwrap_or(true);
        let response = if !authorized {
            unauthorized_response()
        } else {
            match (request.method(), request.url()) {
                (&Method::Post, "/api/invoke") => {
                    let mut body = String::new();
                    let result = request.as_reader().read_to_string(&mut body);
                    match result
                        .map_err(|e| e.to_string())
                        .and_then(|_| {
                            serde_json::from_str::<InvokeRequest>(&body).map_err(|e| e.to_string())
                        })
                        .and_then(|req| runtime.block_on(invoke(state, req)))
                    {
                        Ok(value) => {
                            json_response(StatusCode(200), json!({ "ok": true, "value": value }))
                        }
                        Err(error) => {
                            json_response(StatusCode(500), json!({ "ok": false, "error": error }))
                        }
                    }
                }
                (&Method::Get, url) if url.starts_with("/api/events") => {
                    let since = url
                        .split_once("since=")
                        .and_then(|(_, value)| value.parse::<usize>().ok())
                        .unwrap_or(0);
                    let events = state.events.lock().unwrap();
                    let next = events.len();
                    let items = events.iter().skip(since).cloned().collect::<Vec<_>>();
                    json_response(StatusCode(200), json!({ "next": next, "events": items }))
                }
                (&Method::Get, url) => static_response(&state.dist_dir, url),
                _ => text_response(StatusCode(405), "method not allowed"),
            }
        };

        let _ = request.respond(response);
    }
}

async fn invoke(state: Arc<AppState>, req: InvokeRequest) -> Result<Value, String> {
    match req.cmd.as_str() {
        "get_accounts" => to_value(storage::get_accounts(&state.base_dir)),
        "remove_account" => {
            let uid = arg_string(&req.args, "uid")?;
            let mut accounts = storage::get_accounts(&state.base_dir).map_err(|e| e.to_string())?;
            accounts.retain(|a| a.uid != uid);
            storage::save_accounts(&state.base_dir, &accounts).map_err(|e| e.to_string())?;
            Ok(Value::Null)
        }
        "get_history" => to_value(storage::get_history(&state.base_dir)),
        "clear_history" => {
            storage::clear_history(&state.base_dir).map_err(|e| e.to_string())?;
            Ok(Value::Null)
        }
        "get_project_history" => to_value(storage::get_project_history(&state.base_dir)),
        "add_project_history" => {
            let item =
                serde_json::from_value(req.args["item"].clone()).map_err(|e| e.to_string())?;
            storage::add_project_history(&state.base_dir, item).map_err(|e| e.to_string())?;
            Ok(Value::Null)
        }
        "remove_project_history" => {
            storage::remove_project_history_item(
                &state.base_dir,
                arg_string(&req.args, "projectId")?,
                arg_string(&req.args, "skuId")?,
            )
            .map_err(|e| e.to_string())?;
            Ok(Value::Null)
        }
        "get_tasks" => to_value(storage::get_tasks(&state.base_dir)),
        "save_tasks" => {
            storage::save_tasks(&state.base_dir, &req.args["tasks"]).map_err(|e| e.to_string())?;
            Ok(Value::Null)
        }
        "get_settings" => to_value(storage::get_settings(&state.base_dir)),
        "save_settings" => {
            storage::save_settings(&state.base_dir, &req.args["settings"])
                .map_err(|e| e.to_string())?;
            Ok(Value::Null)
        }
        "get_user_info" => api::fetch_user_info(&state.http_client, arg_cookies(&req.args)?)
            .await
            .map_err(|e| e.to_string()),
        "get_login_qrcode" => {
            let (url, key) = auth::generate_qrcode(&state.http_client)
                .await
                .map_err(|e| e.to_string())?;
            Ok(json!([url, key]))
        }
        "poll_login_status" => {
            auth::poll_login(&state.http_client, &arg_string(&req.args, "qrcodeKey")?)
                .await
                .map(Value::String)
                .map_err(|e| e.to_string())
        }
        "add_account" => add_account(&state, arg_cookies(&req.args)?).await,
        "fetch_project" => {
            api::fetch_project_info(&state.http_client, arg_string(&req.args, "id")?)
                .await
                .map_err(|e| e.to_string())
        }
        "fetch_buyer_list" => api::fetch_buyers(
            &state.http_client,
            arg_string(&req.args, "projectId")?,
            arg_cookies(&req.args)?,
        )
        .await
        .map_err(|e| e.to_string()),
        "fetch_address_list" => {
            api::fetch_address_list(&state.http_client, arg_cookies(&req.args)?)
                .await
                .map_err(|e| e.to_string())
        }
        "sync_time" => serde_json::to_value(
            api::sample_time(&state.http_client, api::DEFAULT_TIME_SERVER)
                .await
                .map_err(|e| e.to_string())?,
        )
        .map_err(|e| e.to_string()),
        "start_buy" => start_buy(state, req.args).await.map(Value::String),
        "stop_task" => {
            if let Some(flag) = state
                .tasks
                .lock()
                .unwrap()
                .get(&arg_string(&req.args, "taskId")?)
            {
                flag.store(true, Ordering::Relaxed);
            }
            Ok(Value::Null)
        }
        "open_bilibili_home" => Ok(Value::Null),
        _ => Err(format!("unknown command: {}", req.cmd)),
    }
}

async fn add_account(state: &AppState, cookies: Vec<String>) -> Result<Value, String> {
    let res = api::fetch_user_info(&state.http_client, cookies.clone())
        .await
        .map_err(|e| e.to_string())?;
    if res["code"].as_i64().unwrap_or(-1) != 0 {
        return Err("Invalid cookies".to_string());
    }

    let data = &res["data"];
    let account = storage::Account {
        uid: data["mid"].to_string(),
        name: data["uname"].as_str().unwrap_or("").to_string(),
        face: data["face"].as_str().unwrap_or("").to_string(),
        cookies,
        level: data["level_info"]["current_level"].as_i64().unwrap_or(0) as i32,
        is_vip: data["vipStatus"].as_i64().unwrap_or(0) == 1,
        coins: data["money"].as_f64().unwrap_or(0.0),
    };

    let mut accounts = storage::get_accounts(&state.base_dir).map_err(|e| e.to_string())?;
    accounts.retain(|a| a.uid != account.uid);
    accounts.push(account.clone());
    storage::save_accounts(&state.base_dir, &accounts).map_err(|e| e.to_string())?;
    to_value(Ok(account))
}

async fn start_buy(state: Arc<AppState>, args: Value) -> Result<String, String> {
    let time_start = arg_opt_string(&args, "timeStart").filter(|s| !s.trim().is_empty());
    let mut info: TicketInfo =
        serde_json::from_str(&arg_string(&args, "ticketInfo")?).map_err(|e| e.to_string())?;
    let buyers = args
        .get("buyers")
        .and_then(|v| serde_json::from_value::<Vec<Value>>(v.clone()).ok());

    if let Some(buyers) = buyers {
        if !buyers.is_empty() {
            info.buyer_info = Value::Array(buyers.clone());
            if info.contact_name.as_deref().unwrap_or("").is_empty() {
                info.contact_name = buyers[0]["name"].as_str().map(|s| s.to_string());
            }
            if info.contact_tel.as_deref().unwrap_or("").is_empty() {
                info.contact_tel = buyers[0]["tel"]
                    .as_str()
                    .or_else(|| buyers[0]["mobile"].as_str())
                    .or_else(|| buyers[0]["phone"].as_str())
                    .filter(|s| !s.is_empty() && !s.contains('*'))
                    .map(|s| s.to_string());
            }
        }
    }

    let task_id = Uuid::new_v4().to_string();
    let stop_flag = Arc::new(AtomicBool::new(false));
    state
        .tasks
        .lock()
        .unwrap()
        .insert(task_id.clone(), stop_flag.clone());

    let emitter = WebEmitter {
        events: state.events.clone(),
    };
    let tasks = state.tasks.clone();
    let task_id_clone = task_id.clone();
    let base_dir = state.base_dir.clone();
    let interval = arg_u64(&args, "interval").unwrap_or(1000);
    let mode = arg_u32(&args, "mode").unwrap_or(0);
    let total_attempts = arg_u32(&args, "totalAttempts").unwrap_or(10);
    let proxy = arg_opt_string(&args, "proxy");
    let time_offset = args.get("timeOffset").and_then(|v| v.as_f64());
    let ntp_server = arg_opt_string(&args, "ntpServer");

    tokio::spawn(async move {
        let mut stop_flag = stop_flag;
        let mut allow_pre_sale_restart = true;

        loop {
            let outcome = buy::start_buy_task(
                emitter.clone(),
                task_id_clone.clone(),
                stop_flag.clone(),
                info.clone(),
                interval,
                mode,
                total_attempts,
                time_start.clone(),
                proxy.clone(),
                time_offset,
                ntp_server.clone(),
                allow_pre_sale_restart,
                base_dir.clone(),
            )
            .await;

            match outcome {
                Ok(BuyTaskOutcome::RestartBeforeSale) if allow_pre_sale_restart => {
                    allow_pre_sale_restart = false;
                    let next_stop_flag = Arc::new(AtomicBool::new(false));
                    let should_restart = {
                        let mut task_flags = tasks.lock().unwrap();
                        let was_stopped = task_flags
                            .get(&task_id_clone)
                            .map(|flag| flag.load(Ordering::Relaxed))
                            .unwrap_or_else(|| stop_flag.load(Ordering::Relaxed));
                        if !was_stopped {
                            task_flags.insert(task_id_clone.clone(), next_stop_flag.clone());
                        }
                        !was_stopped
                    };

                    if should_restart {
                        stop_flag = next_stop_flag;
                        continue;
                    }
                }
                Ok(BuyTaskOutcome::RestartBeforeSale) | Ok(BuyTaskOutcome::Finished) => {}
                Err(e) => {
                    eprintln!("Buy task error: {e}");
                }
            }

            break;
        }
        tasks.lock().unwrap().remove(&task_id_clone);
    });

    Ok(task_id)
}

fn arg_string(args: &Value, key: &str) -> Result<String, String> {
    args.get(key)
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .ok_or_else(|| format!("missing string arg: {key}"))
}

fn arg_opt_string(args: &Value, key: &str) -> Option<String> {
    args.get(key)
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
}

fn arg_cookies(args: &Value) -> Result<Vec<String>, String> {
    serde_json::from_value(args["cookies"].clone()).map_err(|e| e.to_string())
}

fn arg_u64(args: &Value, key: &str) -> Result<u64, String> {
    args.get(key)
        .and_then(|v| v.as_u64())
        .ok_or_else(|| format!("missing u64 arg: {key}"))
}

fn arg_u32(args: &Value, key: &str) -> Result<u32, String> {
    arg_u64(args, key).map(|v| v as u32)
}

fn to_value<T: Serialize>(result: anyhow::Result<T>) -> Result<Value, String> {
    serde_json::to_value(result.map_err(|e| e.to_string())?).map_err(|e| e.to_string())
}

fn json_response(status: StatusCode, value: Value) -> Response<std::io::Cursor<Vec<u8>>> {
    Response::from_string(value.to_string())
        .with_status_code(status)
        .with_header(Header::from_bytes(&b"content-type"[..], &b"application/json"[..]).unwrap())
}

fn text_response(status: StatusCode, text: &str) -> Response<std::io::Cursor<Vec<u8>>> {
    Response::from_string(text)
        .with_status_code(status)
        .with_header(
            Header::from_bytes(&b"content-type"[..], &b"text/plain; charset=utf-8"[..]).unwrap(),
        )
}

fn unauthorized_response() -> Response<std::io::Cursor<Vec<u8>>> {
    text_response(StatusCode(401), "authentication required").with_header(
        Header::from_bytes(
            &b"www-authenticate"[..],
            &b"Basic realm=\"bili-ticker-buy\""[..],
        )
        .unwrap(),
    )
}

fn has_basic_auth(request: &tiny_http::Request, password: &str) -> bool {
    let Some(header) = request
        .headers()
        .iter()
        .find(|h| h.field.equiv("authorization"))
    else {
        return false;
    };
    let value = header.value.as_str();
    let Some(encoded) = value.strip_prefix("Basic ") else {
        return false;
    };
    decode_base64(encoded)
        .and_then(|decoded| String::from_utf8(decoded).ok())
        .and_then(|credential| credential.split_once(':').map(|(_, pass)| pass.to_string()))
        .map(|pass| pass == password)
        .unwrap_or(false)
}

fn decode_base64(value: &str) -> Option<Vec<u8>> {
    let mut out = Vec::new();
    let mut buf = 0u32;
    let mut bits = 0u8;

    for byte in value.bytes().filter(|b| !b" \r\n\t".contains(b)) {
        if byte == b'=' {
            break;
        }
        let val = match byte {
            b'A'..=b'Z' => byte - b'A',
            b'a'..=b'z' => byte - b'a' + 26,
            b'0'..=b'9' => byte - b'0' + 52,
            b'+' => 62,
            b'/' => 63,
            _ => return None,
        };
        buf = (buf << 6) | u32::from(val);
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((buf >> bits) as u8);
            buf &= (1 << bits) - 1;
        }
    }

    Some(out)
}

fn static_response(dist_dir: &Path, url: &str) -> Response<std::io::Cursor<Vec<u8>>> {
    let clean = url.split('?').next().unwrap_or("/");
    let path = if clean == "/" {
        dist_dir.join("index.html")
    } else {
        dist_dir.join(clean.trim_start_matches('/'))
    };
    let path = if path.exists() {
        path
    } else {
        dist_dir.join("index.html")
    };

    match fs::read(&path) {
        Ok(bytes) => Response::from_data(bytes).with_header(
            Header::from_bytes(&b"content-type"[..], content_type(&path).as_bytes()).unwrap(),
        ),
        Err(_) => text_response(StatusCode(404), "not found"),
    }
}

fn content_type(path: &Path) -> &'static str {
    match path.extension().and_then(|ext| ext.to_str()).unwrap_or("") {
        "css" => "text/css; charset=utf-8",
        "js" => "application/javascript; charset=utf-8",
        "png" => "image/png",
        "svg" => "image/svg+xml",
        "ico" => "image/x-icon",
        "html" => "text/html; charset=utf-8",
        _ => "application/octet-stream",
    }
}

#[cfg(test)]
mod tests {
    use super::decode_base64;

    #[test]
    fn decodes_basic_auth_payload() {
        assert_eq!(decode_base64("dXNlcjpwYXNz").unwrap(), b"user:pass");
    }
}
