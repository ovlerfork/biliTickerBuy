use crate::api; // Import api module
use crate::storage::{self, HistoryItem};
use crate::util::CTokenGenerator;
use anyhow::Result;
use chrono::{FixedOffset, Local, Timelike};
use log::info;
use reqwest::cookie::Jar;
use reqwest::{Client, Proxy, Url};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::sleep;

const BEIJING_OFFSET_SECONDS: i32 = 8 * 60 * 60;
const TIME_SYNC_FREEZE_BEFORE_START_MS: i64 = 2000;
const TIME_SYNC_OFFSET_JUMP_WARN_MS: i64 = 300;
const PRE_SALE_RESTART_BEFORE_START_MS: i64 = 60_000;
const OPENING_BURST_WINDOW_MS: u128 = 20_000;
const OPENING_MIN_INTERVAL_MS: u64 = 250;
const RECHECK_412_RESET_MS: u128 = 30 * 60 * 1000;
const RECHECK_412_ESCALATE_MS: u128 = 10 * 60 * 1000;
const FIRST_412_COOLDOWN_MS: u64 = 60_000;
const REPEATED_412_COOLDOWN_MS: u64 = 300_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BuyTaskOutcome {
    Finished,
    RestartBeforeSale,
}

/// Error code dictionary from the original Python project
/// Maps error codes to human-readable messages
fn get_error_message(errno: i64) -> &'static str {
    match errno {
        0 => "成功",
        3 => "抢票CD中",
        100001 => "无票",
        100003 => "验证码过期",
        429 => "请求异常 429",
        412 => "请求异常 412",
        219 => "购票暂不可用",
        221 => "购票暂不可用",
        100009 => "库存不足,暂无余票",
        100016 => "项目不可售",
        100017 => "票种不可售",
        100034 => "票价错误",
        100039 => "活动收摊啦,下次要快点哦",
        100041 => "对未发售的票进行抢票",
        100048 => "已经下单，有尚未完成订单",
        100051 => "订单准备过期，重新验证",
        900001 => "当前拥挤，请稍后再试",
        900002 => "当前拥挤，请稍后再试",
        _ => "未知错误码",
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StrategyMode {
    Auto,
    Opening,
    Reflow,
}

impl StrategyMode {
    pub fn from_option(value: Option<&str>) -> Self {
        match value.unwrap_or("auto").trim().to_lowercase().as_str() {
            "opening" | "sale" | "start" => Self::Opening,
            "reflow" | "return" | "refund" => Self::Reflow,
            _ => Self::Auto,
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Auto => "自动",
            Self::Opening => "开票",
            Self::Reflow => "回流",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ActiveStrategy {
    Opening,
    Reflow,
}

impl ActiveStrategy {
    fn label(self) -> &'static str {
        match self {
            Self::Opening => "开票",
            Self::Reflow => "回流",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RequestPhase {
    Prepare,
    Create,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AttemptOutcome {
    Code(i64),
    NetworkError,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PrepareGateDecision {
    allowed: bool,
    remaining_ms: i64,
}

fn prepare_gate_decision(remaining_ms: i64) -> PrepareGateDecision {
    PrepareGateDecision {
        allowed: remaining_ms <= 0,
        remaining_ms,
    }
}

fn resolve_active_strategy(
    requested: StrategyMode,
    scheduled_was_future: bool,
) -> ActiveStrategy {
    match requested {
        StrategyMode::Opening => ActiveStrategy::Opening,
        StrategyMode::Reflow => ActiveStrategy::Reflow,
        StrategyMode::Auto => {
            if scheduled_was_future {
                ActiveStrategy::Opening
            } else {
                ActiveStrategy::Reflow
            }
        }
    }
}

fn clamp_interval(value: u64, min: u64, max: u64) -> u64 {
    value.max(min).min(max)
}

fn jittered_interval(base_ms: u64) -> u64 {
    let jitter = rand::random::<u64>() % 81;
    base_ms.saturating_add(jitter)
}

fn daytime_reflow_base_interval(base_interval_ms: u64) -> u64 {
    let hour = beijing_now().hour();
    if (8..24).contains(&hour) {
        clamp_interval(base_interval_ms, 250, 1_500)
    } else {
        clamp_interval(base_interval_ms.max(1_500), 1_500, 5_000)
    }
}

fn precise_interval_sleep_duration(start: Instant, interval_ms: u64) -> Option<Duration> {
    let interval_duration = Duration::from_millis(interval_ms);
    let elapsed = start.elapsed();
    if elapsed < interval_duration {
        Some(interval_duration - elapsed)
    } else {
        None
    }
}

async fn sleep_with_stop(stop_flag: &AtomicBool, duration: Duration) -> bool {
    let mut remaining = duration;
    while !remaining.is_zero() {
        if stop_flag.load(Ordering::Relaxed) {
            return false;
        }

        let step = remaining.min(Duration::from_millis(500));
        sleep(step).await;
        remaining = remaining.saturating_sub(step);
    }
    !stop_flag.load(Ordering::Relaxed)
}

#[derive(Debug)]
struct AdaptiveDelayController {
    strategy: ActiveStrategy,
    base_interval_ms: u64,
    consecutive_429: u32,
    consecutive_network_errors: u32,
    last_412_at: Option<Instant>,
    opening_started_at: Instant,
    last_interval_ms: u64,
}

impl AdaptiveDelayController {
    fn new(strategy: ActiveStrategy, base_interval_ms: u64) -> Self {
        let base_interval_ms = base_interval_ms.max(1);
        Self {
            strategy,
            base_interval_ms,
            consecutive_429: 0,
            consecutive_network_errors: 0,
            last_412_at: None,
            opening_started_at: Instant::now(),
            last_interval_ms: base_interval_ms,
        }
    }

    fn next_delay(&mut self, phase: RequestPhase, outcome: AttemptOutcome) -> Duration {
        let interval_ms = self.next_delay_ms(phase, outcome);
        self.last_interval_ms = interval_ms;
        Duration::from_millis(interval_ms)
    }

    fn next_delay_ms(&mut self, phase: RequestPhase, outcome: AttemptOutcome) -> u64 {
        match outcome {
            AttemptOutcome::Code(429) => {
                self.consecutive_429 = self.consecutive_429.saturating_add(1);
                self.consecutive_network_errors = 0;
            }
            AttemptOutcome::NetworkError => {
                self.consecutive_network_errors =
                    self.consecutive_network_errors.saturating_add(1);
            }
            _ => {
                self.consecutive_429 = 0;
                self.consecutive_network_errors = 0;
            }
        }

        if let AttemptOutcome::Code(412) = outcome {
            return self.next_412_cooldown_ms();
        }

        match self.strategy {
            ActiveStrategy::Opening => self.opening_delay_ms(phase, outcome),
            ActiveStrategy::Reflow => self.reflow_delay_ms(phase, outcome),
        }
    }

    fn opening_delay_ms(&self, _phase: RequestPhase, outcome: AttemptOutcome) -> u64 {
        match outcome {
            AttemptOutcome::Code(900001) | AttemptOutcome::Code(900002) => 1_000,
            AttemptOutcome::Code(219) | AttemptOutcome::Code(221) => 5_000,
            AttemptOutcome::Code(100051) => 0,
            AttemptOutcome::Code(100009) => {
                if self.in_opening_burst() {
                    clamp_interval(self.base_interval_ms, OPENING_MIN_INTERVAL_MS, 1_000)
                } else {
                    clamp_interval(self.base_interval_ms.max(1_000), 1_000, 3_000)
                }
            }
            AttemptOutcome::Code(429) => {
                let extra = (self.consecutive_429.saturating_sub(1) as u64) * 100;
                jittered_interval(clamp_interval(
                    self.base_interval_ms.saturating_add(extra),
                    OPENING_MIN_INTERVAL_MS,
                    2_000,
                ))
            }
            AttemptOutcome::NetworkError => {
                let extra = self.consecutive_network_errors as u64 * 300;
                clamp_interval(self.base_interval_ms.saturating_add(extra), 500, 5_000)
            }
            _ => {
                if self.in_opening_burst() {
                    clamp_interval(self.base_interval_ms, OPENING_MIN_INTERVAL_MS, 1_000)
                } else {
                    clamp_interval(self.base_interval_ms.max(1_000), 1_000, 3_000)
                }
            }
        }
    }

    fn reflow_delay_ms(&self, _phase: RequestPhase, outcome: AttemptOutcome) -> u64 {
        let base = daytime_reflow_base_interval(self.base_interval_ms);
        match outcome {
            AttemptOutcome::Code(900001) | AttemptOutcome::Code(900002) => 1_000,
            AttemptOutcome::Code(219) | AttemptOutcome::Code(221) => 5_000,
            AttemptOutcome::Code(100051) => 0,
            AttemptOutcome::Code(100009) => clamp_interval(base, 700, 5_000),
            AttemptOutcome::Code(429) => {
                let extra = if self.consecutive_429 <= 3 {
                    0
                } else {
                    (self.consecutive_429 - 3) as u64 * 250
                };
                jittered_interval(clamp_interval(base.saturating_add(extra), 250, 5_000))
            }
            AttemptOutcome::NetworkError => {
                let extra = self.consecutive_network_errors as u64 * 500;
                clamp_interval(base.saturating_add(extra), 1_000, 10_000)
            }
            _ => base,
        }
    }

    fn next_412_cooldown_ms(&mut self) -> u64 {
        let now = Instant::now();
        let cooldown = match self.last_412_at {
            Some(last) if now.duration_since(last).as_millis() <= RECHECK_412_ESCALATE_MS => {
                REPEATED_412_COOLDOWN_MS
            }
            Some(last) if now.duration_since(last).as_millis() <= RECHECK_412_RESET_MS => {
                FIRST_412_COOLDOWN_MS
            }
            _ => FIRST_412_COOLDOWN_MS,
        };
        self.last_412_at = Some(now);
        cooldown
    }

    fn in_opening_burst(&self) -> bool {
        self.opening_started_at.elapsed().as_millis() <= OPENING_BURST_WINDOW_MS
    }

    fn stats_label(&self) -> String {
        format!(
            "策略: {}, 当前间隔: {}ms, 连续429: {}",
            self.strategy.label(),
            self.last_interval_ms,
            self.consecutive_429
        )
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TicketInfo {
    pub project_id: String,
    pub project_name: Option<String>,
    pub screen_id: String,
    pub sku_id: String,
    pub count: u32,
    pub buyer_info: serde_json::Value,
    pub deliver_info: serde_json::Value,
    pub cookies: Vec<String>,
    pub is_hot_project: Option<bool>,
    pub pay_money: Option<u32>,
    pub contact_name: Option<String>,
    pub contact_tel: Option<String>,
}

pub trait TaskEmitter: Clone + Send + Sync + 'static {
    fn emit_log(&self, task_id: &str, message: &str);
    fn emit_payment(&self, task_id: &str, url: &str);
    fn emit_task_result(&self, task_id: &str, success: bool, message: &str);
}

fn emit_log<E: TaskEmitter>(emitter: &E, task_id: &str, message: &str) {
    emitter.emit_log(task_id, message);
    info!("[{}] {}", task_id, message);
}

fn emit_task_result<E: TaskEmitter>(emitter: &E, task_id: &str, success: bool, message: &str) {
    emitter.emit_task_result(task_id, success, message);
}

fn emit_payment<E: TaskEmitter>(emitter: &E, task_id: &str, url: &str) {
    emitter.emit_payment(task_id, url);
}

fn beijing_offset() -> FixedOffset {
    FixedOffset::east_opt(BEIJING_OFFSET_SECONDS).unwrap()
}

fn beijing_now() -> chrono::DateTime<FixedOffset> {
    Local::now().with_timezone(&beijing_offset())
}

fn parse_beijing_time(ts: &str) -> Option<chrono::DateTime<FixedOffset>> {
    for fmt in [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%dT%H:%M",
    ] {
        if let Ok(t) = chrono::NaiveDateTime::parse_from_str(ts, fmt) {
            return t.and_local_timezone(beijing_offset()).single();
        }
    }
    None
}

fn remaining_ms_until(target: &chrono::DateTime<FixedOffset>, current_offset: &AtomicI64) -> i64 {
    let offset_val = current_offset.load(Ordering::Relaxed);
    let target_with_offset = target.clone() - chrono::Duration::milliseconds(offset_val);
    (target_with_offset - beijing_now()).num_milliseconds()
}

fn should_restart_before_sale(remaining_ms: i64) -> bool {
    remaining_ms > PRE_SALE_RESTART_BEFORE_START_MS
}

fn pre_sale_restart_time(target: &chrono::DateTime<FixedOffset>) -> chrono::DateTime<FixedOffset> {
    target.clone() - chrono::Duration::milliseconds(PRE_SALE_RESTART_BEFORE_START_MS)
}

async fn wait_until_task_time<E: TaskEmitter>(
    emitter: &E,
    task_id: &str,
    stop_flag: &AtomicBool,
    current_offset: &AtomicI64,
    target: &chrono::DateTime<FixedOffset>,
    stopped_message: &str,
) -> bool {
    loop {
        if stop_flag.load(Ordering::Relaxed) {
            emit_log(emitter, task_id, stopped_message);
            return false;
        }

        let remaining_ms = remaining_ms_until(target, current_offset);

        if remaining_ms <= 0 {
            return true;
        }

        if remaining_ms > 5000 {
            sleep(Duration::from_secs(1)).await;
        } else if remaining_ms > 1000 {
            sleep(Duration::from_millis(100)).await;
        } else if remaining_ms > 50 {
            sleep(Duration::from_millis(10)).await;
        } else {
            sleep(Duration::from_millis(1)).await;
        }
    }
}

async fn wait_for_prepare_gate<E: TaskEmitter>(
    emitter: &E,
    task_id: &str,
    stop_flag: &AtomicBool,
    current_offset: &AtomicI64,
    target: Option<&chrono::DateTime<FixedOffset>>,
) -> bool {
    if let Some(target) = target {
        let remaining_ms = remaining_ms_until(target, current_offset);
        let decision = prepare_gate_decision(remaining_ms);
        if !decision.allowed {
            emit_log(
                emitter,
                task_id,
                &format!(
                    "Prepare gate waiting until sale time: {} ({}ms remaining)",
                    target.format("%Y-%m-%d %H:%M:%S%.3f"),
                    decision.remaining_ms
                ),
            );
            return wait_until_task_time(
                emitter,
                task_id,
                stop_flag,
                current_offset,
                target,
                "Task stopped by user while waiting for prepare gate.",
            )
            .await;
        }
    }
    true
}

pub async fn start_buy_task<E: TaskEmitter>(
    emitter: E,
    task_id: String,
    stop_flag: Arc<AtomicBool>,
    mut info: TicketInfo,
    interval: u64,
    mode: u32,
    total_attempts: u32,
    time_start: Option<String>,
    proxy: Option<String>,
    time_offset: Option<f64>,
    _ntp_server: Option<String>,
    strategy_mode: Option<String>,
    allow_pre_sale_restart: bool,
    base_dir: std::path::PathBuf,
) -> Result<BuyTaskOutcome> {
    emit_log(&emitter, &task_id, "Starting buy task...");

    if let Some(p) = &proxy {
        emit_log(&emitter, &task_id, &format!("Using proxy: {}", p));
    }
    if let Some(to) = time_offset {
        emit_log(&emitter, &task_id, &format!("Time offset: {}ms", to));
    }

    let jar = Arc::new(Jar::default());
    let url = "https://show.bilibili.com".parse::<Url>().unwrap();

    // Parse cookies
    for cookie_str in &info.cookies {
        for part in cookie_str.split(';') {
            jar.add_cookie_str(part.trim(), &url);
        }
    }

    let mut client_builder = Client::builder()
        .cookie_provider(jar)
        .user_agent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 Edg/126.0.0.0")
        .connect_timeout(Duration::from_secs(3))
        .tcp_keepalive(Duration::from_secs(60))
        .timeout(Duration::from_secs(10))
        .http2_keep_alive_interval(Duration::from_secs(30))
        .http2_keep_alive_timeout(Duration::from_secs(10));

    if let Some(proxy_url) = proxy.as_ref().map(|p| p.trim()).filter(|p| !p.is_empty()) {
        let proxy = Proxy::all(proxy_url)
            .map_err(|e| anyhow::anyhow!("Invalid proxy '{}': {}", proxy_url, e))?;
        client_builder = client_builder.proxy(proxy);
    }

    let client = client_builder.build()?;
    let current_offset = Arc::new(AtomicI64::new(time_offset.unwrap_or(0.0) as i64));
    let mut scheduled_target = None;
    let mut scheduled_was_future = false;

    if let Some(ts) = &time_start {
        emit_log(&emitter, &task_id, &format!("Scheduled start time: {}", ts));

        let target_time = parse_beijing_time(ts);

        if let Some(target) = target_time {
            scheduled_was_future = remaining_ms_until(&target, current_offset.as_ref()) > 0;
            let initial_offset = current_offset.load(Ordering::Relaxed);
            let offset_clone = current_offset.clone();
            let target_for_sync = target.clone();
            let stop_flag_clone = stop_flag.clone();
            let task_id_clone = task_id.clone();
            let emitter_clone = emitter.clone();
            let time_client_clone = client.clone();

            // Spawn background sync task
            tokio::spawn(async move {
                let sync_interval = Duration::from_secs(10);
                let mut pending_offset: Option<i64> = None;
                loop {
                    if stop_flag_clone.load(Ordering::Relaxed) {
                        break;
                    }
                    sleep(sync_interval).await;

                    if remaining_ms_until(&target_for_sync, offset_clone.as_ref())
                        <= TIME_SYNC_FREEZE_BEFORE_START_MS
                    {
                        break;
                    }

                    match api::sample_time(&time_client_clone, api::DEFAULT_TIME_SERVER).await {
                        Ok(result) => {
                            let new_offset = result.diff;
                            let old_offset = offset_clone.load(Ordering::Relaxed);
                            if (new_offset - old_offset).abs() > TIME_SYNC_OFFSET_JUMP_WARN_MS {
                                if pending_offset
                                    .map(|pending| {
                                        (new_offset - pending).abs()
                                            <= TIME_SYNC_OFFSET_JUMP_WARN_MS
                                    })
                                    .unwrap_or(false)
                                {
                                    offset_clone.store(new_offset, Ordering::Relaxed);
                                    pending_offset = None;
                                    emit_log(
                                        &emitter_clone,
                                        &task_id_clone,
                                        &format!(
                                            "Background sync accepted offset jump: {}ms",
                                            new_offset
                                        ),
                                    );
                                } else {
                                    pending_offset = Some(new_offset);
                                    emit_log(&emitter_clone, &task_id_clone, &format!("Background sync offset jump ignored once: {}ms -> {}ms", old_offset, new_offset));
                                }
                            } else {
                                offset_clone.store(new_offset, Ordering::Relaxed);
                                pending_offset = None;
                            }
                        }
                        Err(e) => {
                            emit_log(
                                &emitter_clone,
                                &task_id_clone,
                                &format!("Background sync failed: {}", e),
                            );
                        }
                    }
                }
            });
            emit_log(
                &emitter,
                &task_id,
                &format!(
                    "Waiting until sale time: {} (Initial Offset: {}ms)",
                    target.format("%Y-%m-%d %H:%M:%S%.3f"),
                    initial_offset
                ),
            );

            if !wait_until_task_time(
                &emitter,
                &task_id,
                stop_flag.as_ref(),
                current_offset.as_ref(),
                &target,
                "Task stopped by user while waiting for sale time.",
            )
            .await
            {
                return Ok(BuyTaskOutcome::Finished);
            }

            if allow_pre_sale_restart
                && should_restart_before_sale(remaining_ms_until(&target, current_offset.as_ref()))
            {
                let restart_time = pre_sale_restart_time(&target);
                emit_log(
                    &emitter,
                    &task_id,
                    &format!(
                        "Waiting until cookie refresh time: {}",
                        restart_time.format("%Y-%m-%d %H:%M:%S%.3f")
                    ),
                );

                if !wait_until_task_time(
                    &emitter,
                    &task_id,
                    stop_flag.as_ref(),
                    current_offset.as_ref(),
                    &restart_time,
                    "Task stopped by user while waiting for cookie refresh time.",
                )
                .await
                {
                    return Ok(BuyTaskOutcome::Finished);
                }

                if remaining_ms_until(&target, current_offset.as_ref()) > 0 {
                    emit_log(
                        &emitter,
                        &task_id,
                        "Restarting task one minute before sale to refresh cookies.",
                    );
                    return Ok(BuyTaskOutcome::RestartBeforeSale);
                }
            }

            emit_log(&emitter, &task_id, "Sale time reached! Preparing order...");
            scheduled_target = Some(target);
        } else {
            let message = format!(
                "Invalid scheduled start time format: {}. Expected YYYY-MM-DD HH:mm or YYYY-MM-DD HH:mm:ss.",
                ts
            );
            emit_log(&emitter, &task_id, &message);
            emit_task_result(&emitter, &task_id, false, &message);
            return Ok(BuyTaskOutcome::Finished);
        }
    }

    let requested_strategy = StrategyMode::from_option(strategy_mode.as_deref());
    let active_strategy = resolve_active_strategy(requested_strategy, scheduled_was_future);
    let mut delay_controller = AdaptiveDelayController::new(active_strategy, interval);
    emit_log(
        &emitter,
        &task_id,
        &format!(
            "Strategy mode: {} -> {} | base interval: {}ms",
            requested_strategy.label(),
            active_strategy.label(),
            interval
        ),
    );

    let is_hot = info.is_hot_project.unwrap_or(false);
    let mut ctoken_gen = CTokenGenerator::new(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_secs(),
        0,
        rand::random::<u64>() % 8000 + 2000,
    );

    let mut token_payload = json!({
        "count": info.count,
        "screen_id": info.screen_id,
        "order_type": 1,
        "project_id": info.project_id,
        "sku_id": info.sku_id,
        "buyer_info": info.buyer_info.clone(),
        "ignoreRequestLimit": true,
        "ticket_agent": "",
        "token": "",
        "newRisk": true,
        "requestSource": "neul-next",
    });

    let mut left_time = total_attempts as i32;
    let mut is_running = true;

    // Generate static device ID for this task
    let device_id = format!(
        "{:x}",
        md5::compute(format!("{}{}", task_id, rand::random::<u64>()))
    );

    while is_running {
        if stop_flag.load(Ordering::Relaxed) {
            emit_log(&emitter, &task_id, "Task stopped by user.");
            break;
        }

        if !wait_for_prepare_gate(
            &emitter,
            &task_id,
            stop_flag.as_ref(),
            current_offset.as_ref(),
            scheduled_target.as_ref(),
        )
        .await
        {
            return Ok(BuyTaskOutcome::Finished);
        }

        emit_log(&emitter, &task_id, "1) Preparing order...");

        if is_hot {
            token_payload["token"] = json!(ctoken_gen.generate_ctoken(false));
        } else {
            token_payload["token"] = json!("");
        }

        let prepare_url = format!(
            "https://show.bilibili.com/api/ticket/order/prepare?project_id={}",
            info.project_id
        );
        let prepare_started_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis() as u64;
        let prepare_start = Instant::now();
        let res = client
            .post(&prepare_url)
            .json(&token_payload)
            .send()
            .await;

        let res = match res {
            Ok(res) => res,
            Err(e) => {
                emit_log(&emitter, &task_id, &format!("Prepare request error: {}", e));
                let delay =
                    delay_controller.next_delay(RequestPhase::Prepare, AttemptOutcome::NetworkError);
                emit_log(
                    &emitter,
                    &task_id,
                    &format!(
                        "{} | prepare network retry in {}ms",
                        delay_controller.stats_label(),
                        delay.as_millis()
                    ),
                );
                if !sleep_with_stop(stop_flag.as_ref(), delay).await {
                    emit_log(&emitter, &task_id, "Task stopped by user.");
                    break;
                }
                continue;
            }
        };

        let res_json: serde_json::Value = res.json().await?;

        if res_json["errno"].as_i64().unwrap_or(-1) != 0
            && res_json["code"].as_i64().unwrap_or(-1) != 0
        {
            let errno = res_json["errno"]
                .as_i64()
                .or(res_json["code"].as_i64())
                .unwrap_or(-1);
            let before_sale = scheduled_target
                .as_ref()
                .map(|target| remaining_ms_until(target, current_offset.as_ref()) > 0)
                .unwrap_or(false);

            emit_log(
                &emitter,
                &task_id,
                &format!(
                    "Prepare failed: {} ({}) | Msg: {}",
                    errno,
                    get_error_message(errno),
                    res_json["msg"]
                ),
            );
            let delay =
                delay_controller.next_delay(RequestPhase::Prepare, AttemptOutcome::Code(errno));
            if errno == 412 {
                emit_log(
                    &emitter,
                    &task_id,
                    &format!("412 cooldown: {}s", delay.as_secs()),
                );
            }
            emit_log(
                &emitter,
                &task_id,
                &format!(
                    "{} | next prepare in {}ms",
                    delay_controller.stats_label(),
                    delay.as_millis()
                ),
            );
            if let Some(remaining) =
                precise_interval_sleep_duration(prepare_start, delay.as_millis() as u64)
            {
                if !sleep_with_stop(stop_flag.as_ref(), remaining).await {
                    emit_log(&emitter, &task_id, "Task stopped by user.");
                    break;
                }
            }
            if mode == 1 && !before_sale {
                left_time -= 1;
                if left_time <= 0 {
                    is_running = false;
                    emit_log(&emitter, &task_id, "Total attempts reached. Stopping.");
                    emit_task_result(&emitter, &task_id, false, "达到最大尝试次数，任务停止");
                }
            }
            continue;
        }

        let token = res_json["data"]["token"].as_str().unwrap_or("").to_string();
        let ptoken = res_json["data"]["ptoken"]
            .as_str()
            .unwrap_or("")
            .replace('=', "");

        emit_log(&emitter, &task_id, "2) Creating order...");

        // Prepare create payload
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis() as u64;
        let mut create_payload = json!({
            "project_id": info.project_id,
            "screen_id": info.screen_id,
            "sku_id": info.sku_id,
            "count": info.count,
            "order_type": 1,
            "buyer_info": info.buyer_info.to_string(),
            "deliver_info": info.deliver_info.to_string(),
            "token": token,
            "again": 1,
            "timestamp": now_ms,
            "deviceId": device_id,
            "requestSource": "neul-next",
            "newRisk": true
        });

        if let Some(pay_money) = info.pay_money {
            create_payload["pay_money"] = json!(pay_money);
        }

        // Add contact info
        if let Some(name) = &info.contact_name {
            create_payload["contact_name"] = json!(name);
            create_payload["buyer"] = json!(name);
        }
        if let Some(tel) = &info.contact_tel {
            if !tel.contains('*') {
                create_payload["contact_tel"] = json!(tel);
                create_payload["tel"] = json!(tel);
            }
        }

        let mut success = false;

        // Use user-provided total_attempts, default to 60 if 0 passed accidentally
        let max_attempts = if total_attempts > 0 {
            total_attempts
        } else {
            60
        };

        for attempt in 1..=max_attempts {
            if !is_running {
                break;
            }
            if stop_flag.load(Ordering::Relaxed) {
                emit_log(&emitter, &task_id, "Task stopped by user.");
                is_running = false;
                break;
            }

            let should_log_attempt = attempt == 1 || attempt == max_attempts || attempt % 10 == 0;
            let mut create_url = format!(
                "https://show.bilibili.com/api/ticket/order/createV2?project_id={}",
                info.project_id
            );

            if is_hot {
                let now_ms = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)?
                    .as_millis() as u64;
                create_payload["ctoken"] = json!(ctoken_gen.generate_ctoken(true));
                create_payload["ptoken"] = json!(ptoken);
                create_payload["orderCreateUrl"] =
                    json!("https://show.bilibili.com/api/ticket/order/createV2");
                create_payload["clickPosition"] = json!({
                    "x": rand::random::<u64>() % 501 + 400,
                    "y": rand::random::<u64>() % 501 + 400,
                    "origin": prepare_started_ms,
                    "now": now_ms
                });
                create_url.push_str(&format!("&ptoken={}", ptoken));
            }

            let start = Instant::now();
            let res = client.post(&create_url).json(&create_payload).send().await;

            match res {
                Ok(r) => {
                    let r_json: serde_json::Value = r.json().await.unwrap_or(json!({}));
                    let errno = r_json["errno"]
                        .as_i64()
                        .or(r_json["code"].as_i64())
                        .unwrap_or(-1);

                    if should_log_attempt {
                        emit_log(
                            &emitter,
                            &task_id,
                            &format!(
                                "[Attempt {}/{}] Code: {} ({}) | Msg: {}",
                                attempt,
                                max_attempts,
                                errno,
                                get_error_message(errno),
                                r_json["msg"]
                            ),
                        );
                    }

                    if errno == 0 || errno == 100048 || errno == 100079 {
                        success = true;
                        let order_id = if let Some(s) = r_json["data"]["orderId"].as_str() {
                            s.to_string()
                        } else if let Some(n) = r_json["data"]["orderId"].as_i64() {
                            n.to_string()
                        } else {
                            "".to_string()
                        };

                        if order_id.is_empty() {
                            emit_log(
                                &emitter,
                                &task_id,
                                &format!(
                                    "Existing order state reached without order id: {}",
                                    r_json["msg"]
                                ),
                            );
                            emit_task_result(
                                &emitter,
                                &task_id,
                                true,
                                &format!("已有订单，停止重试: {}", r_json["msg"]),
                            );
                            break;
                        }

                        emit_log(&emitter, &task_id, "Order created successfully!");

                        emit_log(&emitter, &task_id, &format!("Order ID: {}", order_id));

                        let mut pay_url_str = "".to_string();
                        let pay_url_api = format!(
                            "https://show.bilibili.com/api/ticket/order/getPayParam?order_id={}",
                            order_id
                        );

                        if let Ok(pay_res) = client.get(&pay_url_api).send().await {
                            if let Ok(pay_json) = pay_res.json::<serde_json::Value>().await {
                                if let Some(code_url) = pay_json["data"]["code_url"].as_str() {
                                    pay_url_str = code_url.to_string();
                                    emit_payment(&emitter, &task_id, code_url);
                                } else {
                                    emit_log(
                                        &emitter,
                                        &task_id,
                                        &format!("Failed to get payment URL: {:?}", pay_json),
                                    );
                                }
                            }
                        }

                        // Save to history regardless of payment URL
                        let history_item = HistoryItem {
                            order_id: order_id.to_string(),
                            project_name: info
                                .project_name
                                .clone()
                                .unwrap_or(info.project_id.clone()),
                            price: info.pay_money.unwrap_or(0),
                            time: beijing_now().format("%Y-%m-%d %H:%M:%S").to_string(),
                            pay_url: pay_url_str,
                        };
                        if let Err(e) = storage::add_history_item(&base_dir, history_item) {
                            emit_log(
                                &emitter,
                                &task_id,
                                &format!("Warning: Failed to save history: {}", e),
                            );
                        }

                        emit_task_result(
                            &emitter,
                            &task_id,
                            true,
                            &format!("抢票成功！订单号: {}", order_id),
                        );
                        break;
                    }

                    if errno == 100034 {
                        // Price changed
                        if let Some(new_price) = r_json["data"]["pay_money"].as_u64() {
                            emit_log(
                                &emitter,
                                &task_id,
                                &format!("Price updated to: {}", new_price),
                            );
                            info.pay_money = Some(new_price as u32);
                            create_payload["pay_money"] = json!(new_price);
                        }
                    }

                    if errno == 100051 {
                        // Token expired
                        let _ =
                            delay_controller.next_delay(RequestPhase::Create, AttemptOutcome::Code(errno));
                        break;
                    }

                    let delay =
                        delay_controller.next_delay(RequestPhase::Create, AttemptOutcome::Code(errno));
                    if errno == 412 {
                        emit_log(
                            &emitter,
                            &task_id,
                            &format!("412 cooldown: {}s", delay.as_secs()),
                        );
                    }
                    if should_log_attempt || errno == 412 || delay.as_millis() >= 5_000 {
                        emit_log(
                            &emitter,
                            &task_id,
                            &format!(
                                "{} | next create in {}ms",
                                delay_controller.stats_label(),
                                delay.as_millis()
                            ),
                        );
                    }

                    if let Some(remaining) =
                        precise_interval_sleep_duration(start, delay.as_millis() as u64)
                    {
                        if !sleep_with_stop(stop_flag.as_ref(), remaining).await {
                            emit_log(&emitter, &task_id, "Task stopped by user.");
                            is_running = false;
                            break;
                        }
                    }
                    continue;
                }
                Err(e) => {
                    if should_log_attempt {
                        emit_log(
                            &emitter,
                            &task_id,
                            &format!(
                                "[Attempt {}/{}] Request error: {}",
                                attempt, max_attempts, e
                            ),
                        );
                    }
                    let delay =
                        delay_controller.next_delay(RequestPhase::Create, AttemptOutcome::NetworkError);
                    if should_log_attempt || delay.as_millis() >= 5_000 {
                        emit_log(
                            &emitter,
                            &task_id,
                            &format!(
                                "{} | next create in {}ms",
                                delay_controller.stats_label(),
                                delay.as_millis()
                            ),
                        );
                    }
                    if let Some(remaining) =
                        precise_interval_sleep_duration(start, delay.as_millis() as u64)
                    {
                        if !sleep_with_stop(stop_flag.as_ref(), remaining).await {
                            emit_log(&emitter, &task_id, "Task stopped by user.");
                            is_running = false;
                            break;
                        }
                    }
                    continue;
                }
            }
        }

        if success {
            is_running = false;
        } else {
            emit_log(
                &emitter,
                &task_id,
                "Retry attempts exhausted or token expired. Restarting loop...",
            );
            if mode == 1 {
                left_time -= 1;
                if left_time <= 0 {
                    is_running = false;
                    emit_log(&emitter, &task_id, "Total attempts reached. Stopping.");
                    emit_task_result(&emitter, &task_id, false, "达到最大尝试次数，任务停止");
                }
            }
        }
    }

    Ok(BuyTaskOutcome::Finished)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Timelike;
    use std::time::Duration;

    #[test]
    fn parses_scheduled_time_as_beijing_time() {
        let parsed = parse_beijing_time("2026-06-19 12:34:56").unwrap();

        assert_eq!(parsed.offset().local_minus_utc(), BEIJING_OFFSET_SECONDS);
        assert_eq!(parsed.hour(), 12);
        assert_eq!(parsed.minute(), 34);
        assert_eq!(parsed.second(), 56);
    }

    #[test]
    fn parses_frontend_datetime_local_without_seconds() {
        let parsed = parse_beijing_time("2026-06-19 07:10").unwrap();

        assert_eq!(parsed.offset().local_minus_utc(), BEIJING_OFFSET_SECONDS);
        assert_eq!(parsed.hour(), 7);
        assert_eq!(parsed.minute(), 10);
        assert_eq!(parsed.second(), 0);
    }

    #[test]
    fn parses_frontend_datetime_local_separator() {
        let parsed = parse_beijing_time("2026-06-19T07:10").unwrap();

        assert_eq!(parsed.offset().local_minus_utc(), BEIJING_OFFSET_SECONDS);
        assert_eq!(parsed.hour(), 7);
        assert_eq!(parsed.minute(), 10);
        assert_eq!(parsed.second(), 0);
    }

    #[test]
    fn prepare_gate_blocks_before_sale_time() {
        let decision = prepare_gate_decision(1);

        assert!(!decision.allowed);
        assert_eq!(decision.remaining_ms, 1);
    }

    #[test]
    fn prepare_gate_allows_at_sale_time() {
        let decision = prepare_gate_decision(0);

        assert!(decision.allowed);
        assert_eq!(decision.remaining_ms, 0);
    }

    #[test]
    fn token_expiry_uses_immediate_retry_to_reprepare() {
        let mut controller = AdaptiveDelayController::new(ActiveStrategy::Opening, 500);

        assert_eq!(
            controller.next_delay_ms(RequestPhase::Create, AttemptOutcome::Code(100051)),
            0
        );
    }

    #[test]
    fn first_412_cools_down_for_one_minute() {
        let mut controller = AdaptiveDelayController::new(ActiveStrategy::Reflow, 500);

        assert_eq!(
            controller.next_delay_ms(RequestPhase::Create, AttemptOutcome::Code(412)),
            FIRST_412_COOLDOWN_MS
        );
    }

    #[test]
    fn repeated_412_within_ten_minutes_cools_down_for_five_minutes() {
        let mut controller = AdaptiveDelayController::new(ActiveStrategy::Reflow, 500);
        controller.last_412_at = Some(Instant::now() - Duration::from_secs(9 * 60));

        assert_eq!(
            controller.next_delay_ms(RequestPhase::Create, AttemptOutcome::Code(412)),
            REPEATED_412_COOLDOWN_MS
        );
    }

    #[test]
    fn old_412_resets_to_one_minute_cooldown() {
        let mut controller = AdaptiveDelayController::new(ActiveStrategy::Reflow, 500);
        controller.last_412_at = Some(Instant::now() - Duration::from_secs(31 * 60));

        assert_eq!(
            controller.next_delay_ms(RequestPhase::Create, AttemptOutcome::Code(412)),
            FIRST_412_COOLDOWN_MS
        );
    }

    #[test]
    fn busy_code_waits_at_least_one_second() {
        let mut controller = AdaptiveDelayController::new(ActiveStrategy::Opening, 250);

        assert!(
            controller.next_delay_ms(RequestPhase::Create, AttemptOutcome::Code(900001)) >= 1_000
        );
        assert!(
            controller.next_delay_ms(RequestPhase::Create, AttemptOutcome::Code(900002)) >= 1_000
        );
    }

    #[test]
    fn auto_strategy_uses_opening_when_schedule_was_future() {
        assert_eq!(
            resolve_active_strategy(StrategyMode::Auto, true),
            ActiveStrategy::Opening
        );
    }

    #[test]
    fn auto_strategy_uses_reflow_without_future_schedule() {
        assert_eq!(
            resolve_active_strategy(StrategyMode::Auto, false),
            ActiveStrategy::Reflow
        );
    }

    #[test]
    fn requires_pre_sale_restart_before_final_minute() {
        assert!(should_restart_before_sale(
            PRE_SALE_RESTART_BEFORE_START_MS + 1
        ));
    }

    #[test]
    fn skips_pre_sale_restart_inside_final_minute() {
        assert!(!should_restart_before_sale(
            PRE_SALE_RESTART_BEFORE_START_MS
        ));
        assert!(!should_restart_before_sale(1));
        assert!(!should_restart_before_sale(0));
        assert!(!should_restart_before_sale(-1));
    }

    #[test]
    fn pre_sale_restart_time_is_one_minute_before_sale() {
        let target = parse_beijing_time("2026-06-19 12:34:56").unwrap();
        let restart_at = pre_sale_restart_time(&target);

        assert_eq!(
            (target - restart_at).num_milliseconds(),
            PRE_SALE_RESTART_BEFORE_START_MS
        );
        assert_eq!(restart_at.hour(), 12);
        assert_eq!(restart_at.minute(), 33);
        assert_eq!(restart_at.second(), 56);
    }
}
