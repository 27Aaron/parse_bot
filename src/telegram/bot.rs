use std::{
    collections::{HashMap, VecDeque},
    panic::AssertUnwindSafe,
    sync::{Arc, Mutex as StdMutex, OnceLock},
    time::Duration,
};

use futures_util::FutureExt;
use regex::{Captures, Regex};
use tokio::{
    sync::{Mutex, Semaphore, mpsc},
    task::AbortHandle,
};
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use tracing::{error, info, warn};

use crate::{
    AppError, Result,
    i18n::{self, Language, Status},
    media::{DownloadedMedia, MediaDownloader, MediaProbe, decrypt_file_prefix, probe_media},
    model::{MediaSource, ResolvedPost, TelegramMediaKind, VideoCodec},
    storage::{MediaCache, UserSettings},
    telegram::api::{
        BotCommand, CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, InputFile, Message,
        TelegramClient, TelegramError, Update, VideoMetadata, escape_html,
    },
    wechat::{WechatResolver, extract_share_url},
};

use super::TELEGRAM_FILE_LIMIT_BYTES;

const SHUTDOWN_GRACE: Duration = Duration::from_secs(30);
const FORCED_SHUTDOWN_GRACE: Duration = Duration::from_secs(2);
const COMMAND_CONFIGURATION_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_ACTIVE_TASKS: usize = 10;
const MAX_WAITING_TASKS_PER_USER: usize = 3;
const MAX_WAITING_TASKS: usize = 100;
const MEDIA_PIPELINE_CONCURRENCY: usize = MAX_ACTIVE_TASKS;
const INBOUND_LOG_PREVIEW_CHARS: usize = 240;

fn log_inbound_update(update: &Update) {
    if let Some(callback) = update.callback_query.as_ref() {
        let message = callback.message.as_ref();
        let callback_action = callback.data.as_deref().and_then(safe_callback_action);
        let username = callback
            .sender
            .username
            .as_deref()
            .map(sanitize_inbound_log_preview);
        info!(
            event = "telegram_inbound",
            update_kind = "callback_query",
            user_id = callback.sender.id,
            chat_id = ?message.map(|message| message.chat.id),
            message_id = ?message.map(|message| message.message_id),
            username = ?username.as_deref(),
            callback_action = ?callback_action,
            "收到 Telegram 回调"
        );
        return;
    }

    let Some(message) = update.message.as_ref() else {
        info!(
            event = "telegram_inbound",
            update_kind = "unknown",
            "收到无法识别的 Telegram update"
        );
        return;
    };
    let user_id = message.sender_id();
    let username = message
        .sender
        .as_ref()
        .and_then(|sender| sender.username.as_deref())
        .map(sanitize_inbound_log_preview);

    if let Some(text) = message.text.as_deref() {
        let text = sanitize_inbound_log_preview(text);
        info!(
            event = "telegram_inbound",
            update_kind = "message",
            content_kind = "text",
            user_id = ?user_id,
            chat_id = message.chat.id,
            message_id = message.message_id,
            username = ?username.as_deref(),
            text = %text,
            "收到 Telegram 文本消息"
        );
        return;
    }

    let (content_kind, mime_type, file_size) = if let Some(video) = message.video.as_ref() {
        ("video", video.mime_type.as_deref(), video.file_size)
    } else if let Some(document) = message.document.as_ref() {
        (
            "document",
            document.mime_type.as_deref(),
            document.file_size,
        )
    } else {
        ("other", None, None)
    };
    let mime_type = mime_type.map(sanitize_inbound_log_preview);
    let caption = message.caption.as_deref().map(sanitize_inbound_log_preview);
    info!(
        event = "telegram_inbound",
        update_kind = "message",
        content_kind,
        user_id = ?user_id,
        chat_id = message.chat.id,
        message_id = message.message_id,
        username = ?username.as_deref(),
        mime_type = ?mime_type.as_deref(),
        file_size = ?file_size,
        caption = ?caption.as_deref(),
        "收到 Telegram 非文本消息"
    );
}

fn sanitize_inbound_log_preview(value: &str) -> String {
    static BOT_TOKEN_PATTERN: OnceLock<Regex> = OnceLock::new();
    static LINE_SECRET_PATTERN: OnceLock<Regex> = OnceLock::new();
    static TOKEN_ASSIGNMENT_PATTERN: OnceLock<Regex> = OnceLock::new();
    static BEARER_PATTERN: OnceLock<Regex> = OnceLock::new();
    static URL_PATTERN: OnceLock<Regex> = OnceLock::new();
    static OPAQUE_ID_PATTERN: OnceLock<Regex> = OnceLock::new();

    let bot_token_pattern = BOT_TOKEN_PATTERN.get_or_init(|| {
        Regex::new(r"\d{5,}:[A-Za-z0-9_-]{20,}")
            .expect("the Telegram Bot Token regex is a constant")
    });
    let line_secret_pattern = LINE_SECRET_PATTERN.get_or_init(|| {
        Regex::new(
            r"(?im)\b([a-z0-9_-]*(?:authorization|cookie|password|passwd))\s*([:=])\s*[^\r\n]*",
        )
        .expect("the line secret regex is a constant")
    });
    let token_assignment_pattern = TOKEN_ASSIGNMENT_PATTERN.get_or_init(|| {
        Regex::new(
            r#"(?i)\b([a-z0-9_-]*(?:bot[_-]?token|access[_-]?token|token|api[_-]?(?:hash|key)))\s*([:=])\s*(?:"[^"\r\n]*"|'[^'\r\n]*'|[^\s,;]+)"#,
        )
        .expect("the token assignment regex is a constant")
    });
    let bearer_pattern = BEARER_PATTERN.get_or_init(|| {
        Regex::new(r"(?i)\b(bearer)\s+[^\s,;]+").expect("the bearer credential regex is a constant")
    });
    let url_pattern = URL_PATTERN.get_or_init(|| {
        Regex::new(r#"(?i)\b(?:https?|tg)://[^\s<>"']+"#)
            .expect("the inbound URL regex is a constant")
    });
    let opaque_id_pattern = OPAQUE_ID_PATTERN.get_or_init(|| {
        Regex::new(r"\b[A-Za-z0-9_-]{64,}\b").expect("the opaque identifier regex is a constant")
    });

    let safe_value = value
        .chars()
        .filter(|character| !is_unsafe_log_format_control(*character))
        .map(|character| {
            if character.is_control() && !matches!(character, '\r' | '\n') {
                ' '
            } else {
                character
            }
        })
        .collect::<String>();

    // Redact URL suffixes before inserting angle-bracket placeholders; those
    // placeholders deliberately terminate the URL matcher.
    let redacted = url_pattern.replace_all(&safe_value, |captures: &Captures<'_>| {
        sanitize_inbound_url(&captures[0])
    });
    let redacted = bot_token_pattern.replace_all(&redacted, "<redacted-bot-token>");
    let redacted = line_secret_pattern.replace_all(&redacted, "$1$2<redacted>");
    let redacted = token_assignment_pattern.replace_all(&redacted, "$1$2<redacted>");
    let redacted = bearer_pattern.replace_all(&redacted, "$1 <redacted>");
    let redacted = opaque_id_pattern.replace_all(&redacted, "<redacted-id>");

    let mut single_line = String::with_capacity(redacted.len().min(INBOUND_LOG_PREVIEW_CHARS));
    let mut pending_space = false;
    for character in redacted.chars() {
        if character.is_whitespace() || character.is_control() {
            pending_space = !single_line.is_empty();
            continue;
        }
        if pending_space {
            single_line.push(' ');
            pending_space = false;
        }
        single_line.push(character);
    }

    if single_line.chars().count() <= INBOUND_LOG_PREVIEW_CHARS {
        return single_line;
    }
    let mut preview = single_line
        .chars()
        .take(INBOUND_LOG_PREVIEW_CHARS - 1)
        .collect::<String>();
    preview.push('…');
    preview
}

fn is_unsafe_log_format_control(character: char) -> bool {
    matches!(
        character,
        '\u{061c}'
            | '\u{200b}'..='\u{200f}'
            | '\u{202a}'..='\u{202e}'
            | '\u{2060}'..='\u{206f}'
            | '\u{feff}'
    )
}

fn sanitize_inbound_url(value: &str) -> String {
    let Ok(mut url) = url::Url::parse(value) else {
        let secret_start = value.find(['?', '#']);
        return secret_start.map_or_else(
            || "<redacted-url>".to_owned(),
            |index| format!("{}?<redacted>", &value[..index]),
        );
    };
    let had_credentials = !url.username().is_empty() || url.password().is_some();
    let had_query = url.query().is_some();
    let had_fragment = url.fragment().is_some();
    if !had_credentials && !had_query && !had_fragment {
        return value.to_owned();
    }

    if had_credentials {
        let _ = url.set_username("");
        let _ = url.set_password(None);
    }
    url.set_query(None);
    url.set_fragment(None);
    let mut sanitized = url.to_string();
    if had_query || had_fragment {
        sanitized.push_str("?<redacted>");
    }
    sanitized
}

fn safe_callback_action(data: &str) -> Option<&'static str> {
    let mut parts = data.split('|');
    if parts.next()? != "setting" || parts.next()?.parse::<u64>().is_err() {
        return None;
    }
    let action = parts.next()?;
    let argument = parts.next();
    if parts.next().is_some() {
        return None;
    }
    match (action, argument) {
        ("main", None) => Some("main"),
        ("done", None) => Some("done"),
        ("language", None) => Some("language"),
        ("source", None) => Some("source"),
        ("progress", None) => Some("progress"),
        ("reply", None) => Some("reply"),
        ("cover", None) => Some("cover"),
        ("lang", Some(code)) if Language::from_code(code).is_some() => Some("lang"),
        _ => None,
    }
}

#[derive(Clone)]
pub struct BotService {
    telegram: TelegramClient,
    resolver: WechatResolver,
    downloader: MediaDownloader,
    cache: MediaCache,
    required_channel_id: Option<Arc<str>>,
    scheduler: Arc<Mutex<SchedulerState>>,
    media_slots: Arc<Semaphore>,
    background_tasks: TaskTracker,
    task_abort_handles: Arc<StdMutex<Vec<AbortHandle>>>,
}

#[derive(Clone)]
struct MediaTask {
    chat_id: i64,
    status_message_id: i64,
    post: Arc<ResolvedPost>,
    preferences: UserPreferences,
}

#[derive(Debug, Clone, Copy)]
struct UserPreferences {
    language: Language,
    show_source: bool,
    show_progress: bool,
    reply_to_source: bool,
    show_video_cover: bool,
}

impl From<UserSettings> for UserPreferences {
    fn from(settings: UserSettings) -> Self {
        Self {
            language: settings.language,
            show_source: settings.show_source,
            show_progress: settings.show_progress,
            reply_to_source: settings.reply_to_source,
            show_video_cover: settings.show_video_cover,
        }
    }
}

struct ParseTask {
    task_id: u64,
    user_id: u64,
    chat_id: i64,
    source_message_id: i64,
    share_url: url::Url,
    status_message_id: Option<i64>,
    cancellation: CancellationToken,
    preferences: UserPreferences,
}

#[derive(Debug, Clone, Copy)]
struct SchedulerLimits {
    max_active: usize,
    max_waiting_per_user: usize,
    max_waiting: usize,
}

impl Default for SchedulerLimits {
    fn default() -> Self {
        Self {
            max_active: MAX_ACTIVE_TASKS,
            max_waiting_per_user: MAX_WAITING_TASKS_PER_USER,
            max_waiting: MAX_WAITING_TASKS,
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct WaitingTask {
    task_id: u64,
    user_id: u64,
    ready: bool,
}

#[derive(Debug)]
struct FairTaskScheduler {
    limits: SchedulerLimits,
    next_task_id: u64,
    accepting: bool,
    running: HashMap<u64, u64>,
    waiting_by_user: HashMap<u64, VecDeque<WaitingTask>>,
    round_robin: VecDeque<u64>,
    waiting_count: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Admission {
    Started { task_id: u64 },
    Queued { task_id: u64, waiting_count: usize },
    Rejected(QueueRejection),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum QueueRejection {
    UserQueueFull,
    GlobalQueueFull,
    ShuttingDown,
}

impl FairTaskScheduler {
    fn new(limits: SchedulerLimits) -> Self {
        assert!(limits.max_active > 0, "max_active must be positive");
        Self {
            limits,
            next_task_id: 1,
            accepting: true,
            running: HashMap::new(),
            waiting_by_user: HashMap::new(),
            round_robin: VecDeque::new(),
            waiting_count: 0,
        }
    }

    fn admit(&mut self, user_id: u64) -> Admission {
        if !self.accepting {
            return Admission::Rejected(QueueRejection::ShuttingDown);
        }

        let user_is_running = self.running.contains_key(&user_id);
        let user_is_waiting = self.waiting_by_user.contains_key(&user_id);
        if !user_is_running && !user_is_waiting && self.running.len() < self.limits.max_active {
            let task_id = self.allocate_task_id();
            self.running.insert(user_id, task_id);
            return Admission::Started { task_id };
        }

        let user_waiting_count = self.waiting_by_user.get(&user_id).map_or(0, VecDeque::len);
        if user_waiting_count >= self.limits.max_waiting_per_user {
            return Admission::Rejected(QueueRejection::UserQueueFull);
        }
        if self.waiting_count >= self.limits.max_waiting {
            return Admission::Rejected(QueueRejection::GlobalQueueFull);
        }

        let task_id = self.allocate_task_id();
        let queue = self.waiting_by_user.entry(user_id).or_default();
        if queue.is_empty() {
            self.round_robin.push_back(user_id);
        }
        queue.push_back(WaitingTask {
            task_id,
            user_id,
            ready: false,
        });
        self.waiting_count += 1;

        Admission::Queued {
            task_id,
            waiting_count: self.waiting_count,
        }
    }

    fn mark_ready(&mut self, task_id: u64) -> Vec<u64> {
        for queue in self.waiting_by_user.values_mut() {
            if let Some(task) = queue.iter_mut().find(|task| task.task_id == task_id) {
                task.ready = true;
                break;
            }
        }
        self.dispatch_ready()
    }

    fn complete(&mut self, task_id: u64, user_id: u64) -> Vec<u64> {
        if self.running.get(&user_id).copied() != Some(task_id) {
            return Vec::new();
        }
        self.running.remove(&user_id);
        self.move_user_to_back(user_id);
        self.dispatch_ready()
    }

    fn cancel_waiting(&mut self, task_id: u64) -> Vec<u64> {
        let Some(user_id) = self.waiting_by_user.iter().find_map(|(user_id, queue)| {
            queue
                .iter()
                .any(|task| task.task_id == task_id)
                .then_some(*user_id)
        }) else {
            return Vec::new();
        };

        let queue_is_empty = {
            let queue = self
                .waiting_by_user
                .get_mut(&user_id)
                .expect("located waiting queue must still exist");
            let position = queue
                .iter()
                .position(|task| task.task_id == task_id)
                .expect("located waiting task must still exist");
            queue.remove(position);
            queue.is_empty()
        };
        self.waiting_count -= 1;
        if queue_is_empty {
            self.waiting_by_user.remove(&user_id);
            self.round_robin
                .retain(|queued_user| *queued_user != user_id);
        }
        self.dispatch_ready()
    }

    fn shutdown(&mut self) -> Vec<u64> {
        self.accepting = false;
        let task_ids = self
            .waiting_by_user
            .values()
            .flat_map(|queue| queue.iter().map(|task| task.task_id))
            .collect();
        self.waiting_by_user.clear();
        self.round_robin.clear();
        self.waiting_count = 0;
        task_ids
    }

    fn allocate_task_id(&mut self) -> u64 {
        let task_id = self.next_task_id;
        self.next_task_id = self
            .next_task_id
            .checked_add(1)
            .expect("task id space exhausted");
        task_id
    }

    fn move_user_to_back(&mut self, user_id: u64) {
        if let Some(position) = self
            .round_robin
            .iter()
            .position(|queued_user| *queued_user == user_id)
        {
            self.round_robin.remove(position);
            self.round_robin.push_back(user_id);
        }
    }

    fn dispatch_ready(&mut self) -> Vec<u64> {
        let mut dispatched = Vec::new();
        while self.accepting && self.running.len() < self.limits.max_active {
            let Some(task) = self.pop_next_ready() else {
                break;
            };
            self.waiting_count -= 1;
            self.running.insert(task.user_id, task.task_id);
            dispatched.push(task.task_id);
        }
        dispatched
    }

    fn pop_next_ready(&mut self) -> Option<WaitingTask> {
        let users_to_scan = self.round_robin.len();
        for _ in 0..users_to_scan {
            let user_id = self.round_robin.pop_front()?;
            let can_run = !self.running.contains_key(&user_id)
                && self
                    .waiting_by_user
                    .get(&user_id)
                    .and_then(|queue| queue.front())
                    .is_some_and(|task| task.ready);
            if !can_run {
                self.round_robin.push_back(user_id);
                continue;
            }

            let (task, has_more) = {
                let queue = self
                    .waiting_by_user
                    .get_mut(&user_id)
                    .expect("round-robin user must have a waiting queue");
                let task = queue
                    .pop_front()
                    .expect("eligible waiting queue must not be empty");
                (task, !queue.is_empty())
            };
            if has_more {
                self.round_robin.push_back(user_id);
            } else {
                self.waiting_by_user.remove(&user_id);
            }
            return Some(task);
        }
        None
    }
}

struct SchedulerState {
    scheduler: FairTaskScheduler,
    waiting_tasks: HashMap<u64, ParseTask>,
}

impl SchedulerState {
    fn new() -> Self {
        Self {
            scheduler: FairTaskScheduler::new(SchedulerLimits::default()),
            waiting_tasks: HashMap::new(),
        }
    }

    fn take_dispatched(&mut self, task_ids: Vec<u64>) -> Vec<ParseTask> {
        task_ids
            .into_iter()
            .filter_map(|task_id| self.waiting_tasks.remove(&task_id))
            .collect()
    }
}

enum QueueAction {
    Start(ParseTask),
    Wait { task_id: u64, waiting_count: usize },
    Reject(QueueRejection),
}

impl BotService {
    pub fn new(
        telegram: TelegramClient,
        resolver: WechatResolver,
        downloader: MediaDownloader,
        cache: MediaCache,
        required_channel_id: Option<String>,
    ) -> Self {
        Self {
            telegram,
            resolver,
            downloader,
            cache,
            required_channel_id: required_channel_id.map(Arc::from),
            scheduler: Arc::new(Mutex::new(SchedulerState::new())),
            media_slots: Arc::new(Semaphore::new(MEDIA_PIPELINE_CONCURRENCY)),
            background_tasks: TaskTracker::new(),
            task_abort_handles: Arc::new(StdMutex::new(Vec::new())),
        }
    }

    pub async fn run(self, shutdown: CancellationToken) -> Result<()> {
        let me = tokio::select! {
            _ = shutdown.cancelled() => {
                self.shutdown_background_tasks(&shutdown).await;
                return Ok(());
            }
            result = self.telegram.get_me() => result.map_err(AppError::from)?,
        };
        info!(bot_id = me.id, username = ?me.username, "TDLib 已连接");
        self.configure_commands(&shutdown).await;

        'receiving: loop {
            let update = tokio::select! {
                _ = shutdown.cancelled() => break 'receiving,
                result = self.telegram.next_update() => result,
            };
            match update {
                Ok(update) => {
                    if let Err(error) = self.handle_update(update, &shutdown).await {
                        error!(error = %error, "处理 Telegram update 失败");
                    }
                }
                Err(error) if error.is_terminal() => {
                    shutdown.cancel();
                    self.shutdown_background_tasks(&shutdown).await;
                    return Err(AppError::from(error));
                }
                Err(error) => {
                    let delay = error.retry_after().unwrap_or(Duration::from_secs(2));
                    warn!(error = %error, ?delay, "获取 Telegram updates 失败");
                    tokio::select! {
                        _ = shutdown.cancelled() => break 'receiving,
                        _ = tokio::time::sleep(delay) => {}
                    }
                }
            }
        }

        self.shutdown_background_tasks(&shutdown).await;
        Ok(())
    }

    async fn configure_commands(&self, shutdown: &CancellationToken) {
        // Telegram chooses the most specific command set matching the user's
        // language.  The default is Chinese, while the other three sets keep
        // the command menu useful before a user opens the settings panel.
        for (language, language_code) in [
            (Language::Chinese, None),
            (Language::English, Some("en")),
            (Language::Japanese, Some("ja")),
            (Language::Russian, Some("ru")),
        ] {
            let commands = [
                BotCommand::new("start", i18n::command_start(language)),
                BotCommand::new("setting", i18n::command_setting(language)),
            ];
            let result = tokio::select! {
                _ = shutdown.cancelled() => return,
                result = tokio::time::timeout(
                    COMMAND_CONFIGURATION_TIMEOUT,
                    self.telegram.set_my_commands(&commands, language_code),
                ) => result,
            };
            match result {
                Ok(Ok(_)) => {}
                Ok(Err(error)) => {
                    warn!(
                        ?language,
                        error = %error,
                        "设置 Telegram 命令菜单失败，继续运行"
                    );
                }
                Err(_) => {
                    warn!(?language, "设置 Telegram 命令菜单超时，继续运行");
                }
            }
        }
    }

    async fn handle_update(&self, update: Update, shutdown: &CancellationToken) -> Result<()> {
        log_inbound_update(&update);
        if let Some(callback) = update.callback_query {
            return self.handle_callback(callback, shutdown).await;
        }
        if let Some(message) = update.message.filter(|message| message.text.is_some()) {
            return self.handle_message(message, shutdown).await;
        }
        Ok(())
    }

    async fn handle_callback(
        &self,
        callback: CallbackQuery,
        shutdown: &CancellationToken,
    ) -> Result<()> {
        let default_language = Language::Chinese;
        let callback_language =
            Language::from_telegram_code(callback.sender.language_code.as_deref());
        let preferences = UserPreferences::from(
            self.cache
                .get_user_settings_with_default(callback.sender.id, callback_language)
                .await
                .unwrap_or_default(),
        );
        let Some(message) = callback.message.as_ref() else {
            self.answer_callback(
                &callback.id,
                Some(i18n::invalid_setting(default_language)),
                true,
            )
            .await;
            return Ok(());
        };
        if !message.chat.is_private() {
            self.answer_callback(
                &callback.id,
                Some(i18n::only_private_settings(preferences.language)),
                true,
            )
            .await;
            return Ok(());
        }
        let Some(data) = callback.data.as_deref() else {
            self.answer_callback(
                &callback.id,
                Some(i18n::invalid_setting(preferences.language)),
                true,
            )
            .await;
            return Ok(());
        };
        let parts = data.split('|').collect::<Vec<_>>();
        let valid_prefix = parts.first() == Some(&"setting") && parts.len() >= 3;
        let owner_id = parts.get(1).and_then(|value| value.parse::<u64>().ok());
        if !valid_prefix || owner_id != Some(callback.sender.id) {
            self.answer_callback(
                &callback.id,
                Some(i18n::invalid_setting(preferences.language)),
                true,
            )
            .await;
            return Ok(());
        }

        let mut settings = self
            .cache
            .get_user_settings_with_default(callback.sender.id, callback_language)
            .await
            .unwrap_or_default();
        let action = parts[2];
        let mut notice = None;
        match action {
            "main" => {}
            "done" => {
                let referenced_message_id = message
                    .reply_to_message
                    .as_deref()
                    .map(|reply| reply.message_id);
                let result = tokio::select! {
                    _ = shutdown.cancelled() => return Ok(()),
                    result = self.telegram.delete_message(message.chat.id, message.message_id) => result,
                };
                if let Err(error) = result {
                    warn!(error = %error, "删除设置面板失败");
                }
                if let Some(referenced_message_id) = referenced_message_id {
                    let result = tokio::select! {
                        _ = shutdown.cancelled() => return Ok(()),
                        result = self.telegram.delete_message(
                            message.chat.id,
                            referenced_message_id,
                        ) => result,
                    };
                    if let Err(error) = result {
                        warn!(
                            error = %error,
                            message_id = referenced_message_id,
                            "删除设置命令消息失败"
                        );
                    }
                }
                self.answer_callback(&callback.id, None, false).await;
                return Ok(());
            }
            "language" => {
                let text = format!(
                    "<b>{}</b>\n\n{}",
                    i18n::settings_panel_title(settings.language),
                    i18n::language_panel(settings.language)
                );
                let keyboard = language_keyboard(callback.sender.id, settings.language);
                let result = tokio::select! {
                    _ = shutdown.cancelled() => return Ok(()),
                    result = self.telegram.edit_message_text_html(
                        message.chat.id,
                        message.message_id,
                        &text,
                        Some(&keyboard),
                    ) => result,
                };
                result.map_err(AppError::from)?;
                self.answer_callback(&callback.id, None, false).await;
                return Ok(());
            }
            "lang" => {
                let Some(language_code) = parts.get(3).and_then(|value| Language::from_code(value))
                else {
                    self.answer_callback(
                        &callback.id,
                        Some(i18n::invalid_setting(settings.language)),
                        true,
                    )
                    .await;
                    return Ok(());
                };
                settings.language = language_code;
                notice = Some(i18n::settings_saved(language_code));
                self.cache
                    .put_user_settings(callback.sender.id, settings)
                    .await?;
            }
            "source" => {
                settings.show_source = !settings.show_source;
                notice = Some(i18n::settings_saved(settings.language));
                self.cache
                    .put_user_settings(callback.sender.id, settings)
                    .await?;
            }
            "progress" => {
                settings.show_progress = !settings.show_progress;
                notice = Some(i18n::settings_saved(settings.language));
                self.cache
                    .put_user_settings(callback.sender.id, settings)
                    .await?;
            }
            "reply" => {
                settings.reply_to_source = !settings.reply_to_source;
                notice = Some(i18n::settings_saved(settings.language));
                self.cache
                    .put_user_settings(callback.sender.id, settings)
                    .await?;
            }
            "cover" => {
                settings.show_video_cover = !settings.show_video_cover;
                notice = Some(i18n::settings_saved(settings.language));
                self.cache
                    .put_user_settings(callback.sender.id, settings)
                    .await?;
            }
            _ => {
                self.answer_callback(
                    &callback.id,
                    Some(i18n::invalid_setting(settings.language)),
                    true,
                )
                .await;
                return Ok(());
            }
        }

        let preferences = UserPreferences::from(settings);
        let (panel, keyboard) =
            settings_panel(preferences.language, preferences, callback.sender.id);
        let result = tokio::select! {
            _ = shutdown.cancelled() => return Ok(()),
            result = self.telegram.edit_message_text_html(
                message.chat.id,
                message.message_id,
                &panel,
                Some(&keyboard),
            ) => result,
        };
        result.map_err(AppError::from)?;
        self.answer_callback(&callback.id, notice, false).await;
        Ok(())
    }

    async fn answer_callback(&self, callback_id: &str, text: Option<&str>, show_alert: bool) {
        let _ = self
            .telegram
            .answer_callback_query(callback_id, text, show_alert)
            .await;
    }

    async fn send_text_with_policy(
        &self,
        chat_id: i64,
        text: &str,
        source_message_id: i64,
        preferences: UserPreferences,
    ) -> std::result::Result<Message, TelegramError> {
        if preferences.reply_to_source {
            self.telegram
                .send_message_reply(chat_id, text, source_message_id, None)
                .await
        } else {
            self.telegram.send_message(chat_id, text, None).await
        }
    }

    async fn send_html_with_policy(
        &self,
        chat_id: i64,
        text: &str,
        source_message_id: i64,
        preferences: UserPreferences,
        reply_markup: Option<&InlineKeyboardMarkup>,
    ) -> std::result::Result<Message, TelegramError> {
        if preferences.reply_to_source {
            self.telegram
                .send_message_reply_html(chat_id, text, source_message_id, reply_markup)
                .await
        } else {
            self.telegram
                .send_message_html(chat_id, text, reply_markup)
                .await
        }
    }

    async fn handle_message(&self, message: Message, shutdown: &CancellationToken) -> Result<()> {
        let Some(user_id) = message.sender_id() else {
            return Ok(());
        };
        let telegram_language = Language::from_telegram_code(
            message
                .sender
                .as_ref()
                .and_then(|user| user.language_code.as_deref()),
        );
        let preferences = UserPreferences::from(
            self.cache
                .get_user_settings_with_default(user_id, telegram_language)
                .await?,
        );
        let chat_id = message.chat.id;
        if !message.chat.is_private() {
            tokio::select! {
                _ = shutdown.cancelled() => {}
                _ = self.send_text_with_policy(
                    chat_id,
                    i18n::non_private(preferences.language),
                    message.message_id,
                    preferences,
                ) => {}
            }
            return Ok(());
        }

        let text = message.text.as_deref().unwrap_or_default().trim();
        match command_name(text) {
            Some("start") => {
                let help =
                    i18n::start_text(preferences.language, self.required_channel_id.as_deref());
                tokio::select! {
                    _ = shutdown.cancelled() => {}
                    result = self.send_text_with_policy(
                        chat_id,
                        &help,
                        message.message_id,
                        preferences,
                    ) => {
                        result?;
                    }
                }
            }
            Some("setting") => {
                let (panel, keyboard) = settings_panel(preferences.language, preferences, user_id);
                tokio::select! {
                    _ = shutdown.cancelled() => {}
                    result = self.send_html_with_policy(
                        chat_id,
                        &panel,
                        message.message_id,
                        preferences,
                        Some(&keyboard),
                    ) => {
                        result?;
                    }
                }
            }
            Some(_) => {
                tokio::select! {
                    _ = shutdown.cancelled() => {}
                    result = self.send_text_with_policy(
                        chat_id,
                        i18n::direct_link(preferences.language),
                        message.message_id,
                        preferences,
                    ) => {
                        result?;
                    }
                }
            }
            None => {
                self.queue_parse(
                    chat_id,
                    user_id,
                    message.message_id,
                    text.to_owned(),
                    preferences,
                    shutdown,
                )
                .await?;
            }
        }
        Ok(())
    }

    async fn queue_parse(
        &self,
        chat_id: i64,
        user_id: u64,
        source_message_id: i64,
        input: String,
        preferences: UserPreferences,
        shutdown: &CancellationToken,
    ) -> Result<()> {
        let share_url = match extract_share_url(&input) {
            Ok(url) => url,
            Err(error) => {
                let message = error.localized_message(preferences.language);
                tokio::select! {
                    _ = shutdown.cancelled() => return Err(AppError::Cancelled),
                    result = self.send_text_with_policy(
                        chat_id,
                        &message,
                        source_message_id,
                        preferences,
                    ) => result?,
                };
                return Ok(());
            }
        };
        let cancellation = shutdown.child_token();
        let action = {
            let mut state = self.scheduler.lock().await;
            match state.scheduler.admit(user_id) {
                Admission::Started { task_id } => QueueAction::Start(ParseTask {
                    task_id,
                    user_id,
                    chat_id,
                    source_message_id,
                    share_url,
                    status_message_id: None,
                    cancellation,
                    preferences,
                }),
                Admission::Queued {
                    task_id,
                    waiting_count,
                } => {
                    state.waiting_tasks.insert(
                        task_id,
                        ParseTask {
                            task_id,
                            user_id,
                            chat_id,
                            source_message_id,
                            share_url,
                            status_message_id: None,
                            cancellation,
                            preferences,
                        },
                    );
                    QueueAction::Wait {
                        task_id,
                        waiting_count,
                    }
                }
                Admission::Rejected(reason) => QueueAction::Reject(reason),
            }
        };

        match action {
            QueueAction::Start(task) => self.spawn_parse_task(task),
            QueueAction::Wait {
                task_id,
                waiting_count,
            } => {
                let text = i18n::queue(preferences.language, waiting_count);
                let status = tokio::select! {
                    _ = shutdown.cancelled() => {
                        self.withdraw_waiting_task(task_id).await;
                        return Err(AppError::Cancelled);
                    }
                    result = self.send_html_with_policy(
                        chat_id,
                        &text,
                        source_message_id,
                        preferences,
                        None,
                    ) => match result {
                        Ok(message) => message,
                        Err(error) => {
                            self.withdraw_waiting_task(task_id).await;
                            return Err(error.into());
                        }
                    },
                };

                let (found, dispatched) = {
                    let mut state = self.scheduler.lock().await;
                    let found = if let Some(task) = state.waiting_tasks.get_mut(&task_id) {
                        task.status_message_id = Some(status.message_id);
                        true
                    } else {
                        false
                    };
                    let dispatched = if found {
                        let task_ids = state.scheduler.mark_ready(task_id);
                        state.take_dispatched(task_ids)
                    } else {
                        Vec::new()
                    };
                    (found, dispatched)
                };
                if !found {
                    let _ = tokio::time::timeout(
                        Duration::from_secs(1),
                        self.telegram.delete_message(chat_id, status.message_id),
                    )
                    .await;
                    return Err(AppError::Cancelled);
                }
                self.spawn_parse_tasks(dispatched);
            }
            QueueAction::Reject(QueueRejection::UserQueueFull) => {
                tokio::select! {
                    _ = shutdown.cancelled() => return Err(AppError::Cancelled),
                    result = self.send_text_with_policy(
                        chat_id,
                        i18n::user_queue_full(preferences.language),
                        source_message_id,
                        preferences,
                    ) => result?,
                };
            }
            QueueAction::Reject(QueueRejection::GlobalQueueFull) => {
                tokio::select! {
                    _ = shutdown.cancelled() => return Err(AppError::Cancelled),
                    result = self.send_text_with_policy(
                        chat_id,
                        i18n::global_queue_full(preferences.language),
                        source_message_id,
                        preferences,
                    ) => result?,
                };
            }
            QueueAction::Reject(QueueRejection::ShuttingDown) => {
                return Err(AppError::Cancelled);
            }
        }
        Ok(())
    }

    async fn withdraw_waiting_task(&self, task_id: u64) {
        let dispatched = {
            let mut state = self.scheduler.lock().await;
            state.waiting_tasks.remove(&task_id);
            let task_ids = state.scheduler.cancel_waiting(task_id);
            state.take_dispatched(task_ids)
        };
        self.spawn_parse_tasks(dispatched);
    }

    fn spawn_parse_tasks(&self, tasks: Vec<ParseTask>) {
        for task in tasks {
            self.spawn_parse_task(task);
        }
    }

    fn spawn_parse_task(&self, task: ParseTask) {
        let service = self.clone();
        let task_id = task.task_id;
        let user_id = task.user_id;
        let handle = self.background_tasks.spawn(async move {
            let result = AssertUnwindSafe(service.process_parse_task(&task))
                .catch_unwind()
                .await;
            let dispatched = {
                let mut state = service.scheduler.lock().await;
                let task_ids = state.scheduler.complete(task_id, user_id);
                state.take_dispatched(task_ids)
            };

            match result {
                Ok(Err(error)) if !matches!(error, AppError::Cancelled) => {
                    error!(task_id, user_id, error = %error, "视频任务失败");
                }
                Err(_) => {
                    error!(task_id, user_id, "视频任务异常终止，调度名额已释放");
                }
                _ => {}
            }
            service.spawn_parse_tasks(dispatched);
        });
        let abort_handle = handle.abort_handle();
        drop(handle);
        let mut abort_handles = self
            .task_abort_handles
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        abort_handles.retain(|handle| !handle.is_finished());
        abort_handles.push(abort_handle);
    }

    async fn process_parse_task(&self, task: &ParseTask) -> Result<()> {
        let cancellation = &task.cancellation;
        let status_message_id = match task.status_message_id {
            Some(status_message_id) => {
                tokio::select! {
                    _ = cancellation.cancelled() => return Err(AppError::Cancelled),
                    result = self.telegram.edit_message_text_html(
                        task.chat_id,
                        status_message_id,
                        i18n::status(task.preferences.language, Status::Parsing),
                        None,
                    ) => {
                        if let Err(error) = result {
                            warn!(task_id = task.task_id, error = %error, "排队状态更新失败，任务继续执行");
                        }
                    }
                }
                status_message_id
            }
            None => {
                tokio::select! {
                    _ = cancellation.cancelled() => return Err(AppError::Cancelled),
                    result = self.send_html_with_policy(
                        task.chat_id,
                        i18n::status(task.preferences.language, Status::Parsing),
                        task.source_message_id,
                        task.preferences,
                        None,
                    ) => result?.message_id,
                }
            }
        };

        let channel_allowed = self
            .ensure_required_channel(
                task.chat_id,
                task.user_id,
                task.source_message_id,
                cancellation,
                task.preferences,
            )
            .await;
        match channel_allowed {
            Ok(true) => {}
            Ok(false) => {
                tokio::select! {
                    _ = cancellation.cancelled() => {}
                    _ = self.telegram.delete_message(task.chat_id, status_message_id) => {}
                }
                return Ok(());
            }
            Err(error) => {
                self.edit_error_status(
                    task.chat_id,
                    status_message_id,
                    &error,
                    cancellation,
                    task.preferences.language,
                )
                .await;
                return Err(error);
            }
        }

        let resolved = tokio::select! {
            _ = cancellation.cancelled() => Err(AppError::Cancelled),
            result = self.resolver.resolve_url(&task.share_url) => result,
        };

        let post = match resolved {
            Ok(post) => Arc::new(post),
            Err(error) => {
                self.edit_error_status(
                    task.chat_id,
                    status_message_id,
                    &error,
                    cancellation,
                    task.preferences.language,
                )
                .await;
                return Err(error);
            }
        };
        let media_task = MediaTask {
            chat_id: task.chat_id,
            status_message_id,
            post,
            preferences: task.preferences,
        };

        let result = self.run_download(&media_task, cancellation).await;
        if let Err(error) = &result {
            self.edit_error_status(
                task.chat_id,
                status_message_id,
                error,
                cancellation,
                task.preferences.language,
            )
            .await;
        }
        result
    }

    async fn edit_error_status(
        &self,
        chat_id: i64,
        status_message_id: i64,
        error: &AppError,
        cancellation: &CancellationToken,
        language: Language,
    ) {
        let message = error.localized_message(language);
        tokio::select! {
            _ = cancellation.cancelled() => {}
            _ = self.telegram.edit_message_text(
                chat_id,
                status_message_id,
                &message,
                None,
            ) => {}
        }
    }

    async fn run_download(&self, task: &MediaTask, cancellation: &CancellationToken) -> Result<()> {
        if cancellation.is_cancelled() {
            return Err(AppError::Cancelled);
        }
        let caption = format_caption(
            &task.post,
            task.preferences.language,
            task.preferences.show_source,
        );
        let cover = if task.preferences.show_video_cover {
            task.post
                .cover_url
                .as_ref()
                .and_then(|url| InputFile::http_url(url).ok())
        } else {
            None
        };
        if let Some(cached) = self
            .cache
            .get(&task.post.platform, &task.post.post_id)
            .await?
        {
            self.update_status_html(
                task,
                i18n::status(task.preferences.language, Status::Sending),
                cancellation,
            )
            .await;
            let input = InputFile::file_id(cached.file_id.clone())?;
            let cached_edit = tokio::select! {
                _ = cancellation.cancelled() => return Err(AppError::Cancelled),
                result = self.edit_media(
                    task.chat_id,
                    task.status_message_id,
                    cached.kind,
                    &input,
                    &caption,
                    None,
                    cover.as_ref(),
                ) => result,
            };
            let cached_edit = if cached.kind == TelegramMediaKind::Video
                && cover.is_some()
                && cached_edit.is_err()
            {
                tokio::select! {
                    _ = cancellation.cancelled() => return Err(AppError::Cancelled),
                    result = self.edit_media(
                        task.chat_id,
                        task.status_message_id,
                        cached.kind,
                        &input,
                        &caption,
                        None,
                        None,
                    ) => result,
                }
            } else {
                cached_edit
            };
            match cached_edit {
                Ok(_) => return Ok(()),
                Err(error) if error.error_code() == Some(400) => {
                    self.cache
                        .remove(&task.post.platform, &task.post.post_id)
                        .await?;
                }
                Err(error) => return Err(error.into()),
            }
        }

        let mut source = task.post.video.clone();
        let _permit = tokio::select! {
            _ = cancellation.cancelled() => return Err(AppError::Cancelled),
            permit = self.media_slots.acquire() => permit.map_err(|_| AppError::Cancelled)?,
        };
        self.update_status_html(
            task,
            i18n::status(task.preferences.language, Status::Downloading),
            cancellation,
        )
        .await;
        let first_download = self.download_with_status(&source, task, cancellation).await;
        let downloaded = match first_download {
            Ok(downloaded) => downloaded,
            Err(AppError::Expired) => {
                self.update_status_html(
                    task,
                    i18n::status(task.preferences.language, Status::Parsing),
                    cancellation,
                )
                .await;
                let refreshed = tokio::select! {
                    _ = cancellation.cancelled() => return Err(AppError::Cancelled),
                    result = self.resolver.resolve_url(&task.post.canonical_url) => result?,
                };
                source = refreshed.video;
                self.update_status_html(
                    task,
                    i18n::status(task.preferences.language, Status::Downloading),
                    cancellation,
                )
                .await;
                self.download_with_status(&source, task, cancellation)
                    .await?
            }
            Err(error) => return Err(error),
        };

        let result = async {
            if cancellation.is_cancelled() {
                return Err(AppError::Cancelled);
            }
            if downloaded.bytes > TELEGRAM_FILE_LIMIT_BYTES {
                return Err(AppError::MediaTooLarge {
                    actual: downloaded.bytes,
                    limit: TELEGRAM_FILE_LIMIT_BYTES,
                });
            }

            if let Some(decode_key) = source.decode_key {
                tokio::select! {
                    _ = cancellation.cancelled() => return Err(AppError::Cancelled),
                    result = decrypt_file_prefix(&downloaded.path, decode_key) => result?,
                };
            }
            let probe = tokio::select! {
                _ = cancellation.cancelled() => return Err(AppError::Cancelled),
                result = probe_media(&downloaded.path) => result?,
            };

            let kind = telegram_media_kind(probe.codec);
            let video_metadata = telegram_video_metadata(&probe);
            // TDLib performs the Telegram upload internally. Only the start of
            // the request and its successful completion are exposed here.
            self.update_status_html(
                task,
                i18n::status(task.preferences.language, Status::Uploading),
                cancellation,
            )
            .await;

            let input = InputFile::local_path(&downloaded.path)?;
            let edit_result = tokio::select! {
                _ = cancellation.cancelled() => return Err(AppError::Cancelled),
                result = self.edit_media(
                    task.chat_id,
                    task.status_message_id,
                    kind,
                    &input,
                    &caption,
                    Some(video_metadata),
                    cover.as_ref(),
                ) => result,
            };
            let edit_result =
                if kind == TelegramMediaKind::Video && cover.is_some() && edit_result.is_err() {
                    tokio::select! {
                        _ = cancellation.cancelled() => return Err(AppError::Cancelled),
                        result = self.edit_media(
                            task.chat_id,
                            task.status_message_id,
                            kind,
                            &input,
                            &caption,
                            Some(video_metadata),
                            None,
                        ) => result,
                    }
                } else {
                    edit_result
                };
            let edited = match edit_result {
                Ok(message) => message,
                Err(error)
                    if kind == TelegramMediaKind::Video && error.error_code() == Some(400) =>
                {
                    tokio::select! {
                        _ = cancellation.cancelled() => return Err(AppError::Cancelled),
                        result = self.edit_media(
                            task.chat_id,
                            task.status_message_id,
                            TelegramMediaKind::Document,
                            &input,
                            &caption,
                            None,
                            None,
                        ) => result?,
                    }
                }
                Err(error) => return Err(error.into()),
            };

            if let Some(ids) = edited.media_file_ids() {
                if let Err(error) = self
                    .cache
                    .put(
                        &task.post.platform,
                        &task.post.post_id,
                        ids.kind,
                        ids.file_id,
                        Some(ids.file_unique_id),
                    )
                    .await
                {
                    warn!(error = %error, "视频已发送，但 file_id 缓存写入失败");
                }
            } else {
                warn!("视频已发送，但 Telegram 响应缺少 file_id，跳过缓存");
            }
            Ok(())
        }
        .await;

        if let Err(error) = downloaded.cleanup().await {
            warn!(error = %error, "临时媒体文件清理失败");
        }
        result
    }

    async fn download_with_status(
        &self,
        source: &MediaSource,
        task: &MediaTask,
        cancellation: &CancellationToken,
    ) -> Result<DownloadedMedia> {
        let (progress_sender, mut progress_receiver) = mpsc::unbounded_channel();
        let download = self
            .downloader
            .download_with_progress(source, move |progress| {
                let _ = progress_sender.send(progress);
            });
        tokio::pin!(download);

        let mut last_percent = 0_u8;
        loop {
            tokio::select! {
                biased;
                _ = cancellation.cancelled() => return Err(AppError::Cancelled),
                Some(progress) = progress_receiver.recv() => {
                    if task.preferences.show_progress && progress.percent > last_percent {
                        last_percent = progress.percent;
                        self.update_download_status(task, progress.percent, cancellation).await;
                    }
                }
                result = &mut download => {
                    let downloaded = result?;
                    if task.preferences.show_progress && last_percent < 100 {
                        self.update_download_status(task, 100, cancellation).await;
                    }
                    return Ok(downloaded);
                }
            }
        }
    }

    async fn update_download_status(
        &self,
        task: &MediaTask,
        percent: u8,
        cancellation: &CancellationToken,
    ) {
        let text = i18n::download_status(task.preferences.language, percent);
        self.update_status_html(task, &text, cancellation).await;
    }

    async fn update_status_html(
        &self,
        task: &MediaTask,
        text: &str,
        cancellation: &CancellationToken,
    ) {
        tokio::select! {
            _ = cancellation.cancelled() => {}
            result = self.telegram.edit_message_text_html(
                task.chat_id,
                task.status_message_id,
                text,
                None,
            ) => {
                if let Err(error) = result {
                    warn!(error = %error, "状态消息更新失败，媒体任务继续执行");
                }
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn edit_media(
        &self,
        chat_id: i64,
        message_id: i64,
        kind: TelegramMediaKind,
        input: &InputFile,
        caption: &str,
        video_metadata: Option<VideoMetadata>,
        cover: Option<&InputFile>,
    ) -> std::result::Result<Message, TelegramError> {
        match kind {
            TelegramMediaKind::Video => match video_metadata {
                Some(metadata) => {
                    self.telegram
                        .edit_message_video_html_with_metadata_and_cover(
                            chat_id,
                            message_id,
                            input,
                            Some(caption),
                            metadata,
                            cover,
                            None,
                        )
                        .await
                }
                None => {
                    self.telegram
                        .edit_message_video_html_with_cover(
                            chat_id,
                            message_id,
                            input,
                            Some(caption),
                            cover,
                            None,
                        )
                        .await
                }
            },
            TelegramMediaKind::Document => {
                self.telegram
                    .edit_message_document_html(chat_id, message_id, input, Some(caption), None)
                    .await
            }
        }
    }

    async fn ensure_required_channel(
        &self,
        chat_id: i64,
        user_id: u64,
        source_message_id: i64,
        cancellation: &CancellationToken,
        preferences: UserPreferences,
    ) -> Result<bool> {
        let Some(channel_id) = self.required_channel_id.as_deref() else {
            return Ok(true);
        };

        let membership = tokio::select! {
            _ = cancellation.cancelled() => return Err(AppError::Cancelled),
            result = self.telegram.get_chat_member(channel_id, user_id) => result,
        };
        match membership {
            Ok(member) if member.has_joined() => Ok(true),
            Ok(_) => {
                let prompt = format_channel_requirement(preferences.language, channel_id);
                tokio::select! {
                    _ = cancellation.cancelled() => return Err(AppError::Cancelled),
                    result = self.send_html_with_policy(
                        chat_id,
                        &prompt,
                        source_message_id,
                        preferences,
                        None,
                    ) => {
                        result?;
                    }
                }
                Ok(false)
            }
            Err(error) => {
                warn!(
                    error = %error,
                    channel_id,
                    user_id,
                    "频道成员状态验证失败"
                );
                tokio::select! {
                    _ = cancellation.cancelled() => return Err(AppError::Cancelled),
                    result = self.send_text_with_policy(
                        chat_id,
                        i18n::channel_check_failed(preferences.language),
                        source_message_id,
                        preferences,
                    ) => {
                        result?;
                    }
                }
                Ok(false)
            }
        }
    }

    async fn shutdown_background_tasks(&self, shutdown: &CancellationToken) {
        shutdown.cancel();
        let waiting_tasks = {
            let mut state = self.scheduler.lock().await;
            let task_ids = state.scheduler.shutdown();
            state.take_dispatched(task_ids)
        };
        for task in waiting_tasks {
            task.cancellation.cancel();
        }
        self.background_tasks.close();

        if tokio::time::timeout(SHUTDOWN_GRACE, self.background_tasks.wait())
            .await
            .is_ok()
        {
            info!("所有后台视频任务已完成清理");
            return;
        }

        warn!(
            grace_seconds = SHUTDOWN_GRACE.as_secs(),
            "后台视频任务未在宽限期内退出，正在强制终止"
        );
        let abort_handles = self
            .task_abort_handles
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        for handle in abort_handles {
            if !handle.is_finished() {
                handle.abort();
            }
        }
        if tokio::time::timeout(FORCED_SHUTDOWN_GRACE, self.background_tasks.wait())
            .await
            .is_err()
        {
            warn!("部分后台任务在强制终止后仍未完成清理");
        }
    }
}

fn command_name(text: &str) -> Option<&str> {
    text.split_whitespace()
        .next()
        .and_then(|value| value.strip_prefix('/'))
        .map(|value| value.split('@').next().unwrap_or(value))
}

fn telegram_media_kind(codec: VideoCodec) -> TelegramMediaKind {
    match codec {
        VideoCodec::H264 | VideoCodec::H265 => TelegramMediaKind::Video,
        VideoCodec::Unknown => TelegramMediaKind::Document,
    }
}

fn telegram_video_metadata(probe: &MediaProbe) -> VideoMetadata {
    let duration = probe.duration_seconds.and_then(|seconds| {
        let rounded = seconds.ceil();
        (rounded.is_finite() && rounded >= 0.0 && rounded <= f64::from(u32::MAX))
            .then_some(rounded as u32)
    });
    VideoMetadata::new(probe.width, probe.height, duration)
}

fn format_caption(post: &ResolvedPost, language: Language, show_source: bool) -> String {
    format_caption_parts(
        &post.display_title(),
        post.canonical_url.as_str(),
        language,
        show_source,
    )
}

fn format_caption_parts(
    title: &str,
    source: &str,
    language: Language,
    show_source: bool,
) -> String {
    let title = escape_html(title);
    if !show_source {
        return title;
    }
    let source = escape_html(source);
    format!(
        "{title}\n\n<b>▎<a href=\"{source}\">{}</a></b>",
        i18n::source_label(language)
    )
}

fn format_channel_requirement(language: Language, channel_id: &str) -> String {
    let username = channel_id.trim_start_matches('@');
    i18n::channel_requirement(language, channel_id, &format!("https://t.me/{username}"))
}

#[cfg(test)]
fn format_download_status(percent: u8) -> String {
    i18n::download_status(Language::Chinese, percent)
}

#[cfg(test)]
fn format_queue_status(waiting_count: usize) -> String {
    i18n::queue(Language::Chinese, waiting_count)
}

fn settings_panel(
    language: Language,
    preferences: UserPreferences,
    user_id: u64,
) -> (String, InlineKeyboardMarkup) {
    // Keep the message itself compact.  The current values are already shown
    // on the inline buttons below; repeating them in the message creates a
    // large highlighted block in Telegram clients.
    let text = format!("<b>{}</b>", i18n::settings_panel_title(language));
    let keyboard = InlineKeyboardMarkup::new(vec![
        vec![InlineKeyboardButton::callback(
            i18n::language_setting_label(preferences.language),
            format!("setting|{user_id}|language"),
        )],
        vec![
            InlineKeyboardButton::callback(
                i18n::source_setting_label(language, preferences.show_source),
                format!("setting|{user_id}|source"),
            ),
            InlineKeyboardButton::callback(
                i18n::progress_setting_label(language, preferences.show_progress),
                format!("setting|{user_id}|progress"),
            ),
        ],
        vec![
            InlineKeyboardButton::callback(
                i18n::reply_setting_label(language, preferences.reply_to_source),
                format!("setting|{user_id}|reply"),
            ),
            InlineKeyboardButton::callback(
                i18n::cover_setting_label(language, preferences.show_video_cover),
                format!("setting|{user_id}|cover"),
            ),
        ],
        vec![InlineKeyboardButton::callback(
            i18n::done(language),
            format!("setting|{user_id}|done"),
        )],
    ]);
    (text, keyboard)
}

fn language_keyboard(user_id: u64, current_language: Language) -> InlineKeyboardMarkup {
    let buttons = Language::ALL
        .into_iter()
        .map(|language| {
            let marker = if language == current_language {
                "✓ "
            } else {
                ""
            };
            InlineKeyboardButton::callback(
                format!("{marker}{}", language.label()),
                format!("setting|{user_id}|lang|{}", language.code()),
            )
        })
        .collect();
    InlineKeyboardMarkup::new(vec![
        buttons,
        vec![
            InlineKeyboardButton::callback(
                i18n::back(current_language),
                format!("setting|{user_id}|main"),
            ),
            InlineKeyboardButton::callback(
                i18n::done(current_language),
                format!("setting|{user_id}|done"),
            ),
        ],
    ])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recognizes_only_start_as_the_supported_command() {
        assert_eq!(command_name("/start"), Some("start"));
        assert_eq!(command_name("/start@parse_bot"), Some("start"));
        assert_eq!(command_name("/setting"), Some("setting"));
        assert_ne!(command_name("/unknown"), Some("start"));
        assert_eq!(command_name("https://weixin.qq.com/sph/example"), None);
    }

    #[test]
    fn sanitizes_sensitive_inbound_log_text_into_one_line() {
        let bot_token = "123456789:abcdefghijklmnopqrstuvwxyz_1234567890";
        let opaque_id = "A".repeat(80);
        let preview = sanitize_inbound_log_preview(&format!(
            "第一\u{202e}行\n第二行 https://alice:secret@example.test/watch?ticket=private#fragment \
             TELEGRAM_BOT_TOKEN='{bot_token}' id={opaque_id}"
        ));

        assert!(preview.starts_with("第一行 第二行 "));
        assert!(preview.contains("https://example.test/watch?<redacted>"));
        assert!(preview.contains("TELEGRAM_BOT_TOKEN=<redacted>"));
        assert!(preview.contains("id=<redacted-id>"));
        assert!(!preview.contains("alice"));
        assert!(!preview.contains("secret"));
        assert!(!preview.contains("ticket"));
        assert!(!preview.contains(bot_token));
        assert!(!preview.contains(&opaque_id));
        assert!(!preview.chars().any(char::is_control));
        assert!(!preview.contains('\u{202e}'));
    }

    #[test]
    fn redacts_token_bearer_and_whole_line_credentials() {
        let preview = sanitize_inbound_log_preview(
            "ACCESS_TOKEN=private-token Bearer private-bearer\n\
             Cookie=session=private; user=42 still-private\n\
             password: correct horse battery staple\nvisible",
        );

        assert_eq!(
            preview,
            "ACCESS_TOKEN=<redacted> Bearer <redacted> Cookie=<redacted> password:<redacted> visible"
        );
        assert!(!preview.contains("private"));
        assert!(!preview.contains("session"));
        assert!(!preview.contains("correct horse"));
    }

    #[test]
    fn redacts_queries_after_bot_tokens_inside_urls() {
        let bot_token = "123456789:abcdefghijklmnopqrstuvwxyz_1234567890";
        let preview = sanitize_inbound_log_preview(&format!(
            "https://api.telegram.org/bot{bot_token}/sendMessage?chat_id=private#fragment"
        ));

        assert!(preview.contains("<redacted-bot-token>"));
        assert!(preview.ends_with("?<redacted>"));
        assert!(!preview.contains(bot_token));
        assert!(!preview.contains("chat_id"));
        assert!(!preview.contains("private"));
    }

    #[test]
    fn truncates_inbound_log_text_on_unicode_character_boundaries() {
        let preview = sanitize_inbound_log_preview(&"界".repeat(300));

        assert_eq!(preview.chars().count(), INBOUND_LOG_PREVIEW_CHARS);
        assert!(preview.ends_with('…'));
        assert_eq!(
            preview
                .chars()
                .filter(|character| *character == '界')
                .count(),
            239
        );
    }

    #[test]
    fn exposes_only_allowlisted_callback_actions() {
        assert_eq!(safe_callback_action("setting|42|source"), Some("source"));
        assert_eq!(safe_callback_action("setting|42|lang|en"), Some("lang"));
        assert_eq!(safe_callback_action("setting|42|lang|invalid"), None);
        assert_eq!(safe_callback_action("setting|42|source|private"), None);
        assert_eq!(safe_callback_action("secret|42|token"), None);
    }

    #[test]
    fn tries_h264_and_h265_as_previewable_videos() {
        assert_eq!(
            telegram_media_kind(VideoCodec::H264),
            TelegramMediaKind::Video
        );
        assert_eq!(
            telegram_media_kind(VideoCodec::H265),
            TelegramMediaKind::Video
        );
        assert_eq!(
            telegram_media_kind(VideoCodec::Unknown),
            TelegramMediaKind::Document
        );
    }

    #[test]
    fn uses_visible_dimensions_and_rounded_duration_for_telegram() {
        let probe = MediaProbe {
            codec: VideoCodec::H265,
            has_audio: true,
            width: 1080,
            height: 1920,
            duration_seconds: Some(14.745),
        };
        assert_eq!(
            telegram_video_metadata(&probe),
            VideoMetadata::new(1080, 1920, Some(15))
        );
    }

    #[test]
    fn formats_a_safe_title_and_source_link() {
        assert_eq!(
            format_caption_parts(
                "标题 <视频> & 测试",
                "https://weixin.qq.com/sph/example?a=1&b=2",
                Language::Chinese,
                true,
            ),
            "标题 &lt;视频&gt; &amp; 测试\n\n<b>▎<a href=\"https://weixin.qq.com/sph/example?a=1&amp;b=2\">来源</a></b>"
        );
    }

    #[test]
    fn formats_download_progress_in_fifths() {
        assert_eq!(format_download_status(20), "<b>▎下 载 中... | 20%</b>");
        assert_eq!(format_download_status(40), "<b>▎下 载 中... | 40%</b>");
        assert_eq!(format_download_status(60), "<b>▎下 载 中... | 60%</b>");
        assert_eq!(format_download_status(80), "<b>▎下 载 中... | 80%</b>");
        assert_eq!(format_download_status(100), "<b>▎下 载 中... | 100%</b>");
    }

    #[test]
    fn limits_active_workflows_to_ten() {
        assert_eq!(MEDIA_PIPELINE_CONCURRENCY, MAX_ACTIVE_TASKS);
        let mut scheduler = FairTaskScheduler::new(SchedulerLimits::default());
        for user_id in 1..=MAX_ACTIVE_TASKS as u64 {
            assert!(matches!(
                scheduler.admit(user_id),
                Admission::Started { .. }
            ));
        }
        assert_eq!(scheduler.running.len(), MAX_ACTIVE_TASKS);

        let Admission::Queued {
            task_id,
            waiting_count,
        } = scheduler.admit(11)
        else {
            panic!("the eleventh workflow must wait");
        };
        assert_eq!(waiting_count, 1);
        assert!(scheduler.mark_ready(task_id).is_empty());
        assert_eq!(scheduler.running.len(), MAX_ACTIVE_TASKS);
    }

    #[test]
    fn runs_only_one_task_per_user_and_ignores_stale_completions() {
        let mut scheduler = test_scheduler(2);
        let first = started_task_id(scheduler.admit(7));
        let second = queued_task_id(scheduler.admit(7));
        assert!(scheduler.mark_ready(second).is_empty());
        assert!(matches!(scheduler.admit(8), Admission::Started { .. }));

        assert_eq!(scheduler.complete(first, 7), vec![second]);
        assert!(scheduler.complete(first, 7).is_empty());
        assert_eq!(scheduler.running.get(&7), Some(&second));

        let third = queued_task_id(scheduler.admit(7));
        assert!(scheduler.mark_ready(third).is_empty());
        assert_eq!(scheduler.complete(second, 7), vec![third]);
    }

    #[test]
    fn rotates_fairly_between_users() {
        let mut scheduler = test_scheduler(1);
        let first_a = started_task_id(scheduler.admit(1));
        let second_a = queued_task_id(scheduler.admit(1));
        let third_a = queued_task_id(scheduler.admit(1));
        let first_b = queued_task_id(scheduler.admit(2));
        assert!(scheduler.mark_ready(second_a).is_empty());
        assert!(scheduler.mark_ready(third_a).is_empty());
        assert!(scheduler.mark_ready(first_b).is_empty());

        assert_eq!(scheduler.complete(first_a, 1), vec![first_b]);
        assert_eq!(scheduler.complete(first_b, 2), vec![second_a]);
        assert_eq!(scheduler.complete(second_a, 1), vec![third_a]);
    }

    #[test]
    fn limits_each_user_to_three_waiting_tasks() {
        let mut scheduler = test_scheduler(1);
        started_task_id(scheduler.admit(1));
        for _ in 0..MAX_WAITING_TASKS_PER_USER {
            assert!(matches!(scheduler.admit(1), Admission::Queued { .. }));
        }
        assert_eq!(
            scheduler.admit(1),
            Admission::Rejected(QueueRejection::UserQueueFull)
        );
        assert_eq!(scheduler.waiting_count, MAX_WAITING_TASKS_PER_USER);
    }

    #[test]
    fn limits_the_global_waiting_queue_to_one_hundred() {
        let mut scheduler = test_scheduler(1);
        started_task_id(scheduler.admit(1));
        for user_id in 2..=(MAX_WAITING_TASKS as u64 + 1) {
            assert!(matches!(scheduler.admit(user_id), Admission::Queued { .. }));
        }
        assert_eq!(scheduler.waiting_count, MAX_WAITING_TASKS);
        assert_eq!(
            scheduler.admit(MAX_WAITING_TASKS as u64 + 2),
            Admission::Rejected(QueueRejection::GlobalQueueFull)
        );
    }

    #[test]
    fn dispatches_ready_tasks_when_active_workflows_finish() {
        let mut scheduler = test_scheduler(2);
        let first = started_task_id(scheduler.admit(1));
        let second = started_task_id(scheduler.admit(2));
        let third = queued_task_id(scheduler.admit(3));
        let fourth = queued_task_id(scheduler.admit(4));
        assert!(scheduler.mark_ready(third).is_empty());
        assert!(scheduler.mark_ready(fourth).is_empty());

        assert_eq!(scheduler.complete(first, 1), vec![third]);
        assert_eq!(scheduler.complete(second, 2), vec![fourth]);
        assert_eq!(scheduler.running.len(), 2);
    }

    #[test]
    fn cancelling_an_unready_reservation_unblocks_the_queue() {
        let mut scheduler = test_scheduler(1);
        let running = started_task_id(scheduler.admit(1));
        let unready = queued_task_id(scheduler.admit(2));
        let ready = queued_task_id(scheduler.admit(3));
        assert!(scheduler.mark_ready(ready).is_empty());

        assert!(scheduler.cancel_waiting(unready).is_empty());
        assert_eq!(scheduler.complete(running, 1), vec![ready]);
    }

    #[test]
    fn shutdown_rejects_new_work_and_drains_waiting_tasks() {
        let mut scheduler = test_scheduler(2);
        let running = started_task_id(scheduler.admit(1));
        started_task_id(scheduler.admit(2));
        let third = queued_task_id(scheduler.admit(3));
        let fourth = queued_task_id(scheduler.admit(4));
        assert!(scheduler.mark_ready(third).is_empty());
        assert!(scheduler.mark_ready(fourth).is_empty());

        let mut drained = scheduler.shutdown();
        drained.sort_unstable();
        let mut expected = vec![third, fourth];
        expected.sort_unstable();
        assert_eq!(drained, expected);
        assert_eq!(scheduler.waiting_count, 0);
        assert!(scheduler.waiting_by_user.is_empty());
        assert!(scheduler.round_robin.is_empty());
        assert_eq!(
            scheduler.admit(5),
            Admission::Rejected(QueueRejection::ShuttingDown)
        );
        assert!(scheduler.complete(running, 1).is_empty());
    }

    #[test]
    fn formats_a_factual_waiting_queue_status() {
        assert_eq!(
            format_queue_status(12),
            "<b>▎排 队 中...</b>\n\n当前共有 12 个任务等待调度，轮到后会自动解析。"
        );
    }

    #[test]
    fn mentions_the_required_channel_only_when_configured() {
        let public_help = i18n::start_text(Language::Chinese, None);
        assert!(!public_help.contains("关注频道"));
        assert!(!public_help.contains("2000 MB"));
        assert!(!public_help.contains("telegram-bot-api"));

        let gated_help = i18n::start_text(Language::Chinese, Some("@Aaron_Channels"));
        assert!(gated_help.contains("使用前需要关注频道 @Aaron_Channels。"));
    }

    #[test]
    fn formats_a_clickable_channel_requirement() {
        assert_eq!(
            format_channel_requirement(Language::Chinese, "@Aaron_Channels"),
            "使用此机器人前，请先关注频道。\n\n<b>▎<a href=\"https://t.me/Aaron_Channels\">@Aaron_Channels</a></b>\n\n关注后重新发送链接即可。"
        );
    }

    fn test_scheduler(max_active: usize) -> FairTaskScheduler {
        FairTaskScheduler::new(SchedulerLimits {
            max_active,
            max_waiting_per_user: MAX_WAITING_TASKS_PER_USER,
            max_waiting: MAX_WAITING_TASKS,
        })
    }

    fn started_task_id(admission: Admission) -> u64 {
        let Admission::Started { task_id } = admission else {
            panic!("task must start immediately");
        };
        task_id
    }

    fn queued_task_id(admission: Admission) -> u64 {
        let Admission::Queued { task_id, .. } = admission else {
            panic!("task must be queued");
        };
        task_id
    }
}
