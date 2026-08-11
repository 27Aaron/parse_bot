use std::{
    collections::{HashMap, VecDeque},
    fmt,
    future::Future,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use parking_lot::Mutex as ParkingMutex;
use tdlib_rs::{
    enums::{
        AuthorizationState, BotCommandScope, ButtonStyle, CallbackQueryPayload,
        ChatMemberStatus as TdChatMemberStatus, ChatType, ConnectionState,
        FormattedText as TdFormattedText, InlineKeyboardButtonType, InputFile as TdInputFile,
        InputMessageContent, InputMessageReplyTo, Message as TdMessage, MessageContent,
        MessageSender, ReplyMarkup, TextParseMode, Update as TdUpdate, User as TdUser, UserType,
    },
    functions,
    types::{
        BotCommand as TdBotCommand, FormattedText, InlineKeyboardButton as TdInlineKeyboardButton,
        InlineKeyboardButtonTypeCallback, InlineKeyboardButtonTypeUrl, InputFileLocal,
        InputFileRemote, InputMessageDocument, InputMessageReplyToMessage, InputMessageText,
        InputMessageVideo, ReplyMarkupInlineKeyboard,
    },
};
use tokio::{
    io::AsyncReadExt,
    sync::{Mutex, Notify, mpsc, oneshot},
    task::JoinHandle,
};
use tokio_util::task::TaskTracker;
use tracing::{info, warn};
use uuid::Uuid;

use parse_core::media::{DownloadedMedia, MediaDownloader};

use super::api::{
    BotCommand, CallbackQuery, Chat, ChatKind, ChatMember, ChatMemberStatus, Document,
    InlineKeyboardMarkup, InputFile, InputFileSource, Message, TelegramError, TelegramResult,
    Update, User, Video, VideoMetadata,
};

const AUTHORIZATION_TIMEOUT: Duration = Duration::from_secs(60);
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(15);
const TD_CALL_TIMEOUT: Duration = Duration::from_secs(60);
const MEDIA_EDIT_TIMEOUT: Duration = Duration::from_secs(2 * 60 * 60);
const SEND_COMPLETION_TIMEOUT: Duration = Duration::from_secs(2 * 60);
const COVER_MAX_BYTES: u64 = 20 * 1024 * 1024;
const COVER_DOWNLOAD_TIMEOUT: Duration = Duration::from_secs(30);
const UPDATE_CHANNEL_CAPACITY: usize = 1024;
static RAW_DROPPED_UPDATES: AtomicU64 = AtomicU64::new(0);

// tdlib-rs exposes one process-wide `receive` entry point. Starting two pumps
// would let one client consume the other's responses and deadlock both.
static CLIENT_ACTIVE: AtomicBool = AtomicBool::new(false);

#[derive(Clone)]
pub struct TdlibConfig {
    pub api_id: i32,
    pub api_hash: String,
    pub bot_token: String,
    pub database_directory: PathBuf,
    pub files_directory: PathBuf,
    pub cover_downloader: MediaDownloader,
}

impl fmt::Debug for TdlibConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TdlibConfig")
            .field("api_id", &self.api_id)
            .field("api_hash", &"<redacted>")
            .field("bot_token", &"<redacted>")
            .field("database_directory", &"<redacted-path>")
            .field("files_directory", &"<redacted-path>")
            .finish_non_exhaustive()
    }
}

#[derive(Clone)]
pub struct TelegramClient {
    inner: Arc<Inner>,
}

impl fmt::Debug for TelegramClient {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TelegramClient")
            .field("backend", &"tdlib")
            .field("credentials", &"<redacted>")
            .finish_non_exhaustive()
    }
}

struct Inner {
    client_id: i32,
    updates: Mutex<mpsc::Receiver<RawEvent>>,
    send_tracker: Arc<SendTracker>,
    pump_running: Arc<AtomicBool>,
    pump: Mutex<Option<JoinHandle<()>>>,
    close_lock: Mutex<()>,
    closing: Arc<AtomicBool>,
    closed: Arc<AtomicBool>,
    closed_notify: Arc<Notify>,
    terminal: Arc<TerminalState>,
    cover_downloader: MediaDownloader,
    detached_requests: TaskTracker,
}

enum RawEvent {
    Update(Box<TdUpdate>),
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum TerminalReason {
    Closed,
    AuthorizationLost,
    UpdateOverflow,
    UpdateReceiverClosed,
    ReceivePumpFailed,
    RequestTimedOut,
}

impl TerminalReason {
    const fn priority(self) -> u8 {
        match self {
            Self::Closed => 0,
            Self::AuthorizationLost => 1,
            Self::UpdateOverflow | Self::UpdateReceiverClosed => 2,
            Self::ReceivePumpFailed | Self::RequestTimedOut => 3,
        }
    }

    fn error(self) -> TelegramError {
        match self {
            Self::Closed => TelegramError::Closed,
            Self::AuthorizationLost => TelegramError::runtime(
                "receive",
                "TDLib authorization state changed after becoming ready",
            ),
            Self::UpdateOverflow => {
                TelegramError::runtime("receive", "TDLib application update queue overflowed")
            }
            Self::UpdateReceiverClosed => TelegramError::runtime(
                "receive",
                "TDLib application update receiver closed unexpectedly",
            ),
            Self::ReceivePumpFailed => {
                TelegramError::runtime("receive", "TDLib receive task stopped unexpectedly")
            }
            Self::RequestTimedOut => {
                TelegramError::runtime("receive", "TDLib media request timed out")
            }
        }
    }
}

struct TerminalState {
    reason: ParkingMutex<Option<TerminalReason>>,
    notify: Notify,
}

impl TerminalState {
    fn new() -> Self {
        Self {
            reason: ParkingMutex::new(None),
            notify: Notify::new(),
        }
    }

    fn set(&self, reason: TerminalReason) {
        let mut current = self.reason.lock();
        if current.is_none_or(|value| reason.priority() > value.priority()) {
            *current = Some(reason);
        }
        drop(current);
        // Wake already registered consumers and retain one permit for a
        // consumer racing between its terminal check and registration.
        self.notify.notify_waiters();
        self.notify.notify_one();
    }

    fn error(&self) -> Option<TelegramError> {
        (*self.reason.lock()).map(TerminalReason::error)
    }
}

enum SendOutcome {
    Succeeded(Box<tdlib_rs::types::Message>),
    Failed {
        error: tdlib_rs::types::Error,
        need_drop_reply: bool,
        failed_message: Option<Box<tdlib_rs::types::Message>>,
    },
}

#[derive(Debug)]
struct SendFailure {
    error: TelegramError,
    need_drop_reply: bool,
    failed_message: Option<Box<tdlib_rs::types::Message>>,
}

#[derive(Default)]
struct SendTracker {
    state: ParkingMutex<SendTrackerState>,
}

#[derive(Default)]
struct SendTrackerState {
    waiters: HashMap<SendKey, SendWaiter>,
    early_outcomes: HashMap<SendKey, SendOutcome>,
    early_outcome_order: VecDeque<SendKey>,
    next_registration: u64,
}

type SendKey = (i64, i64);

struct SendWaiter {
    registration: u64,
    sender: oneshot::Sender<Result<tdlib_rs::types::Message, SendFailure>>,
}

impl SendTracker {
    fn complete(&self, key: SendKey, outcome: SendOutcome) {
        let mut state = self.state.lock();
        if let Some(waiter) = state.waiters.remove(&key) {
            let _ = waiter.sender.send(outcome_to_result(outcome));
        } else {
            // A completion can beat registration because the receive pump and
            // function observer are independent. Bound abandoned outcomes so
            // cancellation cannot grow this map forever.
            if !state.early_outcomes.contains_key(&key) {
                while state.early_outcomes.len() >= 1024 {
                    let Some(oldest) = state.early_outcome_order.pop_front() else {
                        break;
                    };
                    state.early_outcomes.remove(&oldest);
                }
                state.early_outcome_order.push_back(key);
            }
            state.early_outcomes.insert(key, outcome);
        }
    }

    fn fail_deleted(&self, chat_id: i64, message_ids: Vec<i64>) {
        for message_id in message_ids {
            let key = (chat_id, message_id);
            // TDLib's local yet-unsent IDs are positive and carry the
            // TYPE_YET_UNSENT tag in their two least-significant bits.
            let should_record =
                (message_id & 3) == 1 || self.state.lock().waiters.contains_key(&key);
            if should_record {
                self.complete(
                    key,
                    SendOutcome::Failed {
                        error: tdlib_rs::types::Error {
                            code: 500,
                            message: "pending message was deleted before send completion"
                                .to_owned(),
                        },
                        need_drop_reply: false,
                        failed_message: None,
                    },
                );
            }
        }
    }

    async fn wait(&self, key: SendKey) -> Result<tdlib_rs::types::Message, SendFailure> {
        let (receiver, registration) = {
            let mut state = self.state.lock();
            if let Some(outcome) = state.early_outcomes.remove(&key) {
                state
                    .early_outcome_order
                    .retain(|candidate| *candidate != key);
                return outcome_to_result(outcome);
            }
            if state.waiters.contains_key(&key) {
                return Err(SendFailure {
                    error: TelegramError::runtime(
                        "sendMessage",
                        "duplicate pending TDLib message identifier",
                    ),
                    need_drop_reply: false,
                    failed_message: None,
                });
            }
            let (sender, receiver) = oneshot::channel();
            let registration = state.next_registration;
            state.next_registration = state.next_registration.wrapping_add(1);
            state.waiters.insert(
                key,
                SendWaiter {
                    registration,
                    sender,
                },
            );
            (receiver, registration)
        };
        let _registration = SendWaiterRegistration {
            tracker: self,
            key,
            registration,
        };

        match tokio::time::timeout(SEND_COMPLETION_TIMEOUT, receiver).await {
            Ok(result) => result.unwrap_or(Err(SendFailure {
                error: TelegramError::Closed,
                need_drop_reply: false,
                failed_message: None,
            })),
            Err(_) => Err(SendFailure {
                error: TelegramError::runtime("sendMessage", "TDLib send completion timed out"),
                need_drop_reply: false,
                failed_message: None,
            }),
        }
    }

    fn fail_all(&self) {
        let mut state = self.state.lock();
        state.early_outcomes.clear();
        state.early_outcome_order.clear();
        for (_, waiter) in state.waiters.drain() {
            let _ = waiter.sender.send(Err(SendFailure {
                error: TelegramError::Closed,
                need_drop_reply: false,
                failed_message: None,
            }));
        }
    }
}

struct SendWaiterRegistration<'a> {
    tracker: &'a SendTracker,
    key: SendKey,
    registration: u64,
}

impl Drop for SendWaiterRegistration<'_> {
    fn drop(&mut self) {
        let mut state = self.tracker.state.lock();
        if state
            .waiters
            .get(&self.key)
            .is_some_and(|waiter| waiter.registration == self.registration)
        {
            state.waiters.remove(&self.key);
        }
    }
}

fn outcome_to_result(outcome: SendOutcome) -> Result<tdlib_rs::types::Message, SendFailure> {
    match outcome {
        SendOutcome::Succeeded(message) => Ok(*message),
        SendOutcome::Failed {
            error,
            need_drop_reply,
            failed_message,
        } => Err(SendFailure {
            error: TelegramError::from_tdlib("sendMessage", error),
            need_drop_reply,
            failed_message,
        }),
    }
}

fn send_failure(error: TelegramError) -> SendFailure {
    SendFailure {
        error,
        need_drop_reply: false,
        failed_message: None,
    }
}

impl TelegramClient {
    pub async fn connect(config: TdlibConfig) -> TelegramResult<Self> {
        validate_config(&config)?;
        if CLIENT_ACTIVE
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err(TelegramError::Configuration {
                reason: "only one TDLib client can run in a process",
            });
        }

        let mut active_guard = ActiveGuard {
            armed: true,
            official_closed: None,
        };
        let cover_downloader = config
            .cover_downloader
            .capped_with_timeout(COVER_MAX_BYTES, COVER_DOWNLOAD_TIMEOUT)
            .map_err(|error| {
                TelegramError::runtime("initialize cover downloader", error.to_string())
            })?;
        let client_id = tdlib_rs::create_client();
        let (updates_tx, updates_rx) = mpsc::channel(UPDATE_CHANNEL_CAPACITY);
        let send_tracker = Arc::new(SendTracker::default());
        let pump_running = Arc::new(AtomicBool::new(true));
        let closing = Arc::new(AtomicBool::new(false));
        let closed = Arc::new(AtomicBool::new(false));
        active_guard.official_closed = Some(Arc::clone(&closed));
        let closed_notify = Arc::new(Notify::new());
        let terminal = Arc::new(TerminalState::new());
        let authorized = Arc::new(AtomicBool::new(false));
        let pump = spawn_receive_pump(ReceivePump {
            client_id,
            running: Arc::clone(&pump_running),
            updates: updates_tx,
            send_tracker: Arc::clone(&send_tracker),
            closing: Arc::clone(&closing),
            closed: Arc::clone(&closed),
            closed_notify: Arc::clone(&closed_notify),
            terminal: Arc::clone(&terminal),
            authorized,
        });

        let client = Self {
            inner: Arc::new(Inner {
                client_id,
                updates: Mutex::new(updates_rx),
                send_tracker,
                pump_running,
                pump: Mutex::new(Some(pump)),
                close_lock: Mutex::new(()),
                closing,
                closed,
                closed_notify,
                terminal,
                cover_downloader,
                detached_requests: TaskTracker::new(),
            }),
        };

        let authorization = async {
            client
                .td_call(
                    "setLogVerbosityLevel",
                    functions::set_log_verbosity_level(1, client_id),
                )
                .await?;
            client.authorize(&config).await
        };
        if let Err(error) = tokio::time::timeout(AUTHORIZATION_TIMEOUT, authorization)
            .await
            .map_err(|_| TelegramError::runtime("authorize", "authorization timed out"))
            .and_then(|result| result)
        {
            client.abort_connect().await;
            return Err(error);
        }

        active_guard.armed = false;
        Ok(client)
    }

    async fn authorize(&self, config: &TdlibConfig) -> TelegramResult<()> {
        loop {
            let event = self.recv_raw().await?;
            let RawEvent::Update(update) = event;
            let TdUpdate::AuthorizationState(update) = *update else {
                continue;
            };
            match update.authorization_state {
                AuthorizationState::WaitTdlibParameters => {
                    self.td_call(
                        "setTdlibParameters",
                        functions::set_tdlib_parameters(
                            false,
                            path_to_string(&config.database_directory)?,
                            path_to_string(&config.files_directory)?,
                            String::new(),
                            true,
                            true,
                            false,
                            false,
                            config.api_id,
                            config.api_hash.clone(),
                            "zh-Hans".to_owned(),
                            "parse_bot".to_owned(),
                            std::env::consts::OS.to_owned(),
                            env!("CARGO_PKG_VERSION").to_owned(),
                            self.inner.client_id,
                        ),
                    )
                    .await?;
                }
                AuthorizationState::WaitPhoneNumber => {
                    self.td_call(
                        "checkAuthenticationBotToken",
                        functions::check_authentication_bot_token(
                            config.bot_token.clone(),
                            self.inner.client_id,
                        ),
                    )
                    .await?;
                }
                AuthorizationState::Ready => {
                    self.verify_authenticated_bot(&config.bot_token).await?;
                    return Ok(());
                }
                AuthorizationState::Closing | AuthorizationState::Closed => {
                    return Err(TelegramError::Closed);
                }
                _ => {
                    return Err(TelegramError::runtime(
                        "authorize",
                        "TDLib entered an unsupported bot authorization state",
                    ));
                }
            }
        }
    }

    async fn verify_authenticated_bot(&self, bot_token: &str) -> TelegramResult<()> {
        let expected_id = bot_token
            .split_once(':')
            .and_then(|(id, _)| id.parse::<i64>().ok())
            .filter(|id| *id > 0)
            .ok_or(TelegramError::Configuration {
                reason: "bot token has an invalid format",
            })?;
        let TdUser::User(user) = self
            .td_call("getMe", functions::get_me(self.inner.client_id))
            .await?;
        if !matches!(user.r#type, UserType::Bot(_)) {
            return Err(TelegramError::Configuration {
                reason: "TDLib session is not authenticated as a bot",
            });
        }
        if user.id != expected_id {
            return Err(TelegramError::Configuration {
                reason: "TDLib data directory belongs to a different bot",
            });
        }
        Ok(())
    }

    async fn abort_connect(&self) {
        self.inner.closing.store(true, Ordering::Release);
        if !self.inner.closed.load(Ordering::Acquire) {
            let _ = tokio::time::timeout(SHUTDOWN_TIMEOUT, functions::close(self.inner.client_id))
                .await;
            let _ = tokio::time::timeout(SHUTDOWN_TIMEOUT, self.wait_for_closed()).await;
        }
        self.finish_local_shutdown().await;
    }

    pub async fn close(&self) -> TelegramResult<()> {
        let _guard = self.inner.close_lock.lock().await;
        if self.inner.closed.load(Ordering::Acquire) {
            self.finish_local_shutdown().await;
            return Ok(());
        }

        self.inner.closing.store(true, Ordering::Release);
        let result = async {
            let close_result =
                tokio::time::timeout(SHUTDOWN_TIMEOUT, functions::close(self.inner.client_id))
                    .await
                    .map_err(|_| {
                        TelegramError::runtime("close", "TDLib close request timed out")
                    })?;
            map_td("close", close_result)?;
            tokio::time::timeout(SHUTDOWN_TIMEOUT, self.wait_for_closed())
                .await
                .map_err(|_| TelegramError::runtime("close", "TDLib shutdown timed out"))??;
            Ok(())
        }
        .await;
        self.finish_local_shutdown().await;
        result
    }

    async fn finish_local_shutdown(&self) {
        self.inner.closing.store(true, Ordering::Release);
        self.inner.terminal.set(TerminalReason::Closed);
        self.inner.closed_notify.notify_one();
        self.inner.send_tracker.fail_all();
        self.stop_pump().await;
        self.inner.detached_requests.close();
        if tokio::time::timeout(SHUTDOWN_TIMEOUT, self.inner.detached_requests.wait())
            .await
            .is_err()
        {
            warn!("TDLib 已关闭，但仍有媒体请求未返回；运行时退出时将终止这些任务");
        }
        if self.inner.closed.load(Ordering::Acquire) {
            CLIENT_ACTIVE.store(false, Ordering::Release);
        }
    }

    async fn wait_for_closed(&self) -> TelegramResult<()> {
        while !self.inner.closed.load(Ordering::Acquire) {
            self.inner.closed_notify.notified().await;
        }
        Ok(())
    }

    async fn stop_pump(&self) {
        self.inner.pump_running.store(false, Ordering::Release);
        if let Some(pump) = self.inner.pump.lock().await.take() {
            let _ = pump.await;
        }
    }

    async fn recv_raw(&self) -> TelegramResult<RawEvent> {
        let notified = self.inner.terminal.notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        if let Some(error) = self.inner.terminal.error() {
            return Err(error);
        }

        let mut updates = self.inner.updates.lock().await;
        let event = tokio::select! {
            biased;
            _ = &mut notified => return Err(self.terminal_error()),
            event = updates.recv() => event.ok_or(TelegramError::Closed)?,
        };
        if let Some(error) = self.inner.terminal.error() {
            return Err(error);
        }
        Ok(event)
    }

    async fn prioritize_terminal<T, F>(&self, future: F) -> TelegramResult<T>
    where
        F: Future<Output = TelegramResult<T>>,
    {
        let notified = self.inner.terminal.notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        if let Some(error) = self.inner.terminal.error() {
            return Err(error);
        }
        tokio::pin!(future);
        let result = tokio::select! {
            biased;
            _ = &mut notified => return Err(self.terminal_error()),
            result = &mut future => result,
        };
        if let Some(error) = self.inner.terminal.error() {
            return Err(error);
        }
        result
    }

    fn terminal_error(&self) -> TelegramError {
        self.inner.terminal.error().unwrap_or_else(|| {
            TelegramError::runtime("receive", "TDLib terminal notification had no reason")
        })
    }

    async fn td_call<T, F>(&self, method: &'static str, future: F) -> TelegramResult<T>
    where
        F: Future<Output = Result<T, tdlib_rs::types::Error>>,
    {
        self.prioritize_terminal(async move {
            let result = tokio::time::timeout(TD_CALL_TIMEOUT, future)
                .await
                .map_err(|_| {
                    TelegramError::runtime(
                        method,
                        format!("TDLib call timed out after {}s", TD_CALL_TIMEOUT.as_secs()),
                    )
                })?;
            map_td(method, result)
        })
        .await
    }

    pub async fn next_update(&self) -> TelegramResult<Update> {
        loop {
            match self.recv_raw().await? {
                RawEvent::Update(update) => match *update {
                    TdUpdate::NewMessage(update) if !update.message.is_outgoing => {
                        let message = self
                            .prioritize_terminal(self.normalize_message(update.message, false))
                            .await?;
                        return Ok(Update {
                            message: Some(message),
                            callback_query: None,
                        });
                    }
                    TdUpdate::NewCallbackQuery(update) => {
                        let callback_query = self
                            .prioritize_terminal(self.normalize_callback_query(update))
                            .await?;
                        return Ok(Update {
                            message: None,
                            callback_query: Some(callback_query),
                        });
                    }
                    TdUpdate::NewInlineCallbackQuery(update) => {
                        let sender = self
                            .prioritize_terminal(self.get_user_by_id(update.sender_user_id))
                            .await?;
                        let data = decode_callback_payload(update.payload)?;
                        return Ok(Update {
                            message: None,
                            callback_query: Some(CallbackQuery {
                                id: update.id.to_string(),
                                sender,
                                message: None,
                                data,
                            }),
                        });
                    }
                    TdUpdate::AuthorizationState(update)
                        if matches!(
                            update.authorization_state,
                            AuthorizationState::Closing | AuthorizationState::Closed
                        ) =>
                    {
                        return Err(TelegramError::Closed);
                    }
                    _ => {}
                },
            }
        }
    }

    pub async fn get_me(&self) -> TelegramResult<User> {
        self.ensure_open()?;
        let TdUser::User(user) = self
            .td_call("getMe", functions::get_me(self.inner.client_id))
            .await?;
        normalize_user(user)
    }

    pub async fn set_my_commands(
        &self,
        commands: &[BotCommand],
        language_code: Option<&str>,
    ) -> TelegramResult<bool> {
        self.ensure_open()?;
        let commands = commands
            .iter()
            .map(|command| TdBotCommand {
                command: command.command.clone(),
                description: command.description.clone(),
            })
            .collect();
        self.td_call(
            "setCommands",
            functions::set_commands(
                None::<BotCommandScope>,
                language_code.unwrap_or_default().to_owned(),
                commands,
                self.inner.client_id,
            ),
        )
        .await?;
        Ok(true)
    }

    pub async fn get_chat_member(&self, chat_id: &str, user_id: u64) -> TelegramResult<ChatMember> {
        self.ensure_open()?;
        let chat_id = self.resolve_chat_id(chat_id).await?;
        let user_id = i64::try_from(user_id).map_err(|_| TelegramError::Configuration {
            reason: "Telegram user identifier is out of range",
        })?;
        let member_id = MessageSender::User(tdlib_rs::types::MessageSenderUser { user_id });
        let tdlib_rs::enums::ChatMember::ChatMember(member) = self
            .td_call(
                "getChatMember",
                functions::get_chat_member(chat_id, member_id, self.inner.client_id),
            )
            .await?;
        let (status, is_member) = match member.status {
            TdChatMemberStatus::Creator(creator) => {
                (ChatMemberStatus::Creator, Some(creator.is_member))
            }
            TdChatMemberStatus::Administrator(_) => (ChatMemberStatus::Administrator, Some(true)),
            TdChatMemberStatus::Member(_) => (ChatMemberStatus::Member, Some(true)),
            TdChatMemberStatus::Restricted(restricted) => {
                (ChatMemberStatus::Restricted, Some(restricted.is_member))
            }
            TdChatMemberStatus::Left => (ChatMemberStatus::Left, Some(false)),
            TdChatMemberStatus::Banned(_) => (ChatMemberStatus::Kicked, Some(false)),
        };
        Ok(ChatMember { status, is_member })
    }

    async fn resolve_chat_id(&self, value: &str) -> TelegramResult<i64> {
        if let Ok(chat_id) = value.parse::<i64>() {
            return Ok(chat_id);
        }
        let username = value.strip_prefix('@').unwrap_or(value);
        if username.is_empty() {
            return Err(TelegramError::Configuration {
                reason: "public chat username is empty",
            });
        }
        let tdlib_rs::enums::Chat::Chat(chat) = self
            .td_call(
                "searchPublicChat",
                functions::search_public_chat(username.to_owned(), self.inner.client_id),
            )
            .await?;
        Ok(chat.id)
    }

    pub async fn send_message(
        &self,
        chat_id: i64,
        text: &str,
        reply_markup: Option<&InlineKeyboardMarkup>,
    ) -> TelegramResult<Message> {
        self.send_message_inner(chat_id, text, false, None, reply_markup)
            .await
    }

    pub async fn send_message_html(
        &self,
        chat_id: i64,
        text: &str,
        reply_markup: Option<&InlineKeyboardMarkup>,
    ) -> TelegramResult<Message> {
        self.send_message_inner(chat_id, text, true, None, reply_markup)
            .await
    }

    pub async fn send_message_reply(
        &self,
        chat_id: i64,
        text: &str,
        reply_to_message_id: i64,
        reply_markup: Option<&InlineKeyboardMarkup>,
    ) -> TelegramResult<Message> {
        self.send_message_inner(
            chat_id,
            text,
            false,
            Some(reply_to_message_id),
            reply_markup,
        )
        .await
    }

    pub async fn send_message_reply_html(
        &self,
        chat_id: i64,
        text: &str,
        reply_to_message_id: i64,
        reply_markup: Option<&InlineKeyboardMarkup>,
    ) -> TelegramResult<Message> {
        self.send_message_inner(chat_id, text, true, Some(reply_to_message_id), reply_markup)
            .await
    }

    async fn send_message_inner(
        &self,
        chat_id: i64,
        text: &str,
        html: bool,
        reply_to_message_id: Option<i64>,
        reply_markup: Option<&InlineKeyboardMarkup>,
    ) -> TelegramResult<Message> {
        self.ensure_open()?;
        let text = self.formatted_text("sendMessage", text, html).await?;
        let content = InputMessageContent::InputMessageText(InputMessageText {
            text,
            link_preview_options: None,
            clear_draft: false,
        });
        let reply_to = reply_to_message_id.map(|message_id| {
            InputMessageReplyTo::Message(InputMessageReplyToMessage {
                message_id,
                quote: None,
                checklist_task_id: 0,
            })
        });
        let reply_markup = normalize_reply_markup(reply_markup)?;
        let message = match self
            .send_td_message(
                chat_id,
                reply_to.clone(),
                reply_markup.clone(),
                content.clone(),
            )
            .await
        {
            Ok(message) => message,
            Err(failure) if reply_to.is_some() && failure.need_drop_reply => self
                .resend_without_reply(chat_id, failure)
                .await
                .map_err(|failure| failure.error)?,
            Err(failure) => return Err(failure.error),
        };
        self.normalize_message(message, false).await
    }

    async fn send_td_message(
        &self,
        chat_id: i64,
        reply_to: Option<InputMessageReplyTo>,
        reply_markup: Option<ReplyMarkup>,
        content: InputMessageContent,
    ) -> Result<tdlib_rs::types::Message, SendFailure> {
        let result = self
            .td_call(
                "sendMessage",
                functions::send_message(
                    chat_id,
                    None,
                    reply_to,
                    None,
                    reply_markup,
                    content,
                    self.inner.client_id,
                ),
            )
            .await
            .map_err(send_failure)?;
        let TdMessage::Message(message) = result;
        self.wait_for_send(message).await
    }

    async fn resend_without_reply(
        &self,
        chat_id: i64,
        failure: SendFailure,
    ) -> Result<tdlib_rs::types::Message, SendFailure> {
        let Some(failed_message) = failure.failed_message.as_ref() else {
            return Err(failure);
        };
        let Some(tdlib_rs::enums::MessageSendingState::Failed(state)) =
            failed_message.sending_state.as_ref()
        else {
            return Err(failure);
        };
        if !state.can_retry {
            return Err(failure);
        }
        let retry_after = if state.retry_after.is_finite() && state.retry_after > 0.0 {
            Duration::from_secs_f64(state.retry_after.min(SEND_COMPLETION_TIMEOUT.as_secs_f64()))
        } else {
            Duration::ZERO
        };
        let message_id = failed_message.id;
        if !retry_after.is_zero() {
            self.prioritize_terminal(async {
                tokio::time::sleep(retry_after).await;
                Ok(())
            })
            .await
            .map_err(send_failure)?;
        }
        let tdlib_rs::enums::Messages::Messages(messages) = self
            .td_call(
                "resendMessages",
                functions::resend_messages(
                    chat_id,
                    vec![message_id],
                    None,
                    0,
                    self.inner.client_id,
                ),
            )
            .await
            .map_err(send_failure)?;
        let message = messages
            .messages
            .into_iter()
            .next()
            .flatten()
            .ok_or_else(|| {
                send_failure(TelegramError::runtime(
                    "resendMessages",
                    "TDLib did not resend the failed reply",
                ))
            })?;
        self.wait_for_send(message).await
    }

    pub async fn edit_message_text(
        &self,
        chat_id: i64,
        message_id: i64,
        text: &str,
        reply_markup: Option<&InlineKeyboardMarkup>,
    ) -> TelegramResult<Message> {
        self.edit_message_text_inner(chat_id, message_id, text, false, reply_markup)
            .await
    }

    pub async fn edit_message_text_html(
        &self,
        chat_id: i64,
        message_id: i64,
        text: &str,
        reply_markup: Option<&InlineKeyboardMarkup>,
    ) -> TelegramResult<Message> {
        self.edit_message_text_inner(chat_id, message_id, text, true, reply_markup)
            .await
    }

    async fn edit_message_text_inner(
        &self,
        chat_id: i64,
        message_id: i64,
        text: &str,
        html: bool,
        reply_markup: Option<&InlineKeyboardMarkup>,
    ) -> TelegramResult<Message> {
        self.ensure_open()?;
        let text = self.formatted_text("editMessageText", text, html).await?;
        let content = InputMessageContent::InputMessageText(InputMessageText {
            text,
            link_preview_options: None,
            clear_draft: false,
        });
        let TdMessage::Message(message) = self
            .td_call(
                "editMessageText",
                functions::edit_message_text(
                    chat_id,
                    message_id,
                    normalize_reply_markup(reply_markup)?,
                    content,
                    self.inner.client_id,
                ),
            )
            .await?;
        self.normalize_message(message, false).await
    }

    pub async fn edit_message_video_html_with_cover(
        &self,
        chat_id: i64,
        message_id: i64,
        video: &InputFile,
        caption: Option<&str>,
        cover: Option<&InputFile>,
        reply_markup: Option<&InlineKeyboardMarkup>,
    ) -> TelegramResult<Message> {
        self.edit_message_video_inner(
            chat_id,
            message_id,
            video,
            caption,
            None,
            cover,
            reply_markup,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn edit_message_video_html_with_metadata_and_cover(
        &self,
        chat_id: i64,
        message_id: i64,
        video: &InputFile,
        caption: Option<&str>,
        metadata: VideoMetadata,
        cover: Option<&InputFile>,
        reply_markup: Option<&InlineKeyboardMarkup>,
    ) -> TelegramResult<Message> {
        self.edit_message_video_inner(
            chat_id,
            message_id,
            video,
            caption,
            Some(metadata),
            cover,
            reply_markup,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn edit_message_video_inner(
        &self,
        chat_id: i64,
        message_id: i64,
        video: &InputFile,
        caption: Option<&str>,
        metadata: Option<VideoMetadata>,
        cover: Option<&InputFile>,
        reply_markup: Option<&InlineKeyboardMarkup>,
    ) -> TelegramResult<Message> {
        self.ensure_open()?;
        let video = self.prepare_input_file(video).await?;
        let cover = match cover {
            Some(cover) => Some(self.prepare_input_file(cover).await?),
            None => None,
        };
        let caption = self.optional_html("editMessageMedia", caption).await?;
        let (width, height, duration) = match metadata {
            Some(metadata) => (
                checked_i32(metadata.width, "video width is out of range")?,
                checked_i32(metadata.height, "video height is out of range")?,
                metadata
                    .duration
                    .map(|value| checked_i32(value, "video duration is out of range"))
                    .transpose()?
                    .unwrap_or(0),
            ),
            None => (0, 0, 0),
        };
        let content = InputMessageContent::InputMessageVideo(InputMessageVideo {
            video: video.input.clone(),
            thumbnail: None,
            cover: cover.as_ref().map(|value| value.input.clone()),
            start_timestamp: 0,
            added_sticker_file_ids: Vec::new(),
            duration,
            width,
            height,
            supports_streaming: true,
            caption,
            show_caption_above_media: false,
            self_destruct_type: None,
            has_spoiler: false,
        });
        let mut files = vec![video];
        if let Some(cover) = cover {
            files.push(cover);
        }
        let TdMessage::Message(message) = self
            .edit_media_detached(
                chat_id,
                message_id,
                normalize_reply_markup(reply_markup)?,
                content,
                files,
            )
            .await?;
        self.normalize_message(message, false).await
    }

    pub async fn edit_message_document_html(
        &self,
        chat_id: i64,
        message_id: i64,
        document: &InputFile,
        caption: Option<&str>,
        reply_markup: Option<&InlineKeyboardMarkup>,
    ) -> TelegramResult<Message> {
        self.ensure_open()?;
        let document = self.prepare_input_file(document).await?;
        let content = InputMessageContent::InputMessageDocument(InputMessageDocument {
            document: document.input.clone(),
            thumbnail: None,
            disable_content_type_detection: true,
            caption: self.optional_html("editMessageMedia", caption).await?,
        });
        let TdMessage::Message(message) = self
            .edit_media_detached(
                chat_id,
                message_id,
                normalize_reply_markup(reply_markup)?,
                content,
                vec![document],
            )
            .await?;
        self.normalize_message(message, false).await
    }

    async fn edit_media_detached(
        &self,
        chat_id: i64,
        message_id: i64,
        reply_markup: Option<ReplyMarkup>,
        content: InputMessageContent,
        files: Vec<PreparedInputFile>,
    ) -> TelegramResult<TdMessage> {
        self.ensure_open()?;
        let client_id = self.inner.client_id;
        let request = self.inner.detached_requests.spawn(async move {
            let result = map_td(
                "editMessageMedia",
                functions::edit_message_media(
                    chat_id,
                    message_id,
                    reply_markup,
                    content,
                    client_id,
                )
                .await,
            );
            // The detached task, rather than its caller, owns all local paths
            // until TDLib resolves the request. Dropping the caller's future
            // therefore cannot unlink a file still being uploaded.
            drop(files);
            result
        });
        let terminal = Arc::clone(&self.inner.terminal);
        self.prioritize_terminal(async move {
            match tokio::time::timeout(MEDIA_EDIT_TIMEOUT, request).await {
                Ok(result) => result.map_err(|_| {
                    TelegramError::runtime("editMessageMedia", "TDLib edit task failed")
                })?,
                Err(_) => {
                    // Keep the detached task (and its upload lease) alive until
                    // TDLib closes, but make the client terminal so no new
                    // media pipeline can replace this timed-out one.
                    terminal.set(TerminalReason::RequestTimedOut);
                    Err(TelegramError::runtime(
                        "editMessageMedia",
                        "TDLib media edit timed out",
                    ))
                }
            }
        })
        .await
    }

    pub async fn delete_message(&self, chat_id: i64, message_id: i64) -> TelegramResult<bool> {
        self.ensure_open()?;
        self.td_call(
            "deleteMessages",
            functions::delete_messages(chat_id, vec![message_id], true, self.inner.client_id),
        )
        .await?;
        Ok(true)
    }

    pub async fn answer_callback_query(
        &self,
        callback_query_id: &str,
        text: Option<&str>,
        show_alert: bool,
    ) -> TelegramResult<bool> {
        self.ensure_open()?;
        let callback_query_id =
            callback_query_id
                .parse::<i64>()
                .map_err(|_| TelegramError::Configuration {
                    reason: "callback query identifier is invalid",
                })?;
        self.td_call(
            "answerCallbackQuery",
            functions::answer_callback_query(
                callback_query_id,
                text.unwrap_or_default().to_owned(),
                show_alert,
                String::new(),
                0,
                self.inner.client_id,
            ),
        )
        .await?;
        Ok(true)
    }

    fn ensure_open(&self) -> TelegramResult<()> {
        if self.inner.closing.load(Ordering::Acquire) || self.inner.closed.load(Ordering::Acquire) {
            Err(TelegramError::Closed)
        } else {
            Ok(())
        }
    }

    async fn wait_for_send(
        &self,
        message: tdlib_rs::types::Message,
    ) -> Result<tdlib_rs::types::Message, SendFailure> {
        if let Some(error) = self.inner.terminal.error() {
            return Err(send_failure(error));
        }
        if message.sending_state.is_none() {
            return Ok(message);
        }
        let notified = self.inner.terminal.notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        if let Some(error) = self.inner.terminal.error() {
            return Err(send_failure(error));
        }
        let wait = self.inner.send_tracker.wait((message.chat_id, message.id));
        tokio::pin!(wait);
        let result = tokio::select! {
            biased;
            _ = &mut notified => return Err(send_failure(self.terminal_error())),
            result = &mut wait => result,
        };
        if let Some(error) = self.inner.terminal.error() {
            return Err(send_failure(error));
        }
        result
    }

    async fn formatted_text(
        &self,
        method: &'static str,
        text: &str,
        html: bool,
    ) -> TelegramResult<FormattedText> {
        if !html {
            return Ok(FormattedText {
                text: text.to_owned(),
                entities: Vec::new(),
            });
        }
        let TdFormattedText::FormattedText(text) = self
            .td_call(
                method,
                functions::parse_text_entities(
                    text.to_owned(),
                    TextParseMode::Html,
                    self.inner.client_id,
                ),
            )
            .await?;
        Ok(text)
    }

    async fn optional_html(
        &self,
        method: &'static str,
        text: Option<&str>,
    ) -> TelegramResult<Option<FormattedText>> {
        match text {
            Some(text) => self.formatted_text(method, text, true).await.map(Some),
            None => Ok(None),
        }
    }

    async fn prepare_input_file(&self, file: &InputFile) -> TelegramResult<PreparedInputFile> {
        match file.source() {
            InputFileSource::RemoteId(id) => Ok(PreparedInputFile {
                input: TdInputFile::Remote(InputFileRemote { id: id.clone() }),
                _downloaded: None,
                lease_path: None,
            }),
            InputFileSource::LocalPath(path) => {
                let lease_path = create_upload_lease(path).await?;
                Ok(PreparedInputFile {
                    input: TdInputFile::Local(InputFileLocal {
                        path: path_to_string(&lease_path)?,
                    }),
                    _downloaded: None,
                    lease_path: Some(lease_path),
                })
            }
            InputFileSource::HttpUrl(url) => {
                let mut temporary = self
                    .inner
                    .cover_downloader
                    .download_url(url)
                    .await
                    .map_err(|error| TelegramError::runtime("download cover", error.to_string()))?;
                give_cover_image_extension(&mut temporary).await?;
                let input = TdInputFile::Local(InputFileLocal {
                    path: path_to_string(&temporary.path)?,
                });
                Ok(PreparedInputFile {
                    input,
                    _downloaded: Some(temporary),
                    lease_path: None,
                })
            }
        }
    }

    async fn normalize_callback_query(
        &self,
        update: tdlib_rs::types::UpdateNewCallbackQuery,
    ) -> TelegramResult<CallbackQuery> {
        let (sender, message) =
            tokio::try_join!(self.get_user_by_id(update.sender_user_id), async {
                match self
                    .td_call(
                        "getCallbackQueryMessage",
                        functions::get_callback_query_message(
                            update.chat_id,
                            update.message_id,
                            update.id,
                            self.inner.client_id,
                        ),
                    )
                    .await
                {
                    Ok(TdMessage::Message(message)) => {
                        Ok(Some(self.normalize_message(message, true).await?))
                    }
                    Err(error) if error.is_terminal() => Err(error),
                    Err(_) => Ok(None),
                }
            })?;
        Ok(CallbackQuery {
            id: update.id.to_string(),
            sender,
            message,
            data: decode_callback_payload(update.payload)?,
        })
    }

    async fn normalize_message(
        &self,
        message: tdlib_rs::types::Message,
        include_reply: bool,
    ) -> TelegramResult<Message> {
        let reply_to_message = if include_reply
            && matches!(
                message.reply_to,
                Some(tdlib_rs::enums::MessageReplyTo::Message(_))
            ) {
            match self
                .td_call(
                    "getRepliedMessage",
                    functions::get_replied_message(
                        message.chat_id,
                        message.id,
                        self.inner.client_id,
                    ),
                )
                .await
            {
                Ok(TdMessage::Message(reply)) => {
                    Some(Box::new(self.normalize_message_shallow(reply).await?))
                }
                Err(error) if error.is_terminal() => return Err(error),
                Err(_) => None,
            }
        } else {
            None
        };
        let mut normalized = self.normalize_message_shallow(message).await?;
        normalized.reply_to_message = reply_to_message;
        Ok(normalized)
    }

    async fn normalize_message_shallow(
        &self,
        message: tdlib_rs::types::Message,
    ) -> TelegramResult<Message> {
        let sender_user_id = match message.sender_id {
            MessageSender::User(sender) => Some(sender.user_id),
            MessageSender::Chat(_) => None,
        };
        let (chat, sender) = tokio::try_join!(self.get_chat_by_id(message.chat_id), async {
            match sender_user_id {
                Some(user_id) => self.get_user_by_id(user_id).await.map(Some),
                None => Ok(None),
            }
        })?;

        let (text, caption, video, document) = match message.content {
            MessageContent::MessageText(content) => (Some(content.text.text), None, None, None),
            MessageContent::MessageVideo(content) => {
                let file = content.video.video;
                (
                    None,
                    nonempty(content.caption.text),
                    Some(Video {
                        file_id: file.remote.id,
                        file_unique_id: file.remote.unique_id,
                        file_size: known_file_size(file.size),
                    }),
                    None,
                )
            }
            MessageContent::MessageDocument(content) => {
                let file = content.document.document;
                (
                    None,
                    nonempty(content.caption.text),
                    None,
                    Some(Document {
                        file_id: file.remote.id,
                        file_unique_id: file.remote.unique_id,
                        file_size: known_file_size(file.size),
                    }),
                )
            }
            _ => (None, None, None, None),
        };

        Ok(Message {
            message_id: message.id,
            chat,
            sender,
            text,
            caption,
            reply_to_message: None,
            video,
            document,
        })
    }

    async fn get_user_by_id(&self, user_id: i64) -> TelegramResult<User> {
        let TdUser::User(user) = self
            .td_call(
                "getUser",
                functions::get_user(user_id, self.inner.client_id),
            )
            .await?;
        normalize_user(user)
    }

    async fn get_chat_by_id(&self, chat_id: i64) -> TelegramResult<Chat> {
        let tdlib_rs::enums::Chat::Chat(chat) = self
            .td_call(
                "getChat",
                functions::get_chat(chat_id, self.inner.client_id),
            )
            .await?;
        match chat.r#type {
            ChatType::Private(_) => Ok(Chat {
                id: chat.id,
                kind: ChatKind::Private,
            }),
            ChatType::BasicGroup(_) => Ok(Chat {
                id: chat.id,
                kind: ChatKind::Group,
            }),
            ChatType::Supergroup(supergroup_type) => Ok(Chat {
                id: chat.id,
                kind: if supergroup_type.is_channel {
                    ChatKind::Channel
                } else {
                    ChatKind::Supergroup
                },
            }),
            ChatType::Secret(_) => Ok(Chat {
                id: chat.id,
                kind: ChatKind::Unknown,
            }),
        }
    }
}

struct PreparedInputFile {
    input: TdInputFile,
    // DownloadedMedia removes its path on drop. Local source files instead get
    // a hard-link lease so cancellation can delete the caller's original path
    // without invalidating an already-dispatched TDLib request.
    _downloaded: Option<DownloadedMedia>,
    lease_path: Option<PathBuf>,
}

impl Drop for PreparedInputFile {
    fn drop(&mut self) {
        if let Some(path) = &self.lease_path {
            let _ = std::fs::remove_file(path);
        }
    }
}

struct ActiveGuard {
    armed: bool,
    official_closed: Option<Arc<AtomicBool>>,
}

impl Drop for ActiveGuard {
    fn drop(&mut self) {
        if self.armed
            && self
                .official_closed
                .as_ref()
                .is_none_or(|closed| closed.load(Ordering::Acquire))
        {
            CLIENT_ACTIVE.store(false, Ordering::Release);
        }
    }
}

struct ReceivePump {
    client_id: i32,
    running: Arc<AtomicBool>,
    updates: mpsc::Sender<RawEvent>,
    send_tracker: Arc<SendTracker>,
    closing: Arc<AtomicBool>,
    closed: Arc<AtomicBool>,
    closed_notify: Arc<Notify>,
    terminal: Arc<TerminalState>,
    authorized: Arc<AtomicBool>,
}

fn spawn_receive_pump(pump: ReceivePump) -> JoinHandle<()> {
    tokio::spawn(async move {
        while pump.running.load(Ordering::Acquire) {
            let received = tokio::task::spawn_blocking(tdlib_rs::receive).await;
            let Ok(received) = received else {
                pump.terminal.set(TerminalReason::ReceivePumpFailed);
                pump.send_tracker.fail_all();
                break;
            };
            let Some((update, received_client_id)) = received else {
                continue;
            };
            if received_client_id != pump.client_id {
                continue;
            }

            match update {
                TdUpdate::ConnectionState(update) => log_connection_state(&update.state),
                TdUpdate::MessageSendSucceeded(update) => pump.send_tracker.complete(
                    (update.message.chat_id, update.old_message_id),
                    SendOutcome::Succeeded(Box::new(update.message)),
                ),
                TdUpdate::MessageSendFailed(update) => {
                    let need_drop_reply = matches!(
                        update.message.sending_state.as_ref(),
                        Some(tdlib_rs::enums::MessageSendingState::Failed(state))
                            if state.need_drop_reply
                    );
                    pump.send_tracker.complete(
                        (update.message.chat_id, update.old_message_id),
                        SendOutcome::Failed {
                            error: update.error,
                            need_drop_reply,
                            failed_message: Some(Box::new(update.message)),
                        },
                    );
                }
                TdUpdate::DeleteMessages(update) => {
                    pump.send_tracker
                        .fail_deleted(update.chat_id, update.message_ids);
                }
                TdUpdate::AuthorizationState(update) => {
                    let is_closed =
                        matches!(update.authorization_state, AuthorizationState::Closed);
                    let is_ready = matches!(update.authorization_state, AuthorizationState::Ready);
                    let was_ready = if is_ready {
                        pump.authorized.swap(true, Ordering::AcqRel)
                    } else {
                        pump.authorized.load(Ordering::Acquire)
                    };

                    if !is_ready && was_ready {
                        let reason = if pump.closing.load(Ordering::Acquire)
                            || matches!(
                                update.authorization_state,
                                AuthorizationState::Closing | AuthorizationState::Closed
                            ) {
                            TerminalReason::Closed
                        } else {
                            TerminalReason::AuthorizationLost
                        };
                        pump.terminal.set(reason);
                    }

                    if is_closed {
                        pump.terminal.set(TerminalReason::Closed);
                        pump.closed.store(true, Ordering::Release);
                        // `notify_one` retains a permit when close has not
                        // started waiting yet.
                        pump.closed_notify.notify_one();
                        pump.send_tracker.fail_all();
                        break;
                    }

                    // Authorization needs the first Ready update, along with
                    // all pre-Ready states. Post-Ready non-Ready states are
                    // terminal and must never sit behind application backlog.
                    if was_ready {
                        continue;
                    }
                    try_dispatch_update(
                        &pump.updates,
                        &pump.terminal,
                        TdUpdate::AuthorizationState(update),
                        UpdateOverflowPolicy::Terminal,
                    );
                }
                update @ (TdUpdate::NewMessage(_)
                | TdUpdate::NewCallbackQuery(_)
                | TdUpdate::NewInlineCallbackQuery(_)) => {
                    try_dispatch_update(
                        &pump.updates,
                        &pump.terminal,
                        update,
                        UpdateOverflowPolicy::DropNewest,
                    );
                }
                _ => {}
            }
        }
    })
}

fn log_connection_state(state: &ConnectionState) {
    let (state_code, description) = connection_state_description(state);
    info!(
        event = "telegram_connection_state",
        state = state_code,
        description,
        "Telegram 连接状态已更新"
    );
}

fn connection_state_description(state: &ConnectionState) -> (&'static str, &'static str) {
    match state {
        ConnectionState::WaitingForNetwork => ("waiting_for_network", "等待网络可用"),
        ConnectionState::ConnectingToProxy => ("connecting_to_proxy", "正在连接代理服务器"),
        ConnectionState::Connecting => ("connecting", "正在连接 Telegram"),
        ConnectionState::Updating => ("updating", "正在同步离线期间的数据"),
        ConnectionState::Ready => ("ready", "Telegram 连接已就绪"),
    }
}

fn try_dispatch_update(
    updates: &mpsc::Sender<RawEvent>,
    terminal: &TerminalState,
    update: TdUpdate,
    overflow_policy: UpdateOverflowPolicy,
) {
    if terminal.error().is_some() {
        return;
    }
    match updates.try_send(RawEvent::Update(Box::new(update))) {
        Ok(()) => {}
        Err(mpsc::error::TrySendError::Full(_)) => match overflow_policy {
            UpdateOverflowPolicy::Terminal => terminal.set(TerminalReason::UpdateOverflow),
            UpdateOverflowPolicy::DropNewest => {
                let dropped_total = RAW_DROPPED_UPDATES
                    .fetch_add(1, Ordering::Relaxed)
                    .saturating_add(1);
                if dropped_total == 1 || dropped_total.is_multiple_of(1_000) {
                    warn!(
                        event = "telegram_raw_update_dropped",
                        reason = "application_queue_full",
                        dropped_total,
                        "TDLib update 队列已满，丢弃最新业务 update"
                    );
                }
            }
        },
        Err(mpsc::error::TrySendError::Closed(_)) => {
            terminal.set(TerminalReason::UpdateReceiverClosed);
        }
    }
}

#[derive(Clone, Copy)]
enum UpdateOverflowPolicy {
    Terminal,
    DropNewest,
}

fn validate_config(config: &TdlibConfig) -> TelegramResult<()> {
    if config.api_id <= 0 {
        return Err(TelegramError::Configuration {
            reason: "api_id must be positive",
        });
    }
    if config.api_hash.trim().is_empty() {
        return Err(TelegramError::Configuration {
            reason: "api_hash must not be empty",
        });
    }
    if config.bot_token.trim().is_empty() {
        return Err(TelegramError::Configuration {
            reason: "bot token must not be empty",
        });
    }
    if !config.database_directory.is_absolute() || !config.files_directory.is_absolute() {
        return Err(TelegramError::Configuration {
            reason: "TDLib directories must be absolute paths",
        });
    }
    if config.database_directory == config.files_directory {
        return Err(TelegramError::Configuration {
            reason: "TDLib database and files directories must be different",
        });
    }
    Ok(())
}

fn normalize_user(user: tdlib_rs::types::User) -> TelegramResult<User> {
    Ok(User {
        id: u64::try_from(user.id).map_err(|_| {
            TelegramError::runtime("normalize user", "negative Telegram user identifier")
        })?,
        username: user
            .usernames
            .and_then(|usernames| usernames.active_usernames.into_iter().next()),
        language_code: nonempty(user.language_code),
    })
}

fn normalize_reply_markup(
    markup: Option<&InlineKeyboardMarkup>,
) -> TelegramResult<Option<ReplyMarkup>> {
    let Some(markup) = markup else {
        return Ok(None);
    };
    let rows = markup
        .inline_keyboard
        .iter()
        .map(|row| {
            row.iter()
                .map(|button| {
                    let r#type = match (&button.callback_data, &button.url) {
                        (Some(data), None) => {
                            if data.is_empty() || data.len() > 64 {
                                return Err(TelegramError::Configuration {
                                    reason: "callback data must contain 1-64 UTF-8 bytes",
                                });
                            }
                            InlineKeyboardButtonType::Callback(InlineKeyboardButtonTypeCallback {
                                data: BASE64_STANDARD.encode(data.as_bytes()),
                            })
                        }
                        (None, Some(url)) => {
                            InlineKeyboardButtonType::Url(InlineKeyboardButtonTypeUrl {
                                url: url.clone(),
                            })
                        }
                        _ => {
                            return Err(TelegramError::Configuration {
                                reason: "inline keyboard button must have exactly one action",
                            });
                        }
                    };
                    Ok(TdInlineKeyboardButton {
                        text: button.text.clone(),
                        icon_custom_emoji_id: 0,
                        style: ButtonStyle::Default,
                        r#type,
                    })
                })
                .collect::<TelegramResult<Vec<_>>>()
        })
        .collect::<TelegramResult<Vec<_>>>()?;
    Ok(Some(ReplyMarkup::InlineKeyboard(
        ReplyMarkupInlineKeyboard { rows },
    )))
}

fn decode_callback_payload(payload: CallbackQueryPayload) -> TelegramResult<Option<String>> {
    let encoded = match payload {
        CallbackQueryPayload::Data(data) => data.data,
        CallbackQueryPayload::DataWithPassword(data) => data.data,
        CallbackQueryPayload::Game(_) => return Ok(None),
    };
    let bytes = BASE64_STANDARD.decode(encoded).map_err(|_| {
        TelegramError::runtime("decode callback query", "invalid base64 callback payload")
    })?;
    String::from_utf8(bytes).map(Some).map_err(|_| {
        TelegramError::runtime("decode callback query", "callback payload is not UTF-8")
    })
}

fn map_td<T>(method: &'static str, result: Result<T, tdlib_rs::types::Error>) -> TelegramResult<T> {
    result.map_err(|error| TelegramError::from_tdlib(method, error))
}

fn checked_i32(value: u32, reason: &'static str) -> TelegramResult<i32> {
    i32::try_from(value).map_err(|_| TelegramError::InvalidInputFile { reason })
}

fn known_file_size(value: i64) -> Option<u64> {
    u64::try_from(value).ok().filter(|value| *value > 0)
}

fn nonempty(value: String) -> Option<String> {
    (!value.is_empty()).then_some(value)
}

async fn create_upload_lease(source: &Path) -> TelegramResult<PathBuf> {
    let parent = source.parent().ok_or(TelegramError::InvalidInputFile {
        reason: "local media path has no parent directory",
    })?;
    let mut file_name = format!(".tdlib-upload-lease-{}", Uuid::new_v4().hyphenated());
    if let Some(extension) = source.extension().and_then(|value| value.to_str()) {
        file_name.push('.');
        file_name.push_str(extension);
    }
    let lease_path = parent.join(file_name);
    tokio::fs::hard_link(source, &lease_path)
        .await
        .map_err(|error| TelegramError::runtime("prepare upload lease", error.to_string()))?;
    Ok(lease_path)
}

async fn give_cover_image_extension(downloaded: &mut DownloadedMedia) -> TelegramResult<()> {
    // A signed URL can return an HTML error body while retaining a `.jpg`
    // suffix. Inspect the downloaded bytes instead of trusting that suffix.
    let mut file = tokio::fs::File::open(&downloaded.path)
        .await
        .map_err(|error| TelegramError::runtime("inspect cover", error.to_string()))?;
    let mut header = [0_u8; 16];
    let length = file
        .read(&mut header)
        .await
        .map_err(|error| TelegramError::runtime("inspect cover", error.to_string()))?;
    let extension = if length >= 3 && header[..3] == [0xff, 0xd8, 0xff] {
        "jpg"
    } else if length >= 8 && header[..8] == *b"\x89PNG\r\n\x1a\n" {
        "png"
    } else if length >= 12 && header[..4] == *b"RIFF" && header[8..12] == *b"WEBP" {
        "webp"
    } else {
        return Err(TelegramError::InvalidInputFile {
            reason: "downloaded video cover is not a supported image",
        });
    };
    drop(file);

    let image_path = downloaded.path.with_extension(extension);
    tokio::fs::rename(&downloaded.path, &image_path)
        .await
        .map_err(|error| TelegramError::runtime("prepare cover", error.to_string()))?;
    downloaded.path = image_path;
    Ok(())
}

fn path_to_string(path: &Path) -> TelegramResult<String> {
    path.to_str()
        .map(str::to_owned)
        .ok_or(TelegramError::InvalidInputFile {
            reason: "path is not valid UTF-8",
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn describes_every_tdlib_connection_state_without_debug_payloads() {
        assert_eq!(
            connection_state_description(&ConnectionState::WaitingForNetwork),
            ("waiting_for_network", "等待网络可用")
        );
        assert_eq!(
            connection_state_description(&ConnectionState::ConnectingToProxy),
            ("connecting_to_proxy", "正在连接代理服务器")
        );
        assert_eq!(
            connection_state_description(&ConnectionState::Connecting),
            ("connecting", "正在连接 Telegram")
        );
        assert_eq!(
            connection_state_description(&ConnectionState::Updating),
            ("updating", "正在同步离线期间的数据")
        );
        assert_eq!(
            connection_state_description(&ConnectionState::Ready),
            ("ready", "Telegram 连接已就绪")
        );
    }

    #[test]
    fn callback_data_round_trips_through_tdlib_base64() {
        let markup = InlineKeyboardMarkup::new(vec![vec![
            super::super::api::InlineKeyboardButton::callback("设置", "setting|42|main"),
        ]]);
        let Some(ReplyMarkup::InlineKeyboard(markup)) =
            normalize_reply_markup(Some(&markup)).unwrap()
        else {
            panic!("expected inline keyboard");
        };
        let InlineKeyboardButtonType::Callback(button) = &markup.rows[0][0].r#type else {
            panic!("expected callback button");
        };
        assert_eq!(
            decode_callback_payload(CallbackQueryPayload::Data(
                tdlib_rs::types::CallbackQueryPayloadData {
                    data: button.data.clone(),
                }
            ))
            .unwrap(),
            Some("setting|42|main".to_owned())
        );
    }

    #[test]
    fn callback_data_enforces_telegram_byte_limit_before_base64() {
        let markup = InlineKeyboardMarkup::new(vec![vec![
            super::super::api::InlineKeyboardButton::callback("too large", "界".repeat(22)),
        ]]);
        assert!(normalize_reply_markup(Some(&markup)).is_err());
    }

    #[test]
    fn callback_data_round_trips_padding_unicode_and_the_64_byte_boundary() {
        for data in [
            "a".to_owned(),
            "ab".to_owned(),
            "界".to_owned(),
            "x".repeat(64),
        ] {
            let markup = InlineKeyboardMarkup::new(vec![vec![
                super::super::api::InlineKeyboardButton::callback("test", data.clone()),
            ]]);
            let Some(ReplyMarkup::InlineKeyboard(markup)) =
                normalize_reply_markup(Some(&markup)).unwrap()
            else {
                panic!("expected inline keyboard");
            };
            let InlineKeyboardButtonType::Callback(button) = &markup.rows[0][0].r#type else {
                panic!("expected callback button");
            };
            let decoded = decode_callback_payload(CallbackQueryPayload::Data(
                tdlib_rs::types::CallbackQueryPayloadData {
                    data: button.data.clone(),
                },
            ))
            .unwrap();
            assert_eq!(decoded, Some(data));
        }
    }

    #[tokio::test]
    async fn hard_link_lease_survives_source_removal_and_cleans_up_on_drop() {
        let directory = std::env::temp_dir().join(format!(
            "parse-bot-tdlib-lease-test-{}",
            Uuid::new_v4().hyphenated()
        ));
        tokio::fs::create_dir_all(&directory).await.unwrap();
        let source = directory.join("source.mp4");
        tokio::fs::write(&source, b"lease-content").await.unwrap();

        let lease_path = create_upload_lease(&source).await.unwrap();
        tokio::fs::remove_file(&source).await.unwrap();
        assert_eq!(
            tokio::fs::read(&lease_path).await.unwrap(),
            b"lease-content"
        );

        let prepared = PreparedInputFile {
            input: TdInputFile::Local(InputFileLocal {
                path: path_to_string(&lease_path).unwrap(),
            }),
            _downloaded: None,
            lease_path: Some(lease_path.clone()),
        };
        drop(prepared);
        assert!(!lease_path.exists());
        tokio::fs::remove_dir_all(directory).await.unwrap();
    }

    #[tokio::test]
    async fn cover_detection_uses_bytes_instead_of_a_trusted_url_suffix() {
        let directory = std::env::temp_dir().join(format!(
            "parse-bot-tdlib-cover-test-{}",
            Uuid::new_v4().hyphenated()
        ));
        tokio::fs::create_dir_all(&directory).await.unwrap();

        let fake_jpeg_path = directory.join("signed-cover.jpg");
        tokio::fs::write(&fake_jpeg_path, b"<html>expired</html>")
            .await
            .unwrap();
        let mut fake_jpeg = DownloadedMedia {
            path: fake_jpeg_path,
            bytes: 20,
        };
        assert!(matches!(
            give_cover_image_extension(&mut fake_jpeg).await,
            Err(TelegramError::InvalidInputFile { .. })
        ));

        let real_jpeg_path = directory.join("signed-cover.png");
        tokio::fs::write(&real_jpeg_path, b"\xff\xd8\xffjpeg")
            .await
            .unwrap();
        let mut real_jpeg = DownloadedMedia {
            path: real_jpeg_path,
            bytes: 7,
        };
        give_cover_image_extension(&mut real_jpeg).await.unwrap();
        assert_eq!(
            real_jpeg.path.extension().and_then(|value| value.to_str()),
            Some("jpg")
        );

        drop(fake_jpeg);
        drop(real_jpeg);
        tokio::fs::remove_dir_all(directory).await.unwrap();
    }

    #[test]
    fn bounded_update_overflow_sets_a_terminal_reason() {
        let (updates, _receiver) = mpsc::channel(1);
        let terminal = TerminalState::new();
        let ready = || {
            TdUpdate::AuthorizationState(tdlib_rs::types::UpdateAuthorizationState {
                authorization_state: AuthorizationState::Ready,
            })
        };
        try_dispatch_update(&updates, &terminal, ready(), UpdateOverflowPolicy::Terminal);
        assert!(terminal.error().is_none());
        try_dispatch_update(&updates, &terminal, ready(), UpdateOverflowPolicy::Terminal);
        assert!(matches!(
            *terminal.reason.lock(),
            Some(TerminalReason::UpdateOverflow)
        ));
    }

    #[test]
    fn business_update_overflow_is_load_shed_without_killing_the_client() {
        let (updates, _receiver) = mpsc::channel(1);
        let terminal = TerminalState::new();
        let ready = || {
            TdUpdate::AuthorizationState(tdlib_rs::types::UpdateAuthorizationState {
                authorization_state: AuthorizationState::Ready,
            })
        };
        try_dispatch_update(
            &updates,
            &terminal,
            ready(),
            UpdateOverflowPolicy::DropNewest,
        );
        try_dispatch_update(
            &updates,
            &terminal,
            ready(),
            UpdateOverflowPolicy::DropNewest,
        );

        assert!(terminal.error().is_none());
    }

    #[test]
    fn receive_pump_failure_has_highest_terminal_priority() {
        let terminal = TerminalState::new();
        terminal.set(TerminalReason::UpdateOverflow);
        terminal.set(TerminalReason::ReceivePumpFailed);
        terminal.set(TerminalReason::Closed);
        assert!(matches!(
            *terminal.reason.lock(),
            Some(TerminalReason::ReceivePumpFailed)
        ));
    }

    #[tokio::test]
    async fn send_waiter_cancellation_unregisters_without_replacing_duplicates() {
        let tracker = Arc::new(SendTracker::default());
        let key = (42, 101);
        let waiting_tracker = Arc::clone(&tracker);
        let waiting = tokio::spawn(async move { waiting_tracker.wait(key).await });
        for _ in 0..10 {
            if tracker.state.lock().waiters.contains_key(&key) {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(tracker.state.lock().waiters.contains_key(&key));

        let duplicate = tracker.wait(key).await.unwrap_err();
        assert!(duplicate.error.to_string().contains("duplicate pending"));
        assert!(tracker.state.lock().waiters.contains_key(&key));

        waiting.abort();
        let _ = waiting.await;
        assert!(!tracker.state.lock().waiters.contains_key(&key));
    }

    #[test]
    fn early_send_outcomes_evict_in_insertion_order() {
        let tracker = SendTracker::default();
        for message_id in 0..=1024 {
            tracker.complete(
                (1, message_id),
                SendOutcome::Failed {
                    error: tdlib_rs::types::Error {
                        code: 500,
                        message: "test".to_owned(),
                    },
                    need_drop_reply: false,
                    failed_message: None,
                },
            );
        }
        let state = tracker.state.lock();
        assert_eq!(state.early_outcomes.len(), 1024);
        assert!(!state.early_outcomes.contains_key(&(1, 0)));
        assert!(state.early_outcomes.contains_key(&(1, 1024)));
        assert_eq!(state.early_outcome_order.front(), Some(&(1, 1)));
    }
}
