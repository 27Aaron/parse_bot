use std::{
    fmt,
    path::Path,
    sync::{Arc, OnceLock},
    time::Duration,
};

use futures_util::StreamExt;
use regex::Regex;
use reqwest::Client;
use serde::{Deserialize, Serialize, Serializer, de::DeserializeOwned};
use url::Url;

use crate::{AppError, model::TelegramMediaKind};

const GET_UPDATES_GRACE: Duration = Duration::from_secs(10);
const MAX_API_RESPONSE_BYTES: usize = 8 * 1024 * 1024;
const HTML_PARSE_MODE: &str = "HTML";

pub type TelegramResult<T> = std::result::Result<T, TelegramError>;

#[derive(Default)]
struct SendVideoOptions<'a> {
    parse_mode: Option<&'a str>,
    reply_parameters: Option<&'a ReplyParameters>,
    metadata: Option<VideoMetadata>,
    reply_markup: Option<&'a InlineKeyboardMarkup>,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TelegramError {
    #[error("Telegram Bot API configuration is invalid: {reason}")]
    Configuration { reason: &'static str },

    #[error("Telegram Bot API transport failure while calling {method}: {reason}")]
    Transport {
        method: &'static str,
        reason: &'static str,
    },

    #[error("Telegram Bot API returned HTTP {status} while calling {method}")]
    HttpStatus { method: &'static str, status: u16 },

    #[error("Telegram Bot API returned an invalid response while calling {method}")]
    InvalidResponse { method: &'static str },

    #[error("Telegram Bot API rejected {method} with code {error_code:?}: {description}")]
    Api {
        method: &'static str,
        error_code: Option<i64>,
        description: String,
        retry_after: Option<Duration>,
        migrate_to_chat_id: Option<i64>,
    },

    #[error("invalid Telegram input file: {reason}")]
    InvalidInputFile { reason: &'static str },
}

impl TelegramError {
    pub fn retry_after(&self) -> Option<Duration> {
        match self {
            Self::Api { retry_after, .. } => *retry_after,
            _ => None,
        }
    }

    pub fn error_code(&self) -> Option<i64> {
        match self {
            Self::Api { error_code, .. } => *error_code,
            _ => None,
        }
    }

    pub fn migrate_to_chat_id(&self) -> Option<i64> {
        match self {
            Self::Api {
                migrate_to_chat_id, ..
            } => *migrate_to_chat_id,
            _ => None,
        }
    }

    pub fn is_rate_limited(&self) -> bool {
        self.error_code() == Some(429) || self.retry_after().is_some()
    }
}

impl From<TelegramError> for AppError {
    fn from(error: TelegramError) -> Self {
        if error.is_rate_limited() {
            Self::RateLimited
        } else {
            Self::Telegram(error.to_string())
        }
    }
}

#[derive(Clone)]
pub struct TelegramClient {
    http: Client,
    base_url: Url,
    token: Arc<str>,
}

impl fmt::Debug for TelegramClient {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TelegramClient")
            .field("endpoint", &"<redacted>")
            .field("token", &"<redacted>")
            .finish_non_exhaustive()
    }
}

impl TelegramClient {
    pub fn new(base_url: Url, token: impl Into<String>) -> TelegramResult<Self> {
        let http = Client::builder()
            .connect_timeout(Duration::from_secs(15))
            .no_proxy()
            .build()
            .map_err(|_| TelegramError::Configuration {
                reason: "failed to initialize HTTP client",
            })?;
        Self::with_http_client(base_url, token, http)
    }

    pub fn with_http_client(
        mut base_url: Url,
        token: impl Into<String>,
        http: Client,
    ) -> TelegramResult<Self> {
        if !matches!(base_url.scheme(), "http" | "https")
            || base_url.host_str().is_none()
            || !base_url.username().is_empty()
            || base_url.password().is_some()
            || base_url.query().is_some()
            || base_url.fragment().is_some()
        {
            return Err(TelegramError::Configuration {
                reason: "base URL must be an HTTP(S) origin or path without credentials, query, or fragment",
            });
        }

        let token = token.into();
        if token.is_empty()
            || !token
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b':' | b'_' | b'-'))
        {
            return Err(TelegramError::Configuration {
                reason: "bot token has an invalid format",
            });
        }

        let normalized_path = format!("{}/", base_url.path().trim_end_matches('/'));
        base_url.set_path(&normalized_path);

        Ok(Self {
            http,
            base_url,
            token: Arc::from(token),
        })
    }

    pub async fn get_me(&self) -> TelegramResult<User> {
        self.call("getMe", &EmptyRequest {}, None).await
    }

    pub async fn get_chat_member(&self, chat_id: &str, user_id: u64) -> TelegramResult<ChatMember> {
        self.call(
            "getChatMember",
            &GetChatMemberRequest { chat_id, user_id },
            None,
        )
        .await
    }

    pub async fn get_updates(
        &self,
        offset: Option<i64>,
        timeout_secs: u32,
    ) -> TelegramResult<Vec<Update>> {
        let request = GetUpdatesRequest {
            offset,
            limit: 100,
            timeout: timeout_secs,
            allowed_updates: ["message"],
        };
        let timeout =
            Duration::from_secs(u64::from(timeout_secs)).saturating_add(GET_UPDATES_GRACE);
        self.call("getUpdates", &request, Some(timeout)).await
    }

    pub async fn send_message(
        &self,
        chat_id: i64,
        text: &str,
        reply_markup: Option<&InlineKeyboardMarkup>,
    ) -> TelegramResult<Message> {
        self.send_message_inner(chat_id, text, None, None, reply_markup)
            .await
    }

    /// Sends a message as a reply to another message in the same chat.
    ///
    /// Telegram keeps the reply relationship when this message is edited, so
    /// callers can send a temporary "parsing" status and later replace it with
    /// the download choices without losing the quoted source message.
    pub async fn send_message_reply(
        &self,
        chat_id: i64,
        text: &str,
        reply_to_message_id: i64,
        reply_markup: Option<&InlineKeyboardMarkup>,
    ) -> TelegramResult<Message> {
        let reply_parameters = ReplyParameters::new(reply_to_message_id);
        self.send_message_inner(chat_id, text, None, Some(&reply_parameters), reply_markup)
            .await
    }

    /// Sends Telegram-supported HTML as a reply in the same chat.
    pub async fn send_message_reply_html(
        &self,
        chat_id: i64,
        text: &str,
        reply_to_message_id: i64,
        reply_markup: Option<&InlineKeyboardMarkup>,
    ) -> TelegramResult<Message> {
        let reply_parameters = ReplyParameters::new(reply_to_message_id);
        self.send_message_inner(
            chat_id,
            text,
            Some(HTML_PARSE_MODE),
            Some(&reply_parameters),
            reply_markup,
        )
        .await
    }

    async fn send_message_inner(
        &self,
        chat_id: i64,
        text: &str,
        parse_mode: Option<&str>,
        reply_parameters: Option<&ReplyParameters>,
        reply_markup: Option<&InlineKeyboardMarkup>,
    ) -> TelegramResult<Message> {
        self.call(
            "sendMessage",
            &SendMessageRequest {
                chat_id,
                text,
                parse_mode,
                reply_parameters,
                reply_markup,
            },
            None,
        )
        .await
    }

    pub async fn edit_message_text(
        &self,
        chat_id: i64,
        message_id: i64,
        text: &str,
        reply_markup: Option<&InlineKeyboardMarkup>,
    ) -> TelegramResult<Message> {
        self.edit_message_text_inner(chat_id, message_id, text, None, reply_markup)
            .await
    }

    /// Edits a message using Telegram-supported HTML formatting.
    pub async fn edit_message_text_html(
        &self,
        chat_id: i64,
        message_id: i64,
        text: &str,
        reply_markup: Option<&InlineKeyboardMarkup>,
    ) -> TelegramResult<Message> {
        self.edit_message_text_inner(
            chat_id,
            message_id,
            text,
            Some(HTML_PARSE_MODE),
            reply_markup,
        )
        .await
    }

    async fn edit_message_text_inner(
        &self,
        chat_id: i64,
        message_id: i64,
        text: &str,
        parse_mode: Option<&str>,
        reply_markup: Option<&InlineKeyboardMarkup>,
    ) -> TelegramResult<Message> {
        self.call(
            "editMessageText",
            &EditMessageTextRequest {
                chat_id,
                message_id,
                text,
                parse_mode,
                reply_markup,
            },
            None,
        )
        .await
    }

    pub async fn delete_message(&self, chat_id: i64, message_id: i64) -> TelegramResult<bool> {
        self.call(
            "deleteMessage",
            &DeleteMessageRequest {
                chat_id,
                message_id,
            },
            None,
        )
        .await
    }

    pub async fn answer_callback_query(
        &self,
        callback_query_id: &str,
        text: Option<&str>,
        show_alert: bool,
    ) -> TelegramResult<bool> {
        self.call(
            "answerCallbackQuery",
            &AnswerCallbackQueryRequest {
                callback_query_id,
                text,
                show_alert,
                cache_time: 0,
            },
            None,
        )
        .await
    }

    pub async fn send_video(
        &self,
        chat_id: i64,
        video: &InputFile,
        caption: Option<&str>,
        reply_markup: Option<&InlineKeyboardMarkup>,
    ) -> TelegramResult<Message> {
        self.send_video_inner(
            chat_id,
            video,
            caption,
            SendVideoOptions {
                reply_markup,
                ..SendVideoOptions::default()
            },
        )
        .await
    }

    /// Sends a streamable video whose caption contains Telegram-supported HTML.
    pub async fn send_video_html(
        &self,
        chat_id: i64,
        video: &InputFile,
        caption: Option<&str>,
        reply_markup: Option<&InlineKeyboardMarkup>,
    ) -> TelegramResult<Message> {
        self.send_video_inner(
            chat_id,
            video,
            caption,
            SendVideoOptions {
                parse_mode: Some(HTML_PARSE_MODE),
                reply_markup,
                ..SendVideoOptions::default()
            },
        )
        .await
    }

    /// Sends an HTML-captioned streamable video as a reply in the same chat.
    pub async fn send_video_reply_html(
        &self,
        chat_id: i64,
        video: &InputFile,
        caption: Option<&str>,
        reply_to_message_id: i64,
        reply_markup: Option<&InlineKeyboardMarkup>,
    ) -> TelegramResult<Message> {
        let reply_parameters = ReplyParameters::new(reply_to_message_id);
        self.send_video_inner(
            chat_id,
            video,
            caption,
            SendVideoOptions {
                parse_mode: Some(HTML_PARSE_MODE),
                reply_parameters: Some(&reply_parameters),
                reply_markup,
                ..SendVideoOptions::default()
            },
        )
        .await
    }

    /// Sends an HTML-captioned streamable video with explicit display metadata
    /// as a reply in the same chat.
    pub async fn send_video_reply_html_with_metadata(
        &self,
        chat_id: i64,
        video: &InputFile,
        caption: Option<&str>,
        reply_to_message_id: i64,
        metadata: VideoMetadata,
        reply_markup: Option<&InlineKeyboardMarkup>,
    ) -> TelegramResult<Message> {
        let reply_parameters = ReplyParameters::new(reply_to_message_id);
        self.send_video_inner(
            chat_id,
            video,
            caption,
            SendVideoOptions {
                parse_mode: Some(HTML_PARSE_MODE),
                reply_parameters: Some(&reply_parameters),
                metadata: Some(metadata),
                reply_markup,
            },
        )
        .await
    }

    async fn send_video_inner(
        &self,
        chat_id: i64,
        video: &InputFile,
        caption: Option<&str>,
        options: SendVideoOptions<'_>,
    ) -> TelegramResult<Message> {
        self.call(
            "sendVideo",
            &SendVideoRequest {
                chat_id,
                video,
                caption,
                parse_mode: options.parse_mode,
                supports_streaming: true,
                width: options.metadata.map(|value| value.width),
                height: options.metadata.map(|value| value.height),
                duration: options.metadata.and_then(|value| value.duration),
                reply_parameters: options.reply_parameters,
                reply_markup: options.reply_markup,
            },
            None,
        )
        .await
    }

    pub async fn send_document(
        &self,
        chat_id: i64,
        document: &InputFile,
        caption: Option<&str>,
        reply_markup: Option<&InlineKeyboardMarkup>,
    ) -> TelegramResult<Message> {
        self.send_document_inner(chat_id, document, caption, None, None, reply_markup)
            .await
    }

    /// Sends a document whose caption contains Telegram-supported HTML.
    pub async fn send_document_html(
        &self,
        chat_id: i64,
        document: &InputFile,
        caption: Option<&str>,
        reply_markup: Option<&InlineKeyboardMarkup>,
    ) -> TelegramResult<Message> {
        self.send_document_inner(
            chat_id,
            document,
            caption,
            Some(HTML_PARSE_MODE),
            None,
            reply_markup,
        )
        .await
    }

    /// Sends an HTML-captioned document as a reply in the same chat.
    pub async fn send_document_reply_html(
        &self,
        chat_id: i64,
        document: &InputFile,
        caption: Option<&str>,
        reply_to_message_id: i64,
        reply_markup: Option<&InlineKeyboardMarkup>,
    ) -> TelegramResult<Message> {
        let reply_parameters = ReplyParameters::new(reply_to_message_id);
        self.send_document_inner(
            chat_id,
            document,
            caption,
            Some(HTML_PARSE_MODE),
            Some(&reply_parameters),
            reply_markup,
        )
        .await
    }

    async fn send_document_inner(
        &self,
        chat_id: i64,
        document: &InputFile,
        caption: Option<&str>,
        parse_mode: Option<&str>,
        reply_parameters: Option<&ReplyParameters>,
        reply_markup: Option<&InlineKeyboardMarkup>,
    ) -> TelegramResult<Message> {
        self.call(
            "sendDocument",
            &SendDocumentRequest {
                chat_id,
                document,
                caption,
                parse_mode,
                reply_parameters,
                reply_markup,
            },
            None,
        )
        .await
    }

    async fn call<T, B>(
        &self,
        method: &'static str,
        body: &B,
        timeout: Option<Duration>,
    ) -> TelegramResult<T>
    where
        T: DeserializeOwned,
        B: Serialize + ?Sized,
    {
        let endpoint = self.endpoint(method)?;
        let mut request = self.http.post(endpoint).json(body);
        if let Some(timeout) = timeout {
            request = request.timeout(timeout);
        }

        let response = request
            .send()
            .await
            .map_err(|error| TelegramError::Transport {
                method,
                reason: classify_reqwest_error(&error),
            })?;
        let status = response.status().as_u16();
        if response
            .content_length()
            .is_some_and(|length| length > MAX_API_RESPONSE_BYTES as u64)
        {
            return Err(TelegramError::InvalidResponse { method });
        }
        let mut bytes = Vec::new();
        let mut stream = response.bytes_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|error| TelegramError::Transport {
                method,
                reason: classify_reqwest_error(&error),
            })?;
            let next_len = bytes
                .len()
                .checked_add(chunk.len())
                .filter(|length| *length <= MAX_API_RESPONSE_BYTES)
                .ok_or(TelegramError::InvalidResponse { method })?;
            bytes.reserve(next_len.saturating_sub(bytes.len()));
            bytes.extend_from_slice(&chunk);
        }
        let envelope: ApiResponse<T> = serde_json::from_slice(&bytes)
            .map_err(|_| TelegramError::InvalidResponse { method })?;

        if !envelope.ok {
            let parameters = envelope.parameters.unwrap_or_default();
            return Err(TelegramError::Api {
                method,
                error_code: envelope.error_code,
                description: self.sanitize_description(
                    envelope
                        .description
                        .as_deref()
                        .unwrap_or("Telegram Bot API rejected the request"),
                ),
                retry_after: parameters.retry_after.map(Duration::from_secs),
                migrate_to_chat_id: parameters.migrate_to_chat_id,
            });
        }

        if !(200..300).contains(&status) {
            return Err(TelegramError::HttpStatus { method, status });
        }

        envelope
            .result
            .ok_or(TelegramError::InvalidResponse { method })
    }

    fn endpoint(&self, method: &'static str) -> TelegramResult<Url> {
        let endpoint = format!("{}bot{}/{}", self.base_url.as_str(), self.token, method);
        Url::parse(&endpoint).map_err(|_| TelegramError::Configuration {
            reason: "could not construct Bot API endpoint",
        })
    }

    fn sanitize_description(&self, description: &str) -> String {
        static URL_PATTERN: OnceLock<Regex> = OnceLock::new();
        static PATH_PATTERN: OnceLock<Regex> = OnceLock::new();
        let pattern = URL_PATTERN.get_or_init(|| {
            Regex::new(r#"(?i)\b(?:https?|file)://[^\s\"'<>]+"#)
                .expect("the redaction regex is a constant")
        });
        let path_pattern = PATH_PATTERN.get_or_init(|| {
            Regex::new(r#"(?m)(?:[A-Za-z]:\\|/)[^\s\"'<>]+"#)
                .expect("the path redaction regex is a constant")
        });

        let without_token = description.replace(self.token.as_ref(), "<redacted>");
        let without_base = without_token.replace(self.base_url.as_str(), "<redacted-url>");
        let redacted = pattern.replace_all(&without_base, "<redacted-url>");
        let redacted = path_pattern.replace_all(&redacted, "<redacted-path>");
        let mut value = redacted.chars().take(512).collect::<String>();
        if redacted.chars().count() > 512 {
            value.push('…');
        }
        value
    }
}

fn classify_reqwest_error(error: &reqwest::Error) -> &'static str {
    if error.is_timeout() {
        "request timed out"
    } else if error.is_connect() {
        "connection failed"
    } else if error.is_body() {
        "response body failed"
    } else if error.is_decode() {
        "response decoding failed"
    } else if error.is_request() {
        "request could not be sent"
    } else {
        "HTTP client failure"
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct InputFile {
    value: String,
}

impl InputFile {
    pub fn file_id(value: impl Into<String>) -> TelegramResult<Self> {
        let value = value.into();
        if value.trim().is_empty() || value.starts_with("file://") {
            return Err(TelegramError::InvalidInputFile {
                reason: "file_id must be non-empty and must not be a file URI",
            });
        }
        Ok(Self { value })
    }

    pub fn local_path(path: impl AsRef<Path>) -> TelegramResult<Self> {
        let path = path.as_ref();
        if !path.is_absolute() {
            return Err(TelegramError::InvalidInputFile {
                reason: "local media path must be absolute",
            });
        }
        let value = Url::from_file_path(path)
            .map_err(|_| TelegramError::InvalidInputFile {
                reason: "local media path cannot be represented as a file URI",
            })?
            .to_string();
        Ok(Self { value })
    }

    pub fn from_api_value(value: impl Into<String>) -> TelegramResult<Self> {
        let value = value.into();
        if !value.starts_with("file://") {
            return Self::file_id(value);
        }

        let url = Url::parse(&value).map_err(|_| TelegramError::InvalidInputFile {
            reason: "file URI is invalid",
        })?;
        let path = url
            .to_file_path()
            .map_err(|_| TelegramError::InvalidInputFile {
                reason: "file URI does not identify a local path",
            })?;
        Self::local_path(path)
    }

    pub fn as_api_value(&self) -> &str {
        &self.value
    }

    pub fn is_local(&self) -> bool {
        self.value.starts_with("file://")
    }
}

impl fmt::Debug for InputFile {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InputFile")
            .field("kind", &if self.is_local() { "local" } else { "file_id" })
            .field("value", &"<redacted>")
            .finish()
    }
}

impl Serialize for InputFile {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.value)
    }
}

/// Escapes untrusted text for Telegram's HTML parse mode.
///
/// The result is safe both as visible text and inside a double-quoted HTML
/// attribute, which is useful when building a caption with an `<a href>` link.
pub fn escape_html(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for character in value.chars() {
        match character {
            '&' => escaped.push_str("&amp;"),
            '<' => escaped.push_str("&lt;"),
            '>' => escaped.push_str("&gt;"),
            '"' => escaped.push_str("&quot;"),
            '\'' => escaped.push_str("&#39;"),
            _ => escaped.push(character),
        }
    }
    escaped
}

/// Display metadata passed explicitly to Telegram's `sendVideo` method.
///
/// Supplying the visible dimensions avoids Telegram inferring the coded width
/// (for example 1088 instead of 1080) and rendering a portrait H.265 preview
/// with the wrong aspect ratio.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VideoMetadata {
    pub width: u32,
    pub height: u32,
    pub duration: Option<u32>,
}

impl VideoMetadata {
    pub const fn new(width: u32, height: u32, duration: Option<u32>) -> Self {
        Self {
            width,
            height,
            duration,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct ReplyParameters {
    pub message_id: i64,
    pub allow_sending_without_reply: bool,
}

impl ReplyParameters {
    pub const fn new(message_id: i64) -> Self {
        Self {
            message_id,
            allow_sending_without_reply: true,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InlineKeyboardMarkup {
    pub inline_keyboard: Vec<Vec<InlineKeyboardButton>>,
}

impl InlineKeyboardMarkup {
    pub fn new(rows: Vec<Vec<InlineKeyboardButton>>) -> Self {
        Self {
            inline_keyboard: rows,
        }
    }

    pub fn single_row(buttons: Vec<InlineKeyboardButton>) -> Self {
        Self::new(vec![buttons])
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InlineKeyboardButton {
    pub text: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub callback_data: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
}

impl InlineKeyboardButton {
    pub fn callback(text: impl Into<String>, callback_data: impl Into<String>) -> Self {
        Self {
            text: text.into(),
            callback_data: Some(callback_data.into()),
            url: None,
        }
    }

    pub fn url(text: impl Into<String>, url: impl Into<String>) -> Self {
        Self {
            text: text.into(),
            callback_data: None,
            url: Some(url.into()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct User {
    pub id: u64,
    pub is_bot: bool,
    pub first_name: String,
    pub last_name: Option<String>,
    pub username: Option<String>,
    pub language_code: Option<String>,
    pub is_premium: Option<bool>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ChatMemberStatus {
    Creator,
    Administrator,
    Member,
    Restricted,
    Left,
    Kicked,
    #[serde(other)]
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct ChatMember {
    pub status: ChatMemberStatus,
    #[serde(default)]
    pub is_member: Option<bool>,
}

impl ChatMember {
    pub fn has_joined(&self) -> bool {
        match self.status {
            ChatMemberStatus::Creator
            | ChatMemberStatus::Administrator
            | ChatMemberStatus::Member => true,
            ChatMemberStatus::Restricted => self.is_member == Some(true),
            ChatMemberStatus::Left | ChatMemberStatus::Kicked | ChatMemberStatus::Unknown => false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ChatKind {
    Private,
    Group,
    Supergroup,
    Channel,
    #[serde(other)]
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Chat {
    pub id: i64,
    #[serde(rename = "type")]
    pub kind: ChatKind,
    pub title: Option<String>,
    pub username: Option<String>,
    pub first_name: Option<String>,
    pub last_name: Option<String>,
}

impl Chat {
    pub fn is_private(&self) -> bool {
        self.kind == ChatKind::Private
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Video {
    pub file_id: String,
    pub file_unique_id: String,
    pub width: u32,
    pub height: u32,
    pub duration: u32,
    pub file_name: Option<String>,
    pub mime_type: Option<String>,
    pub file_size: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Document {
    pub file_id: String,
    pub file_unique_id: String,
    pub file_name: Option<String>,
    pub mime_type: Option<String>,
    pub file_size: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Message {
    pub message_id: i64,
    pub date: i64,
    pub chat: Chat,
    #[serde(rename = "from")]
    pub sender: Option<User>,
    pub text: Option<String>,
    pub caption: Option<String>,
    pub reply_to_message: Option<Box<Message>>,
    pub video: Option<Video>,
    pub document: Option<Document>,
}

impl Message {
    pub fn sender_id(&self) -> Option<u64> {
        self.sender.as_ref().map(|user| user.id)
    }

    pub fn reply_text(&self) -> Option<&str> {
        self.reply_to_message
            .as_deref()
            .and_then(|message| message.text.as_deref())
    }

    pub fn media_file_ids(&self) -> Option<MediaFileIds<'_>> {
        if let Some(video) = &self.video {
            return Some(MediaFileIds {
                kind: TelegramMediaKind::Video,
                file_id: &video.file_id,
                file_unique_id: &video.file_unique_id,
            });
        }
        self.document.as_ref().map(|document| MediaFileIds {
            kind: TelegramMediaKind::Document,
            file_id: &document.file_id,
            file_unique_id: &document.file_unique_id,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MediaFileIds<'a> {
    pub kind: TelegramMediaKind,
    pub file_id: &'a str,
    pub file_unique_id: &'a str,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CallbackQuery {
    pub id: String,
    #[serde(rename = "from")]
    pub sender: User,
    pub message: Option<Message>,
    pub inline_message_id: Option<String>,
    pub chat_instance: Option<String>,
    pub data: Option<String>,
}

impl CallbackQuery {
    pub fn chat_id(&self) -> Option<i64> {
        self.message.as_ref().map(|message| message.chat.id)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Update {
    pub update_id: i64,
    pub message: Option<Message>,
    pub edited_message: Option<Message>,
    pub callback_query: Option<CallbackQuery>,
}

impl Update {
    pub fn private_text_message(&self) -> Option<&Message> {
        self.message
            .as_ref()
            .filter(|message| message.chat.is_private() && message.text.is_some())
    }
}

#[derive(Debug, Deserialize)]
struct ApiResponse<T> {
    ok: bool,
    result: Option<T>,
    description: Option<String>,
    error_code: Option<i64>,
    parameters: Option<ResponseParameters>,
}

#[derive(Debug, Default, Deserialize)]
struct ResponseParameters {
    migrate_to_chat_id: Option<i64>,
    retry_after: Option<u64>,
}

#[derive(Serialize)]
struct EmptyRequest {}

#[derive(Serialize)]
struct GetChatMemberRequest<'a> {
    chat_id: &'a str,
    user_id: u64,
}

#[derive(Serialize)]
struct GetUpdatesRequest<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    offset: Option<i64>,
    limit: u8,
    timeout: u32,
    allowed_updates: [&'a str; 1],
}

#[derive(Serialize)]
struct SendMessageRequest<'a> {
    chat_id: i64,
    text: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    parse_mode: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    reply_parameters: Option<&'a ReplyParameters>,
    #[serde(skip_serializing_if = "Option::is_none")]
    reply_markup: Option<&'a InlineKeyboardMarkup>,
}

#[derive(Serialize)]
struct EditMessageTextRequest<'a> {
    chat_id: i64,
    message_id: i64,
    text: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    parse_mode: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    reply_markup: Option<&'a InlineKeyboardMarkup>,
}

#[derive(Serialize)]
struct DeleteMessageRequest {
    chat_id: i64,
    message_id: i64,
}

#[derive(Serialize)]
struct AnswerCallbackQueryRequest<'a> {
    callback_query_id: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    text: Option<&'a str>,
    show_alert: bool,
    cache_time: u32,
}

#[derive(Serialize)]
struct SendVideoRequest<'a> {
    chat_id: i64,
    video: &'a InputFile,
    #[serde(skip_serializing_if = "Option::is_none")]
    caption: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    parse_mode: Option<&'a str>,
    supports_streaming: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    width: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    height: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    duration: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    reply_parameters: Option<&'a ReplyParameters>,
    #[serde(skip_serializing_if = "Option::is_none")]
    reply_markup: Option<&'a InlineKeyboardMarkup>,
}

#[derive(Serialize)]
struct SendDocumentRequest<'a> {
    chat_id: i64,
    document: &'a InputFile,
    #[serde(skip_serializing_if = "Option::is_none")]
    caption: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    parse_mode: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    reply_parameters: Option<&'a ReplyParameters>,
    #[serde(skip_serializing_if = "Option::is_none")]
    reply_markup: Option<&'a InlineKeyboardMarkup>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_chat_membership_statuses() {
        for status in ["creator", "administrator", "member"] {
            let member: ChatMember =
                serde_json::from_value(serde_json::json!({ "status": status })).unwrap();
            assert!(member.has_joined(), "{status}");
            assert_eq!(member.is_member, None);
        }

        let restricted_member: ChatMember = serde_json::from_value(serde_json::json!({
            "status": "restricted",
            "is_member": true
        }))
        .unwrap();
        assert!(restricted_member.has_joined());

        for value in [
            serde_json::json!({ "status": "restricted", "is_member": false }),
            serde_json::json!({ "status": "restricted" }),
            serde_json::json!({ "status": "left" }),
            serde_json::json!({ "status": "kicked" }),
            serde_json::json!({ "status": "future_status", "is_member": true }),
        ] {
            let member: ChatMember = serde_json::from_value(value).unwrap();
            assert!(!member.has_joined(), "{member:?}");
        }
    }

    #[test]
    fn serializes_get_chat_member_request() {
        let request = GetChatMemberRequest {
            chat_id: "@required_channel",
            user_id: 42,
        };
        let value = serde_json::to_value(request).unwrap();

        assert_eq!(value["chat_id"], "@required_channel");
        assert_eq!(value["user_id"], 42);
    }

    #[test]
    fn long_polling_requests_only_message_updates() {
        let request = GetUpdatesRequest {
            offset: Some(101),
            limit: 100,
            timeout: 30,
            allowed_updates: ["message"],
        };
        let value = serde_json::to_value(request).unwrap();

        assert_eq!(value["offset"], 101);
        assert_eq!(value["allowed_updates"], serde_json::json!(["message"]));
    }

    #[test]
    fn deserializes_private_text_reply_and_callback_update() {
        let json = r#"
        {
          "ok": true,
          "result": [
            {
              "update_id": 101,
              "message": {
                "message_id": 11,
                "date": 1720000000,
                "chat": {"id": 42, "type": "private", "first_name": "A"},
                "from": {"id": 42, "is_bot": false, "first_name": "A"},
                "text": "https://weixin.qq.com/sph/example",
                "reply_to_message": {
                  "message_id": 10,
                  "date": 1719999999,
                  "chat": {"id": 42, "type": "private"},
                  "from": {"id": 42, "is_bot": false, "first_name": "A"},
                  "text": "https://weixin.qq.com/sph/example"
                }
              }
            },
            {
              "update_id": 102,
              "callback_query": {
                "id": "callback-1",
                "from": {"id": 42, "is_bot": false, "first_name": "A"},
                "chat_instance": "instance-1",
                "data": "download:nonce",
                "message": {
                  "message_id": 12,
                  "date": 1720000001,
                  "chat": {"id": 42, "type": "private"},
                  "text": "请选择"
                }
              }
            }
          ]
        }
        "#;

        let response: ApiResponse<Vec<Update>> = serde_json::from_str(json).unwrap();
        let updates = response.result.unwrap();
        let message = updates[0].private_text_message().unwrap();
        assert_eq!(message.sender_id(), Some(42));
        assert_eq!(
            message.reply_text(),
            Some("https://weixin.qq.com/sph/example")
        );

        let callback = updates[1].callback_query.as_ref().unwrap();
        assert_eq!(callback.data.as_deref(), Some("download:nonce"));
        assert_eq!(callback.chat_id(), Some(42));
    }

    #[test]
    fn deserializes_video_and_document_file_identifiers() {
        let video_json = r#"
        {
          "message_id": 20,
          "date": 1720000100,
          "chat": {"id": 42, "type": "private"},
          "video": {
            "file_id": "video-file-id",
            "file_unique_id": "video-unique-id",
            "width": 1920,
            "height": 1080,
            "duration": 60,
            "file_size": 123456
          }
        }
        "#;
        let video: Message = serde_json::from_str(video_json).unwrap();
        let ids = video.media_file_ids().unwrap();
        assert_eq!(ids.kind, TelegramMediaKind::Video);
        assert_eq!(ids.file_id, "video-file-id");
        assert_eq!(ids.file_unique_id, "video-unique-id");

        let document_json = r#"
        {
          "message_id": 21,
          "date": 1720000101,
          "chat": {"id": 42, "type": "private"},
          "document": {
            "file_id": "document-file-id",
            "file_unique_id": "document-unique-id",
            "file_name": "video.mp4",
            "mime_type": "video/mp4",
            "file_size": 234567
          }
        }
        "#;
        let document: Message = serde_json::from_str(document_json).unwrap();
        let ids = document.media_file_ids().unwrap();
        assert_eq!(ids.kind, TelegramMediaKind::Document);
        assert_eq!(ids.file_id, "document-file-id");
        assert_eq!(ids.file_unique_id, "document-unique-id");
    }

    #[test]
    fn deserializes_retry_after_from_api_error() {
        let json = r#"
        {
          "ok": false,
          "error_code": 429,
          "description": "Too Many Requests: retry after 7",
          "parameters": {"retry_after": 7}
        }
        "#;
        let response: ApiResponse<serde_json::Value> = serde_json::from_str(json).unwrap();
        assert!(!response.ok);
        assert_eq!(response.error_code, Some(429));
        assert_eq!(response.parameters.unwrap().retry_after, Some(7));
    }

    #[test]
    fn serializes_inline_keyboard_and_input_files() {
        let keyboard = InlineKeyboardMarkup::single_row(vec![InlineKeyboardButton::callback(
            "操作",
            "action:nonce",
        )]);
        let value = serde_json::to_value(&keyboard).unwrap();
        assert_eq!(
            value["inline_keyboard"][0][0]["callback_data"],
            "action:nonce"
        );

        let file_id = InputFile::file_id("cached-file-id").unwrap();
        assert_eq!(serde_json::to_value(&file_id).unwrap(), "cached-file-id");

        #[cfg(unix)]
        {
            let local = InputFile::local_path("/tmp/video with spaces.mp4").unwrap();
            assert!(local.is_local());
            assert_eq!(
                serde_json::to_value(&local).unwrap(),
                "file:///tmp/video%20with%20spaces.mp4"
            );
        }
    }

    #[test]
    fn serializes_reply_parameters_for_source_message() {
        let reply_parameters = ReplyParameters::new(73);
        let request = SendMessageRequest {
            chat_id: 42,
            text: "正在解析链接…",
            parse_mode: Some(HTML_PARSE_MODE),
            reply_parameters: Some(&reply_parameters),
            reply_markup: None,
        };

        let value = serde_json::to_value(request).unwrap();
        assert_eq!(value["reply_parameters"]["message_id"], 73);
        assert_eq!(
            value["reply_parameters"]["allow_sending_without_reply"],
            true
        );
        assert!(value.get("reply_to_message_id").is_none());
        assert!(value.get("reply_markup").is_none());
        assert_eq!(value["parse_mode"], "HTML");

        let edit = serde_json::to_value(EditMessageTextRequest {
            chat_id: 42,
            message_id: 74,
            text: "<b>▎下 载 中... | 20%</b>",
            parse_mode: Some(HTML_PARSE_MODE),
            reply_markup: None,
        })
        .unwrap();
        assert_eq!(edit["parse_mode"], "HTML");
        assert_eq!(edit["message_id"], 74);
    }

    #[test]
    fn serializes_html_parse_mode_for_media_captions() {
        let input = InputFile::file_id("cached-file-id").unwrap();
        let reply_parameters = ReplyParameters::new(73);
        let video = serde_json::to_value(SendVideoRequest {
            chat_id: 42,
            video: &input,
            caption: Some("标题\n<a href=\"https://example.com\">来源</a>"),
            parse_mode: Some(HTML_PARSE_MODE),
            supports_streaming: true,
            width: Some(1080),
            height: Some(1920),
            duration: Some(7),
            reply_parameters: Some(&reply_parameters),
            reply_markup: None,
        })
        .unwrap();
        assert_eq!(video["parse_mode"], "HTML");
        assert_eq!(video["supports_streaming"], true);
        assert_eq!(video["width"], 1080);
        assert_eq!(video["height"], 1920);
        assert_eq!(video["duration"], 7);
        assert_eq!(video["reply_parameters"]["message_id"], 73);

        let document = serde_json::to_value(SendDocumentRequest {
            chat_id: 42,
            document: &input,
            caption: Some("标题\n<a href=\"https://example.com\">来源</a>"),
            parse_mode: Some(HTML_PARSE_MODE),
            reply_parameters: Some(&reply_parameters),
            reply_markup: None,
        })
        .unwrap();
        assert_eq!(document["parse_mode"], "HTML");
        assert_eq!(document["reply_parameters"]["message_id"], 73);
    }

    #[test]
    fn omits_video_display_metadata_when_not_supplied() {
        let input = InputFile::file_id("cached-file-id").unwrap();
        let video = serde_json::to_value(SendVideoRequest {
            chat_id: 42,
            video: &input,
            caption: None,
            parse_mode: None,
            supports_streaming: true,
            width: None,
            height: None,
            duration: None,
            reply_parameters: None,
            reply_markup: None,
        })
        .unwrap();

        assert!(video.get("width").is_none());
        assert!(video.get("height").is_none());
        assert!(video.get("duration").is_none());
    }

    #[test]
    fn escapes_untrusted_html_text_and_attributes() {
        assert_eq!(
            escape_html("A&B <标题> \"来源\" '视频号'"),
            "A&amp;B &lt;标题&gt; &quot;来源&quot; &#39;视频号&#39;"
        );
        assert_eq!(
            escape_html("https://example.com/?a=1&b=\"2\""),
            "https://example.com/?a=1&amp;b=&quot;2&quot;"
        );
    }

    #[test]
    fn debug_and_api_errors_redact_credentials_and_urls() {
        let token = "123456:TOP_SECRET";
        let base = Url::parse("http://127.0.0.1:8081/private/").unwrap();
        let client = TelegramClient::new(base.clone(), token).unwrap();
        let debug = format!("{client:?}");
        assert!(!debug.contains(token));
        assert!(!debug.contains(base.as_str()));

        let description = format!(
            "request to {}bot{token}/sendVideo failed; file://private/video.mp4; stat /Users/private/video.mp4 failed",
            base.as_str()
        );
        let redacted = client.sanitize_description(&description);
        assert!(!redacted.contains(token));
        assert!(!redacted.contains(base.as_str()));
        assert!(!redacted.contains("file://private/video.mp4"));
        assert!(!redacted.contains("/Users/private/video.mp4"));
    }
}
