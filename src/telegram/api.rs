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

pub type TelegramResult<T> = std::result::Result<T, TelegramError>;

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

    pub async fn get_updates(
        &self,
        offset: Option<i64>,
        timeout_secs: u32,
    ) -> TelegramResult<Vec<Update>> {
        let request = GetUpdatesRequest {
            offset,
            limit: 100,
            timeout: timeout_secs,
            allowed_updates: ["message", "callback_query"],
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
        self.call(
            "sendMessage",
            &SendMessageRequest {
                chat_id,
                text,
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
        self.call(
            "editMessageText",
            &EditMessageTextRequest {
                chat_id,
                message_id,
                text,
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
        self.call(
            "sendVideo",
            &SendVideoRequest {
                chat_id,
                video,
                caption,
                supports_streaming: true,
                reply_markup,
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
        self.call(
            "sendDocument",
            &SendDocumentRequest {
                chat_id,
                document,
                caption,
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
struct GetUpdatesRequest<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    offset: Option<i64>,
    limit: u8,
    timeout: u32,
    allowed_updates: [&'a str; 2],
}

#[derive(Serialize)]
struct SendMessageRequest<'a> {
    chat_id: i64,
    text: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    reply_markup: Option<&'a InlineKeyboardMarkup>,
}

#[derive(Serialize)]
struct EditMessageTextRequest<'a> {
    chat_id: i64,
    message_id: i64,
    text: &'a str,
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
    supports_streaming: bool,
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
    reply_markup: Option<&'a InlineKeyboardMarkup>,
}

#[cfg(test)]
mod tests {
    use super::*;

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
                "text": "/parse",
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
            "file_name": "original.mp4",
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
        let keyboard = InlineKeyboardMarkup::single_row(vec![
            InlineKeyboardButton::callback("下载视频", "compatible:nonce"),
            InlineKeyboardButton::callback("下载原视频", "original:nonce"),
        ]);
        let value = serde_json::to_value(&keyboard).unwrap();
        assert_eq!(
            value["inline_keyboard"][0][0]["callback_data"],
            "compatible:nonce"
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
