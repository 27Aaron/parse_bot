use std::{
    fmt,
    path::{Path, PathBuf},
    sync::OnceLock,
    time::Duration,
};

use regex::Regex;
use url::Url;

use crate::{AppError, model::TelegramMediaKind};

pub use super::tdlib::TelegramClient;

pub type TelegramResult<T> = std::result::Result<T, TelegramError>;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TelegramError {
    #[error("TDLib configuration is invalid: {reason}")]
    Configuration { reason: &'static str },

    #[error("TDLib runtime failure during {operation}: {description}")]
    Runtime {
        operation: &'static str,
        description: String,
    },

    #[error("Telegram rejected {method} with code {error_code:?}: {description}")]
    Api {
        method: &'static str,
        error_code: Option<i64>,
        description: String,
        retry_after: Option<Duration>,
    },

    #[error("invalid Telegram input file: {reason}")]
    InvalidInputFile { reason: &'static str },

    #[error("TDLib client is closed")]
    Closed,
}

impl TelegramError {
    pub(crate) fn from_tdlib(method: &'static str, error: tdlib_rs::types::Error) -> Self {
        let description = sanitize_description(&error.message);
        let retry_after = parse_retry_after(&description);
        Self::Api {
            method,
            error_code: Some(i64::from(error.code)),
            description,
            retry_after,
        }
    }

    pub(crate) fn runtime(operation: &'static str, description: impl AsRef<str>) -> Self {
        Self::Runtime {
            operation,
            description: sanitize_description(description.as_ref()),
        }
    }

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

    pub fn is_rate_limited(&self) -> bool {
        matches!(self.error_code(), Some(420 | 429)) || self.retry_after().is_some()
    }

    /// Returns whether retrying the same video without its optional cover can
    /// make progress. Generic edit failures and rate limits must not trigger a
    /// second upload of the main media file.
    pub fn is_cover_failure(&self) -> bool {
        match self {
            Self::Runtime { operation, .. } => {
                matches!(
                    *operation,
                    "download cover" | "inspect cover" | "prepare cover"
                )
            }
            Self::InvalidInputFile { reason } => reason.to_ascii_lowercase().contains("cover"),
            Self::Api {
                error_code: Some(400),
                description,
                ..
            } => {
                let description = description.to_ascii_uppercase();
                [
                    "COVER",
                    "THUMBNAIL",
                    "WEBPAGE_CURL_FAILED",
                    "WEBPAGE_MEDIA_EMPTY",
                    "PHOTO_INVALID_DIMENSIONS",
                    "PHOTO_EXT_INVALID",
                    "PHOTO_CONTENT_TYPE_INVALID",
                    "PHOTO_INVALID",
                    "PHOTO_SAVE_FILE_INVALID",
                    "IMAGE_PROCESS_FAILED",
                ]
                .iter()
                .any(|marker| description.contains(marker))
            }
            _ => false,
        }
    }

    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Closed
                | Self::Runtime {
                    operation: "receive",
                    ..
                }
        )
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

fn parse_retry_after(description: &str) -> Option<Duration> {
    static RETRY_PATTERN: OnceLock<Regex> = OnceLock::new();
    let pattern = RETRY_PATTERN.get_or_init(|| {
        Regex::new(r"(?i)(?:retry after|flood_wait_)[^0-9]*(\d{1,9})")
            .expect("the retry-after regex is a constant")
    });
    pattern
        .captures(description)
        .and_then(|captures| captures.get(1))
        .and_then(|value| value.as_str().parse::<u64>().ok())
        .map(Duration::from_secs)
}

fn sanitize_description(description: &str) -> String {
    static URL_PATTERN: OnceLock<Regex> = OnceLock::new();
    static PATH_PATTERN: OnceLock<Regex> = OnceLock::new();
    static TOKEN_PATTERN: OnceLock<Regex> = OnceLock::new();
    static OPAQUE_ID_PATTERN: OnceLock<Regex> = OnceLock::new();

    let url_pattern = URL_PATTERN.get_or_init(|| {
        Regex::new(r#"(?i)\b(?:https?|file)://[^\s\"'<>]+"#)
            .expect("the URL redaction regex is a constant")
    });
    let path_pattern = PATH_PATTERN.get_or_init(|| {
        Regex::new(r#"(?m)(?:[A-Za-z]:\\|/)[^\s\"'<>]+"#)
            .expect("the path redaction regex is a constant")
    });
    let token_pattern = TOKEN_PATTERN.get_or_init(|| {
        Regex::new(r"\b\d{5,}:[A-Za-z0-9_-]{20,}\b")
            .expect("the token redaction regex is a constant")
    });
    let opaque_id_pattern = OPAQUE_ID_PATTERN.get_or_init(|| {
        Regex::new(r"\b[A-Za-z0-9_-]{64,}\b")
            .expect("the opaque identifier redaction regex is a constant")
    });

    let redacted = token_pattern.replace_all(description, "<redacted>");
    let redacted = url_pattern.replace_all(&redacted, "<redacted-url>");
    let redacted = path_pattern.replace_all(&redacted, "<redacted-path>");
    let redacted = opaque_id_pattern.replace_all(&redacted, "<redacted-id>");
    let mut value = redacted.chars().take(512).collect::<String>();
    if redacted.chars().count() > 512 {
        value.push('…');
    }
    value
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BotCommand {
    pub command: String,
    pub description: String,
}

impl BotCommand {
    pub fn new(command: impl Into<String>, description: impl Into<String>) -> Self {
        Self {
            command: command.into(),
            description: description.into(),
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct InputFile {
    source: InputFileSource,
}

#[derive(Clone, PartialEq, Eq)]
pub(crate) enum InputFileSource {
    RemoteId(String),
    LocalPath(PathBuf),
    HttpUrl(Url),
}

impl InputFile {
    pub fn file_id(value: impl Into<String>) -> TelegramResult<Self> {
        let value = value.into();
        if value.trim().is_empty()
            || value.starts_with("file://")
            || value.starts_with("http://")
            || value.starts_with("https://")
        {
            return Err(TelegramError::InvalidInputFile {
                reason: "file_id must be a non-empty Telegram remote identifier",
            });
        }
        Ok(Self {
            source: InputFileSource::RemoteId(value),
        })
    }

    pub fn local_path(path: impl AsRef<Path>) -> TelegramResult<Self> {
        let path = path.as_ref();
        if !path.is_absolute() {
            return Err(TelegramError::InvalidInputFile {
                reason: "local media path must be absolute",
            });
        }
        Ok(Self {
            source: InputFileSource::LocalPath(path.to_path_buf()),
        })
    }

    pub fn http_url(url: &Url) -> TelegramResult<Self> {
        if url.scheme() != "https"
            || url.host_str().is_none()
            || !url.username().is_empty()
            || url.password().is_some()
            || url.fragment().is_some()
        {
            return Err(TelegramError::InvalidInputFile {
                reason: "remote media URL must be HTTPS without credentials or fragment",
            });
        }
        Ok(Self {
            source: InputFileSource::HttpUrl(url.clone()),
        })
    }

    pub fn is_local(&self) -> bool {
        matches!(self.source, InputFileSource::LocalPath(_))
    }

    pub(crate) fn source(&self) -> &InputFileSource {
        &self.source
    }
}

impl fmt::Debug for InputFile {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let kind = match self.source {
            InputFileSource::RemoteId(_) => "remote_id",
            InputFileSource::LocalPath(_) => "local_path",
            InputFileSource::HttpUrl(_) => "http_url",
        };
        formatter
            .debug_struct("InputFile")
            .field("kind", &kind)
            .field("value", &"<redacted>")
            .finish()
    }
}

/// Escapes untrusted text before it is parsed using TDLib's Bot API-compatible
/// HTML parser.
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InlineKeyboardMarkup {
    pub inline_keyboard: Vec<Vec<InlineKeyboardButton>>,
}

impl InlineKeyboardMarkup {
    pub fn new(rows: Vec<Vec<InlineKeyboardButton>>) -> Self {
        Self {
            inline_keyboard: rows,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InlineKeyboardButton {
    pub text: String,
    pub callback_data: Option<String>,
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
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct User {
    pub id: u64,
    pub is_bot: bool,
    pub first_name: String,
    pub last_name: Option<String>,
    pub username: Option<String>,
    pub language_code: Option<String>,
    pub is_premium: Option<bool>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChatMemberStatus {
    Creator,
    Administrator,
    Member,
    Restricted,
    Left,
    Kicked,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChatMember {
    pub status: ChatMemberStatus,
    pub is_member: Option<bool>,
}

impl ChatMember {
    pub fn has_joined(&self) -> bool {
        match self.status {
            ChatMemberStatus::Creator => self.is_member == Some(true),
            ChatMemberStatus::Administrator | ChatMemberStatus::Member => true,
            ChatMemberStatus::Restricted => self.is_member == Some(true),
            ChatMemberStatus::Left | ChatMemberStatus::Kicked | ChatMemberStatus::Unknown => false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChatKind {
    Private,
    Group,
    Supergroup,
    Channel,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Chat {
    pub id: i64,
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

#[derive(Debug, Clone, PartialEq, Eq)]
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Document {
    pub file_id: String,
    pub file_unique_id: String,
    pub file_name: Option<String>,
    pub mime_type: Option<String>,
    pub file_size: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Message {
    pub message_id: i64,
    pub date: i64,
    pub chat: Chat,
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

    pub fn media_file_ids(&self) -> Option<MediaFileIds<'_>> {
        if let Some(video) = &self.video
            && !video.file_id.is_empty()
        {
            return Some(MediaFileIds {
                kind: TelegramMediaKind::Video,
                file_id: &video.file_id,
                file_unique_id: &video.file_unique_id,
            });
        }
        self.document
            .as_ref()
            .filter(|document| !document.file_id.is_empty())
            .map(|document| MediaFileIds {
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CallbackQuery {
    pub id: String,
    pub sender: User,
    pub message: Option<Message>,
    pub inline_message_id: Option<String>,
    pub chat_instance: Option<String>,
    pub data: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Update {
    pub message: Option<Message>,
    pub callback_query: Option<CallbackQuery>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn escapes_untrusted_html_text_and_attributes() {
        assert_eq!(escape_html("<&>\"'"), "&lt;&amp;&gt;&quot;&#39;");
    }

    #[test]
    fn input_file_debug_never_exposes_its_value() {
        let input = InputFile::file_id("opaque-telegram-file-id").unwrap();
        let debug = format!("{input:?}");
        assert!(debug.contains("remote_id"));
        assert!(!debug.contains("opaque-telegram-file-id"));
        assert!(InputFile::local_path("relative/file.mp4").is_err());
    }

    #[test]
    fn tdlib_errors_redact_credentials_urls_paths_and_long_ids() {
        let error = tdlib_rs::types::Error {
            code: 429,
            message: format!(
                "retry after 7 token 123456789:abcdefghijklmnopqrstuvwxyz_123456 URL https://example.test/private path /tmp/private/file id {}",
                "A".repeat(80)
            ),
        };
        let error = TelegramError::from_tdlib("sendMessage", error);
        let rendered = error.to_string();
        assert_eq!(error.retry_after(), Some(Duration::from_secs(7)));
        assert!(!rendered.contains("123456789:"));
        assert!(!rendered.contains("example.test"));
        assert!(!rendered.contains("/tmp/private"));
        assert!(!rendered.contains(&"A".repeat(80)));
    }

    #[test]
    fn only_receive_shutdown_errors_are_terminal() {
        assert!(TelegramError::Closed.is_terminal());
        assert!(TelegramError::runtime("receive", "pump stopped").is_terminal());
        assert!(!TelegramError::runtime("sendMessage", "temporary failure").is_terminal());
    }

    #[test]
    fn retries_without_cover_only_for_cover_specific_failures() {
        assert!(TelegramError::runtime("download cover", "temporary failure").is_cover_failure());
        assert!(
            TelegramError::InvalidInputFile {
                reason: "downloaded video cover is not a supported image"
            }
            .is_cover_failure()
        );
        assert!(
            TelegramError::Api {
                method: "editMessageMedia",
                error_code: Some(400),
                description: "WEBPAGE_MEDIA_EMPTY".into(),
                retry_after: None,
            }
            .is_cover_failure()
        );
        assert!(
            TelegramError::Api {
                method: "editMessageMedia",
                error_code: Some(400),
                description: "PHOTO_INVALID_DIMENSIONS".into(),
                retry_after: None,
            }
            .is_cover_failure()
        );

        for error in [
            TelegramError::Api {
                method: "editMessageMedia",
                error_code: Some(400),
                description: "MESSAGE_NOT_MODIFIED".into(),
                retry_after: None,
            },
            TelegramError::Api {
                method: "editMessageMedia",
                error_code: Some(429),
                description: "cover flood wait".into(),
                retry_after: Some(Duration::from_secs(5)),
            },
            TelegramError::runtime("editMessageMedia", "temporary failure"),
            TelegramError::Closed,
        ] {
            assert!(!error.is_cover_failure(), "unexpected cover retry: {error}");
        }
    }

    #[test]
    fn empty_remote_media_identifier_is_not_cacheable() {
        let message = Message {
            message_id: 1,
            date: 0,
            chat: Chat {
                id: 1,
                kind: ChatKind::Private,
                title: None,
                username: None,
                first_name: None,
                last_name: None,
            },
            sender: None,
            text: None,
            caption: None,
            reply_to_message: None,
            video: Some(Video {
                file_id: String::new(),
                file_unique_id: String::new(),
                width: 1,
                height: 1,
                duration: 1,
                file_name: None,
                mime_type: None,
                file_size: None,
            }),
            document: None,
        };
        assert!(message.media_file_ids().is_none());
    }

    #[test]
    fn creator_must_still_be_a_channel_member() {
        let active_creator = ChatMember {
            status: ChatMemberStatus::Creator,
            is_member: Some(true),
        };
        let departed_creator = ChatMember {
            status: ChatMemberStatus::Creator,
            is_member: Some(false),
        };

        assert!(active_creator.has_joined());
        assert!(!departed_creator.has_joined());
    }
}
