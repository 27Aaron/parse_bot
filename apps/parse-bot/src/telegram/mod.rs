use std::str::FromStr;

pub mod api;
pub mod bot;
mod tdlib;

/// Telegram Bot 单文件上传的固定上限（十进制 2000 MB）。
pub const TELEGRAM_FILE_LIMIT_BYTES: u64 = 2_000_000_000;

pub use bot::BotService;
pub use tdlib::{TdlibConfig, TelegramClient};

/// How a cached/uploaded file is represented to Telegram clients.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TelegramMediaKind {
    Video,
    Document,
}

impl TelegramMediaKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Video => "video",
            Self::Document => "document",
        }
    }
}

impl FromStr for TelegramMediaKind {
    type Err = ();

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        match value {
            "video" => Ok(Self::Video),
            "document" => Ok(Self::Document),
            _ => Err(()),
        }
    }
}
