pub mod api;
pub mod bot;
mod tdlib;

/// Telegram Bot 单文件上传的固定上限（十进制 2000 MB）。
pub const TELEGRAM_FILE_LIMIT_BYTES: u64 = 2_000_000_000;

pub use bot::BotService;
pub use tdlib::{TdlibConfig, TelegramClient};
