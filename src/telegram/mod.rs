pub mod api;
pub mod bot;
mod tdlib;

pub use bot::BotService;
pub use tdlib::{TdlibConfig, TelegramClient};
