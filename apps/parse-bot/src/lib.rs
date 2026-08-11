//! Telegram delivery shell for [`parse_core`].
//!
//! This package owns chat interaction, persistence, and process configuration.
//! Platform resolve/download logic lives in `parse-core` so other delivery
//! targets (Feishu, CLI, …) can reuse it without this bot.

pub mod config;
pub mod error;
pub mod i18n;
pub mod storage;
pub mod telegram;

pub use error::{AppError, Result};

// Re-export the resolve/download surface so the binary and tests have a single
// dependency edge when assembling the bot.
pub use parse_core::{
    ParseHub, ResolvedPost,
    media::{self, DownloadedMedia, MediaDownloader, MediaProbe, decrypt_file_prefix, probe_media},
    model::{MediaSource, MediaSourceKind, REVIEWED_WECHAT_MEDIA_HOSTS, VideoCodec},
    wechat,
};
