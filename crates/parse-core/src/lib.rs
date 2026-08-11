//! Multi-platform social media resolve and download core.
//!
//! This crate is delivery-agnostic: it does not know about Telegram, Feishu, or
//! other chat products. Apps depend on [`ParseHub`] plus [`media`] helpers.

pub mod error;
pub mod hub;
pub mod media;
pub mod model;
pub mod wechat;

pub use error::{Error, Result};
pub use hub::ParseHub;
pub use model::{
    MediaSource, MediaSourceKind, REVIEWED_WECHAT_MEDIA_HOSTS, ResolvedPost, VideoCodec,
};
