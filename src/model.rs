use std::{fmt, str::FromStr};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use url::Url;

pub(crate) const REVIEWED_WECHAT_MEDIA_HOSTS: &[&str] = &[
    "finder.video.qq.com",
    "findermp.video.qq.com",
    "finder.video.wechat.com",
    "findermp.video.wechat.com",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VideoCodec {
    H264,
    H265,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MediaProvenance {
    H264,
    H265,
    Generic,
    ExplicitOrigin,
    DerivedOriginal,
}

#[derive(Clone)]
pub struct MediaSource {
    pub url: Url,
    pub codec: VideoCodec,
    pub provenance: MediaProvenance,
    pub width: Option<u32>,
    pub height: Option<u32>,
    pub size_hint: Option<u64>,
    pub decode_key: Option<u64>,
}

impl fmt::Debug for MediaSource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MediaSource")
            .field("host", &self.url.host_str().unwrap_or("<invalid>"))
            .field("url", &"<redacted>")
            .field("codec", &self.codec)
            .field("provenance", &self.provenance)
            .field("width", &self.width)
            .field("height", &self.height)
            .field("size_hint", &self.size_hint)
            .field("has_decode_key", &self.decode_key.is_some())
            .finish()
    }
}

#[derive(Clone)]
pub struct ResolvedPost {
    pub platform: String,
    pub post_id: String,
    pub canonical_url: Url,
    pub author: Option<String>,
    pub title: Option<String>,
    pub description: Option<String>,
    pub cover_url: Option<Url>,
    pub video: MediaSource,
    pub expires_at: Option<DateTime<Utc>>,
}

impl fmt::Debug for ResolvedPost {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ResolvedPost")
            .field("platform", &self.platform)
            .field("post_id", &self.post_id)
            .field("canonical_url", &"<redacted>")
            .field("author", &self.author)
            .field("title", &self.title)
            .field("has_cover", &self.cover_url.is_some())
            .field("video", &self.video)
            .field("expires_at", &self.expires_at)
            .finish()
    }
}

impl ResolvedPost {
    pub fn display_title(&self) -> String {
        self.title
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .or_else(|| {
                self.description
                    .as_deref()
                    .filter(|value| !value.trim().is_empty())
            })
            .unwrap_or("微信视频号视频")
            .chars()
            .take(180)
            .collect()
    }
}

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
