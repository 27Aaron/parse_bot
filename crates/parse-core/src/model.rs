use std::fmt;

use serde::{Deserialize, Serialize};
use url::Url;

/// Reviewed WeChat Channels CDN hosts allowed for media download.
pub const REVIEWED_WECHAT_MEDIA_HOSTS: &[&str] = &[
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
pub enum MediaSourceKind {
    H264,
    H265,
    Generic,
    Direct,
    Derived,
}

#[derive(Clone)]
pub struct MediaSource {
    pub url: Url,
    pub codec: VideoCodec,
    pub provenance: MediaSourceKind,
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

/// Platform-agnostic resolved post ready for download / delivery.
#[derive(Clone)]
pub struct ResolvedPost {
    pub platform: String,
    pub post_id: String,
    pub canonical_url: Url,
    pub title: Option<String>,
    pub cover_url: Option<Url>,
    pub video: MediaSource,
    pub fallback_videos: Vec<MediaSource>,
}

impl fmt::Debug for ResolvedPost {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ResolvedPost")
            .field("platform", &self.platform)
            .field("post_id", &self.post_id)
            .field("canonical_url", &"<redacted>")
            .field("title", &self.title)
            .field("has_cover", &self.cover_url.is_some())
            .field("video", &self.video)
            .field("fallback_video_count", &self.fallback_videos.len())
            .finish()
    }
}

impl ResolvedPost {
    pub fn media_sources(&self) -> impl Iterator<Item = &MediaSource> {
        std::iter::once(&self.video).chain(self.fallback_videos.iter())
    }

    pub fn display_title(&self) -> String {
        self.title
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or(default_title_for_platform(&self.platform))
            .chars()
            .take(180)
            .collect()
    }
}

fn default_title_for_platform(platform: &str) -> &'static str {
    match platform {
        "wechat_channels" => "微信视频号视频",
        // Future platforms can add their own fallback titles here.
        _ => "视频",
    }
}
