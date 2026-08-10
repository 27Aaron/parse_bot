use std::{sync::Arc, time::Duration};

use chrono::{DateTime, Utc};
use futures_util::StreamExt;
use regex::Regex;
use reqwest::{
    Client, Response, StatusCode,
    header::{ACCEPT, ACCEPT_LANGUAGE, CONTENT_TYPE, COOKIE, ORIGIN, REFERER, USER_AGENT},
    redirect::Policy,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use url::Url;
use uuid::Uuid;

use crate::{
    AppError, Result,
    model::{MediaProvenance, MediaSource, REVIEWED_WECHAT_MEDIA_HOSTS, ResolvedPost, VideoCodec},
};

const PARSE_ENDPOINT: &str = "https://yuanbao.tencent.com/api/weixin/get_parse_result";
const FEED_ENDPOINT: &str = "https://channels.weixin.qq.com/finder-preview/api/feed/get_feed_info";
const YUANBAO_ORIGIN: &str = "https://yuanbao.tencent.com";
const CHANNELS_ORIGIN: &str = "https://channels.weixin.qq.com";
const YUANBAO_AGENT_ID: &str = "naQivTmsDa/cf4d0079-ed1b-4c55-a3f3-2ca1379727d1";
const YUANBAO_REFERER: &str =
    "https://yuanbao.tencent.com/chat/naQivTmsDa/cf4d0079-ed1b-4c55-a3f3-2ca1379727d1";
const USER_AGENT_VALUE: &str = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) \
    AppleWebKit/537.36 (KHTML, like Gecko) Chrome/148.0.0.0 Safari/537.36";
const SEC_CH_UA_VALUE: &str =
    r#""Chromium";v="148", "Google Chrome";v="148", "Not/A)Brand";v="99""#;
const MAX_JSON_BYTES: usize = 2 * 1024 * 1024;
#[derive(Clone)]
pub struct WechatResolver {
    client: Client,
    cookie: Arc<str>,
    parse_endpoint: Url,
    feed_endpoint: Url,
    timeout: Duration,
}

impl std::fmt::Debug for WechatResolver {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("WechatResolver")
            .field("cookie", &"<redacted>")
            .field("endpoints", &"<redacted>")
            .finish_non_exhaustive()
    }
}

impl WechatResolver {
    pub fn new(cookie: impl Into<String>, timeout: Duration) -> Result<Self> {
        Self::with_endpoints(
            cookie,
            timeout,
            Url::parse(PARSE_ENDPOINT).expect("constant parse endpoint must be valid"),
            Url::parse(FEED_ENDPOINT).expect("constant feed endpoint must be valid"),
        )
    }

    fn with_endpoints(
        cookie: impl Into<String>,
        timeout: Duration,
        parse_endpoint: Url,
        feed_endpoint: Url,
    ) -> Result<Self> {
        let cookie = cookie.into();
        if cookie.trim().is_empty() {
            return Err(AppError::Config("WECHAT_YUANBAO_COOKIE 不能为空".into()));
        }
        if timeout.is_zero() {
            return Err(AppError::Config("视频号解析超时必须大于零".into()));
        }
        for endpoint in [&parse_endpoint, &feed_endpoint] {
            if endpoint.scheme() != "https" && !endpoint_is_loopback_http(endpoint) {
                return Err(AppError::Config(
                    "视频号解析 endpoint 必须使用 HTTPS".into(),
                ));
            }
        }

        let client = Client::builder()
            .connect_timeout(Duration::from_secs(10))
            .timeout(timeout)
            .redirect(Policy::none())
            .no_proxy()
            .build()
            .map_err(|_| AppError::Config("无法初始化视频号 HTTP 客户端".into()))?;

        Ok(Self {
            client,
            cookie: Arc::from(cookie),
            parse_endpoint,
            feed_endpoint,
            timeout,
        })
    }

    pub async fn resolve_text(&self, input: &str) -> Result<ResolvedPost> {
        let url = extract_share_url(input)?;
        let normalized = normalize_share_url(&url)?;
        tokio::time::timeout(self.timeout, self.resolve_normalized(normalized))
            .await
            .map_err(|_| AppError::Network("视频号解析总超时".into()))?
    }

    pub async fn resolve_url(&self, url: &Url) -> Result<ResolvedPost> {
        let normalized = normalize_share_url(url)?;
        tokio::time::timeout(self.timeout, self.resolve_normalized(normalized))
            .await
            .map_err(|_| AppError::Network("视频号解析总超时".into()))?
    }

    async fn resolve_normalized(&self, normalized: NormalizedShareUrl) -> Result<ResolvedPost> {
        let parse_data = self.request_parse(&normalized.canonical_url).await?;
        let playable_url =
            Url::parse(parse_data.playable_url.trim()).map_err(|_| AppError::UpstreamChanged)?;
        let general_token = query_value(&playable_url, "token").ok_or(AppError::UpstreamChanged)?;
        let export_id = query_value(&playable_url, "eid")
            .or_else(|| non_empty(parse_data.wx_export_id.clone()))
            .ok_or(AppError::UpstreamChanged)?;

        let feed = self.request_feed(&export_id, &general_token).await?;
        build_post(normalized, parse_data, feed, export_id)
    }

    async fn request_parse(&self, share_url: &Url) -> Result<ParseData> {
        let payload = ParseRequest {
            kind: "video_channel_url",
            url: share_url.as_str(),
            scene: 1,
        };

        let mut request = self
            .client
            .post(self.parse_endpoint.clone())
            .header(ACCEPT, "application/json, text/plain, */*")
            .header(ACCEPT_LANGUAGE, "zh-CN,zh;q=0.9,en;q=0.8")
            .header(CONTENT_TYPE, "application/json")
            .header(ORIGIN, YUANBAO_ORIGIN)
            .header(REFERER, YUANBAO_REFERER)
            .header(USER_AGENT, USER_AGENT_VALUE)
            .header("sec-ch-ua", SEC_CH_UA_VALUE)
            .header("sec-ch-ua-mobile", "?0")
            .header("sec-ch-ua-platform", r#""macOS""#)
            .header("sec-fetch-dest", "empty")
            .header("sec-fetch-mode", "cors")
            .header("sec-fetch-site", "same-origin")
            .header("x-agentid", YUANBAO_AGENT_ID)
            .header("x-instance-id", "5")
            .header("x-language", "zh-CN")
            .header("x-os_version", "Mac OS(10.15.7)-Blink")
            .header("x-platform", "mac")
            .header("x-requested-with", "XMLHttpRequest")
            .header("x-source", "web")
            .header("x-web-third-source", "main")
            .header("x-webdriver", "0")
            .header("x-webversion", "2.69.0")
            .header("x-ybuitest", "0")
            .header(COOKIE, self.cookie.as_ref())
            .json(&payload);

        if let Some(user_id) = cookie_value(&self.cookie, "hy_user") {
            request = request.header("t-userid", &user_id).header("x-id", user_id);
        }
        if let Some(device_id) = cookie_value(&self.cookie, "_qimei_uuid42") {
            request = request
                .header("x-device-id", &device_id)
                .header("x-hy93", device_id);
        }

        let response = request
            .send()
            .await
            .map_err(|error| map_network_error(&error))?;
        map_status(response.status(), true)?;
        let value = read_json(response).await?;

        let code = value.get("code").and_then(Value::as_i64).unwrap_or(0);
        if code != 0 {
            return if response_looks_like_login(&value) {
                Err(AppError::LoginRequired)
            } else {
                Err(AppError::UpstreamChanged)
            };
        }

        let data = value.get("data").cloned().ok_or_else(|| {
            if response_looks_like_login(&value) {
                AppError::LoginRequired
            } else {
                AppError::UpstreamChanged
            }
        })?;
        serde_json::from_value::<ParseData>(data).map_err(|_| AppError::UpstreamChanged)
    }

    async fn request_feed(&self, export_id: &str, general_token: &str) -> Result<Value> {
        let rid = format!(
            "{:x}-{}",
            Utc::now().timestamp(),
            &Uuid::new_v4().simple().to_string()[..8]
        );
        let mut endpoint = self.feed_endpoint.clone();
        endpoint
            .query_pairs_mut()
            .append_pair("_rid", &rid)
            .append_pair(
                "_pageUrl",
                "https://channels.weixin.qq.com/finder-preview/pages/feed",
            );

        let mut referer = Url::parse("https://channels.weixin.qq.com/finder-preview/pages/feed")
            .expect("constant referer must be valid");
        referer
            .query_pairs_mut()
            .append_pair("entry_card_type", "48")
            .append_pair("comment_scene", "39")
            .append_pair("appid", "0")
            .append_pair("token", general_token)
            .append_pair("entry_scene", "0")
            .append_pair("eid", export_id);

        let response = self
            .client
            .post(endpoint)
            .header(ACCEPT, "application/json, text/plain, */*")
            .header(ACCEPT_LANGUAGE, "zh-CN,zh;q=0.9,en;q=0.8")
            .header(CONTENT_TYPE, "application/json")
            .header(ORIGIN, CHANNELS_ORIGIN)
            .header(REFERER, referer.as_str())
            .header(USER_AGENT, USER_AGENT_VALUE)
            .header("sec-ch-ua", SEC_CH_UA_VALUE)
            .header("sec-ch-ua-mobile", "?0")
            .header("sec-ch-ua-platform", r#""macOS""#)
            .header("sec-fetch-dest", "empty")
            .header("sec-fetch-mode", "cors")
            .header("sec-fetch-site", "same-origin")
            .json(&FeedRequest {
                base_req: FeedBaseRequest { general_token },
                export_id,
            })
            .send()
            .await
            .map_err(|error| map_network_error(&error))?;
        map_status(response.status(), false)?;
        let value = read_json(response).await?;

        let err_code = value
            .get("errCode")
            .or_else(|| value.get("errcode"))
            .and_then(Value::as_i64)
            .unwrap_or(0);
        if err_code != 0 {
            return if response_looks_like_login(&value) {
                Err(AppError::LoginRequired)
            } else if value_to_text(value.get("errMsg")).contains("不存在") {
                Err(AppError::NotFound)
            } else {
                Err(AppError::UpstreamChanged)
            };
        }
        Ok(value)
    }
}

#[derive(Debug, Clone)]
struct NormalizedShareUrl {
    share_id: String,
    canonical_url: Url,
}

pub fn extract_share_url(input: &str) -> Result<Url> {
    static URL_PATTERN: std::sync::OnceLock<Regex> = std::sync::OnceLock::new();
    let pattern = URL_PATTERN.get_or_init(|| {
        Regex::new(r#"https?://[^\s<>\"']+"#).expect("constant URL regex must compile")
    });

    for matched in pattern.find_iter(input) {
        let candidate = matched.as_str().trim_end_matches([
            '。', '，', ',', '.', '！', '!', '？', '?', ')', '）', ']', '】',
        ]);
        let Ok(url) = Url::parse(candidate) else {
            continue;
        };
        if let Ok(normalized) = normalize_share_url(&url) {
            return Ok(normalized.canonical_url);
        }
    }
    Err(AppError::UnsupportedUrl)
}

fn normalize_share_url(url: &Url) -> Result<NormalizedShareUrl> {
    if url.scheme() != "https"
        || !url.username().is_empty()
        || url.password().is_some()
        || url.port().is_some()
        || url.fragment().is_some()
    {
        return Err(AppError::UnsupportedUrl);
    }

    let host = url.host_str().ok_or(AppError::UnsupportedUrl)?;
    if host.ends_with('.') {
        return Err(AppError::UnsupportedUrl);
    }
    let host = host.to_ascii_lowercase();

    let share_id = match host.as_str() {
        "weixin.qq.com" => {
            let segments = url
                .path_segments()
                .ok_or(AppError::UnsupportedUrl)?
                .filter(|segment| !segment.is_empty())
                .collect::<Vec<_>>();
            if segments.len() != 2 || segments[0] != "sph" {
                return Err(AppError::UnsupportedUrl);
            }
            segments[1].to_owned()
        }
        "channels.weixin.qq.com" if url.path() == "/finder-preview/pages/sph" => url
            .query_pairs()
            .find_map(|(key, value)| (key == "id").then(|| value.into_owned()))
            .ok_or(AppError::UnsupportedUrl)?,
        _ => return Err(AppError::UnsupportedUrl),
    };

    if !(6..=128).contains(&share_id.len())
        || !share_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
    {
        return Err(AppError::UnsupportedUrl);
    }

    let canonical_url = Url::parse(&format!("https://weixin.qq.com/sph/{share_id}"))
        .expect("validated share id always creates a URL");
    Ok(NormalizedShareUrl {
        share_id,
        canonical_url,
    })
}

pub fn derive_original_url(source: &Url) -> Option<Url> {
    if source.scheme() != "https"
        || source.host_str()? != "finder.video.qq.com"
        || !source.username().is_empty()
        || source.password().is_some()
        || source.port().is_some()
        || source.fragment().is_some()
    {
        return None;
    }

    let file_key = query_value(source, "encfilekey")?;
    let token = query_value(source, "token")?;
    let mut original = source.clone();
    original.set_query(None);
    original.set_fragment(None);
    original
        .query_pairs_mut()
        .append_pair("encfilekey", &file_key)
        .append_pair("token", &token);

    Some(original)
}

fn build_post(
    normalized: NormalizedShareUrl,
    parse_data: ParseData,
    feed: Value,
    export_id: String,
) -> Result<ResolvedPost> {
    let data = if feed.get("feedInfo").is_some() {
        &feed
    } else {
        feed.get("data")
            .and_then(|value| {
                if value.get("feedInfo").is_some() {
                    Some(value)
                } else {
                    value.get("data")
                }
            })
            .ok_or(AppError::UpstreamChanged)?
    };
    let feed_info = data.get("feedInfo").ok_or(AppError::UpstreamChanged)?;
    let author_info = data.get("authorInfo");

    let mut candidates = Vec::new();
    push_candidate(
        &mut candidates,
        feed_info.get("h264VideoInfo"),
        MediaProvenance::H264,
        VideoCodec::H264,
    );
    push_direct_candidate(
        &mut candidates,
        feed_info,
        "videoUrl",
        MediaProvenance::Generic,
        VideoCodec::Unknown,
    );
    push_candidate(
        &mut candidates,
        feed_info.get("h265VideoInfo"),
        MediaProvenance::H265,
        VideoCodec::H265,
    );
    push_direct_candidate(
        &mut candidates,
        feed_info,
        "originVideoUrl",
        MediaProvenance::ExplicitOrigin,
        VideoCodec::Unknown,
    );

    let explicit_original = candidates
        .iter()
        .find(|source| source.provenance == MediaProvenance::ExplicitOrigin)
        .cloned();
    let video = if let Some(explicit_original) = explicit_original {
        explicit_original
    } else {
        [
            MediaProvenance::H264,
            MediaProvenance::H265,
            MediaProvenance::Generic,
        ]
        .into_iter()
        .filter_map(|provenance| {
            candidates
                .iter()
                .find(|source| source.provenance == provenance)
        })
        .find_map(|candidate| {
            derive_original_url(&candidate.url).map(|url| MediaSource {
                url,
                codec: candidate.codec,
                provenance: MediaProvenance::DerivedOriginal,
                width: candidate.width,
                height: candidate.height,
                size_hint: None,
                decode_key: candidate.decode_key,
            })
        })
        .ok_or(AppError::OriginalUnavailable)?
    };

    let description = text_at(feed_info, "description").or_else(|| non_empty(parse_data.desc));
    let author = author_info
        .and_then(|value| text_at(value, "nickname"))
        .or_else(|| non_empty(parse_data.author));
    let cover_url = text_at(feed_info, "coverUrl")
        .or_else(|| non_empty(parse_data.cover_url))
        .and_then(|raw| Url::parse(&raw).ok())
        .filter(is_safe_https_url);
    let expires_at = data
        .get("sceneInfo")
        .and_then(|value| number_at(value, "expiredTime"))
        .and_then(|timestamp| i64::try_from(timestamp).ok())
        .and_then(|timestamp| DateTime::from_timestamp(timestamp, 0));

    Ok(ResolvedPost {
        platform: "wechat_channels".into(),
        post_id: non_empty(export_id).unwrap_or(normalized.share_id),
        canonical_url: normalized.canonical_url,
        author,
        title: description.clone(),
        description,
        cover_url,
        video,
        expires_at,
    })
}

fn push_candidate(
    output: &mut Vec<MediaSource>,
    value: Option<&Value>,
    provenance: MediaProvenance,
    codec: VideoCodec,
) {
    let Some(value) = value else { return };
    push_direct_candidate(output, value, "videoUrl", provenance, codec);
}

fn push_direct_candidate(
    output: &mut Vec<MediaSource>,
    value: &Value,
    url_field: &str,
    provenance: MediaProvenance,
    codec: VideoCodec,
) {
    let Some(raw_url) = text_at(value, url_field) else {
        return;
    };
    let Ok(url) = Url::parse(&raw_url) else {
        return;
    };
    if !is_allowed_media_url(&url) || output.iter().any(|source| source.url == url) {
        return;
    }

    output.push(MediaSource {
        url,
        codec,
        provenance,
        width: number_at(value, "width").and_then(|value| u32::try_from(value).ok()),
        height: number_at(value, "height").and_then(|value| u32::try_from(value).ok()),
        size_hint: number_at(value, "fileSize"),
        decode_key: text_at(value, "decodeKey")
            .and_then(|value| value.parse::<u64>().ok())
            .or_else(|| number_at(value, "decodeKey")),
    });
}

fn is_allowed_media_url(url: &Url) -> bool {
    url.scheme() == "https"
        && url.username().is_empty()
        && url.password().is_none()
        && url.port().is_none()
        && url.fragment().is_none()
        && url
            .host_str()
            .is_some_and(|host| !host.ends_with('.') && REVIEWED_WECHAT_MEDIA_HOSTS.contains(&host))
}

fn is_safe_https_url(url: &Url) -> bool {
    url.scheme() == "https"
        && url.username().is_empty()
        && url.password().is_none()
        && url.port().is_none()
        && url.host_str().is_some()
}

fn endpoint_is_loopback_http(url: &Url) -> bool {
    url.scheme() == "http" && matches!(url.host_str(), Some("127.0.0.1" | "localhost" | "::1"))
}

fn query_value(url: &Url, name: &str) -> Option<String> {
    url.query_pairs()
        .find_map(|(key, value)| (key == name && !value.is_empty()).then(|| value.into_owned()))
}

fn cookie_value(cookie: &str, name: &str) -> Option<String> {
    cookie.split(';').find_map(|part| {
        let (key, value) = part.trim().split_once('=')?;
        (key == name && !value.is_empty()).then(|| value.to_owned())
    })
}

fn text_at(value: &Value, key: &str) -> Option<String> {
    value.get(key).and_then(value_to_string).and_then(non_empty)
}

fn number_at(value: &Value, key: &str) -> Option<u64> {
    let value = value.get(key)?;
    value
        .as_u64()
        .or_else(|| value.as_str()?.parse::<u64>().ok())
}

fn value_to_string(value: &Value) -> Option<String> {
    match value {
        Value::String(value) => Some(value.clone()),
        Value::Number(value) => Some(value.to_string()),
        _ => None,
    }
}

fn value_to_text(value: Option<&Value>) -> String {
    value
        .and_then(value_to_string)
        .unwrap_or_default()
        .to_ascii_lowercase()
}

fn non_empty(value: String) -> Option<String> {
    (!value.trim().is_empty()).then(|| value.trim().to_owned())
}

fn response_looks_like_login(value: &Value) -> bool {
    let message = value_to_text(value.get("msg")) + &value_to_text(value.get("errMsg"));
    ["login", "登录", "cookie", "unauthorized", "未登录"]
        .iter()
        .any(|word| message.contains(word))
}

fn map_status(status: StatusCode, yuanbao: bool) -> Result<()> {
    match status {
        status if status.is_success() => Ok(()),
        StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN if yuanbao => Err(AppError::LoginRequired),
        StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => Err(AppError::NotFound),
        StatusCode::NOT_FOUND | StatusCode::GONE => Err(AppError::NotFound),
        StatusCode::TOO_MANY_REQUESTS => Err(AppError::RateLimited),
        _ => Err(AppError::Network(format!(
            "上游返回 HTTP {}",
            status.as_u16()
        ))),
    }
}

fn map_network_error(error: &reqwest::Error) -> AppError {
    if error.is_timeout() {
        AppError::Network("上游请求超时".into())
    } else {
        // Formatting reqwest::Error can include a signed URL or endpoint.
        AppError::Network("无法连接上游服务".into())
    }
}

async fn read_json(response: Response) -> Result<Value> {
    if response
        .content_length()
        .is_some_and(|length| length > MAX_JSON_BYTES as u64)
    {
        return Err(AppError::UpstreamChanged);
    }
    let mut bytes = Vec::new();
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|error| map_network_error(&error))?;
        let next_len = bytes
            .len()
            .checked_add(chunk.len())
            .filter(|length| *length <= MAX_JSON_BYTES)
            .ok_or(AppError::UpstreamChanged)?;
        bytes.reserve(next_len.saturating_sub(bytes.len()));
        bytes.extend_from_slice(&chunk);
    }
    serde_json::from_slice(&bytes).map_err(|_| AppError::UpstreamChanged)
}

#[derive(Serialize)]
struct ParseRequest<'a> {
    #[serde(rename = "type")]
    kind: &'a str,
    url: &'a str,
    scene: u8,
}

#[derive(Debug, Deserialize)]
struct ParseData {
    #[serde(default)]
    wx_export_id: String,
    #[serde(default)]
    cover_url: String,
    #[serde(default)]
    author: String,
    #[serde(default)]
    desc: String,
    playable_url: String,
}

#[derive(Serialize)]
struct FeedRequest<'a> {
    #[serde(rename = "baseReq")]
    base_req: FeedBaseRequest<'a>,
    #[serde(rename = "exportId")]
    export_id: &'a str,
}

#[derive(Serialize)]
struct FeedBaseRequest<'a> {
    #[serde(rename = "generalToken")]
    general_token: &'a str,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_and_canonicalizes_share_url_from_text() {
        let url = extract_share_url("看看这个 https://weixin.qq.com/sph/A27pGwf5f9。").unwrap();
        assert_eq!(url.as_str(), "https://weixin.qq.com/sph/A27pGwf5f9");
    }

    #[test]
    fn supports_strict_preview_url() {
        let input =
            Url::parse("https://channels.weixin.qq.com/finder-preview/pages/sph?id=A27pGwf5f9")
                .unwrap();
        let normalized = normalize_share_url(&input).unwrap();
        assert_eq!(
            normalized.canonical_url.as_str(),
            "https://weixin.qq.com/sph/A27pGwf5f9"
        );
    }

    #[test]
    fn rejects_spoofed_or_insecure_urls() {
        for raw in [
            "http://weixin.qq.com/sph/A27pGwf5f9",
            "https://weixin.qq.com.evil.test/sph/A27pGwf5f9",
            "https://weixin.qq.com./sph/A27pGwf5f9",
            "https://user@weixin.qq.com/sph/A27pGwf5f9",
            "https://weixin.qq.com/other/A27pGwf5f9",
        ] {
            assert!(
                normalize_share_url(&Url::parse(raw).unwrap()).is_err(),
                "{raw}"
            );
        }
    }

    #[test]
    fn derives_original_url_structurally() {
        let source = Url::parse(
            "https://finder.video.qq.com/path/video.mp4?token=t%2Bv&quality=hd&encfilekey=e%26k",
        )
        .unwrap();
        let original = derive_original_url(&source).unwrap();
        assert_eq!(
            original.as_str(),
            "https://finder.video.qq.com/path/video.mp4?encfilekey=e%26k&token=t%2Bv"
        );
    }

    #[test]
    fn normalizes_clean_original_and_rejects_incomplete_urls() {
        let already_clean =
            Url::parse("https://finder.video.qq.com/video.mp4?encfilekey=key&token=token").unwrap();
        let missing = Url::parse("https://finder.video.qq.com/video.mp4?token=token").unwrap();
        let reverse_order =
            Url::parse("https://finder.video.qq.com/video.mp4?token=token&encfilekey=key").unwrap();
        assert_eq!(derive_original_url(&already_clean).unwrap(), already_clean);
        assert!(derive_original_url(&missing).is_none());
        assert_eq!(
            derive_original_url(&reverse_order).unwrap().query(),
            Some("encfilekey=key&token=token")
        );
        let dotted =
            Url::parse("https://finder.video.qq.com./video.mp4?encfilekey=key&token=token")
                .unwrap();
        assert!(derive_original_url(&dotted).is_none());
    }

    #[test]
    fn parses_cookie_without_logging_or_decoding_it() {
        let cookie = "a=1; hy_user=user-id; token=a=b=c";
        assert_eq!(cookie_value(cookie, "hy_user").as_deref(), Some("user-id"));
        assert_eq!(cookie_value(cookie, "token").as_deref(), Some("a=b=c"));
    }

    #[test]
    fn builds_post_from_feed_fixture_and_derives_original_from_preferred_h264_seed() {
        let normalized = NormalizedShareUrl {
            share_id: "A27pGwf5f9".to_owned(),
            canonical_url: Url::parse("https://weixin.qq.com/sph/A27pGwf5f9").unwrap(),
        };
        let parse_data = ParseData {
            wx_export_id: "fallback-export-id".to_owned(),
            cover_url: String::new(),
            author: "备用作者".to_owned(),
            desc: "备用描述".to_owned(),
            playable_url:
                "https://channels.weixin.qq.com/finder-preview/pages/feed?token=dummy&eid=export-id"
                    .to_owned(),
        };
        let feed = serde_json::json!({
            "errCode": 0,
            "data": {
                "feedInfo": {
                    "description": "测试视频",
                    "coverUrl": "https://finder.video.qq.com/cover.jpg",
                    "videoUrl": "https://finder.video.qq.com/generic.mp4?encfilekey=g&token=t&quality=normal",
                    "h264VideoInfo": {
                        "videoUrl": "https://finder.video.qq.com/h264.mp4?encfilekey=h&token=t&quality=hd",
                        "width": 1920,
                        "height": 1080,
                        "fileSize": "123456"
                    },
                    "h265VideoInfo": {
                        "videoUrl": "https://finder.video.qq.com/h265.mp4?encfilekey=x&token=t&quality=hd"
                    }
                },
                "authorInfo": {"nickname": "测试作者"},
                "sceneInfo": {"expiredTime": 1893456000}
            }
        });

        let post = build_post(normalized, parse_data, feed, "export-id".to_owned()).unwrap();
        assert_eq!(post.post_id, "export-id");
        assert_eq!(post.author.as_deref(), Some("测试作者"));
        assert_eq!(post.description.as_deref(), Some("测试视频"));
        assert_eq!(post.video.provenance, MediaProvenance::DerivedOriginal);
        assert_eq!(post.video.codec, VideoCodec::H264);
        assert_eq!(post.video.url.query(), Some("encfilekey=h&token=t"));
        assert_eq!(post.video.size_hint, None);
    }

    #[test]
    fn accepts_root_level_feed_shape_and_prefers_explicit_origin() {
        let normalized = NormalizedShareUrl {
            share_id: "A27pGwf5f9".to_owned(),
            canonical_url: Url::parse("https://weixin.qq.com/sph/A27pGwf5f9").unwrap(),
        };
        let parse_data = ParseData {
            wx_export_id: String::new(),
            cover_url: String::new(),
            author: String::new(),
            desc: String::new(),
            playable_url: "https://example.invalid/?token=dummy".to_owned(),
        };
        let feed = serde_json::json!({
            "feedInfo": {
                "h264VideoInfo": {
                    "videoUrl": "https://finder.video.qq.com/candidate.mp4?encfilekey=k&token=t&quality=hd"
                },
                "originVideoUrl": "https://finder.video.qq.com/original.mp4?token=t"
            }
        });

        let post = build_post(normalized, parse_data, feed, "export-id".to_owned()).unwrap();
        assert_eq!(post.video.provenance, MediaProvenance::ExplicitOrigin);
        assert_eq!(
            post.video.url.as_str(),
            "https://finder.video.qq.com/original.mp4?token=t"
        );
    }

    #[test]
    fn rejects_a_post_when_original_cannot_be_derived() {
        let normalized = NormalizedShareUrl {
            share_id: "A27pGwf5f9".to_owned(),
            canonical_url: Url::parse("https://weixin.qq.com/sph/A27pGwf5f9").unwrap(),
        };
        let parse_data = ParseData {
            wx_export_id: String::new(),
            cover_url: String::new(),
            author: String::new(),
            desc: String::new(),
            playable_url: "https://example.invalid/?token=dummy".to_owned(),
        };
        let feed = serde_json::json!({
            "feedInfo": {
                "videoUrl": "https://finder.video.qq.com/fallback.mp4?token=t",
                "h265VideoInfo": {
                    "videoUrl": "https://finder.video.qq.com/h265.mp4?token=t"
                }
            }
        });

        let error = build_post(normalized, parse_data, feed, "export-id".to_owned()).unwrap_err();
        assert!(matches!(error, AppError::OriginalUnavailable));
    }

    #[test]
    fn tries_h265_when_h264_cannot_derive_an_original() {
        let normalized = NormalizedShareUrl {
            share_id: "A27pGwf5f9".to_owned(),
            canonical_url: Url::parse("https://weixin.qq.com/sph/A27pGwf5f9").unwrap(),
        };
        let parse_data = ParseData {
            wx_export_id: String::new(),
            cover_url: String::new(),
            author: String::new(),
            desc: String::new(),
            playable_url: "https://example.invalid/?token=dummy".to_owned(),
        };
        let feed = serde_json::json!({
            "feedInfo": {
                "h264VideoInfo": {
                    "videoUrl": "https://finder.video.qq.com/h264.mp4?token=t"
                },
                "h265VideoInfo": {
                    "videoUrl": "https://finder.video.qq.com/h265.mp4?encfilekey=k&token=t&quality=hd"
                },
                "videoUrl": "https://finder.video.qq.com/fallback.mp4?token=t"
            }
        });

        let post = build_post(normalized, parse_data, feed, "export-id".to_owned()).unwrap();
        assert_eq!(post.video.provenance, MediaProvenance::DerivedOriginal);
        assert_eq!(post.video.codec, VideoCodec::H265);
        assert_eq!(
            post.video.url.as_str(),
            "https://finder.video.qq.com/h265.mp4?encfilekey=k&token=t"
        );
    }
}
