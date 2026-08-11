use std::{sync::Arc, time::Duration};

use chrono::Utc;
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
    Error, Result,
    model::{MediaSource, MediaSourceKind, REVIEWED_WECHAT_MEDIA_HOSTS, ResolvedPost, VideoCodec},
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
const RESOLVE_TIMEOUT: Duration = Duration::from_secs(30);
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
    pub fn new(cookie: impl Into<String>) -> Result<Self> {
        Self::with_endpoints(
            cookie,
            Url::parse(PARSE_ENDPOINT).expect("constant parse endpoint must be valid"),
            Url::parse(FEED_ENDPOINT).expect("constant feed endpoint must be valid"),
        )
    }

    fn with_endpoints(
        cookie: impl Into<String>,
        parse_endpoint: Url,
        feed_endpoint: Url,
    ) -> Result<Self> {
        let timeout = RESOLVE_TIMEOUT;
        let cookie = cookie.into();
        if cookie.trim().is_empty() {
            return Err(Error::Config("WECHAT_YUANBAO_COOKIE 不能为空".into()));
        }
        for endpoint in [&parse_endpoint, &feed_endpoint] {
            if endpoint.scheme() != "https" && !endpoint_is_loopback_http(endpoint) {
                return Err(Error::Config("视频号解析 endpoint 必须使用 HTTPS".into()));
            }
        }

        let client = Client::builder()
            .connect_timeout(Duration::from_secs(10))
            .timeout(timeout)
            .redirect(Policy::none())
            .no_proxy()
            .build()
            .map_err(|_| Error::Config("无法初始化视频号 HTTP 客户端".into()))?;

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
            .map_err(|_| Error::Network("视频号解析总超时".into()))?
    }

    pub async fn resolve_url(&self, url: &Url) -> Result<ResolvedPost> {
        let normalized = normalize_share_url(url)?;
        tokio::time::timeout(self.timeout, self.resolve_normalized(normalized))
            .await
            .map_err(|_| Error::Network("视频号解析总超时".into()))?
    }

    async fn resolve_normalized(&self, normalized: NormalizedShareUrl) -> Result<ResolvedPost> {
        let parse_data = self.request_parse(&normalized.canonical_url).await?;
        let playable_url =
            Url::parse(parse_data.playable_url.trim()).map_err(|_| Error::UpstreamChanged)?;
        let general_token = query_value(&playable_url, "token").ok_or(Error::UpstreamChanged)?;
        let export_id = query_value(&playable_url, "eid")
            .or_else(|| non_empty(parse_data.wx_export_id.clone()))
            .ok_or(Error::UpstreamChanged)?;

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
                Err(Error::LoginRequired)
            } else {
                Err(Error::UpstreamChanged)
            };
        }

        let data = value.get("data").cloned().ok_or_else(|| {
            if response_looks_like_login(&value) {
                Error::LoginRequired
            } else {
                Error::UpstreamChanged
            }
        })?;
        serde_json::from_value::<ParseData>(data).map_err(|_| Error::UpstreamChanged)
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
                Err(Error::LoginRequired)
            } else if value_to_text(value.get("errMsg")).contains("不存在") {
                Err(Error::NotFound)
            } else {
                Err(Error::UpstreamChanged)
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
        Regex::new(r#"https://weixin\.qq\.com/sph/[^\s<>\"']+"#)
            .expect("constant URL regex must compile")
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
    Err(Error::UnsupportedUrl)
}

fn normalize_share_url(url: &Url) -> Result<NormalizedShareUrl> {
    if url.scheme() != "https"
        || !url.username().is_empty()
        || url.password().is_some()
        || url.port().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(Error::UnsupportedUrl);
    }

    let host = url.host_str().ok_or(Error::UnsupportedUrl)?;
    if host.ends_with('.') {
        return Err(Error::UnsupportedUrl);
    }
    let host = host.to_ascii_lowercase();

    if host != "weixin.qq.com" {
        return Err(Error::UnsupportedUrl);
    }
    let share_id = url
        .path()
        .strip_prefix("/sph/")
        .filter(|value| !value.is_empty() && !value.contains('/'))
        .ok_or(Error::UnsupportedUrl)?
        .to_owned();

    if !(6..=128).contains(&share_id.len())
        || !share_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
    {
        return Err(Error::UnsupportedUrl);
    }

    let canonical_url = Url::parse(&format!("https://weixin.qq.com/sph/{share_id}"))
        .expect("validated share id always creates a URL");
    Ok(NormalizedShareUrl {
        share_id,
        canonical_url,
    })
}

pub fn derive_direct_media_url(source: &Url) -> Option<Url> {
    if !is_allowed_media_url(source) {
        return None;
    }

    let file_key = query_value(source, "encfilekey")?;
    let token = query_value(source, "token")?;
    let mut direct = source.clone();
    direct.set_query(None);
    direct.set_fragment(None);
    direct
        .query_pairs_mut()
        .append_pair("encfilekey", &file_key)
        .append_pair("token", &token);

    Some(direct)
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
            .ok_or(Error::UpstreamChanged)?
    };
    let feed_info = data.get("feedInfo").ok_or(Error::UpstreamChanged)?;

    let mut candidates = Vec::new();
    push_candidate(
        &mut candidates,
        feed_info.get("h264VideoInfo"),
        MediaSourceKind::H264,
        VideoCodec::H264,
    );
    push_candidate(
        &mut candidates,
        feed_info.get("h265VideoInfo"),
        MediaSourceKind::H265,
        VideoCodec::H265,
    );
    push_source(
        &mut candidates,
        feed_info,
        "videoUrl",
        MediaSourceKind::Generic,
        VideoCodec::Unknown,
    );

    let direct_source = parse_source(
        feed_info,
        "originVideoUrl",
        MediaSourceKind::Direct,
        VideoCodec::Unknown,
    )
    .map(|mut direct| {
        if let Some(candidate) = matching_candidate(&direct, &candidates) {
            let same_url = direct.url == candidate.url;
            merge_source_metadata(&mut direct, candidate, same_url);
        }
        direct
    });

    let mut derived_sources = Vec::new();
    for candidate in [
        MediaSourceKind::H264,
        MediaSourceKind::H265,
        MediaSourceKind::Generic,
    ]
    .into_iter()
    .filter_map(|kind| candidates.iter().find(|source| source.provenance == kind))
    {
        if let Some(url) = derive_direct_media_url(&candidate.url) {
            let source = MediaSource {
                url,
                codec: candidate.codec,
                provenance: MediaSourceKind::Derived,
                width: candidate.width,
                height: candidate.height,
                size_hint: None,
                decode_key: candidate.decode_key,
            };
            if !derived_sources
                .iter()
                .any(|existing: &MediaSource| sources_are_equivalent(existing, &source))
            {
                derived_sources.push(source);
            }
        }
    }

    let (video, fallback_videos) = if let Some(direct_source) = direct_source {
        let fallback_videos = derived_sources
            .into_iter()
            .filter(|source| !sources_are_equivalent(source, &direct_source))
            .collect();
        (direct_source, fallback_videos)
    } else {
        if derived_sources.is_empty() {
            return Err(Error::MediaUnavailable);
        }
        let video = derived_sources.remove(0);
        (video, derived_sources)
    };

    let title = text_at(feed_info, "description").or_else(|| non_empty(parse_data.desc));
    let cover_url = text_at(feed_info, "coverUrl")
        .or_else(|| non_empty(parse_data.cover_url))
        .and_then(|raw| Url::parse(&raw).ok())
        .filter(is_allowed_media_url);
    Ok(ResolvedPost {
        platform: "wechat_channels".into(),
        post_id: non_empty(export_id).unwrap_or(normalized.share_id),
        canonical_url: normalized.canonical_url,
        title,
        cover_url,
        video,
        fallback_videos,
    })
}

fn push_candidate(
    output: &mut Vec<MediaSource>,
    value: Option<&Value>,
    kind: MediaSourceKind,
    codec: VideoCodec,
) {
    let Some(value) = value else { return };
    push_source(output, value, "videoUrl", kind, codec);
}

fn push_source(
    output: &mut Vec<MediaSource>,
    value: &Value,
    url_field: &str,
    kind: MediaSourceKind,
    codec: VideoCodec,
) {
    let Some(source) = parse_source(value, url_field, kind, codec) else {
        return;
    };
    if let Some(existing) = output
        .iter_mut()
        .find(|existing| sources_are_equivalent(existing, &source))
    {
        merge_source_metadata(existing, &source, true);
    } else {
        output.push(source);
    }
}

fn parse_source(
    value: &Value,
    url_field: &str,
    kind: MediaSourceKind,
    codec: VideoCodec,
) -> Option<MediaSource> {
    let raw_url = text_at(value, url_field)?;
    let url = Url::parse(&raw_url).ok()?;
    if !is_allowed_media_url(&url) {
        return None;
    }

    Some(MediaSource {
        url,
        codec,
        provenance: kind,
        width: number_at(value, "width").and_then(|value| u32::try_from(value).ok()),
        height: number_at(value, "height").and_then(|value| u32::try_from(value).ok()),
        size_hint: number_at(value, "fileSize"),
        decode_key: text_at(value, "decodeKey")
            .and_then(|value| value.parse::<u64>().ok())
            .or_else(|| number_at(value, "decodeKey")),
    })
}

fn matching_candidate<'a>(
    direct: &MediaSource,
    candidates: &'a [MediaSource],
) -> Option<&'a MediaSource> {
    // An exact URL is the strongest identity, but it is still ambiguous when
    // the feed attaches different decode keys to the same byte location.
    if let Some(candidate) =
        unique_matching_candidate(candidates, |candidate| candidate.url == direct.url)
    {
        return Some(candidate);
    }

    if let Some(direct_url) = derive_direct_media_url(&direct.url)
        && let Some(candidate) = unique_matching_candidate(candidates, |candidate| {
            derive_direct_media_url(&candidate.url).as_ref() == Some(&direct_url)
        })
    {
        return Some(candidate);
    }

    unique_matching_candidate(candidates, |candidate| {
        has_matching_media_identity(&direct.url, &candidate.url)
    })
}

fn sources_are_equivalent(left: &MediaSource, right: &MediaSource) -> bool {
    left.url == right.url && left.decode_key == right.decode_key
}

fn unique_matching_candidate(
    candidates: &[MediaSource],
    predicate: impl Fn(&MediaSource) -> bool,
) -> Option<&MediaSource> {
    let mut matches = candidates.iter().filter(|candidate| predicate(candidate));
    let candidate = matches.next()?;
    matches.next().is_none().then_some(candidate)
}

fn has_matching_media_identity(left: &Url, right: &Url) -> bool {
    if left.scheme() != right.scheme()
        || left.host_str() != right.host_str()
        || left.port_or_known_default() != right.port_or_known_default()
        || left.path() != right.path()
    {
        return false;
    }

    let left_file_key = query_value(left, "encfilekey");
    let right_file_key = query_value(right, "encfilekey");
    let left_token = query_value(left, "token");
    let right_token = query_value(right, "token");
    let file_key_matches = matches!(
        (&left_file_key, &right_file_key),
        (Some(left), Some(right)) if left == right
    );
    let token_matches = matches!(
        (&left_token, &right_token),
        (Some(left), Some(right)) if left == right
    );
    let file_key_conflicts = matches!(
        (&left_file_key, &right_file_key),
        (Some(left), Some(right)) if left != right
    );
    let token_conflicts = matches!(
        (&left_token, &right_token),
        (Some(left), Some(right)) if left != right
    );

    !file_key_conflicts && !token_conflicts && (file_key_matches || token_matches)
}

fn merge_source_metadata(target: &mut MediaSource, source: &MediaSource, inherit_size_hint: bool) {
    if target.codec == VideoCodec::Unknown {
        target.codec = source.codec;
    }
    if target.width.is_none() {
        target.width = source.width;
    }
    if target.height.is_none() {
        target.height = source.height;
    }
    if target.decode_key.is_none() {
        target.decode_key = source.decode_key;
    }
    if inherit_size_hint && target.size_hint.is_none() {
        target.size_hint = source.size_hint;
    }
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
        StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN if yuanbao => Err(Error::LoginRequired),
        StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => Err(Error::NotFound),
        StatusCode::NOT_FOUND | StatusCode::GONE => Err(Error::NotFound),
        StatusCode::TOO_MANY_REQUESTS => Err(Error::RateLimited),
        _ => Err(Error::Network(format!("上游返回 HTTP {}", status.as_u16()))),
    }
}

fn map_network_error(error: &reqwest::Error) -> Error {
    if error.is_timeout() {
        Error::Network("上游请求超时".into())
    } else {
        // Formatting reqwest::Error can include a signed URL or endpoint.
        Error::Network("无法连接上游服务".into())
    }
}

async fn read_json(response: Response) -> Result<Value> {
    if response
        .content_length()
        .is_some_and(|length| length > MAX_JSON_BYTES as u64)
    {
        return Err(Error::UpstreamChanged);
    }
    let mut bytes = Vec::new();
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|error| map_network_error(&error))?;
        let next_len = bytes
            .len()
            .checked_add(chunk.len())
            .filter(|length| *length <= MAX_JSON_BYTES)
            .ok_or(Error::UpstreamChanged)?;
        bytes.reserve(next_len.saturating_sub(bytes.len()));
        bytes.extend_from_slice(&chunk);
    }
    serde_json::from_slice(&bytes).map_err(|_| Error::UpstreamChanged)
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
    fn rejects_preview_and_other_link_forms_during_extraction() {
        for input in [
            "https://channels.weixin.qq.com/finder-preview/pages/sph?id=A27pGwf5f9",
            "http://weixin.qq.com/sph/A27pGwf5f9",
            "https://WEIXIN.QQ.COM/sph/A27pGwf5f9",
            "https://weixin.qq.com/other/A27pGwf5f9",
        ] {
            assert!(matches!(
                extract_share_url(input),
                Err(Error::UnsupportedUrl)
            ));
        }
    }

    #[test]
    fn rejects_spoofed_or_insecure_urls() {
        for raw in [
            "http://weixin.qq.com/sph/A27pGwf5f9",
            "https://weixin.qq.com.evil.test/sph/A27pGwf5f9",
            "https://weixin.qq.com./sph/A27pGwf5f9",
            "https://user@weixin.qq.com/sph/A27pGwf5f9",
            "https://weixin.qq.com/other/A27pGwf5f9",
            "https://weixin.qq.com/sph/A27pGwf5f9/",
            "https://weixin.qq.com/sph/A27pGwf5f9?from=share",
            "https://weixin.qq.com/sph/A27pGwf5f9#fragment",
        ] {
            assert!(
                normalize_share_url(&Url::parse(raw).unwrap()).is_err(),
                "{raw}"
            );
        }
    }

    #[test]
    fn derives_direct_media_url_structurally() {
        let source = Url::parse(
            "https://finder.video.qq.com/path/video.mp4?token=t%2Bv&quality=hd&encfilekey=e%26k",
        )
        .unwrap();
        let direct = derive_direct_media_url(&source).unwrap();
        assert_eq!(
            direct.as_str(),
            "https://finder.video.qq.com/path/video.mp4?encfilekey=e%26k&token=t%2Bv"
        );
    }

    #[test]
    fn derives_direct_media_urls_for_every_reviewed_host() {
        for host in REVIEWED_WECHAT_MEDIA_HOSTS {
            let source = Url::parse(&format!(
                "https://{host}/video.mp4?token=token&quality=hd&encfilekey=key"
            ))
            .unwrap();
            let direct = derive_direct_media_url(&source).unwrap();
            assert_eq!(direct.host_str(), Some(*host));
            assert_eq!(direct.query(), Some("encfilekey=key&token=token"));
        }
    }

    #[test]
    fn normalizes_clean_direct_url_and_rejects_incomplete_urls() {
        let already_clean =
            Url::parse("https://finder.video.qq.com/video.mp4?encfilekey=key&token=token").unwrap();
        let missing = Url::parse("https://finder.video.qq.com/video.mp4?token=token").unwrap();
        let reverse_order =
            Url::parse("https://finder.video.qq.com/video.mp4?token=token&encfilekey=key").unwrap();
        assert_eq!(
            derive_direct_media_url(&already_clean).unwrap(),
            already_clean
        );
        assert!(derive_direct_media_url(&missing).is_none());
        assert_eq!(
            derive_direct_media_url(&reverse_order).unwrap().query(),
            Some("encfilekey=key&token=token")
        );
        let dotted =
            Url::parse("https://finder.video.qq.com./video.mp4?encfilekey=key&token=token")
                .unwrap();
        assert!(derive_direct_media_url(&dotted).is_none());
    }

    #[test]
    fn matches_media_identity_only_when_path_and_signed_values_are_compatible() {
        let direct =
            Url::parse("https://finder.video.qq.com/shared.mp4?token=shared-token").unwrap();
        let candidate =
            Url::parse("https://finder.video.qq.com/shared.mp4?token=shared-token&encfilekey=key")
                .unwrap();
        assert!(has_matching_media_identity(&direct, &candidate));

        let conflicting_token =
            Url::parse("https://finder.video.qq.com/shared.mp4?token=other-token").unwrap();
        let conflicting_key = Url::parse(
            "https://finder.video.qq.com/shared.mp4?token=shared-token&encfilekey=other-key",
        )
        .unwrap();
        let keyed_direct =
            Url::parse("https://finder.video.qq.com/shared.mp4?token=shared-token&encfilekey=key")
                .unwrap();
        let other_path =
            Url::parse("https://finder.video.qq.com/other.mp4?token=shared-token").unwrap();
        assert!(!has_matching_media_identity(&direct, &conflicting_token));
        assert!(!has_matching_media_identity(
            &keyed_direct,
            &conflicting_key
        ));
        assert!(!has_matching_media_identity(&direct, &other_path));
    }

    #[test]
    fn parses_cookie_without_logging_or_decoding_it() {
        let cookie = "a=1; hy_user=user-id; token=a=b=c";
        assert_eq!(cookie_value(cookie, "hy_user").as_deref(), Some("user-id"));
        assert_eq!(cookie_value(cookie, "token").as_deref(), Some("a=b=c"));
    }

    #[test]
    fn builds_post_from_feed_fixture_and_derives_media_from_preferred_h264_seed() {
        let normalized = NormalizedShareUrl {
            share_id: "A27pGwf5f9".to_owned(),
            canonical_url: Url::parse("https://weixin.qq.com/sph/A27pGwf5f9").unwrap(),
        };
        let parse_data = ParseData {
            wx_export_id: "fallback-export-id".to_owned(),
            cover_url: String::new(),
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
        assert_eq!(post.title.as_deref(), Some("测试视频"));
        assert_eq!(post.video.provenance, MediaSourceKind::Derived);
        assert_eq!(post.video.codec, VideoCodec::H264);
        assert_eq!(post.video.url.query(), Some("encfilekey=h&token=t"));
        assert_eq!(post.video.size_hint, None);
        assert_eq!(
            post.fallback_videos
                .iter()
                .map(|source| source.codec)
                .collect::<Vec<_>>(),
            [VideoCodec::H265, VideoCodec::Unknown]
        );
    }

    #[test]
    fn accepts_root_level_feed_shape_and_prefers_direct_source() {
        let normalized = NormalizedShareUrl {
            share_id: "A27pGwf5f9".to_owned(),
            canonical_url: Url::parse("https://weixin.qq.com/sph/A27pGwf5f9").unwrap(),
        };
        let parse_data = ParseData {
            wx_export_id: String::new(),
            cover_url: String::new(),
            desc: String::new(),
            playable_url: "https://example.invalid/?token=dummy".to_owned(),
        };
        let feed = serde_json::json!({
            "feedInfo": {
                "h264VideoInfo": {
                    "videoUrl": "https://finder.video.qq.com/candidate.mp4?encfilekey=k&token=t&quality=hd"
                },
                "originVideoUrl": "https://finder.video.qq.com/direct.mp4?token=t"
            }
        });

        let post = build_post(normalized, parse_data, feed, "export-id".to_owned()).unwrap();
        assert_eq!(post.video.provenance, MediaSourceKind::Direct);
        assert_eq!(
            post.video.url.as_str(),
            "https://finder.video.qq.com/direct.mp4?token=t"
        );
        assert_eq!(post.fallback_videos.len(), 1);
        assert_eq!(post.fallback_videos[0].codec, VideoCodec::H264);
    }

    #[test]
    fn preserves_direct_identity_and_merges_metadata_when_urls_are_equal() {
        let normalized = NormalizedShareUrl {
            share_id: "A27pGwf5f9".to_owned(),
            canonical_url: Url::parse("https://weixin.qq.com/sph/A27pGwf5f9").unwrap(),
        };
        let parse_data = ParseData {
            wx_export_id: String::new(),
            cover_url: String::new(),
            desc: String::new(),
            playable_url: "https://example.invalid/?token=dummy".to_owned(),
        };
        let shared_url = "https://findermp.video.qq.com/shared.mp4?token=t";
        let feed = serde_json::json!({
            "feedInfo": {
                "h264VideoInfo": {
                    "videoUrl": shared_url,
                    "width": 1080,
                    "height": 1920,
                    "fileSize": 7654321,
                    "decodeKey": "2136343393"
                },
                "originVideoUrl": shared_url
            }
        });

        let post = build_post(normalized, parse_data, feed, "export-id".to_owned()).unwrap();
        assert_eq!(post.video.provenance, MediaSourceKind::Direct);
        assert_eq!(post.video.codec, VideoCodec::H264);
        assert_eq!(
            (post.video.width, post.video.height),
            (Some(1080), Some(1920))
        );
        assert_eq!(post.video.size_hint, Some(7_654_321));
        assert_eq!(post.video.decode_key, Some(2_136_343_393));
    }

    #[test]
    fn direct_source_inherits_safe_matching_candidate_metadata() {
        let normalized = NormalizedShareUrl {
            share_id: "A27pGwf5f9".to_owned(),
            canonical_url: Url::parse("https://weixin.qq.com/sph/A27pGwf5f9").unwrap(),
        };
        let parse_data = ParseData {
            wx_export_id: String::new(),
            cover_url: String::new(),
            desc: String::new(),
            playable_url: "https://example.invalid/?token=dummy".to_owned(),
        };
        let feed = serde_json::json!({
            "feedInfo": {
                "h265VideoInfo": {
                    "videoUrl": "https://finder.video.wechat.com/shared.mp4?quality=hd&token=t&encfilekey=k",
                    "width": 1920,
                    "height": 1080,
                    "fileSize": 123456,
                    "decodeKey": 987654321
                },
                "originVideoUrl": "https://finder.video.wechat.com/shared.mp4?token=t"
            }
        });

        let post = build_post(normalized, parse_data, feed, "export-id".to_owned()).unwrap();
        assert_eq!(post.video.provenance, MediaSourceKind::Direct);
        assert_eq!(post.video.codec, VideoCodec::H265);
        assert_eq!(
            (post.video.width, post.video.height),
            (Some(1920), Some(1080))
        );
        assert_eq!(post.video.decode_key, Some(987_654_321));
        assert_eq!(post.video.size_hint, None);
    }

    #[test]
    fn direct_source_does_not_inherit_an_ambiguous_decode_key() {
        let normalized = NormalizedShareUrl {
            share_id: "A27pGwf5f9".to_owned(),
            canonical_url: Url::parse("https://weixin.qq.com/sph/A27pGwf5f9").unwrap(),
        };
        let parse_data = ParseData {
            wx_export_id: String::new(),
            cover_url: String::new(),
            desc: String::new(),
            playable_url: "https://example.invalid/?token=dummy".to_owned(),
        };
        let feed = serde_json::json!({
            "feedInfo": {
                "h264VideoInfo": {
                    "videoUrl": "https://finder.video.qq.com/shared.mp4?encfilekey=h264-key&token=shared-token",
                    "decodeKey": 111
                },
                "h265VideoInfo": {
                    "videoUrl": "https://finder.video.qq.com/shared.mp4?encfilekey=h265-key&token=shared-token",
                    "decodeKey": 222
                },
                "originVideoUrl": "https://finder.video.qq.com/shared.mp4?token=shared-token"
            }
        });

        let post = build_post(normalized, parse_data, feed, "export-id".to_owned()).unwrap();
        assert_eq!(post.video.provenance, MediaSourceKind::Direct);
        assert_eq!(post.video.codec, VideoCodec::Unknown);
        assert_eq!(post.video.decode_key, None);
    }

    #[test]
    fn same_url_candidates_keep_distinct_decode_keys_as_fallbacks() {
        let normalized = NormalizedShareUrl {
            share_id: "A27pGwf5f9".to_owned(),
            canonical_url: Url::parse("https://weixin.qq.com/sph/A27pGwf5f9").unwrap(),
        };
        let parse_data = ParseData {
            wx_export_id: String::new(),
            cover_url: String::new(),
            desc: String::new(),
            playable_url: "https://example.invalid/?token=dummy".to_owned(),
        };
        let feed = serde_json::json!({
            "feedInfo": {
                "h264VideoInfo": {
                    "videoUrl": "https://finder.video.qq.com/shared.mp4?encfilekey=k&token=t&quality=hd",
                    "decodeKey": 111
                },
                "h265VideoInfo": {
                    "videoUrl": "https://finder.video.qq.com/shared.mp4?token=t&encfilekey=k&quality=sd",
                    "decodeKey": 222
                },
                "originVideoUrl": "https://finder.video.qq.com/shared.mp4?encfilekey=k&token=t"
            }
        });

        let post = build_post(normalized, parse_data, feed, "export-id".to_owned()).unwrap();
        assert_eq!(post.video.decode_key, None);
        assert_eq!(
            post.fallback_videos
                .iter()
                .map(|source| (source.codec, source.decode_key))
                .collect::<Vec<_>>(),
            [(VideoCodec::H264, Some(111)), (VideoCodec::H265, Some(222))]
        );
        assert!(
            post.fallback_videos
                .iter()
                .all(|source| source.url == post.video.url)
        );
    }

    #[test]
    fn rejects_a_post_when_no_media_source_can_be_derived() {
        let normalized = NormalizedShareUrl {
            share_id: "A27pGwf5f9".to_owned(),
            canonical_url: Url::parse("https://weixin.qq.com/sph/A27pGwf5f9").unwrap(),
        };
        let parse_data = ParseData {
            wx_export_id: String::new(),
            cover_url: String::new(),
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
        assert!(matches!(error, Error::MediaUnavailable));
    }

    #[test]
    fn tries_h265_when_h264_cannot_derive_a_media_source() {
        let normalized = NormalizedShareUrl {
            share_id: "A27pGwf5f9".to_owned(),
            canonical_url: Url::parse("https://weixin.qq.com/sph/A27pGwf5f9").unwrap(),
        };
        let parse_data = ParseData {
            wx_export_id: String::new(),
            cover_url: String::new(),
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
        assert_eq!(post.video.provenance, MediaSourceKind::Derived);
        assert_eq!(post.video.codec, VideoCodec::H265);
        assert_eq!(
            post.video.url.as_str(),
            "https://finder.video.qq.com/h265.mp4?encfilekey=k&token=t"
        );
    }
}
