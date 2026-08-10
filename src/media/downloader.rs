use std::{
    collections::{HashMap, HashSet},
    fs::{File, OpenOptions},
    io::Write,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use reqwest::{
    Client, Response, StatusCode,
    header::{
        ACCEPT, ACCEPT_ENCODING, CONTENT_ENCODING, CONTENT_LENGTH, LOCATION, ORIGIN, REFERER,
        USER_AGENT,
    },
    redirect::Policy,
};
use sha2::{Digest, Sha256};
use tokio::{sync::mpsc, time::timeout};
use url::{Host, Url};
use uuid::Uuid;

use crate::{
    error::{AppError, Result},
    model::{MediaSource, REVIEWED_WECHAT_MEDIA_HOSTS},
};

const MAX_REDIRECTS: usize = 5;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(2 * 60 * 60);
const DNS_TIMEOUT: Duration = Duration::from_secs(5);
const CHANNELS_ORIGIN: &str = "https://channels.weixin.qq.com";
const CHANNELS_REFERER: &str = "https://channels.weixin.qq.com/";
const MEDIA_USER_AGENT: &str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 \
     (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36";

// Keep this deliberately narrow. New Tencent CDN names must be reviewed before
// being added; accepting all of qq.com would turn redirects into an SSRF surface.
/// A completed media download. The caller owns the file until it calls
/// [`DownloadedMedia::cleanup`].
#[derive(Debug)]
#[must_use = "downloaded media must be uploaded or explicitly cleaned up"]
pub struct DownloadedMedia {
    pub path: PathBuf,
    pub bytes: u64,
    /// Lower-case SHA-256 of the source bytes before any optional decryption.
    pub source_sha256: String,
}

impl DownloadedMedia {
    /// Explicitly remove the downloaded file. A file already removed is treated
    /// as successfully cleaned up, which makes retrying cleanup safe.
    pub async fn cleanup(&self) -> Result<()> {
        match tokio::fs::remove_file(&self.path).await {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(AppError::Io(error)),
        }
    }
}

impl Drop for DownloadedMedia {
    fn drop(&mut self) {
        // Best-effort RAII cleanup also covers task cancellation and runtime
        // shutdown. Explicit cleanup is still used so errors can be reported.
        let _ = std::fs::remove_file(&self.path);
    }
}

/// Downloads media into a shared task directory while enforcing URL, network,
/// redirect, and byte-count limits.
#[derive(Clone, Debug)]
pub struct MediaDownloader {
    shared_dir: Arc<PathBuf>,
    max_bytes: u64,
    allowed_hosts: Arc<HashSet<String>>,
    request_timeout: Duration,
}

impl MediaDownloader {
    /// Construct a downloader using the reviewed WeChat media CDN allowlist.
    pub fn new(shared_dir: impl Into<PathBuf>, max_bytes: u64) -> Result<Self> {
        if max_bytes == 0 {
            return Err(AppError::Config("媒体下载大小上限必须大于零".to_owned()));
        }

        let allowed_hosts = REVIEWED_WECHAT_MEDIA_HOSTS
            .iter()
            .map(|host| (*host).to_owned())
            .collect();

        Self::with_options(shared_dir, max_bytes, allowed_hosts, REQUEST_TIMEOUT)
    }

    pub fn with_options(
        shared_dir: impl Into<PathBuf>,
        max_bytes: u64,
        allowed_hosts: HashSet<String>,
        request_timeout: Duration,
    ) -> Result<Self> {
        if max_bytes == 0 {
            return Err(AppError::Config("媒体下载大小上限必须大于零".to_owned()));
        }
        if request_timeout.is_zero() {
            return Err(AppError::Config("媒体下载超时必须大于零".to_owned()));
        }
        let allowed_hosts = allowed_hosts
            .into_iter()
            .map(|host| host.to_ascii_lowercase())
            .collect::<HashSet<_>>();
        if allowed_hosts.is_empty() || allowed_hosts.iter().any(|host| !valid_host_name(host)) {
            return Err(AppError::Config("媒体主机允许列表无效".to_owned()));
        }

        Ok(Self {
            shared_dir: Arc::new(shared_dir.into()),
            max_bytes,
            allowed_hosts: Arc::new(allowed_hosts),
            request_timeout,
        })
    }

    pub fn max_bytes(&self) -> u64 {
        self.max_bytes
    }

    /// Download a resolved media source. Its parser-provided size hint is an
    /// early limit only; Content-Length and streamed bytes are checked again.
    pub async fn download(&self, source: &MediaSource) -> Result<DownloadedMedia> {
        self.download_url(&source.url, source.size_hint).await
    }

    /// Download a media URL with an optional trusted-or-untrusted size hint.
    pub async fn download_url(&self, url: &Url, size_hint: Option<u64>) -> Result<DownloadedMedia> {
        timeout(
            self.request_timeout,
            self.download_url_within_deadline(url, size_hint),
        )
        .await
        .map_err(|_| AppError::Download("媒体下载总超时".to_owned()))?
    }

    async fn download_url_within_deadline(
        &self,
        url: &Url,
        size_hint: Option<u64>,
    ) -> Result<DownloadedMedia> {
        if let Some(actual) = size_hint.filter(|size| *size > self.max_bytes) {
            return Err(AppError::MediaTooLarge {
                actual,
                limit: self.max_bytes,
            });
        }

        let response = self.follow_redirects(url.clone()).await?;
        check_response_status(response.status())?;
        reject_encoded_response(&response)?;

        if let Some(actual) = checked_content_length(&response)?
            && actual > self.max_bytes
        {
            return Err(AppError::MediaTooLarge {
                actual,
                limit: self.max_bytes,
            });
        }

        self.stream_response(response).await
    }

    async fn follow_redirects(&self, initial_url: Url) -> Result<Response> {
        let mut current = initial_url;
        let mut pinned_clients = HashMap::<String, Client>::new();

        for redirect_count in 0..=MAX_REDIRECTS {
            let host = validate_media_url(&current, &self.allowed_hosts)?.to_ascii_lowercase();
            if !pinned_clients.contains_key(&host) {
                let addresses = resolve_public_addresses(&host).await?;
                let client = pinned_http_client(&host, &addresses, self.request_timeout)?;
                pinned_clients.insert(host.clone(), client);
            }
            let client = pinned_clients
                .get(&host)
                .ok_or_else(|| AppError::Download("媒体 HTTP 客户端初始化失败".to_owned()))?;

            let response = self.request_with_client(client, &current).await?;

            if !response.status().is_redirection() {
                return Ok(response);
            }

            if redirect_count == MAX_REDIRECTS {
                return Err(AppError::Download("媒体重定向次数过多".to_owned()));
            }

            let location = response
                .headers()
                .get(LOCATION)
                .ok_or_else(|| AppError::Download("媒体重定向缺少 Location".to_owned()))?
                .to_str()
                .map_err(|_| AppError::Download("媒体重定向地址无效".to_owned()))?;

            current = current
                .join(location)
                .map_err(|_| AppError::Download("媒体重定向地址无效".to_owned()))?;
        }

        Err(AppError::Download("媒体重定向次数过多".to_owned()))
    }

    async fn request_with_client(&self, client: &Client, url: &Url) -> Result<Response> {
        client
            .get(url.clone())
            .header(ORIGIN, CHANNELS_ORIGIN)
            .header(REFERER, CHANNELS_REFERER)
            .header(USER_AGENT, MEDIA_USER_AGENT)
            .header(ACCEPT, "*/*")
            .header(ACCEPT_ENCODING, "identity")
            .send()
            .await
            .map_err(map_reqwest_download_error)
    }

    async fn stream_response(&self, mut response: Response) -> Result<DownloadedMedia> {
        tokio::fs::create_dir_all(self.shared_dir.as_path())
            .await
            .map_err(|_| AppError::Storage(self.shared_dir.as_ref().clone()))?;

        let path = random_task_path(self.shared_dir.as_path());
        let pending_file = create_private_file(path.clone()).await?;
        let (sender, mut receiver) = mpsc::channel(4);
        let writer_limit = self.max_bytes;

        let writer = tokio::task::spawn_blocking(move || -> Result<DownloadedMedia> {
            write_chunks(pending_file, &mut receiver, writer_limit)
        });

        let mut streamed_bytes = 0_u64;
        let stream_result: Result<()> = async {
            while let Some(chunk) = response.chunk().await.map_err(map_reqwest_download_error)? {
                streamed_bytes = streamed_bytes.checked_add(chunk.len() as u64).ok_or(
                    AppError::MediaTooLarge {
                        actual: u64::MAX,
                        limit: self.max_bytes,
                    },
                )?;

                if streamed_bytes > self.max_bytes {
                    return Err(AppError::MediaTooLarge {
                        actual: streamed_bytes,
                        limit: self.max_bytes,
                    });
                }

                sender
                    .send(chunk.to_vec())
                    .await
                    .map_err(|_| AppError::Download("媒体文件写入失败".to_owned()))?;
            }
            Ok(())
        }
        .await;

        drop(sender);
        let writer_result = match writer.await {
            Ok(result) => result,
            Err(_) => {
                let _ = tokio::fs::remove_file(&path).await;
                return Err(AppError::Download("媒体文件写入任务失败".to_owned()));
            }
        };

        let outcome = match stream_result.and(writer_result) {
            Ok(outcome) => outcome,
            Err(error) => return Err(error),
        };

        if outcome.bytes != streamed_bytes {
            return Err(AppError::Download("媒体文件写入不完整".to_owned()));
        }

        let disk_bytes = match tokio::fs::metadata(&outcome.path).await {
            Ok(metadata) => metadata.len(),
            Err(_) => return Err(AppError::Storage(self.shared_dir.as_ref().clone())),
        };

        if disk_bytes > self.max_bytes {
            return Err(AppError::MediaTooLarge {
                actual: disk_bytes,
                limit: self.max_bytes,
            });
        }
        if disk_bytes != outcome.bytes {
            return Err(AppError::Download("媒体文件落盘大小不一致".to_owned()));
        }

        Ok(outcome)
    }
}

fn pinned_http_client(
    host: &str,
    addresses: &[SocketAddr],
    request_timeout: Duration,
) -> Result<Client> {
    Client::builder()
        .redirect(Policy::none())
        .connect_timeout(CONNECT_TIMEOUT)
        .timeout(request_timeout)
        .https_only(true)
        .no_proxy()
        .resolve_to_addrs(host, addresses)
        .build()
        .map_err(|_| AppError::Config("无法初始化媒体 HTTP 客户端".to_owned()))
}

fn validate_media_url<'a>(url: &'a Url, allowed_hosts: &HashSet<String>) -> Result<&'a str> {
    if url.scheme() != "https" {
        return Err(AppError::Download("媒体地址必须使用 HTTPS".to_owned()));
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err(AppError::Download("媒体地址不能包含用户凭据".to_owned()));
    }
    if url.port().is_some_and(|port| port != 443) {
        return Err(AppError::Download("媒体地址不能使用非标准端口".to_owned()));
    }
    if url.fragment().is_some() {
        return Err(AppError::Download("媒体地址不能包含片段标识".to_owned()));
    }

    let host = match url.host() {
        Some(Host::Domain(host)) if !host.ends_with('.') => host,
        _ => return Err(AppError::Download("媒体地址主机无效".to_owned())),
    };
    let normalized = host.to_ascii_lowercase();
    if !allowed_hosts.contains(&normalized) {
        return Err(AppError::Download("媒体地址主机不在允许列表中".to_owned()));
    }

    Ok(host)
}

async fn resolve_public_addresses(host: &str) -> Result<Vec<SocketAddr>> {
    let addresses = timeout(DNS_TIMEOUT, tokio::net::lookup_host((host, 443)))
        .await
        .map_err(|_| AppError::Download("媒体主机 DNS 解析超时".to_owned()))?
        .map_err(|_| AppError::Download("媒体主机 DNS 解析失败".to_owned()))?
        .collect::<Vec<_>>();

    if addresses.is_empty() {
        return Err(AppError::Download("媒体主机没有可用 DNS 地址".to_owned()));
    }
    if addresses
        .iter()
        .any(|address| is_forbidden_ip(address.ip()))
    {
        return Err(AppError::Download(
            "媒体主机解析到了不允许的网络地址".to_owned(),
        ));
    }

    Ok(addresses)
}

fn is_forbidden_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => is_forbidden_ipv4(ip),
        IpAddr::V6(ip) => is_forbidden_ipv6(ip),
    }
}

fn is_forbidden_ipv4(ip: Ipv4Addr) -> bool {
    let [a, b, c, _d] = ip.octets();

    a == 0
        || a == 10
        || a == 127
        || (a == 100 && (64..=127).contains(&b)) // carrier-grade NAT
        || (a == 169 && b == 254) // link-local and metadata endpoints
        || (a == 172 && (16..=31).contains(&b))
        || (a == 192 && b == 168)
        || (a == 192 && b == 0 && c == 0)
        || (a == 192 && b == 0 && c == 2) // documentation
        || (a == 192 && b == 88 && c == 99)
        || (a == 198 && (b == 18 || b == 19)) // benchmarking
        || (a == 198 && b == 51 && c == 100) // documentation
        || (a == 203 && b == 0 && c == 113) // documentation
        || a >= 224 // multicast, reserved, and broadcast
}

fn is_forbidden_ipv6(ip: Ipv6Addr) -> bool {
    if let Some(mapped) = ip.to_ipv4_mapped() {
        return is_forbidden_ipv4(mapped);
    }

    let segments = ip.segments();
    ip.is_unspecified()
        || ip.is_loopback()
        || ip.is_multicast()
        || (segments[0] & 0xfe00) == 0xfc00 // unique-local fc00::/7
        || (segments[0] & 0xffc0) == 0xfe80 // link-local fe80::/10
        || (segments[0] & 0xffc0) == 0xfec0 // deprecated site-local fec0::/10
        || (segments[0] == 0x0064 && segments[1] == 0xff9b) // NAT64 transition ranges
        || segments[0] == 0x2002 // 6to4
        || (segments[0] == 0x2001 && segments[1] == 0) // Teredo
        || (segments[0] == 0x2001 && segments[1] == 0x0db8) // documentation
}

fn check_response_status(status: StatusCode) -> Result<()> {
    match status {
        StatusCode::OK => Ok(()),
        StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => Err(AppError::Expired),
        StatusCode::NOT_FOUND | StatusCode::GONE => Err(AppError::NotFound),
        StatusCode::TOO_MANY_REQUESTS => Err(AppError::RateLimited),
        status => Err(AppError::Download(format!(
            "媒体服务器返回 HTTP {}",
            status.as_u16()
        ))),
    }
}

fn checked_content_length(response: &Response) -> Result<Option<u64>> {
    let Some(raw) = response.headers().get(CONTENT_LENGTH) else {
        return Ok(None);
    };
    let raw = raw
        .to_str()
        .map_err(|_| AppError::Download("媒体 Content-Length 无效".to_owned()))?;
    let length = raw
        .parse::<u64>()
        .map_err(|_| AppError::Download("媒体 Content-Length 无效".to_owned()))?;
    Ok(Some(length))
}

fn reject_encoded_response(response: &Response) -> Result<()> {
    if let Some(value) = response.headers().get(CONTENT_ENCODING) {
        let value = value
            .to_str()
            .map_err(|_| AppError::Download("媒体 Content-Encoding 无效".to_owned()))?;
        if !value.eq_ignore_ascii_case("identity") {
            return Err(AppError::Download(
                "媒体服务器忽略了 identity 编码要求".to_owned(),
            ));
        }
    }
    Ok(())
}

fn map_reqwest_download_error(error: reqwest::Error) -> AppError {
    if error.is_timeout() {
        AppError::Download("媒体请求超时".to_owned())
    } else {
        // Do not format `error`: reqwest errors may include the signed URL.
        AppError::Download("媒体网络请求失败".to_owned())
    }
}

fn random_task_path(directory: &Path) -> PathBuf {
    directory.join(format!("{}.mp4", Uuid::new_v4().simple()))
}

async fn create_private_file(path: PathBuf) -> Result<PendingFile> {
    let storage_path = path.clone();
    tokio::task::spawn_blocking(move || {
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        options.open(&path).map(|file| PendingFile::new(path, file))
    })
    .await
    .map_err(|_| AppError::Storage(storage_path.clone()))?
    .map_err(|_| AppError::Storage(storage_path))
}

fn write_chunks(
    mut pending_file: PendingFile,
    receiver: &mut mpsc::Receiver<Vec<u8>>,
    max_bytes: u64,
) -> Result<DownloadedMedia> {
    let mut bytes = 0_u64;
    let mut hasher = Sha256::new();

    while let Some(chunk) = receiver.blocking_recv() {
        bytes = bytes
            .checked_add(chunk.len() as u64)
            .ok_or(AppError::MediaTooLarge {
                actual: u64::MAX,
                limit: max_bytes,
            })?;
        if bytes > max_bytes {
            return Err(AppError::MediaTooLarge {
                actual: bytes,
                limit: max_bytes,
            });
        }

        pending_file.file_mut()?.write_all(&chunk)?;
        hasher.update(&chunk);
    }

    pending_file.finish(bytes, hex::encode(hasher.finalize()))
}

struct PendingFile {
    path: PathBuf,
    file: Option<File>,
    armed: bool,
}

impl PendingFile {
    fn new(path: PathBuf, file: File) -> Self {
        Self {
            path,
            file: Some(file),
            armed: true,
        }
    }

    fn file_mut(&mut self) -> Result<&mut File> {
        self.file
            .as_mut()
            .ok_or_else(|| AppError::Download("临时媒体文件已经关闭".to_owned()))
    }

    fn finish(mut self, bytes: u64, source_sha256: String) -> Result<DownloadedMedia> {
        let file = self.file_mut()?;
        file.flush()?;
        file.sync_all()?;
        drop(self.file.take());
        self.armed = false;
        Ok(DownloadedMedia {
            path: self.path.clone(),
            bytes,
            source_sha256,
        })
    }
}

impl Drop for PendingFile {
    fn drop(&mut self) {
        // Close first so cleanup is reliable on every supported platform.
        drop(self.file.take());
        if self.armed {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

fn valid_host_name(host: &str) -> bool {
    !host.is_empty()
        && !host.starts_with('.')
        && !host.ends_with('.')
        && !host.contains("..")
        && host
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-'))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn allowed_hosts() -> HashSet<String> {
        REVIEWED_WECHAT_MEDIA_HOSTS
            .iter()
            .map(|host| (*host).to_owned())
            .collect()
    }

    #[test]
    fn accepts_only_reviewed_https_media_urls() {
        let allowed = allowed_hosts();
        let valid = Url::parse("https://finder.video.qq.com/path?token=secret").unwrap();
        assert_eq!(
            validate_media_url(&valid, &allowed).unwrap(),
            "finder.video.qq.com"
        );

        for raw in [
            "http://finder.video.qq.com/path",
            "https://user:pass@finder.video.qq.com/path",
            "https://finder.video.qq.com:444/path",
            "https://finder.video.qq.com/path#fragment",
            "https://finder.video.qq.com.evil.test/path",
            "https://finder.video.qq.com./path",
            "https://qq.com/path",
            "https://127.0.0.1/path",
        ] {
            let url = Url::parse(raw).unwrap();
            assert!(validate_media_url(&url, &allowed).is_err(), "{raw}");
        }
    }

    #[test]
    fn classifies_non_public_ipv4_addresses() {
        for raw in [
            "0.0.0.0",
            "10.1.2.3",
            "100.64.0.1",
            "127.0.0.1",
            "169.254.169.254",
            "172.16.0.1",
            "192.168.1.1",
            "198.18.0.1",
            "224.0.0.1",
        ] {
            assert!(is_forbidden_ip(raw.parse().unwrap()), "{raw}");
        }
        assert!(!is_forbidden_ip("1.1.1.1".parse().unwrap()));
    }

    #[test]
    fn classifies_non_public_ipv6_addresses() {
        for raw in ["::", "::1", "fc00::1", "fe80::1", "ff02::1", "2001:db8::1"] {
            assert!(is_forbidden_ip(raw.parse().unwrap()), "{raw}");
        }
        assert!(!is_forbidden_ip("2606:4700:4700::1111".parse().unwrap()));
        assert!(is_forbidden_ip("::ffff:127.0.0.1".parse().unwrap()));
    }

    #[tokio::test]
    async fn cleanup_is_idempotent() {
        let directory = std::env::temp_dir().join(format!(
            "parse-bot-downloader-test-{}",
            Uuid::new_v4().simple()
        ));
        std::fs::create_dir_all(&directory).unwrap();
        let path = directory.join("media");
        std::fs::write(&path, b"test").unwrap();
        let media = DownloadedMedia {
            path: path.clone(),
            bytes: 4,
            source_sha256: "unused".to_owned(),
        };

        media.cleanup().await.unwrap();
        media.cleanup().await.unwrap();
        assert!(!path.exists());
        std::fs::remove_dir(directory).unwrap();
    }
}
