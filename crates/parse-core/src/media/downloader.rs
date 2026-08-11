use std::{
    collections::{HashMap, HashSet},
    fs::{File, OpenOptions},
    future::Future,
    io::Write,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex as StdMutex,
        atomic::{AtomicBool, Ordering},
    },
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
use tokio::{sync::mpsc, time::timeout};
use tracing::warn;
use url::{Host, Url};
use uuid::Uuid;

use crate::{
    error::{Error, Result},
    model::{MediaSource, REVIEWED_WECHAT_MEDIA_HOSTS},
};

const MAX_REDIRECTS: usize = 5;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(2 * 60 * 60);
const DOWNLOAD_IDLE_TIMEOUT: Duration = Duration::from_secs(60);
const DNS_TIMEOUT: Duration = Duration::from_secs(5);
const MIN_FREE_DISK_BYTES: u64 = 512 * 1024 * 1024;
const DISK_CHECK_INTERVAL_BYTES: u64 = 16 * 1024 * 1024;
const CHANNELS_ORIGIN: &str = "https://channels.weixin.qq.com";
const CHANNELS_REFERER: &str = "https://channels.weixin.qq.com/";
const MEDIA_USER_AGENT: &str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 \
     (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36";
const PROGRESS_THRESHOLDS: [u8; 5] = [20, 40, 60, 80, 100];
const DOWNLOAD_RETRY_DELAYS: [Duration; 2] = [Duration::from_secs(1), Duration::from_secs(2)];

/// Progress reported after source bytes have actually been written to disk.
///
/// Events are emitted only when the final media response provides a valid,
/// non-zero `Content-Length`. `percent` is therefore always one of 20, 40, 60,
/// 80, or 100 and is never estimated from a parser-provided size hint.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DownloadProgress {
    pub downloaded_bytes: u64,
    pub total_bytes: u64,
    pub percent: u8,
}

type ProgressCallback = Arc<dyn Fn(DownloadProgress) + Send + Sync + 'static>;

// Keep this deliberately narrow. New Tencent CDN names must be reviewed before
// being added; accepting all of qq.com would turn redirects into an SSRF surface.
/// A completed media download. The caller owns the file until it calls
/// [`DownloadedMedia::cleanup`].
#[derive(Debug)]
#[must_use = "downloaded media must be uploaded or explicitly cleaned up"]
pub struct DownloadedMedia {
    pub path: PathBuf,
    pub bytes: u64,
}

impl DownloadedMedia {
    /// Explicitly remove the downloaded file. A file already removed is treated
    /// as successfully cleaned up, which makes retrying cleanup safe.
    pub async fn cleanup(&self) -> Result<()> {
        match tokio::fs::remove_file(&self.path).await {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(Error::Io(error)),
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

/// Downloads media into the configured task workspace while enforcing URL,
/// network, redirect, and byte-count limits.
#[derive(Clone, Debug)]
pub struct MediaDownloader {
    workspace_dir: Arc<PathBuf>,
    max_bytes: u64,
    allowed_hosts: Arc<HashSet<String>>,
    request_timeout: Duration,
    disk_write_budget: Arc<StdMutex<DiskWriteBudget>>,
}

#[derive(Debug, Default)]
struct DiskWriteBudget {
    unchecked_bytes: u64,
}

impl MediaDownloader {
    /// Construct a downloader using the reviewed WeChat media CDN allowlist.
    pub fn new(workspace_dir: impl Into<PathBuf>, max_bytes: u64) -> Result<Self> {
        if max_bytes == 0 {
            return Err(Error::Config("媒体下载大小上限必须大于零".to_owned()));
        }

        let allowed_hosts = REVIEWED_WECHAT_MEDIA_HOSTS
            .iter()
            .map(|host| (*host).to_owned())
            .collect();

        Self::with_options(workspace_dir, max_bytes, allowed_hosts, REQUEST_TIMEOUT)
    }

    pub fn with_options(
        workspace_dir: impl Into<PathBuf>,
        max_bytes: u64,
        allowed_hosts: HashSet<String>,
        request_timeout: Duration,
    ) -> Result<Self> {
        if max_bytes == 0 {
            return Err(Error::Config("媒体下载大小上限必须大于零".to_owned()));
        }
        if request_timeout.is_zero() {
            return Err(Error::Config("媒体下载超时必须大于零".to_owned()));
        }
        let allowed_hosts = allowed_hosts
            .into_iter()
            .map(|host| host.to_ascii_lowercase())
            .collect::<HashSet<_>>();
        if allowed_hosts.is_empty() || allowed_hosts.iter().any(|host| !valid_host_name(host)) {
            return Err(Error::Config("媒体主机允许列表无效".to_owned()));
        }

        Ok(Self {
            workspace_dir: Arc::new(workspace_dir.into()),
            max_bytes,
            allowed_hosts: Arc::new(allowed_hosts),
            request_timeout,
            disk_write_budget: Arc::new(StdMutex::new(DiskWriteBudget::default())),
        })
    }

    /// Return a downloader that shares this instance's storage and network
    /// policy while enforcing an equal or stricter byte limit.
    ///
    /// This is intended for callers that need to fetch a smaller auxiliary
    /// asset without constructing a second HTTP client policy. Asking for a
    /// larger limit never weakens the original downloader's cap.
    pub fn capped(&self, max_bytes: u64) -> Result<Self> {
        if max_bytes == 0 {
            return Err(Error::Config("媒体下载大小上限必须大于零".to_owned()));
        }

        Ok(Self {
            workspace_dir: Arc::clone(&self.workspace_dir),
            max_bytes: self.max_bytes.min(max_bytes),
            allowed_hosts: Arc::clone(&self.allowed_hosts),
            request_timeout: self.request_timeout,
            disk_write_budget: Arc::clone(&self.disk_write_budget),
        })
    }

    /// Return a downloader with stricter byte and request-duration limits.
    pub fn capped_with_timeout(&self, max_bytes: u64, request_timeout: Duration) -> Result<Self> {
        if request_timeout.is_zero() {
            return Err(Error::Config("媒体下载超时必须大于零".to_owned()));
        }
        let mut downloader = self.capped(max_bytes)?;
        downloader.request_timeout = downloader.request_timeout.min(request_timeout);
        Ok(downloader)
    }

    /// Download a resolved media source. Its parser-provided size hint is an
    /// early limit only; Content-Length and streamed bytes are checked again.
    pub async fn download(&self, source: &MediaSource) -> Result<DownloadedMedia> {
        self.download_url_with_callback(&source.url, source.size_hint, None)
            .await
    }

    /// Download a URL without a parser-provided size hint.
    ///
    /// The request still passes through the downloader's host allowlist, DNS
    /// pinning, redirect validation, timeout, and streaming byte limit.
    pub async fn download_url(&self, url: &Url) -> Result<DownloadedMedia> {
        self.download_url_with_callback(url, None, None).await
    }

    /// Download a resolved media source and report actual on-disk progress.
    ///
    /// The callback is synchronous and should return quickly (for example by
    /// sending the event through a channel). It is called at most once for each
    /// 20%, 40%, 60%, 80%, and 100% threshold. If the final response has no trustworthy
    /// total size, the download still succeeds but no percentage event is sent.
    pub async fn download_with_progress<F>(
        &self,
        source: &MediaSource,
        on_progress: F,
    ) -> Result<DownloadedMedia>
    where
        F: Fn(DownloadProgress) + Send + Sync + 'static,
    {
        self.download_url_with_callback(&source.url, source.size_hint, Some(Arc::new(on_progress)))
            .await
    }

    async fn download_url_with_callback(
        &self,
        url: &Url,
        size_hint: Option<u64>,
        progress_callback: Option<ProgressCallback>,
    ) -> Result<DownloadedMedia> {
        timeout(
            self.request_timeout,
            retry_transient_downloads(
                || self.download_url_within_deadline(url, size_hint, progress_callback.clone()),
                &DOWNLOAD_RETRY_DELAYS,
            ),
        )
        .await
        .map_err(|_| Error::Download("媒体下载总超时".to_owned()))?
    }

    async fn download_url_within_deadline(
        &self,
        url: &Url,
        size_hint: Option<u64>,
        progress_callback: Option<ProgressCallback>,
    ) -> Result<DownloadedMedia> {
        if let Some(actual) = size_hint.filter(|size| *size > self.max_bytes) {
            return Err(Error::MediaTooLarge {
                actual,
                limit: self.max_bytes,
            });
        }

        let response = self.follow_redirects(url.clone()).await?;
        check_response_status(response.status())?;
        reject_encoded_response(&response)?;

        let content_length = checked_content_length(&response)?;
        if let Some(actual) = content_length.filter(|actual| *actual > self.max_bytes) {
            return Err(Error::MediaTooLarge {
                actual,
                limit: self.max_bytes,
            });
        }

        self.stream_response(response, content_length, progress_callback)
            .await
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
                .ok_or_else(|| Error::Download("媒体 HTTP 客户端初始化失败".to_owned()))?;

            let response = self.request_with_client(client, &current).await?;

            if !response.status().is_redirection() {
                return Ok(response);
            }

            if redirect_count == MAX_REDIRECTS {
                return Err(Error::Download("媒体重定向次数过多".to_owned()));
            }

            let location = response
                .headers()
                .get(LOCATION)
                .ok_or_else(|| Error::Download("媒体重定向缺少 Location".to_owned()))?
                .to_str()
                .map_err(|_| Error::Download("媒体重定向地址无效".to_owned()))?;

            current = current
                .join(location)
                .map_err(|_| Error::Download("媒体重定向地址无效".to_owned()))?;
        }

        Err(Error::Download("媒体重定向次数过多".to_owned()))
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

    async fn stream_response(
        &self,
        mut response: Response,
        content_length: Option<u64>,
        progress_callback: Option<ProgressCallback>,
    ) -> Result<DownloadedMedia> {
        tokio::fs::create_dir_all(self.workspace_dir.as_path())
            .await
            .map_err(|_| Error::Storage(self.workspace_dir.as_ref().clone()))?;
        let path = random_task_path(self.workspace_dir.as_path());
        let pending_file = create_private_file(path.clone()).await?;
        let (sender, mut receiver) = mpsc::channel(4);
        let writer_limit = self.max_bytes;
        let disk_write_budget = Arc::clone(&self.disk_write_budget);
        let (progress_reporter, _progress_guard) =
            ProgressReporter::new(content_length, progress_callback);

        let writer = tokio::task::spawn_blocking(move || -> Result<WrittenMedia> {
            write_chunks(
                pending_file,
                &mut receiver,
                writer_limit,
                progress_reporter,
                disk_write_budget,
            )
        });

        let mut streamed_bytes = 0_u64;
        let stream_result: Result<()> = async {
            loop {
                let chunk = timeout(DOWNLOAD_IDLE_TIMEOUT, response.chunk())
                    .await
                    .map_err(|_| Error::Network("媒体响应读取超时".to_owned()))?
                    .map_err(map_reqwest_download_error)?;
                let Some(chunk) = chunk else {
                    break;
                };
                streamed_bytes =
                    streamed_bytes
                        .checked_add(chunk.len() as u64)
                        .ok_or(Error::MediaTooLarge {
                            actual: u64::MAX,
                            limit: self.max_bytes,
                        })?;

                if streamed_bytes > self.max_bytes {
                    return Err(Error::MediaTooLarge {
                        actual: streamed_bytes,
                        limit: self.max_bytes,
                    });
                }

                sender
                    .send(chunk)
                    .await
                    .map_err(|_| Error::Download("媒体文件写入失败".to_owned()))?;
            }
            Ok(())
        }
        .await;

        drop(sender);
        let writer_result = match writer.await {
            Ok(result) => result,
            Err(_) => {
                let _ = tokio::fs::remove_file(&path).await;
                return Err(Error::Download("媒体文件写入任务失败".to_owned()));
            }
        };

        let outcome = match stream_result.and(writer_result) {
            Ok(outcome) => outcome,
            Err(error) => return Err(error),
        };

        if outcome.media.bytes != streamed_bytes {
            return Err(Error::Download("媒体文件写入不完整".to_owned()));
        }

        let disk_bytes = match tokio::fs::metadata(&outcome.media.path).await {
            Ok(metadata) => metadata.len(),
            Err(_) => return Err(Error::Storage(self.workspace_dir.as_ref().clone())),
        };

        if disk_bytes > self.max_bytes {
            return Err(Error::MediaTooLarge {
                actual: disk_bytes,
                limit: self.max_bytes,
            });
        }
        if disk_bytes != outcome.media.bytes {
            return Err(Error::Download("媒体文件落盘大小不一致".to_owned()));
        }

        if let Some(expected) = content_length
            && disk_bytes != expected
        {
            return Err(Error::Network(
                "媒体文件大小与 Content-Length 不一致".to_owned(),
            ));
        }

        let WrittenMedia {
            media,
            mut progress_reporter,
        } = outcome;
        if let Some(reporter) = &mut progress_reporter {
            reporter.report_complete(media.bytes);
        }

        Ok(media)
    }
}

async fn retry_transient_downloads<T, F, Fut>(
    mut operation: F,
    retry_delays: &[Duration],
) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T>>,
{
    let max_attempts = retry_delays.len() + 1;
    for attempt in 1..=max_attempts {
        match operation().await {
            Ok(value) => return Ok(value),
            Err(error) if is_transient_download_error(&error) && attempt < max_attempts => {
                let delay = retry_delays[attempt - 1];
                warn!(
                    event = "media_download_retry",
                    attempt,
                    max_attempts,
                    ?delay,
                    error = %error,
                    "媒体下载遇到临时错误，准备重试"
                );
                tokio::time::sleep(delay).await;
            }
            Err(error) => return Err(error),
        }
    }
    unreachable!("the download retry loop always returns on its final attempt")
}

fn is_transient_download_error(error: &Error) -> bool {
    // A 429 may carry a server-specific Retry-After value that Error does
    // not preserve. Surface it instead of guessing a delay and adding load.
    matches!(error, Error::Network(_))
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
        .map_err(|_| Error::Config("无法初始化媒体 HTTP 客户端".to_owned()))
}

fn validate_media_url<'a>(url: &'a Url, allowed_hosts: &HashSet<String>) -> Result<&'a str> {
    if url.scheme() != "https" {
        return Err(Error::Download("媒体地址必须使用 HTTPS".to_owned()));
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err(Error::Download("媒体地址不能包含用户凭据".to_owned()));
    }
    if url.port().is_some_and(|port| port != 443) {
        return Err(Error::Download("媒体地址不能使用非标准端口".to_owned()));
    }
    if url.fragment().is_some() {
        return Err(Error::Download("媒体地址不能包含片段标识".to_owned()));
    }

    let host = match url.host() {
        Some(Host::Domain(host)) if !host.ends_with('.') => host,
        _ => return Err(Error::Download("媒体地址主机无效".to_owned())),
    };
    let normalized = host.to_ascii_lowercase();
    if !allowed_hosts.contains(&normalized) {
        return Err(Error::Download("媒体地址主机不在允许列表中".to_owned()));
    }

    Ok(host)
}

async fn resolve_public_addresses(host: &str) -> Result<Vec<SocketAddr>> {
    let addresses = timeout(DNS_TIMEOUT, tokio::net::lookup_host((host, 443)))
        .await
        .map_err(|_| Error::Network("媒体主机 DNS 解析超时".to_owned()))?
        .map_err(|_| Error::Network("媒体主机 DNS 解析失败".to_owned()))?
        .collect::<Vec<_>>();

    if addresses.is_empty() {
        return Err(Error::Network("媒体主机没有可用 DNS 地址".to_owned()));
    }
    if addresses
        .iter()
        .any(|address| is_forbidden_ip(address.ip()))
    {
        return Err(Error::Download(
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
        StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => Err(Error::Expired),
        StatusCode::NOT_FOUND | StatusCode::GONE => Err(Error::NotFound),
        StatusCode::TOO_MANY_REQUESTS => Err(Error::RateLimited),
        StatusCode::REQUEST_TIMEOUT | StatusCode::TOO_EARLY => Err(Error::Network(format!(
            "媒体服务器暂时不可用（HTTP {}）",
            status.as_u16()
        ))),
        status if status.is_server_error() => Err(Error::Network(format!(
            "媒体服务器暂时不可用（HTTP {}）",
            status.as_u16()
        ))),
        status => Err(Error::Download(format!(
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
        .map_err(|_| Error::Download("媒体 Content-Length 无效".to_owned()))?;
    let length = raw
        .parse::<u64>()
        .map_err(|_| Error::Download("媒体 Content-Length 无效".to_owned()))?;
    Ok(Some(length))
}

fn reject_encoded_response(response: &Response) -> Result<()> {
    if let Some(value) = response.headers().get(CONTENT_ENCODING) {
        let value = value
            .to_str()
            .map_err(|_| Error::Download("媒体 Content-Encoding 无效".to_owned()))?;
        if !value.eq_ignore_ascii_case("identity") {
            return Err(Error::Download(
                "媒体服务器忽略了 identity 编码要求".to_owned(),
            ));
        }
    }
    Ok(())
}

fn map_reqwest_download_error(error: reqwest::Error) -> Error {
    if error.is_timeout() {
        Error::Network("媒体请求超时".to_owned())
    } else {
        // Do not format `error`: reqwest errors may include the signed URL.
        Error::Network("媒体网络请求失败".to_owned())
    }
}

fn random_task_path(directory: &Path) -> PathBuf {
    directory.join(format!("{}.mp4", Uuid::new_v4().hyphenated()))
}

#[cfg(unix)]
fn ensure_free_disk_space(directory: &Path, pending_write_bytes: u64) -> Result<()> {
    use std::{ffi::CString, mem::MaybeUninit, os::unix::ffi::OsStrExt};

    let path = CString::new(directory.as_os_str().as_bytes())
        .map_err(|_| Error::Storage(directory.to_owned()))?;
    let mut statistics = MaybeUninit::<libc::statvfs>::uninit();
    // SAFETY: `path` is NUL-terminated and `statistics` points to writable,
    // correctly aligned storage. A successful call initializes the structure.
    if unsafe { libc::statvfs(path.as_ptr(), statistics.as_mut_ptr()) } != 0 {
        return Err(Error::Storage(directory.to_owned()));
    }
    // SAFETY: statvfs returned success immediately above.
    let statistics = unsafe { statistics.assume_init() };
    let block_size = if statistics.f_frsize == 0 {
        statistics.f_bsize
    } else {
        statistics.f_frsize
    };
    let available_bytes = u128::from(statistics.f_bavail) * u128::from(block_size);
    if !disk_space_is_sufficient(available_bytes, pending_write_bytes) {
        return Err(Error::Storage(directory.to_owned()));
    }
    Ok(())
}

#[cfg(unix)]
fn disk_space_is_sufficient(available_bytes: u128, pending_write_bytes: u64) -> bool {
    let required_bytes = u128::from(MIN_FREE_DISK_BYTES.saturating_add(pending_write_bytes));
    available_bytes >= required_bytes
}

#[cfg(not(unix))]
fn ensure_free_disk_space(_directory: &Path, _pending_write_bytes: u64) -> Result<()> {
    // The supported deployment targets are Unix. Keep the write path portable;
    // non-Unix deployments should enforce a volume quota externally.
    Ok(())
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
    .map_err(|_| Error::Storage(storage_path.clone()))?
    .map_err(|_| Error::Storage(storage_path))
}

fn write_chunks<T: AsRef<[u8]>>(
    mut pending_file: PendingFile,
    receiver: &mut mpsc::Receiver<T>,
    max_bytes: u64,
    mut progress_reporter: Option<ProgressReporter>,
    disk_write_budget: Arc<StdMutex<DiskWriteBudget>>,
) -> Result<WrittenMedia> {
    let mut bytes = 0_u64;
    let mut checked_disk_space = false;

    while let Some(chunk) = receiver.blocking_recv() {
        let chunk = chunk.as_ref();
        let next_bytes = bytes
            .checked_add(chunk.len() as u64)
            .ok_or(Error::MediaTooLarge {
                actual: u64::MAX,
                limit: max_bytes,
            })?;
        if next_bytes > max_bytes {
            return Err(Error::MediaTooLarge {
                actual: next_bytes,
                limit: max_bytes,
            });
        }

        let chunk_bytes = chunk.len() as u64;
        let mut disk_budget = disk_write_budget
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let projected_bytes = disk_budget.unchecked_bytes.saturating_add(chunk_bytes);
        if !checked_disk_space
            || disk_budget.unchecked_bytes == 0
            || projected_bytes > DISK_CHECK_INTERVAL_BYTES
        {
            ensure_free_disk_space(
                pending_file
                    .path
                    .parent()
                    .ok_or_else(|| Error::Storage(pending_file.path.clone()))?,
                DISK_CHECK_INTERVAL_BYTES.max(chunk_bytes),
            )?;
            disk_budget.unchecked_bytes = 0;
            checked_disk_space = true;
        }
        pending_file.file_mut()?.write_all(chunk)?;
        disk_budget.unchecked_bytes = if chunk_bytes >= DISK_CHECK_INTERVAL_BYTES {
            0
        } else {
            disk_budget.unchecked_bytes.saturating_add(chunk_bytes)
        };
        drop(disk_budget);
        bytes = next_bytes;
        if let Some(reporter) = &mut progress_reporter {
            reporter.report_intermediate(bytes);
        }
    }

    let media = pending_file.finish(bytes)?;
    Ok(WrittenMedia {
        media,
        progress_reporter,
    })
}

struct WrittenMedia {
    media: DownloadedMedia,
    progress_reporter: Option<ProgressReporter>,
}

struct ProgressReporter {
    callback: ProgressCallback,
    total_bytes: u64,
    next_threshold: usize,
    active: Arc<AtomicBool>,
}

impl ProgressReporter {
    fn new(
        content_length: Option<u64>,
        callback: Option<ProgressCallback>,
    ) -> (Option<Self>, Option<ProgressGuard>) {
        let Some(total_bytes) = content_length.filter(|total| *total > 0) else {
            return (None, None);
        };
        let Some(callback) = callback else {
            return (None, None);
        };

        let active = Arc::new(AtomicBool::new(true));
        (
            Some(Self {
                callback,
                total_bytes,
                next_threshold: 0,
                active: Arc::clone(&active),
            }),
            Some(ProgressGuard { active }),
        )
    }

    fn report_intermediate(&mut self, downloaded_bytes: u64) {
        self.report_crossed(downloaded_bytes, false);
    }

    fn report_complete(&mut self, downloaded_bytes: u64) {
        self.report_crossed(downloaded_bytes, true);
    }

    fn report_crossed(&mut self, downloaded_bytes: u64, include_complete: bool) {
        while let Some(&percent) = PROGRESS_THRESHOLDS.get(self.next_threshold) {
            if percent == 100 && !include_complete {
                break;
            }
            if u128::from(downloaded_bytes) * 100
                < u128::from(self.total_bytes) * u128::from(percent)
            {
                break;
            }

            self.next_threshold += 1;
            if self.active.load(Ordering::Acquire) {
                (self.callback)(DownloadProgress {
                    downloaded_bytes,
                    total_bytes: self.total_bytes,
                    percent,
                });
            }
        }
    }
}

struct ProgressGuard {
    active: Arc<AtomicBool>,
}

impl Drop for ProgressGuard {
    fn drop(&mut self) {
        self.active.store(false, Ordering::Release);
    }
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
            .ok_or_else(|| Error::Download("临时媒体文件已经关闭".to_owned()))
    }

    fn finish(mut self, bytes: u64) -> Result<DownloadedMedia> {
        let file = self.file_mut()?;
        file.flush()?;
        drop(self.file.take());
        self.armed = false;
        Ok(DownloadedMedia {
            path: self.path.clone(),
            bytes,
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
    use std::sync::{Mutex, atomic::AtomicUsize};

    use super::*;

    fn allowed_hosts() -> HashSet<String> {
        REVIEWED_WECHAT_MEDIA_HOSTS
            .iter()
            .map(|host| (*host).to_owned())
            .collect()
    }

    #[tokio::test]
    async fn retries_only_transient_download_failures() {
        let transient_attempts = AtomicUsize::new(0);
        let value = retry_transient_downloads(
            || {
                let attempt = transient_attempts.fetch_add(1, Ordering::SeqCst);
                async move {
                    if attempt < 2 {
                        Err(Error::Network("temporary".into()))
                    } else {
                        Ok(42_u8)
                    }
                }
            },
            &[Duration::ZERO, Duration::ZERO],
        )
        .await
        .unwrap();
        assert_eq!(value, 42);
        assert_eq!(transient_attempts.load(Ordering::SeqCst), 3);

        let permanent_attempts = AtomicUsize::new(0);
        let error = retry_transient_downloads(
            || {
                permanent_attempts.fetch_add(1, Ordering::SeqCst);
                async { Err::<(), _>(Error::NotFound) }
            },
            &[Duration::ZERO, Duration::ZERO],
        )
        .await
        .unwrap_err();
        assert!(matches!(error, Error::NotFound));
        assert_eq!(permanent_attempts.load(Ordering::SeqCst), 1);

        let rate_limited_attempts = AtomicUsize::new(0);
        let error = retry_transient_downloads(
            || {
                rate_limited_attempts.fetch_add(1, Ordering::SeqCst);
                async { Err::<(), _>(Error::RateLimited) }
            },
            &[Duration::ZERO, Duration::ZERO],
        )
        .await
        .unwrap_err();
        assert!(matches!(error, Error::RateLimited));
        assert_eq!(rate_limited_attempts.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn cancelling_a_retry_backoff_returns_immediately() {
        let attempts = AtomicUsize::new(0);
        let retry_delays = [Duration::from_secs(60)];
        let retrying = retry_transient_downloads(
            || {
                attempts.fetch_add(1, Ordering::SeqCst);
                async { Err::<(), _>(Error::Network("temporary".into())) }
            },
            &retry_delays,
        );

        assert!(
            tokio::time::timeout(Duration::from_millis(20), retrying)
                .await
                .is_err()
        );
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn classifies_only_temporary_http_statuses_for_retry() {
        assert!(matches!(
            check_response_status(StatusCode::INTERNAL_SERVER_ERROR),
            Err(Error::Network(_))
        ));
        assert!(matches!(
            check_response_status(StatusCode::REQUEST_TIMEOUT),
            Err(Error::Network(_))
        ));
        assert!(matches!(
            check_response_status(StatusCode::TOO_MANY_REQUESTS),
            Err(Error::RateLimited)
        ));
        assert!(matches!(
            check_response_status(StatusCode::NOT_FOUND),
            Err(Error::NotFound)
        ));
        assert!(matches!(
            check_response_status(StatusCode::BAD_REQUEST),
            Err(Error::Download(_))
        ));
    }

    #[test]
    fn uses_a_canonical_hyphenated_uuid_for_temporary_video_names() {
        let path = random_task_path(Path::new("media"));
        let file_name = path.file_name().unwrap().to_str().unwrap();
        let uuid = file_name.strip_suffix(".mp4").unwrap();

        assert_eq!(file_name.len(), 40);
        assert_eq!(uuid.as_bytes()[8], b'-');
        assert_eq!(uuid.as_bytes()[13], b'-');
        assert_eq!(uuid.as_bytes()[18], b'-');
        assert_eq!(uuid.as_bytes()[23], b'-');
        assert_eq!(
            Uuid::parse_str(uuid).unwrap().hyphenated().to_string(),
            uuid
        );
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
    fn capped_downloader_preserves_policy_and_only_tightens_limit() {
        let downloader = MediaDownloader::with_options(
            "media",
            100,
            HashSet::from(["EXAMPLE.COM".to_owned()]),
            Duration::from_secs(17),
        )
        .unwrap();

        let tighter = downloader.capped(25).unwrap();
        assert_eq!(tighter.max_bytes, 25);
        assert_eq!(tighter.workspace_dir, downloader.workspace_dir);
        assert_eq!(tighter.allowed_hosts, downloader.allowed_hosts);
        assert_eq!(tighter.request_timeout, downloader.request_timeout);
        assert!(Arc::ptr_eq(
            &tighter.workspace_dir,
            &downloader.workspace_dir
        ));
        assert!(Arc::ptr_eq(
            &tighter.allowed_hosts,
            &downloader.allowed_hosts
        ));

        let requested_expansion = downloader.capped(1_000).unwrap();
        assert_eq!(requested_expansion.max_bytes, 100);
    }

    #[test]
    fn capped_downloader_rejects_zero_limit() {
        let downloader = MediaDownloader::with_options(
            "media",
            100,
            HashSet::from(["example.com".to_owned()]),
            Duration::from_secs(17),
        )
        .unwrap();

        assert!(matches!(downloader.capped(0), Err(Error::Config(_))));
    }

    #[test]
    fn capped_downloader_can_also_tighten_the_timeout() {
        let downloader = MediaDownloader::with_options(
            "media",
            100,
            HashSet::from(["example.com".to_owned()]),
            Duration::from_secs(120),
        )
        .unwrap();

        let tighter = downloader
            .capped_with_timeout(25, Duration::from_secs(30))
            .unwrap();
        assert_eq!(tighter.max_bytes, 25);
        assert_eq!(tighter.request_timeout, Duration::from_secs(30));

        let unchanged = downloader
            .capped_with_timeout(1_000, Duration::from_secs(300))
            .unwrap();
        assert_eq!(unchanged.max_bytes, 100);
        assert_eq!(unchanged.request_timeout, Duration::from_secs(120));
        assert!(downloader.capped_with_timeout(25, Duration::ZERO).is_err());
    }

    #[tokio::test]
    async fn direct_url_download_still_rejects_disallowed_hosts_before_network_io() {
        let downloader = MediaDownloader::with_options(
            "media",
            100,
            HashSet::from(["allowed.example".to_owned()]),
            Duration::from_secs(17),
        )
        .unwrap();
        let disallowed = Url::parse("https://127.0.0.1/cover.jpg").unwrap();

        let error = downloader.download_url(&disallowed).await.unwrap_err();
        assert!(matches!(error, Error::Download(_)));
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

    #[cfg(unix)]
    #[test]
    fn preserves_disk_headroom_before_accepting_a_write() {
        let reserve = u128::from(MIN_FREE_DISK_BYTES);

        assert!(!disk_space_is_sufficient(reserve - 1, 0));
        assert!(disk_space_is_sufficient(reserve, 0));
        assert!(!disk_space_is_sufficient(reserve + 9, 10));
        assert!(disk_space_is_sufficient(reserve + 10, 10));
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
        };

        media.cleanup().await.unwrap();
        media.cleanup().await.unwrap();
        assert!(!path.exists());
        std::fs::remove_dir(directory).unwrap();
    }

    #[test]
    fn reports_each_crossed_threshold_once_after_writes() {
        let directory = std::env::temp_dir().join(format!(
            "parse-bot-progress-test-{}",
            Uuid::new_v4().simple()
        ));
        std::fs::create_dir_all(&directory).unwrap();
        let path = directory.join("media");
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .unwrap();
        let pending_file = PendingFile::new(path.clone(), file);

        let events = Arc::new(Mutex::new(Vec::new()));
        let callback_events = Arc::clone(&events);
        let callback: ProgressCallback = Arc::new(move |progress| {
            callback_events.lock().unwrap().push(progress);
        });
        let (reporter, guard) = ProgressReporter::new(Some(8), Some(callback));
        let guard = guard.unwrap();

        let (sender, mut receiver) = mpsc::channel(4);
        sender.try_send(vec![1, 2]).unwrap();
        sender.try_send(vec![3, 4, 5, 6]).unwrap();
        sender.try_send(vec![7, 8]).unwrap();
        drop(sender);

        let disk_write_budget = Arc::new(StdMutex::new(DiskWriteBudget::default()));
        let mut outcome =
            write_chunks(pending_file, &mut receiver, 8, reporter, disk_write_budget).unwrap();
        assert_eq!(
            std::fs::read(&outcome.media.path).unwrap(),
            (1_u8..=8).collect::<Vec<_>>()
        );
        assert_eq!(
            events
                .lock()
                .unwrap()
                .iter()
                .map(|event| (event.percent, event.downloaded_bytes, event.total_bytes))
                .collect::<Vec<_>>(),
            [(20, 2, 8), (40, 6, 8), (60, 6, 8), (80, 8, 8)]
        );

        outcome
            .progress_reporter
            .as_mut()
            .unwrap()
            .report_complete(outcome.media.bytes);
        assert_eq!(
            events
                .lock()
                .unwrap()
                .iter()
                .map(|event| event.percent)
                .collect::<Vec<_>>(),
            [20, 40, 60, 80, 100]
        );

        drop(guard);
        drop(outcome);
        assert!(!path.exists());
        std::fs::remove_dir(directory).unwrap();
    }

    #[test]
    fn does_not_report_percent_without_a_trusted_total() {
        let callback: ProgressCallback = Arc::new(|_| panic!("unexpected progress event"));
        let (reporter, guard) = ProgressReporter::new(None, Some(callback));
        assert!(reporter.is_none());
        assert!(guard.is_none());
    }

    #[test]
    fn progress_guard_suppresses_events_after_cancellation() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let callback_events = Arc::clone(&events);
        let callback: ProgressCallback = Arc::new(move |progress| {
            callback_events.lock().unwrap().push(progress);
        });
        let (mut reporter, guard) = ProgressReporter::new(Some(100), Some(callback));
        drop(guard);

        reporter.as_mut().unwrap().report_intermediate(75);
        reporter.as_mut().unwrap().report_complete(100);

        assert!(events.lock().unwrap().is_empty());
    }
}
