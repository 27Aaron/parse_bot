use std::{collections::HashSet, env, path::PathBuf, time::Duration};

use parse_core::{
    media::{MediaDownloader, decrypt_file_prefix, probe_media},
    model::{MediaSource, MediaSourceKind, ResolvedPost},
    wechat::WechatResolver,
};
use uuid::Uuid;

const SAMPLE_SHARE_URL: &str = "https://weixin.qq.com/sph/A27pGwf5f9";
const ALLOWED_MEDIA_HOSTS: &[&str] = &[
    "finder.video.qq.com",
    "findermp.video.qq.com",
    "finder.video.wechat.com",
    "findermp.video.wechat.com",
];
const LIVE_DOWNLOAD_LIMIT_BYTES: u64 = 512 * 1024 * 1024;
const LIVE_DOWNLOAD_TIMEOUT: Duration = Duration::from_secs(20 * 60);

#[tokio::test]
#[ignore = "requires WECHAT_YUANBAO_COOKIE and live Tencent endpoints"]
async fn resolves_wechat_channels_sample() {
    let post = resolve_sample().await;

    assert!(
        post.platform == "wechat_channels",
        "unexpected platform identifier"
    );
    assert!(
        post.canonical_url.as_str() == SAMPLE_SHARE_URL,
        "unexpected canonical share URL"
    );
    assert!(!post.post_id.trim().is_empty(), "missing post identifier");
    assert_safe_media_source(&post.video);

    if post.video.provenance == MediaSourceKind::Derived {
        assert!(
            post.video.size_hint.is_none(),
            "derived source must not reuse a candidate size hint"
        );
        let query: Vec<_> = post.video.url.query_pairs().collect();
        assert!(query.len() == 2, "derived source query shape changed");
        assert!(
            query[0].0 == "encfilekey" && !query[0].1.is_empty(),
            "derived source is missing encfilekey"
        );
        assert!(
            query[1].0 == "token" && !query[1].1.is_empty(),
            "derived source is missing token"
        );
    }
}

#[tokio::test]
#[ignore = "downloads live Tencent media and requires WECHAT_YUANBAO_COOKIE plus ffprobe"]
async fn downloads_decrypts_and_probes_wechat_channels_sample() {
    let post = resolve_sample().await;
    let directory = TestDirectory::new();
    let allowed_hosts = ALLOWED_MEDIA_HOSTS
        .iter()
        .map(|host| (*host).to_owned())
        .collect::<HashSet<_>>();
    let downloader = MediaDownloader::with_options(
        directory.path(),
        LIVE_DOWNLOAD_LIMIT_BYTES,
        allowed_hosts,
        LIVE_DOWNLOAD_TIMEOUT,
    )
    .unwrap_or_else(|_| panic!("failed to initialize the live media downloader"));
    let downloaded = downloader
        .download(&post.video)
        .await
        .unwrap_or_else(|_| panic!("live WeChat media download failed"));

    assert!(downloaded.bytes > 0, "downloaded media is empty");
    if let Some(decode_key) = post.video.decode_key {
        decrypt_file_prefix(&downloaded.path, decode_key)
            .await
            .unwrap_or_else(|_| panic!("live WeChat media decryption failed"));
    }
    let probe = probe_media(&downloaded.path)
        .await
        .unwrap_or_else(|_| panic!("live WeChat media probe failed"));
    assert!(
        probe.width > 0 && probe.height > 0,
        "invalid video dimensions"
    );

    let path = downloaded.path.clone();
    downloaded
        .cleanup()
        .await
        .unwrap_or_else(|_| panic!("failed to clean up live test media"));
    assert!(!path.exists(), "live test media was not removed");
}

async fn resolve_sample() -> ResolvedPost {
    let _ = dotenvy::from_filename(".env.local");
    let cookie = match env::var("WECHAT_YUANBAO_COOKIE") {
        Ok(cookie) if !cookie.trim().is_empty() => cookie,
        Ok(_) | Err(env::VarError::NotPresent) => {
            panic!("WECHAT_YUANBAO_COOKIE is required for this ignored live test")
        }
        Err(env::VarError::NotUnicode(_)) => {
            panic!("WECHAT_YUANBAO_COOKIE must contain valid Unicode")
        }
    };

    let resolver = WechatResolver::new(cookie)
        .unwrap_or_else(|_| panic!("failed to initialize the WeChat resolver"));
    resolver
        .resolve_text(SAMPLE_SHARE_URL)
        .await
        .unwrap_or_else(|_| panic!("live WeChat resolution failed"))
}

struct TestDirectory(PathBuf);

impl TestDirectory {
    fn new() -> Self {
        let path = env::temp_dir().join(format!(
            "parse-bot-wechat-live-{}",
            Uuid::new_v4().hyphenated()
        ));
        std::fs::create_dir_all(&path)
            .unwrap_or_else(|_| panic!("failed to create live test directory"));
        Self(path)
    }

    fn path(&self) -> &std::path::Path {
        &self.0
    }
}

impl Drop for TestDirectory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn assert_safe_media_source(source: &MediaSource) {
    assert!(
        source.url.scheme() == "https",
        "media source must use HTTPS"
    );
    assert!(
        source.url.username().is_empty()
            && source.url.password().is_none()
            && source.url.port().is_none(),
        "media source contains unexpected authority components"
    );
    assert!(
        source
            .url
            .host_str()
            .is_some_and(|host| ALLOWED_MEDIA_HOSTS.contains(&host)),
        "media source host is not allowlisted"
    );
}
