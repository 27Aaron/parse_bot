use std::{env, time::Duration};

use parse_bot::{
    model::{MediaProvenance, MediaSource},
    wechat::WechatResolver,
};

const SAMPLE_SHARE_URL: &str = "https://weixin.qq.com/sph/A27pGwf5f9";
const ALLOWED_MEDIA_HOSTS: &[&str] = &[
    "finder.video.qq.com",
    "findermp.video.qq.com",
    "finder.video.wechat.com",
    "findermp.video.wechat.com",
];

#[tokio::test]
#[ignore = "requires WECHAT_YUANBAO_COOKIE and live Tencent endpoints"]
async fn resolves_wechat_channels_sample() {
    let _ = dotenvy::from_filename(".env.local");
    let cookie = match env::var("WECHAT_YUANBAO_COOKIE") {
        Ok(cookie) if !cookie.trim().is_empty() => cookie,
        Ok(_) | Err(env::VarError::NotPresent) => return,
        Err(env::VarError::NotUnicode(_)) => {
            panic!("WECHAT_YUANBAO_COOKIE must contain valid Unicode")
        }
    };

    let resolver = match WechatResolver::new(cookie, Duration::from_secs(30)) {
        Ok(resolver) => resolver,
        Err(_) => panic!("failed to initialize the WeChat resolver"),
    };
    let post = match resolver.resolve_text(SAMPLE_SHARE_URL).await {
        Ok(post) => post,
        Err(_) => panic!("live WeChat resolution failed"),
    };

    assert!(
        post.platform == "wechat_channels",
        "unexpected platform identifier"
    );
    assert!(
        post.canonical_url.as_str() == SAMPLE_SHARE_URL,
        "unexpected canonical share URL"
    );
    assert!(!post.post_id.trim().is_empty(), "missing post identifier");
    assert!(!post.candidates.is_empty(), "no media candidates returned");
    assert_safe_media_source(&post.compatible);
    assert!(
        post.candidates
            .iter()
            .any(|candidate| candidate.url == post.compatible.url),
        "compatible source is absent from the candidate set"
    );

    let expected_provenance = if post
        .candidates
        .iter()
        .any(|candidate| candidate.provenance == MediaProvenance::H264)
    {
        MediaProvenance::H264
    } else if post
        .candidates
        .iter()
        .any(|candidate| candidate.provenance == MediaProvenance::Generic)
    {
        MediaProvenance::Generic
    } else {
        MediaProvenance::H265
    };
    assert!(
        post.compatible.provenance == expected_provenance,
        "compatible source does not follow H264/generic/H265 preference"
    );

    if let Some(original) = &post.original {
        assert_safe_media_source(original);
        assert!(
            original.url != post.compatible.url,
            "original and compatible sources must differ"
        );
        assert!(
            matches!(
                original.provenance,
                MediaProvenance::ExplicitOrigin | MediaProvenance::DerivedOriginal
            ),
            "unexpected original-source provenance"
        );

        if original.provenance == MediaProvenance::DerivedOriginal {
            assert!(
                original.url.host_str() == Some("finder.video.qq.com"),
                "derived original source has an unexpected host"
            );
            let query: Vec<_> = original.url.query_pairs().collect();
            assert!(query.len() == 2, "derived original query shape changed");
            assert!(
                query[0].0 == "encfilekey" && !query[0].1.is_empty(),
                "derived original is missing encfilekey"
            );
            assert!(
                query[1].0 == "token" && !query[1].1.is_empty(),
                "derived original is missing token"
            );
        }
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
