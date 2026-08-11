use url::Url;

use crate::{
    Error, ResolvedPost, Result,
    wechat::{self, WechatResolver},
};

/// Multi-platform resolve facade.
///
/// Delivery apps (Telegram bot, Feishu bot, CLI) depend on this type rather than
/// individual platform resolvers so new platforms can be registered here without
/// touching product code.
#[derive(Debug, Clone)]
pub struct ParseHub {
    wechat: WechatResolver,
}

impl ParseHub {
    /// Build a hub with the platforms currently supported by this build.
    pub fn new(wechat_yuanbao_cookie: impl Into<String>) -> Result<Self> {
        Ok(Self {
            wechat: WechatResolver::new(wechat_yuanbao_cookie)?,
        })
    }

    /// Extract a supported share URL from free-form text, or reject early.
    pub fn extract_share_url(input: &str) -> Result<Url> {
        // First supported matcher wins. Add more platforms here as they land.
        match wechat::extract_share_url(input) {
            Ok(url) => Ok(url),
            Err(Error::UnsupportedUrl) => Err(Error::UnsupportedUrl),
            Err(error) => Err(error),
        }
    }

    pub async fn resolve_text(&self, input: &str) -> Result<ResolvedPost> {
        // Try platforms in registration order.
        match wechat::extract_share_url(input) {
            Ok(_) => return self.wechat.resolve_text(input).await,
            Err(Error::UnsupportedUrl) => {}
            Err(error) => return Err(error),
        }
        Err(Error::UnsupportedUrl)
    }

    pub async fn resolve_url(&self, url: &Url) -> Result<ResolvedPost> {
        match wechat::extract_share_url(url.as_str()) {
            Ok(_) => return self.wechat.resolve_url(url).await,
            Err(Error::UnsupportedUrl) => {}
            Err(error) => return Err(error),
        }
        Err(Error::UnsupportedUrl)
    }

    /// Access the WeChat resolver for platform-specific configuration/tests.
    pub fn wechat(&self) -> &WechatResolver {
        &self.wechat
    }
}
