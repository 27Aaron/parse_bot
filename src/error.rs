use std::path::PathBuf;

#[derive(Debug, thiserror::Error)]
pub enum AppError {
    #[error("配置错误：{0}")]
    Config(String),

    #[error("暂不支持这个链接")]
    UnsupportedUrl,

    #[error("解析凭据已失效，请更新元宝 Cookie")]
    LoginRequired,

    #[error("内容不存在、已删除或不可见")]
    NotFound,

    #[error("该视频暂时无法取得可用媒体地址")]
    MediaUnavailable,

    #[error("微信接口结构可能已经变化")]
    UpstreamChanged,

    #[error("上游请求过于频繁，请稍后再试")]
    RateLimited,

    #[error("网络请求失败：{0}")]
    Network(String),

    #[error("下载失败：{0}")]
    Download(String),

    #[error("媒体文件无效：{0}")]
    InvalidMedia(String),

    #[error("媒体文件超过允许大小：{actual} > {limit} 字节")]
    MediaTooLarge { actual: u64, limit: u64 },

    #[error("临时目录不可用：{0}")]
    Storage(PathBuf),

    #[error("Telegram 请求失败：{0}")]
    Telegram(String),

    #[error("任务已过期，请重新发送链接")]
    Expired,

    #[error("没有权限执行这个操作")]
    Forbidden,

    #[error("任务已取消")]
    Cancelled,

    #[error("数据库错误：{0}")]
    Database(String),

    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    Json(#[from] serde_json::Error),

    #[error(transparent)]
    Url(#[from] url::ParseError),
}

impl AppError {
    pub fn user_message(&self) -> &'static str {
        match self {
            Self::Config(_) => "服务配置错误，请检查日志",
            Self::UnsupportedUrl => "暂不支持这个链接",
            Self::LoginRequired => "解析凭据已失效，请更新元宝 Cookie",
            Self::NotFound => "内容不存在、已删除或不可见",
            Self::MediaUnavailable => "该视频暂时无法取得可用视频，请稍后重试",
            Self::UpstreamChanged => "微信接口可能已经变化，请稍后更新程序",
            Self::RateLimited => "请求过于频繁，请稍后再试",
            Self::Network(_) | Self::Download(_) => "下载失败，请稍后重试",
            Self::InvalidMedia(_) => "下载到的内容不是有效视频",
            Self::MediaTooLarge { .. } => "文件超过当前允许的大小上限",
            Self::Storage(_) => "临时存储不可用",
            Self::Telegram(_) => "Telegram 上传失败，请稍后重试",
            Self::Expired => "操作已经过期，请重新发送链接",
            Self::Forbidden => "没有权限执行这个操作",
            Self::Cancelled => "任务已取消",
            Self::Database(_) | Self::Io(_) | Self::Json(_) | Self::Url(_) => {
                "处理失败，请稍后重试"
            }
        }
    }
}

pub type Result<T, E = AppError> = std::result::Result<T, E>;
