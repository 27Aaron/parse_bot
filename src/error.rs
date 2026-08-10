use std::path::PathBuf;

use crate::i18n::Language;

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
    /// Returns the short, safe message shown to a user in the selected language.
    pub fn localized_message(&self, language: Language) -> String {
        match (language, self) {
            (_, Self::Config(_)) => match language {
                Language::Chinese => "服务配置错误，请检查日志".into(),
                Language::English => "The service configuration is invalid. Check the logs.".into(),
                Language::Japanese => {
                    "サービス設定に誤りがあります。ログを確認してください。".into()
                }
                Language::Russian => "Ошибка конфигурации сервиса. Проверьте журнал.".into(),
            },
            (_, Self::UnsupportedUrl) => match language {
                Language::Chinese => "暂不支持这个链接".into(),
                Language::English => "This link is not supported yet.".into(),
                Language::Japanese => "このリンクには対応していません。".into(),
                Language::Russian => "Эта ссылка пока не поддерживается.".into(),
            },
            (_, Self::LoginRequired) => match language {
                Language::Chinese => "解析凭据已失效，请更新元宝 Cookie".into(),
                Language::English => {
                    "The parsing credential has expired. Update the Yuanbao cookie.".into()
                }
                Language::Japanese => {
                    "解析用の認証情報が期限切れです。元宝 Cookie を更新してください。".into()
                }
                Language::Russian => "Данные для разбора истекли. Обновите cookie Yuanbao.".into(),
            },
            (_, Self::NotFound) => match language {
                Language::Chinese => "内容不存在、已删除或不可见".into(),
                Language::English => {
                    "The content does not exist, was deleted, or is unavailable.".into()
                }
                Language::Japanese => {
                    "コンテンツが存在しないか、削除されたか、表示できません。".into()
                }
                Language::Russian => "Контент не существует, удалён или недоступен.".into(),
            },
            (_, Self::MediaUnavailable) => match language {
                Language::Chinese => "该视频暂时无法取得可用视频，请稍后重试".into(),
                Language::English => {
                    "A usable video source is currently unavailable. Try again later.".into()
                }
                Language::Japanese => {
                    "利用可能な動画ソースを取得できません。後でもう一度お試しください。".into()
                }
                Language::Russian => {
                    "Не удалось получить рабочий источник видео. Повторите попытку позже.".into()
                }
            },
            (_, Self::UpstreamChanged) => match language {
                Language::Chinese => "微信接口可能已经变化，请稍后更新程序".into(),
                Language::English => {
                    "The WeChat interface may have changed. Update the program later.".into()
                }
                Language::Japanese => {
                    "WeChat の仕様が変わった可能性があります。後でプログラムを更新してください。"
                        .into()
                }
                Language::Russian => {
                    "Интерфейс WeChat мог измениться. Позже обновите программу.".into()
                }
            },
            (_, Self::RateLimited) => match language {
                Language::Chinese => "请求过于频繁，请稍后再试".into(),
                Language::English => "Too many requests. Please try again later.".into(),
                Language::Japanese => "リクエストが多すぎます。後でもう一度お試しください。".into(),
                Language::Russian => "Слишком много запросов. Повторите попытку позже.".into(),
            },
            (_, Self::Network(_) | Self::Download(_)) => match language {
                Language::Chinese => "下载失败，请稍后重试".into(),
                Language::English => "The download failed. Please try again later.".into(),
                Language::Japanese => {
                    "ダウンロードに失敗しました。後でもう一度お試しください。".into()
                }
                Language::Russian => "Не удалось скачать файл. Повторите попытку позже.".into(),
            },
            (_, Self::InvalidMedia(_)) => match language {
                Language::Chinese => "下载到的内容不是有效视频".into(),
                Language::English => "The downloaded content is not a valid video.".into(),
                Language::Japanese => "ダウンロードした内容は有効な動画ではありません。".into(),
                Language::Russian => "Скачанный файл не является корректным видео.".into(),
            },
            (_, Self::MediaTooLarge { .. }) => match language {
                Language::Chinese => "文件超过当前允许的大小上限".into(),
                Language::English => "The file exceeds the current size limit.".into(),
                Language::Japanese => "ファイルが現在のサイズ上限を超えています。".into(),
                Language::Russian => "Файл превышает текущий допустимый размер.".into(),
            },
            (_, Self::Storage(_)) => match language {
                Language::Chinese => "临时存储不可用".into(),
                Language::English => "Temporary storage is unavailable.".into(),
                Language::Japanese => "一時ストレージを利用できません。".into(),
                Language::Russian => "Временное хранилище недоступно.".into(),
            },
            (_, Self::Telegram(_)) => match language {
                Language::Chinese => "Telegram 上传失败，请稍后重试".into(),
                Language::English => "Telegram upload failed. Please try again later.".into(),
                Language::Japanese => {
                    "Telegram へのアップロードに失敗しました。後でもう一度お試しください。".into()
                }
                Language::Russian => {
                    "Не удалось загрузить файл в Telegram. Повторите попытку позже.".into()
                }
            },
            (_, Self::Expired) => match language {
                Language::Chinese => "任务已过期，请重新发送链接".into(),
                Language::English => "The task expired. Send the link again.".into(),
                Language::Japanese => {
                    "タスクの有効期限が切れました。リンクをもう一度送信してください。".into()
                }
                Language::Russian => "Срок действия задачи истёк. Отправьте ссылку ещё раз.".into(),
            },
            (_, Self::Cancelled) => match language {
                Language::Chinese => "任务已取消".into(),
                Language::English => "The task was cancelled.".into(),
                Language::Japanese => "タスクをキャンセルしました。".into(),
                Language::Russian => "Задача отменена.".into(),
            },
            (_, Self::Database(_) | Self::Io(_) | Self::Json(_) | Self::Url(_)) => match language {
                Language::Chinese => "处理失败，请稍后重试".into(),
                Language::English => "Processing failed. Please try again later.".into(),
                Language::Japanese => "処理に失敗しました。後でもう一度お試しください。".into(),
                Language::Russian => {
                    "Не удалось обработать запрос. Повторите попытку позже.".into()
                }
            },
        }
    }
}

pub type Result<T, E = AppError> = std::result::Result<T, E>;
