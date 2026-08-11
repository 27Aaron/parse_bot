//! User-facing text and language preferences.
//!
//! Telegram provides a language hint, but a user can explicitly choose a
//! language in the settings panel.  Keeping the small catalogue here makes it
//! possible to add translations without scattering literals through the bot
//! workflow.

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub enum Language {
    #[default]
    Chinese,
    English,
    Japanese,
    Russian,
}

impl Language {
    pub const ALL: [Self; 4] = [Self::Chinese, Self::English, Self::Japanese, Self::Russian];

    pub const fn code(self) -> &'static str {
        match self {
            Self::Chinese => "zh",
            Self::English => "en",
            Self::Japanese => "ja",
            Self::Russian => "ru",
        }
    }

    pub const fn label(self) -> &'static str {
        match self {
            Self::Chinese => "中文",
            Self::English => "English",
            Self::Japanese => "日本語",
            Self::Russian => "Русский",
        }
    }

    pub fn from_code(code: &str) -> Option<Self> {
        match code {
            "zh" | "zh-cn" | "zh-hans" | "zh-tw" => Some(Self::Chinese),
            "en" => Some(Self::English),
            "ja" => Some(Self::Japanese),
            "ru" => Some(Self::Russian),
            _ => None,
        }
    }

    pub fn from_telegram_code(code: Option<&str>) -> Self {
        let Some(code) = code else {
            return Self::Chinese;
        };
        let normalized = code.to_ascii_lowercase();
        Self::from_code(&normalized).unwrap_or_else(|| {
            normalized
                .split(['-', '_'])
                .next()
                .and_then(Self::from_code)
                .unwrap_or(Self::Chinese)
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Status {
    Parsing,
    Downloading,
    Sending,
    Uploading,
    WaitingForMedia,
}

pub fn status(language: Language, value: Status) -> &'static str {
    match (language, value) {
        (Language::Chinese, Status::Parsing) => "<b>▎解 析 中...</b>",
        (Language::Chinese, Status::Downloading) => "<b>▎下 载 中...</b>",
        (Language::Chinese, Status::Sending) => "<b>▎发 送 中...</b>",
        (Language::Chinese, Status::Uploading) => "<b>▎上 传 中...</b>",
        (Language::Chinese, Status::WaitingForMedia) => {
            "<b>▎等 待 中...</b>\n\n相同视频正在处理中，完成后将直接发送。"
        }
        (Language::English, Status::Parsing) => "<b>▎Parsing...</b>",
        (Language::English, Status::Downloading) => "<b>▎Downloading...</b>",
        (Language::English, Status::Sending) => "<b>▎Sending...</b>",
        (Language::English, Status::Uploading) => "<b>▎Uploading...</b>",
        (Language::English, Status::WaitingForMedia) => {
            "<b>▎Waiting...</b>\n\nThe same video is already being processed and will be sent when ready."
        }
        (Language::Japanese, Status::Parsing) => "<b>▎解析中...</b>",
        (Language::Japanese, Status::Downloading) => "<b>▎ダウンロード中...</b>",
        (Language::Japanese, Status::Sending) => "<b>▎送信中...</b>",
        (Language::Japanese, Status::Uploading) => "<b>▎アップロード中...</b>",
        (Language::Japanese, Status::WaitingForMedia) => {
            "<b>▎待機中...</b>\n\n同じ動画を処理中です。完了後すぐに送信します。"
        }
        (Language::Russian, Status::Parsing) => "<b>▎Разбор...</b>",
        (Language::Russian, Status::Downloading) => "<b>▎Загрузка...</b>",
        (Language::Russian, Status::Sending) => "<b>▎Отправка...</b>",
        (Language::Russian, Status::Uploading) => "<b>▎Загрузка в Telegram...</b>",
        (Language::Russian, Status::WaitingForMedia) => {
            "<b>▎Ожидание...</b>\n\nТакое же видео уже обрабатывается и будет отправлено после завершения."
        }
    }
}

pub fn download_status(language: Language, percent: u8) -> String {
    let label = match language {
        Language::Chinese => "下 载 中...",
        Language::English => "Downloading...",
        Language::Japanese => "ダウンロード中...",
        Language::Russian => "Загрузка...",
    };
    format!("<b>▎{label} | {percent}%</b>")
}

pub fn source_label(language: Language) -> &'static str {
    match language {
        Language::Chinese => "来源",
        Language::English => "Source",
        Language::Japanese => "出典",
        Language::Russian => "Источник",
    }
}

pub fn start_text(language: Language, required_channel_id: Option<&str>) -> String {
    let mut text = match language {
        Language::Chinese => "发送微信视频号链接，我会自动解析并发送视频。".to_owned(),
        Language::English => {
            "Send a WeChat Channels link and I will parse and send the video.".to_owned()
        }
        Language::Japanese => {
            "WeChat Channels のリンクを送ると、動画を解析して送信します。".to_owned()
        }
        Language::Russian => {
            "Отправьте ссылку WeChat Channels, и я разберу и отправлю видео.".to_owned()
        }
    };
    if let Some(channel_id) = required_channel_id {
        let suffix = match language {
            Language::Chinese => format!("\n\n使用前需要关注频道 {channel_id}。"),
            Language::English => format!("\n\nPlease follow {channel_id} before using the bot."),
            Language::Japanese => {
                format!("\n\n利用前にチャンネル {channel_id} をフォローしてください。")
            }
            Language::Russian => {
                format!("\n\nПеред использованием подпишитесь на канал {channel_id}.")
            }
        };
        text.push_str(&suffix);
    }
    text
}

pub fn non_private(language: Language) -> &'static str {
    match language {
        Language::Chinese => "此机器人只在私聊中工作。",
        Language::English => "This bot works in private chats only.",
        Language::Japanese => "このボットはプライベートチャットでのみ利用できます。",
        Language::Russian => "Этот бот работает только в личном чате.",
    }
}

pub fn direct_link(language: Language) -> &'static str {
    match language {
        Language::Chinese => "请直接发送微信视频号链接。",
        Language::English => "Please send a WeChat Channels link directly.",
        Language::Japanese => "WeChat Channels のリンクを直接送信してください。",
        Language::Russian => "Отправьте ссылку WeChat Channels напрямую.",
    }
}

pub fn command_start(language: Language) -> &'static str {
    match language {
        Language::Chinese => "开始",
        Language::English => "Start",
        Language::Japanese => "開始",
        Language::Russian => "Начать",
    }
}

pub fn command_setting(language: Language) -> &'static str {
    match language {
        Language::Chinese => "设置",
        Language::English => "Settings",
        Language::Japanese => "設定",
        Language::Russian => "Настройки",
    }
}

pub fn queue(language: Language, waiting_count: usize) -> String {
    match language {
        Language::Chinese => format!(
            "<b>▎排 队 中...</b>\n\n当前共有 {waiting_count} 个任务等待调度，轮到后会自动解析。"
        ),
        Language::English => format!(
            "<b>▎Queued...</b>\n\nThere are {waiting_count} tasks waiting. Your task will start automatically."
        ),
        Language::Japanese => format!(
            "<b>▎待機中...</b>\n\n現在 {waiting_count} 件のタスクが待機中です。順番になると自動的に解析します。"
        ),
        Language::Russian => format!(
            "<b>▎В очереди...</b>\n\nСейчас ожидают {waiting_count} задач. Разбор начнётся автоматически."
        ),
    }
}

pub fn user_queue_full(language: Language) -> &'static str {
    match language {
        Language::Chinese => "你的等待队列已满（最多 3 个任务），请等待当前任务完成后再发送。",
        Language::English => {
            "Your waiting queue is full (up to 3 tasks). Please wait for a task to finish."
        }
        Language::Japanese => {
            "待機キューが満杯です（最大3件）。現在のタスクが終わるまでお待ちください。"
        }
        Language::Russian => {
            "Ваша очередь заполнена (не более 3 задач). Дождитесь завершения текущей задачи."
        }
    }
}

pub fn global_queue_full(language: Language) -> &'static str {
    match language {
        Language::Chinese => "当前等待队列已满（最多 100 个任务），请稍后再发送链接。",
        Language::English => "The waiting queue is full (up to 100 tasks). Please try again later.",
        Language::Japanese => "待機キューが満杯です（最大100件）。後でもう一度お試しください。",
        Language::Russian => "Очередь заполнена (не более 100 задач). Повторите попытку позже.",
    }
}

pub fn channel_requirement(language: Language, channel_id: &str, channel_url: &str) -> String {
    let label = crate::telegram::api::escape_html(channel_id);
    let url = crate::telegram::api::escape_html(channel_url);
    match language {
        Language::Chinese => format!(
            "使用此机器人前，请先关注频道。\n\n<b>▎<a href=\"{url}\">{label}</a></b>\n\n关注后重新发送链接即可。"
        ),
        Language::English => format!(
            "Please follow the channel before using this bot.\n\n<b>▎<a href=\"{url}\">{label}</a></b>\n\nThen send the link again."
        ),
        Language::Japanese => format!(
            "このボットを使う前にチャンネルをフォローしてください。\n\n<b>▎<a href=\"{url}\">{label}</a></b>\n\nフォロー後、リンクをもう一度送信してください。"
        ),
        Language::Russian => format!(
            "Перед использованием подпишитесь на канал.\n\n<b>▎<a href=\"{url}\">{label}</a></b>\n\nЗатем отправьте ссылку ещё раз."
        ),
    }
}

pub fn channel_check_failed(language: Language) -> &'static str {
    match language {
        Language::Chinese => "暂时无法验证频道关注状态，请稍后重试。",
        Language::English => {
            "The channel subscription could not be verified. Please try again later."
        }
        Language::Japanese => {
            "チャンネルのフォロー状態を確認できません。後でもう一度お試しください。"
        }
        Language::Russian => "Не удалось проверить подписку на канал. Повторите попытку позже.",
    }
}

pub fn settings_panel_title(language: Language) -> &'static str {
    match language {
        Language::Chinese => "▎配置面板 - 个人配置",
        Language::English => "▎Settings Panel - Personal Settings",
        Language::Japanese => "▎設定パネル - 個人設定",
        Language::Russian => "▎Панель настроек - Личные настройки",
    }
}

pub fn language_setting_label(language: Language) -> String {
    match language {
        Language::Chinese => format!("语言：{}", language.label()),
        Language::English => format!("Language: {}", language.label()),
        Language::Japanese => format!("言語：{}", language.label()),
        Language::Russian => format!("Язык: {}", language.label()),
    }
}

pub fn source_setting_label(language: Language, enabled: bool) -> String {
    let value = setting_value(language, enabled);
    match language {
        Language::Chinese => format!("显示来源：{value}"),
        Language::English => format!("Show Source: {value}"),
        Language::Japanese => format!("出典を表示：{value}"),
        Language::Russian => format!("Показывать источник: {value}"),
    }
}

pub fn progress_setting_label(language: Language, enabled: bool) -> String {
    let value = setting_value(language, enabled);
    match language {
        Language::Chinese => format!("显示进度：{value}"),
        Language::English => format!("Show progress: {value}"),
        Language::Japanese => format!("進捗を表示：{value}"),
        Language::Russian => format!("Показывать прогресс: {value}"),
    }
}

pub fn reply_setting_label(language: Language, enabled: bool) -> String {
    let value = setting_value(language, enabled);
    match language {
        Language::Chinese => format!("回复消息：{value}"),
        Language::English => format!("Reply to message: {value}"),
        Language::Japanese => format!("メッセージに返信：{value}"),
        Language::Russian => format!("Отвечать на сообщение: {value}"),
    }
}

pub fn cover_setting_label(language: Language, enabled: bool) -> String {
    let value = setting_value(language, enabled);
    match language {
        Language::Chinese => format!("视频封面：{value}"),
        Language::English => format!("Video cover: {value}"),
        Language::Japanese => format!("動画カバー：{value}"),
        Language::Russian => format!("Обложка видео: {value}"),
    }
}

pub fn language_panel(language: Language) -> String {
    match language {
        Language::Chinese => "请选择界面语言：".to_owned(),
        Language::English => "Choose the interface language:".to_owned(),
        Language::Japanese => "表示言語を選択してください：".to_owned(),
        Language::Russian => "Выберите язык интерфейса:".to_owned(),
    }
}

pub fn back(language: Language) -> &'static str {
    match language {
        Language::Chinese => "返回",
        Language::English => "Back",
        Language::Japanese => "戻る",
        Language::Russian => "Назад",
    }
}

pub fn done(language: Language) -> &'static str {
    match language {
        Language::Chinese => "完成",
        Language::English => "Done",
        Language::Japanese => "完了",
        Language::Russian => "Готово",
    }
}

pub fn settings_saved(language: Language) -> &'static str {
    match language {
        Language::Chinese => "已更新",
        Language::English => "Updated",
        Language::Japanese => "更新しました",
        Language::Russian => "Обновлено",
    }
}

pub fn setting_value(language: Language, enabled: bool) -> &'static str {
    match (language, enabled) {
        (Language::Chinese, true) => "开启",
        (Language::Chinese, false) => "关闭",
        (Language::English, true) => "On",
        (Language::English, false) => "Off",
        (Language::Japanese, true) => "オン",
        (Language::Japanese, false) => "オフ",
        (Language::Russian, true) => "Вкл.",
        (Language::Russian, false) => "Выкл.",
    }
}

pub fn invalid_setting(language: Language) -> &'static str {
    match language {
        Language::Chinese => "设置操作无效，请重新打开设置。",
        Language::English => "That setting action is no longer valid. Please reopen Settings.",
        Language::Japanese => "設定操作が無効です。設定をもう一度開いてください。",
        Language::Russian => "Это действие настроек недействительно. Откройте настройки снова.",
    }
}

pub fn only_private_settings(language: Language) -> &'static str {
    match language {
        Language::Chinese => "设置只能在私聊中使用。",
        Language::English => "Settings are available in private chats only.",
        Language::Japanese => "設定はプライベートチャットでのみ利用できます。",
        Language::Russian => "Настройки доступны только в личном чате.",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn supports_the_four_configured_languages() {
        assert_eq!(Language::ALL.len(), 4);
        assert_eq!(Language::from_code("zh"), Some(Language::Chinese));
        assert_eq!(Language::from_code("en"), Some(Language::English));
        assert_eq!(Language::from_code("ja"), Some(Language::Japanese));
        assert_eq!(Language::from_code("ru"), Some(Language::Russian));
        assert_eq!(
            Language::from_telegram_code(Some("en-US")),
            Language::English
        );
    }

    #[test]
    fn settings_have_user_controls() {
        assert!(!language_setting_label(Language::Chinese).is_empty());
        assert!(!source_setting_label(Language::Chinese, true).is_empty());
        assert!(!progress_setting_label(Language::Chinese, true).is_empty());
        assert!(!reply_setting_label(Language::Chinese, true).is_empty());
        assert!(!cover_setting_label(Language::Chinese, true).is_empty());
        assert_eq!(
            settings_panel_title(Language::Chinese),
            "▎配置面板 - 个人配置"
        );
        assert_eq!(done(Language::Chinese), "完成");
    }

    #[test]
    fn shared_media_waiting_status_is_available_in_every_language() {
        for language in Language::ALL {
            let message = status(language, Status::WaitingForMedia);
            assert!(message.starts_with("<b>"));
            assert!(message.contains('\n'));
            assert!(!message.is_empty());
        }
    }
}
