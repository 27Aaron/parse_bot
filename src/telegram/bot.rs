use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::{Duration, Instant},
};

use tokio::sync::{Mutex, RwLock, Semaphore, mpsc};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::{
    AppError, Result,
    media::{DownloadedMedia, MediaDownloader, MediaProbe, decrypt_file_prefix, probe_media},
    model::{MediaSource, ResolvedPost, TelegramMediaKind, VideoCodec},
    storage::MediaCache,
    telegram::api::{
        CallbackQuery, InputFile, Message, TelegramClient, TelegramError, Update, VideoMetadata,
        escape_html,
    },
    wechat::WechatResolver,
};

const UPDATE_TIMEOUT_SECS: u32 = 30;

#[derive(Clone)]
pub struct BotService {
    telegram: TelegramClient,
    resolver: WechatResolver,
    downloader: MediaDownloader,
    cache: MediaCache,
    required_channel_id: Option<Arc<str>>,
    telegram_hard_limit: u64,
    callback_ttl: Duration,
    pending: Arc<RwLock<HashMap<String, PendingDownload>>>,
    active_tasks: Arc<Mutex<HashMap<u64, ActiveTask>>>,
    resolve_slots: Arc<Semaphore>,
    download_slots: Arc<Semaphore>,
}

#[derive(Clone)]
struct PendingDownload {
    owner_user_id: u64,
    chat_id: i64,
    source_message_id: i64,
    status_message_id: i64,
    post: Arc<ResolvedPost>,
    expires_at: Instant,
}

#[derive(Clone)]
struct ActiveTask {
    nonce: String,
    cancellation: CancellationToken,
}

impl BotService {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        telegram: TelegramClient,
        resolver: WechatResolver,
        downloader: MediaDownloader,
        cache: MediaCache,
        required_channel_id: Option<String>,
        telegram_hard_limit: u64,
        callback_ttl: Duration,
    ) -> Self {
        Self {
            telegram,
            resolver,
            downloader,
            cache,
            required_channel_id: required_channel_id.map(Arc::from),
            telegram_hard_limit,
            callback_ttl,
            pending: Arc::new(RwLock::new(HashMap::new())),
            active_tasks: Arc::new(Mutex::new(HashMap::new())),
            resolve_slots: Arc::new(Semaphore::new(4)),
            download_slots: Arc::new(Semaphore::new(1)),
        }
    }

    pub async fn run(self) -> Result<()> {
        let me = self.telegram.get_me().await.map_err(AppError::from)?;
        info!(bot_id = me.id, username = ?me.username, "Telegram Bot API 已连接");

        let mut offset = None;
        loop {
            match self.telegram.get_updates(offset, UPDATE_TIMEOUT_SECS).await {
                Ok(updates) => {
                    for update in updates {
                        let next_offset = update.update_id.saturating_add(1);
                        if let Err(error) = self.handle_update(update).await {
                            error!(error = %error, "处理 Telegram update 失败");
                        }
                        offset = Some(next_offset);
                    }
                }
                Err(error) => {
                    let delay = error.retry_after().unwrap_or(Duration::from_secs(2));
                    warn!(error = %error, ?delay, "获取 Telegram updates 失败");
                    tokio::time::sleep(delay).await;
                }
            }
        }
    }

    async fn handle_update(&self, update: Update) -> Result<()> {
        if let Some(callback) = update.callback_query {
            return self.handle_callback(callback).await;
        }
        if let Some(message) = update.message.filter(|message| message.text.is_some()) {
            return self.handle_message(message).await;
        }
        Ok(())
    }

    async fn handle_message(&self, message: Message) -> Result<()> {
        let Some(user_id) = message.sender_id() else {
            return Ok(());
        };
        let chat_id = message.chat.id;
        if !message.chat.is_private() {
            let _ = self
                .telegram
                .send_message(chat_id, "首版只在私聊中工作。", None)
                .await;
            return Ok(());
        }

        let text = message.text.as_deref().unwrap_or_default().trim();
        let command = text
            .split_whitespace()
            .next()
            .and_then(|value| value.strip_prefix('/'))
            .map(|value| value.split('@').next().unwrap_or(value));

        match command {
            Some("start" | "help") => {
                let help = format_help_text(self.required_channel_id.as_deref());
                self.telegram.send_message(chat_id, &help, None).await?;
            }
            Some("parse") => {
                let inline = text.split_once(char::is_whitespace).map(|(_, value)| value);
                let replied = message
                    .reply_to_message
                    .as_deref()
                    .and_then(|reply| reply.text.as_deref().map(|text| (text, reply.message_id)));
                let (input, source_message_id) = inline
                    .filter(|value| !value.trim().is_empty())
                    .map(|value| (value, message.message_id))
                    .or(replied)
                    .ok_or(AppError::UnsupportedUrl)?;
                self.begin_parse(chat_id, user_id, source_message_id, input)
                    .await?;
            }
            Some("status") => {
                let active = self.active_tasks.lock().await.contains_key(&user_id);
                let text = if active {
                    "当前有一个媒体任务正在运行。"
                } else {
                    "当前没有正在运行的媒体任务。"
                };
                self.telegram.send_message(chat_id, text, None).await?;
            }
            Some("cancel") => {
                let active = self.active_tasks.lock().await.get(&user_id).cloned();
                if let Some(active) = active {
                    active.cancellation.cancel();
                    self.telegram
                        .send_message(
                            chat_id,
                            "已请求取消任务；如果 Telegram 已经接收上传请求，文件仍可能发送完成。",
                            None,
                        )
                        .await?;
                } else {
                    self.telegram
                        .send_message(chat_id, "当前没有可以取消的任务。", None)
                        .await?;
                }
            }
            Some(_) => {
                self.telegram
                    .send_message(chat_id, "暂不支持这个命令。发送 /start 查看用法。", None)
                    .await?;
            }
            None => {
                self.begin_parse(chat_id, user_id, message.message_id, text)
                    .await?
            }
        }
        Ok(())
    }

    async fn begin_parse(
        &self,
        chat_id: i64,
        user_id: u64,
        source_message_id: i64,
        input: &str,
    ) -> Result<()> {
        if !self
            .ensure_required_channel(chat_id, user_id, source_message_id)
            .await?
        {
            return Ok(());
        }

        if self.active_tasks.lock().await.contains_key(&user_id) {
            self.telegram
                .send_message_reply(
                    chat_id,
                    "你已经有一个原画视频任务正在运行。",
                    source_message_id,
                    None,
                )
                .await?;
            return Ok(());
        }

        let status = self
            .telegram
            .send_message_reply_html(chat_id, "<b>▎解 析 中...</b>", source_message_id, None)
            .await?;
        let _permit = self
            .resolve_slots
            .acquire()
            .await
            .map_err(|_| AppError::Cancelled)?;

        let post = match self.resolver.resolve_text(input).await {
            Ok(post) => Arc::new(post),
            Err(error) => {
                let _ = self
                    .telegram
                    .edit_message_text(chat_id, status.message_id, error.user_message(), None)
                    .await;
                return Err(error);
            }
        };

        let nonce = Uuid::new_v4().simple().to_string();
        if let Err(error) = self
            .telegram
            .edit_message_text_html(chat_id, status.message_id, "<b>▎下 载 中...</b>", None)
            .await
        {
            warn!(error = %error, "下载状态消息更新失败，任务继续执行");
        }

        let cancellation = CancellationToken::new();
        let already_active = {
            let mut active = self.active_tasks.lock().await;
            match active.entry(user_id) {
                std::collections::hash_map::Entry::Occupied(_) => true,
                std::collections::hash_map::Entry::Vacant(entry) => {
                    entry.insert(ActiveTask {
                        nonce: nonce.clone(),
                        cancellation: cancellation.clone(),
                    });
                    false
                }
            }
        };
        if already_active {
            self.telegram
                .edit_message_text(
                    chat_id,
                    status.message_id,
                    "你已经有一个原画视频任务正在运行。",
                    None,
                )
                .await?;
            return Ok(());
        }

        let active_nonces = self
            .active_tasks
            .lock()
            .await
            .values()
            .map(|task| task.nonce.clone())
            .collect::<HashSet<_>>();
        self.pending.write().await.retain(|nonce, value| {
            value.expires_at > Instant::now() || active_nonces.contains(nonce)
        });
        let pending = PendingDownload {
            owner_user_id: user_id,
            chat_id,
            source_message_id,
            status_message_id: status.message_id,
            post,
            expires_at: Instant::now() + self.callback_ttl,
        };
        self.pending
            .write()
            .await
            .insert(nonce.clone(), pending.clone());

        let service = self.clone();
        tokio::spawn(async move {
            let result = service.run_download(&pending, &cancellation).await;
            {
                let mut active = service.active_tasks.lock().await;
                if active.get(&user_id).is_some_and(|task| task.nonce == nonce) {
                    active.remove(&user_id);
                }
            }
            service.pending.write().await.remove(&nonce);

            if let Err(error) = &result {
                let _ = service
                    .telegram
                    .edit_message_text(
                        pending.chat_id,
                        pending.status_message_id,
                        error.user_message(),
                        None,
                    )
                    .await;
                error!(error = %error, "原画视频任务失败");
            }
        });
        Ok(())
    }

    async fn handle_callback(&self, callback: CallbackQuery) -> Result<()> {
        let data = callback.data.as_deref().ok_or(AppError::Expired)?;

        if let Some(nonce) = data.strip_prefix("cancel:") {
            return self.cancel_callback(&callback, nonce).await;
        }
        self.telegram
            .answer_callback_query(&callback.id, Some("画质选择已移除，请重新发送链接"), true)
            .await?;
        Ok(())
    }

    async fn cancel_callback(&self, callback: &CallbackQuery, nonce: &str) -> Result<()> {
        let pending = self.pending.read().await.get(nonce).cloned();
        let Some(pending) = pending else {
            self.telegram
                .answer_callback_query(&callback.id, Some("操作已经过期"), true)
                .await?;
            return Ok(());
        };
        if pending.owner_user_id != callback.sender.id
            || callback.chat_id() != Some(pending.chat_id)
        {
            return Err(AppError::Forbidden);
        }

        let active = self
            .active_tasks
            .lock()
            .await
            .get(&pending.owner_user_id)
            .filter(|task| task.nonce == nonce)
            .cloned();
        if let Some(active) = active {
            active.cancellation.cancel();
        } else if pending.expires_at <= Instant::now() {
            self.pending.write().await.remove(nonce);
            self.telegram
                .answer_callback_query(&callback.id, Some("操作已经过期"), true)
                .await?;
            return Ok(());
        } else {
            self.pending.write().await.remove(nonce);
            self.telegram
                .edit_message_text(pending.chat_id, pending.status_message_id, "已取消。", None)
                .await?;
        }
        self.telegram
            .answer_callback_query(&callback.id, Some("已取消"), false)
            .await?;
        Ok(())
    }

    async fn run_download(
        &self,
        pending: &PendingDownload,
        cancellation: &CancellationToken,
    ) -> Result<()> {
        if cancellation.is_cancelled() {
            return Err(AppError::Cancelled);
        }
        let caption = format_caption(&pending.post);
        if let Some(cached) = self
            .cache
            .get(&pending.post.platform, &pending.post.post_id)
            .await?
        {
            if let Err(error) = self
                .telegram
                .edit_message_text_html(
                    pending.chat_id,
                    pending.status_message_id,
                    "<b>▎发 送 中...</b>",
                    None,
                )
                .await
            {
                warn!(error = %error, "缓存发送状态消息更新失败，任务继续执行");
            }
            let input = InputFile::file_id(cached.file_id.clone())?;
            let cached_send = tokio::select! {
                _ = cancellation.cancelled() => return Err(AppError::Cancelled),
                result = self.send_media(
                    pending.chat_id,
                    cached.kind,
                    &input,
                    &caption,
                    pending.source_message_id,
                    None,
                ) => result,
            };
            match cached_send {
                Ok(_) => {
                    let _ = self
                        .telegram
                        .delete_message(pending.chat_id, pending.status_message_id)
                        .await;
                    return Ok(());
                }
                Err(error) if error.error_code() == Some(400) => {
                    self.cache
                        .remove(&pending.post.platform, &pending.post.post_id)
                        .await?;
                    self.update_status_html(pending, "<b>▎下 载 中...</b>")
                        .await;
                }
                Err(error) => return Err(error.into()),
            }
        }

        let mut source = pending.post.video.clone();
        let _permit = tokio::select! {
            _ = cancellation.cancelled() => return Err(AppError::Cancelled),
            permit = self.download_slots.acquire() => permit.map_err(|_| AppError::Cancelled)?,
        };
        let first_download = self
            .download_with_status(&source, pending, cancellation)
            .await;
        let downloaded = match first_download {
            Ok(downloaded) => downloaded,
            Err(AppError::Expired) => {
                self.update_status_html(pending, "<b>▎解 析 中...</b>")
                    .await;
                let refreshed = tokio::select! {
                    _ = cancellation.cancelled() => return Err(AppError::Cancelled),
                    result = self.resolver.resolve_url(&pending.post.canonical_url) => result?,
                };
                source = refreshed.video;
                self.download_with_status(&source, pending, cancellation)
                    .await?
            }
            Err(error) => return Err(error),
        };

        let result = async {
            if cancellation.is_cancelled() {
                return Err(AppError::Cancelled);
            }
            if downloaded.bytes > self.telegram_hard_limit {
                return Err(AppError::MediaTooLarge {
                    actual: downloaded.bytes,
                    limit: self.telegram_hard_limit,
                });
            }

            if let Some(decode_key) = source.decode_key {
                tokio::select! {
                    _ = cancellation.cancelled() => return Err(AppError::Cancelled),
                    result = decrypt_file_prefix(&downloaded.path, decode_key) => result?,
                };
            }
            let probe = tokio::select! {
                _ = cancellation.cancelled() => return Err(AppError::Cancelled),
                result = probe_media(&downloaded.path) => result?,
            };

            let kind = telegram_media_kind(probe.codec);
            let video_metadata = telegram_video_metadata(&probe);
            // With a local Bot API `file://` input, TDLib performs the real
            // Telegram upload internally and exposes no byte progress. Only
            // the start of the request and its successful completion are real.
            self.update_status_html(pending, "<b>▎上 传 中...</b>")
                .await;

            let input = InputFile::local_path(&downloaded.path)?;
            let send_result = tokio::select! {
                _ = cancellation.cancelled() => return Err(AppError::Cancelled),
                result = self.send_media(
                    pending.chat_id,
                    kind,
                    &input,
                    &caption,
                    pending.source_message_id,
                    Some(video_metadata),
                ) => result,
            };
            let sent = match send_result {
                Ok(message) => message,
                Err(error)
                    if kind == TelegramMediaKind::Video && error.error_code() == Some(400) =>
                {
                    tokio::select! {
                        _ = cancellation.cancelled() => return Err(AppError::Cancelled),
                        result = self.send_media(
                            pending.chat_id,
                            TelegramMediaKind::Document,
                            &input,
                            &caption,
                            pending.source_message_id,
                            None,
                        ) => result?,
                    }
                }
                Err(error) => return Err(error.into()),
            };

            let _ = self
                .telegram
                .edit_message_text_html(
                    pending.chat_id,
                    pending.status_message_id,
                    "<b>▎上 传 中... | 100%</b>",
                    None,
                )
                .await;

            let ids = sent
                .media_file_ids()
                .ok_or_else(|| AppError::Telegram("发送成功但响应缺少 file_id".into()))?;
            if let Err(error) = self
                .cache
                .put(
                    &pending.post.platform,
                    &pending.post.post_id,
                    ids.kind,
                    ids.file_id,
                    Some(ids.file_unique_id),
                )
                .await
            {
                warn!(error = %error, "视频已发送，但 file_id 缓存写入失败");
            }

            tokio::time::sleep(Duration::from_millis(500)).await;
            let _ = self
                .telegram
                .delete_message(pending.chat_id, pending.status_message_id)
                .await;
            Ok(())
        }
        .await;

        if let Err(error) = downloaded.cleanup().await {
            warn!(error = %error, "临时媒体文件清理失败");
        }
        result
    }

    async fn download_with_status(
        &self,
        source: &MediaSource,
        pending: &PendingDownload,
        cancellation: &CancellationToken,
    ) -> Result<DownloadedMedia> {
        let (progress_sender, mut progress_receiver) = mpsc::unbounded_channel();
        let download = self
            .downloader
            .download_with_progress(source, move |progress| {
                let _ = progress_sender.send(progress);
            });
        tokio::pin!(download);

        let mut last_percent = 0_u8;
        loop {
            tokio::select! {
                biased;
                _ = cancellation.cancelled() => return Err(AppError::Cancelled),
                Some(progress) = progress_receiver.recv() => {
                    if progress.percent > last_percent {
                        last_percent = progress.percent;
                        self.update_download_status(pending, progress.percent).await;
                    }
                }
                result = &mut download => {
                    let downloaded = result?;
                    if last_percent < 100 {
                        self.update_download_status(pending, 100).await;
                    }
                    return Ok(downloaded);
                }
            }
        }
    }

    async fn update_download_status(&self, pending: &PendingDownload, percent: u8) {
        let text = format_download_status(percent);
        self.update_status_html(pending, &text).await;
    }

    async fn update_status_html(&self, pending: &PendingDownload, text: &str) {
        if let Err(error) = self
            .telegram
            .edit_message_text_html(pending.chat_id, pending.status_message_id, text, None)
            .await
        {
            warn!(error = %error, "状态消息更新失败，媒体任务继续执行");
        }
    }

    async fn send_media(
        &self,
        chat_id: i64,
        kind: TelegramMediaKind,
        input: &InputFile,
        caption: &str,
        reply_to_message_id: i64,
        video_metadata: Option<VideoMetadata>,
    ) -> std::result::Result<Message, TelegramError> {
        match kind {
            TelegramMediaKind::Video => match video_metadata {
                Some(metadata) => {
                    self.telegram
                        .send_video_reply_html_with_metadata(
                            chat_id,
                            input,
                            Some(caption),
                            reply_to_message_id,
                            metadata,
                            None,
                        )
                        .await
                }
                None => {
                    self.telegram
                        .send_video_reply_html(
                            chat_id,
                            input,
                            Some(caption),
                            reply_to_message_id,
                            None,
                        )
                        .await
                }
            },
            TelegramMediaKind::Document => {
                self.telegram
                    .send_document_reply_html(
                        chat_id,
                        input,
                        Some(caption),
                        reply_to_message_id,
                        None,
                    )
                    .await
            }
        }
    }

    async fn ensure_required_channel(
        &self,
        chat_id: i64,
        user_id: u64,
        source_message_id: i64,
    ) -> Result<bool> {
        let Some(channel_id) = self.required_channel_id.as_deref() else {
            return Ok(true);
        };

        match self.telegram.get_chat_member(channel_id, user_id).await {
            Ok(member) if member.has_joined() => Ok(true),
            Ok(_) => {
                let prompt = format_channel_requirement(channel_id);
                self.telegram
                    .send_message_reply_html(chat_id, &prompt, source_message_id, None)
                    .await?;
                Ok(false)
            }
            Err(error) => {
                warn!(
                    error = %error,
                    channel_id,
                    user_id,
                    "频道成员状态验证失败"
                );
                self.telegram
                    .send_message_reply(
                        chat_id,
                        "暂时无法验证频道关注状态，请稍后重试。",
                        source_message_id,
                        None,
                    )
                    .await?;
                Ok(false)
            }
        }
    }
}

fn telegram_media_kind(codec: VideoCodec) -> TelegramMediaKind {
    match codec {
        VideoCodec::H264 | VideoCodec::H265 => TelegramMediaKind::Video,
        VideoCodec::Unknown => TelegramMediaKind::Document,
    }
}

fn telegram_video_metadata(probe: &MediaProbe) -> VideoMetadata {
    let duration = probe.duration_seconds.and_then(|seconds| {
        let rounded = seconds.ceil();
        (rounded.is_finite() && rounded >= 0.0 && rounded <= f64::from(u32::MAX))
            .then_some(rounded as u32)
    });
    VideoMetadata::new(probe.width, probe.height, duration)
}

fn format_caption(post: &ResolvedPost) -> String {
    format_caption_parts(&post.display_title(), post.canonical_url.as_str())
}

fn format_caption_parts(title: &str, source: &str) -> String {
    let title = escape_html(title);
    let source = escape_html(source);
    format!("{title}\n\n<b>▎<a href=\"{source}\">Source</a></b>")
}

fn format_help_text(required_channel_id: Option<&str>) -> String {
    let mut text = String::from(
        "发送微信视频号链接或分享文案，我会自动下载并发送原画视频。\n\n新文件最大支持 2000 MB；大文件需要自建 telegram-bot-api --local。",
    );
    if let Some(channel_id) = required_channel_id {
        text.push_str("\n\n使用前需要关注频道 ");
        text.push_str(channel_id);
        text.push('。');
    }
    text
}

fn format_channel_requirement(channel_id: &str) -> String {
    let username = channel_id.trim_start_matches('@');
    let label = escape_html(channel_id);
    let url = escape_html(&format!("https://t.me/{username}"));
    format!(
        "使用此机器人前，请先关注频道。\n\n<b>▎<a href=\"{url}\">{label}</a></b>\n\n关注后重新发送链接即可。"
    )
}

fn format_download_status(percent: u8) -> String {
    format!("<b>▎下 载 中... | {percent}%</b>")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tries_h264_and_h265_as_previewable_videos() {
        assert_eq!(
            telegram_media_kind(VideoCodec::H264),
            TelegramMediaKind::Video
        );
        assert_eq!(
            telegram_media_kind(VideoCodec::H265),
            TelegramMediaKind::Video
        );
        assert_eq!(
            telegram_media_kind(VideoCodec::Unknown),
            TelegramMediaKind::Document
        );
    }

    #[test]
    fn uses_visible_dimensions_and_rounded_duration_for_telegram() {
        let probe = MediaProbe {
            codec: VideoCodec::H265,
            has_audio: true,
            width: 1080,
            height: 1920,
            duration_seconds: Some(14.745),
        };
        assert_eq!(
            telegram_video_metadata(&probe),
            VideoMetadata::new(1080, 1920, Some(15))
        );
    }

    #[test]
    fn formats_a_safe_title_and_source_link() {
        assert_eq!(
            format_caption_parts(
                "标题 <原画> & 测试",
                "https://weixin.qq.com/sph/example?a=1&b=2"
            ),
            "标题 &lt;原画&gt; &amp; 测试\n\n<b>▎<a href=\"https://weixin.qq.com/sph/example?a=1&amp;b=2\">Source</a></b>"
        );
    }

    #[test]
    fn formats_download_progress_in_fifths() {
        assert_eq!(format_download_status(20), "<b>▎下 载 中... | 20%</b>");
        assert_eq!(format_download_status(40), "<b>▎下 载 中... | 40%</b>");
        assert_eq!(format_download_status(60), "<b>▎下 载 中... | 60%</b>");
        assert_eq!(format_download_status(80), "<b>▎下 载 中... | 80%</b>");
        assert_eq!(format_download_status(100), "<b>▎下 载 中... | 100%</b>");
    }

    #[test]
    fn mentions_the_required_channel_only_when_configured() {
        let public_help = format_help_text(None);
        assert!(!public_help.contains("关注频道"));

        let gated_help = format_help_text(Some("@Aaron_Channels"));
        assert!(gated_help.contains("使用前需要关注频道 @Aaron_Channels。"));
    }

    #[test]
    fn formats_a_clickable_channel_requirement() {
        assert_eq!(
            format_channel_requirement("@Aaron_Channels"),
            "使用此机器人前，请先关注频道。\n\n<b>▎<a href=\"https://t.me/Aaron_Channels\">@Aaron_Channels</a></b>\n\n关注后重新发送链接即可。"
        );
    }
}
