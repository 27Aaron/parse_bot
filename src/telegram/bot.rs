use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::{Duration, Instant},
};

use tokio::sync::{Mutex, RwLock, Semaphore};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::{
    AppError, Result,
    media::{MediaDownloader, decrypt_file_prefix, probe_media},
    model::{MediaVariant, ResolvedPost, TelegramMediaKind, VideoCodec},
    storage::MediaCache,
    telegram::api::{
        CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, InputFile, Message,
        TelegramClient, TelegramError, Update,
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
    allowed_user_ids: Arc<HashSet<u64>>,
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
        allowed_user_ids: HashSet<u64>,
        telegram_hard_limit: u64,
        callback_ttl: Duration,
    ) -> Self {
        Self {
            telegram,
            resolver,
            downloader,
            cache,
            allowed_user_ids: Arc::new(allowed_user_ids),
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
        if !self.is_allowed(user_id) {
            let _ = self
                .telegram
                .send_message(chat_id, "这个机器人是私人使用的。", None)
                .await;
            return Ok(());
        }
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
                self.telegram
                    .send_message(
                        chat_id,
                        "发送微信视频号链接或分享文案，我会解析后提供“下载视频”和“下载原视频”按钮。\n\n新文件最大支持 2000 MB；大文件需要自建 telegram-bot-api --local。",
                        None,
                    )
                    .await?;
            }
            Some("parse") => {
                let inline = text.split_once(char::is_whitespace).map(|(_, value)| value);
                let input = inline
                    .filter(|value| !value.trim().is_empty())
                    .or_else(|| message.reply_text())
                    .ok_or(AppError::UnsupportedUrl)?;
                self.begin_parse(chat_id, user_id, input).await?;
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
            None => self.begin_parse(chat_id, user_id, text).await?,
        }
        Ok(())
    }

    async fn begin_parse(&self, chat_id: i64, user_id: u64, input: &str) -> Result<()> {
        let status = self
            .telegram
            .send_message(chat_id, "正在解析链接…", None)
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
        let mut buttons = vec![InlineKeyboardButton::callback(
            "下载视频",
            format!("dl:{nonce}:c"),
        )];
        if post.original.is_some() {
            buttons.push(InlineKeyboardButton::callback(
                "下载原视频",
                format!("dl:{nonce}:o"),
            ));
        }
        let keyboard = InlineKeyboardMarkup::new(vec![
            buttons,
            vec![InlineKeyboardButton::callback(
                "取消",
                format!("cancel:{nonce}"),
            )],
        ]);
        let details = format_post(&post);

        self.pending
            .write()
            .await
            .retain(|_, value| value.expires_at > Instant::now());
        self.pending.write().await.insert(
            nonce,
            PendingDownload {
                owner_user_id: user_id,
                chat_id,
                status_message_id: status.message_id,
                post,
                expires_at: Instant::now() + self.callback_ttl,
            },
        );
        self.telegram
            .edit_message_text(chat_id, status.message_id, &details, Some(&keyboard))
            .await?;
        Ok(())
    }

    async fn handle_callback(&self, callback: CallbackQuery) -> Result<()> {
        let user_id = callback.sender.id;
        if !self.is_allowed(user_id) {
            let _ = self
                .telegram
                .answer_callback_query(&callback.id, Some("没有权限"), true)
                .await;
            return Ok(());
        }
        let data = callback.data.as_deref().ok_or(AppError::Expired)?;

        if let Some(nonce) = data.strip_prefix("cancel:") {
            return self.cancel_callback(&callback, nonce).await;
        }

        let mut parts = data.split(':');
        if parts.next() != Some("dl") {
            return Err(AppError::Expired);
        }
        let nonce = parts.next().ok_or(AppError::Expired)?;
        let variant = match parts.next() {
            Some("c") => MediaVariant::Compatible,
            Some("o") => MediaVariant::Original,
            _ => return Err(AppError::Expired),
        };
        if parts.next().is_some() {
            return Err(AppError::Expired);
        }

        let pending = self.pending.read().await.get(nonce).cloned();
        let Some(pending) = pending else {
            self.telegram
                .answer_callback_query(&callback.id, Some("操作已过期，请重新发送链接"), true)
                .await?;
            return Ok(());
        };
        if pending.expires_at <= Instant::now() {
            self.pending.write().await.remove(nonce);
            return Err(AppError::Expired);
        }
        if pending.owner_user_id != user_id || callback.chat_id() != Some(pending.chat_id) {
            return Err(AppError::Forbidden);
        }
        if pending.post.source(variant).is_none() {
            return Err(AppError::Expired);
        }

        self.telegram
            .answer_callback_query(&callback.id, Some("开始处理"), false)
            .await?;

        let nonce = nonce.to_owned();
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
                .send_message(pending.chat_id, "你已经有一个任务正在运行。", None)
                .await?;
            return Ok(());
        }

        let service = self.clone();
        tokio::spawn(async move {
            let result = service
                .run_download(&pending, &nonce, variant, &cancellation)
                .await;
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
                error!(error = %error, "媒体任务失败");
            }
        });
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
        if pending.expires_at <= Instant::now() {
            self.pending.write().await.remove(nonce);
            self.telegram
                .answer_callback_query(&callback.id, Some("操作已经过期"), true)
                .await?;
            return Ok(());
        }
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
        nonce: &str,
        variant: MediaVariant,
        cancellation: &CancellationToken,
    ) -> Result<()> {
        if cancellation.is_cancelled() {
            return Err(AppError::Cancelled);
        }
        let caption = pending.post.display_title();
        if let Some(cached) = self
            .cache
            .get(&pending.post.platform, &pending.post.post_id, variant)
            .await?
        {
            let input = InputFile::file_id(cached.file_id.clone())?;
            let cached_send = tokio::select! {
                _ = cancellation.cancelled() => return Err(AppError::Cancelled),
                result = self.send_media(
                    pending.chat_id,
                    cached.kind,
                    &input,
                    &caption,
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
                        .remove(&pending.post.platform, &pending.post.post_id, variant)
                        .await?;
                }
                Err(error) => return Err(error.into()),
            }
        }

        let cancel_keyboard =
            InlineKeyboardMarkup::single_row(vec![InlineKeyboardButton::callback(
                "取消",
                format!("cancel:{nonce}"),
            )]);
        self.telegram
            .edit_message_text(
                pending.chat_id,
                pending.status_message_id,
                "正在下载视频…",
                Some(&cancel_keyboard),
            )
            .await?;

        let mut source = pending
            .post
            .source(variant)
            .cloned()
            .ok_or(AppError::Expired)?;
        let _permit = tokio::select! {
            _ = cancellation.cancelled() => return Err(AppError::Cancelled),
            permit = self.download_slots.acquire() => permit.map_err(|_| AppError::Cancelled)?,
        };
        let first_download = tokio::select! {
            _ = cancellation.cancelled() => return Err(AppError::Cancelled),
            result = self.downloader.download(&source) => result,
        };
        let downloaded = match first_download {
            Ok(downloaded) => downloaded,
            Err(AppError::Expired) => {
                self.telegram
                    .edit_message_text(
                        pending.chat_id,
                        pending.status_message_id,
                        "下载地址已过期，正在重新解析…",
                        Some(&cancel_keyboard),
                    )
                    .await?;
                let refreshed = tokio::select! {
                    _ = cancellation.cancelled() => return Err(AppError::Cancelled),
                    result = self.resolver.resolve_url(&pending.post.canonical_url) => result?,
                };
                source = refreshed
                    .source(variant)
                    .cloned()
                    .ok_or(AppError::Expired)?;
                tokio::select! {
                    _ = cancellation.cancelled() => return Err(AppError::Cancelled),
                    result = self.downloader.download(&source) => result?,
                }
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

            self.telegram
                .edit_message_text(
                    pending.chat_id,
                    pending.status_message_id,
                    "正在检查视频…",
                    Some(&cancel_keyboard),
                )
                .await?;
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

            let kind = match variant {
                MediaVariant::Original => TelegramMediaKind::Document,
                MediaVariant::Compatible if probe.codec == VideoCodec::H264 => {
                    TelegramMediaKind::Video
                }
                MediaVariant::Compatible => TelegramMediaKind::Document,
            };
            self.telegram
                .edit_message_text(
                    pending.chat_id,
                    pending.status_message_id,
                    "正在上传到 Telegram…",
                    Some(&cancel_keyboard),
                )
                .await?;

            let input = InputFile::local_path(&downloaded.path)?;
            let send_result = tokio::select! {
                _ = cancellation.cancelled() => return Err(AppError::Cancelled),
                result = self.send_media(
                    pending.chat_id,
                    kind,
                    &input,
                    &caption,
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
                        ) => result?,
                    }
                }
                Err(error) => return Err(error.into()),
            };

            let ids = sent
                .media_file_ids()
                .ok_or_else(|| AppError::Telegram("发送成功但响应缺少 file_id".into()))?;
            if let Err(error) = self
                .cache
                .put(
                    &pending.post.platform,
                    &pending.post.post_id,
                    variant,
                    ids.kind,
                    ids.file_id,
                    Some(ids.file_unique_id),
                )
                .await
            {
                warn!(error = %error, "视频已发送，但 file_id 缓存写入失败");
            }

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

    async fn send_media(
        &self,
        chat_id: i64,
        kind: TelegramMediaKind,
        input: &InputFile,
        caption: &str,
    ) -> std::result::Result<Message, TelegramError> {
        match kind {
            TelegramMediaKind::Video => {
                self.telegram
                    .send_video(chat_id, input, Some(caption), None)
                    .await
            }
            TelegramMediaKind::Document => {
                self.telegram
                    .send_document(chat_id, input, Some(caption), None)
                    .await
            }
        }
    }

    fn is_allowed(&self, user_id: u64) -> bool {
        self.allowed_user_ids.contains(&user_id)
    }
}

fn format_post(post: &ResolvedPost) -> String {
    let mut lines = Vec::new();
    if let Some(author) = post.author.as_deref().filter(|value| !value.is_empty()) {
        lines.push(format!("作者：{author}"));
    }
    lines.push(format!("内容：{}", post.display_title()));
    if post.original.is_some() {
        lines.push("请选择下载版本：".into());
    } else {
        lines.push("上游没有提供独立的原视频候选。".into());
    }
    lines.join("\n")
}
