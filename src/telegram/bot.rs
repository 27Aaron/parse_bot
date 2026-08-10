use std::{
    collections::{HashMap, VecDeque},
    panic::AssertUnwindSafe,
    sync::{Arc, Mutex as StdMutex},
    time::Duration,
};

use futures_util::FutureExt;
use tokio::{
    sync::{Mutex, Semaphore, mpsc},
    task::AbortHandle,
};
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use tracing::{error, info, warn};

use crate::{
    AppError, Result,
    media::{DownloadedMedia, MediaDownloader, MediaProbe, decrypt_file_prefix, probe_media},
    model::{MediaSource, ResolvedPost, TelegramMediaKind, VideoCodec},
    storage::MediaCache,
    telegram::api::{
        InputFile, Message, TelegramClient, TelegramError, Update, VideoMetadata, escape_html,
    },
    wechat::{WechatResolver, extract_share_url},
};

const UPDATE_TIMEOUT_SECS: u32 = 30;
const SHUTDOWN_GRACE: Duration = Duration::from_secs(30);
const FORCED_SHUTDOWN_GRACE: Duration = Duration::from_secs(2);
const MAX_ACTIVE_TASKS: usize = 10;
const MAX_WAITING_TASKS_PER_USER: usize = 3;
const MAX_WAITING_TASKS: usize = 100;
const MEDIA_PIPELINE_CONCURRENCY: usize = MAX_ACTIVE_TASKS;

#[derive(Clone)]
pub struct BotService {
    telegram: TelegramClient,
    resolver: WechatResolver,
    downloader: MediaDownloader,
    cache: MediaCache,
    required_channel_id: Option<Arc<str>>,
    telegram_hard_limit: u64,
    scheduler: Arc<Mutex<SchedulerState>>,
    media_slots: Arc<Semaphore>,
    background_tasks: TaskTracker,
    task_abort_handles: Arc<StdMutex<Vec<AbortHandle>>>,
}

#[derive(Clone)]
struct MediaTask {
    chat_id: i64,
    source_message_id: i64,
    status_message_id: i64,
    post: Arc<ResolvedPost>,
}

struct ParseTask {
    task_id: u64,
    user_id: u64,
    chat_id: i64,
    source_message_id: i64,
    share_url: url::Url,
    status_message_id: Option<i64>,
    cancellation: CancellationToken,
}

#[derive(Debug, Clone, Copy)]
struct SchedulerLimits {
    max_active: usize,
    max_waiting_per_user: usize,
    max_waiting: usize,
}

impl Default for SchedulerLimits {
    fn default() -> Self {
        Self {
            max_active: MAX_ACTIVE_TASKS,
            max_waiting_per_user: MAX_WAITING_TASKS_PER_USER,
            max_waiting: MAX_WAITING_TASKS,
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct WaitingTask {
    task_id: u64,
    user_id: u64,
    ready: bool,
}

#[derive(Debug)]
struct FairTaskScheduler {
    limits: SchedulerLimits,
    next_task_id: u64,
    accepting: bool,
    running: HashMap<u64, u64>,
    waiting_by_user: HashMap<u64, VecDeque<WaitingTask>>,
    round_robin: VecDeque<u64>,
    waiting_count: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Admission {
    Started { task_id: u64 },
    Queued { task_id: u64, waiting_count: usize },
    Rejected(QueueRejection),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum QueueRejection {
    UserQueueFull,
    GlobalQueueFull,
    ShuttingDown,
}

impl FairTaskScheduler {
    fn new(limits: SchedulerLimits) -> Self {
        assert!(limits.max_active > 0, "max_active must be positive");
        Self {
            limits,
            next_task_id: 1,
            accepting: true,
            running: HashMap::new(),
            waiting_by_user: HashMap::new(),
            round_robin: VecDeque::new(),
            waiting_count: 0,
        }
    }

    fn admit(&mut self, user_id: u64) -> Admission {
        if !self.accepting {
            return Admission::Rejected(QueueRejection::ShuttingDown);
        }

        let user_is_running = self.running.contains_key(&user_id);
        let user_is_waiting = self.waiting_by_user.contains_key(&user_id);
        if !user_is_running && !user_is_waiting && self.running.len() < self.limits.max_active {
            let task_id = self.allocate_task_id();
            self.running.insert(user_id, task_id);
            return Admission::Started { task_id };
        }

        let user_waiting_count = self.waiting_by_user.get(&user_id).map_or(0, VecDeque::len);
        if user_waiting_count >= self.limits.max_waiting_per_user {
            return Admission::Rejected(QueueRejection::UserQueueFull);
        }
        if self.waiting_count >= self.limits.max_waiting {
            return Admission::Rejected(QueueRejection::GlobalQueueFull);
        }

        let task_id = self.allocate_task_id();
        let queue = self.waiting_by_user.entry(user_id).or_default();
        if queue.is_empty() {
            self.round_robin.push_back(user_id);
        }
        queue.push_back(WaitingTask {
            task_id,
            user_id,
            ready: false,
        });
        self.waiting_count += 1;

        Admission::Queued {
            task_id,
            waiting_count: self.waiting_count,
        }
    }

    fn mark_ready(&mut self, task_id: u64) -> Vec<u64> {
        for queue in self.waiting_by_user.values_mut() {
            if let Some(task) = queue.iter_mut().find(|task| task.task_id == task_id) {
                task.ready = true;
                break;
            }
        }
        self.dispatch_ready()
    }

    fn complete(&mut self, task_id: u64, user_id: u64) -> Vec<u64> {
        if self.running.get(&user_id).copied() != Some(task_id) {
            return Vec::new();
        }
        self.running.remove(&user_id);
        self.move_user_to_back(user_id);
        self.dispatch_ready()
    }

    fn cancel_waiting(&mut self, task_id: u64) -> Vec<u64> {
        let Some(user_id) = self.waiting_by_user.iter().find_map(|(user_id, queue)| {
            queue
                .iter()
                .any(|task| task.task_id == task_id)
                .then_some(*user_id)
        }) else {
            return Vec::new();
        };

        let queue_is_empty = {
            let queue = self
                .waiting_by_user
                .get_mut(&user_id)
                .expect("located waiting queue must still exist");
            let position = queue
                .iter()
                .position(|task| task.task_id == task_id)
                .expect("located waiting task must still exist");
            queue.remove(position);
            queue.is_empty()
        };
        self.waiting_count -= 1;
        if queue_is_empty {
            self.waiting_by_user.remove(&user_id);
            self.round_robin
                .retain(|queued_user| *queued_user != user_id);
        }
        self.dispatch_ready()
    }

    fn shutdown(&mut self) -> Vec<u64> {
        self.accepting = false;
        let task_ids = self
            .waiting_by_user
            .values()
            .flat_map(|queue| queue.iter().map(|task| task.task_id))
            .collect();
        self.waiting_by_user.clear();
        self.round_robin.clear();
        self.waiting_count = 0;
        task_ids
    }

    fn allocate_task_id(&mut self) -> u64 {
        let task_id = self.next_task_id;
        self.next_task_id = self
            .next_task_id
            .checked_add(1)
            .expect("task id space exhausted");
        task_id
    }

    fn move_user_to_back(&mut self, user_id: u64) {
        if let Some(position) = self
            .round_robin
            .iter()
            .position(|queued_user| *queued_user == user_id)
        {
            self.round_robin.remove(position);
            self.round_robin.push_back(user_id);
        }
    }

    fn dispatch_ready(&mut self) -> Vec<u64> {
        let mut dispatched = Vec::new();
        while self.accepting && self.running.len() < self.limits.max_active {
            let Some(task) = self.pop_next_ready() else {
                break;
            };
            self.waiting_count -= 1;
            self.running.insert(task.user_id, task.task_id);
            dispatched.push(task.task_id);
        }
        dispatched
    }

    fn pop_next_ready(&mut self) -> Option<WaitingTask> {
        let users_to_scan = self.round_robin.len();
        for _ in 0..users_to_scan {
            let user_id = self.round_robin.pop_front()?;
            let can_run = !self.running.contains_key(&user_id)
                && self
                    .waiting_by_user
                    .get(&user_id)
                    .and_then(|queue| queue.front())
                    .is_some_and(|task| task.ready);
            if !can_run {
                self.round_robin.push_back(user_id);
                continue;
            }

            let (task, has_more) = {
                let queue = self
                    .waiting_by_user
                    .get_mut(&user_id)
                    .expect("round-robin user must have a waiting queue");
                let task = queue
                    .pop_front()
                    .expect("eligible waiting queue must not be empty");
                (task, !queue.is_empty())
            };
            if has_more {
                self.round_robin.push_back(user_id);
            } else {
                self.waiting_by_user.remove(&user_id);
            }
            return Some(task);
        }
        None
    }
}

struct SchedulerState {
    scheduler: FairTaskScheduler,
    waiting_tasks: HashMap<u64, ParseTask>,
}

impl SchedulerState {
    fn new() -> Self {
        Self {
            scheduler: FairTaskScheduler::new(SchedulerLimits::default()),
            waiting_tasks: HashMap::new(),
        }
    }

    fn take_dispatched(&mut self, task_ids: Vec<u64>) -> Vec<ParseTask> {
        task_ids
            .into_iter()
            .filter_map(|task_id| self.waiting_tasks.remove(&task_id))
            .collect()
    }
}

enum QueueAction {
    Start(ParseTask),
    Wait { task_id: u64, waiting_count: usize },
    Reject(QueueRejection),
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
    ) -> Self {
        Self {
            telegram,
            resolver,
            downloader,
            cache,
            required_channel_id: required_channel_id.map(Arc::from),
            telegram_hard_limit,
            scheduler: Arc::new(Mutex::new(SchedulerState::new())),
            media_slots: Arc::new(Semaphore::new(MEDIA_PIPELINE_CONCURRENCY)),
            background_tasks: TaskTracker::new(),
            task_abort_handles: Arc::new(StdMutex::new(Vec::new())),
        }
    }

    pub async fn run(self, shutdown: CancellationToken) -> Result<()> {
        let me = tokio::select! {
            _ = shutdown.cancelled() => {
                self.shutdown_background_tasks(&shutdown).await;
                return Ok(());
            }
            result = self.telegram.get_me() => result.map_err(AppError::from)?,
        };
        info!(bot_id = me.id, username = ?me.username, "Telegram Bot API 已连接");

        let mut offset = None;
        'polling: loop {
            let updates = tokio::select! {
                _ = shutdown.cancelled() => break 'polling,
                result = self.telegram.get_updates(offset, UPDATE_TIMEOUT_SECS) => result,
            };
            match updates {
                Ok(updates) => {
                    for update in updates {
                        if shutdown.is_cancelled() {
                            break 'polling;
                        }
                        let next_offset = update.update_id.saturating_add(1);
                        if let Err(error) = self.handle_update(update, &shutdown).await {
                            error!(error = %error, "处理 Telegram update 失败");
                        }
                        offset = Some(next_offset);
                    }
                }
                Err(error) => {
                    let delay = error.retry_after().unwrap_or(Duration::from_secs(2));
                    warn!(error = %error, ?delay, "获取 Telegram updates 失败");
                    tokio::select! {
                        _ = shutdown.cancelled() => break 'polling,
                        _ = tokio::time::sleep(delay) => {}
                    }
                }
            }
        }

        self.shutdown_background_tasks(&shutdown).await;
        Ok(())
    }

    async fn handle_update(&self, update: Update, shutdown: &CancellationToken) -> Result<()> {
        if let Some(message) = update.message.filter(|message| message.text.is_some()) {
            return self.handle_message(message, shutdown).await;
        }
        Ok(())
    }

    async fn handle_message(&self, message: Message, shutdown: &CancellationToken) -> Result<()> {
        let Some(user_id) = message.sender_id() else {
            return Ok(());
        };
        let chat_id = message.chat.id;
        if !message.chat.is_private() {
            tokio::select! {
                _ = shutdown.cancelled() => {}
                _ = self.telegram.send_message(chat_id, "首版只在私聊中工作。", None) => {}
            }
            return Ok(());
        }

        let text = message.text.as_deref().unwrap_or_default().trim();
        match command_name(text) {
            Some("start") => {
                let help = format_help_text(self.required_channel_id.as_deref());
                tokio::select! {
                    _ = shutdown.cancelled() => {}
                    result = self.telegram.send_message(chat_id, &help, None) => {
                        result?;
                    }
                }
            }
            Some(_) => {
                tokio::select! {
                    _ = shutdown.cancelled() => {}
                    result = self.telegram.send_message(
                        chat_id,
                        "请直接发送微信视频号链接。",
                        None,
                    ) => {
                        result?;
                    }
                }
            }
            None => {
                self.queue_parse(
                    chat_id,
                    user_id,
                    message.message_id,
                    text.to_owned(),
                    shutdown,
                )
                .await?;
            }
        }
        Ok(())
    }

    async fn queue_parse(
        &self,
        chat_id: i64,
        user_id: u64,
        source_message_id: i64,
        input: String,
        shutdown: &CancellationToken,
    ) -> Result<()> {
        let share_url = match extract_share_url(&input) {
            Ok(url) => url,
            Err(error) => {
                tokio::select! {
                    _ = shutdown.cancelled() => return Err(AppError::Cancelled),
                    result = self.telegram.send_message_reply(
                        chat_id,
                        error.user_message(),
                        source_message_id,
                        None,
                    ) => result?,
                };
                return Ok(());
            }
        };
        let cancellation = shutdown.child_token();
        let action = {
            let mut state = self.scheduler.lock().await;
            match state.scheduler.admit(user_id) {
                Admission::Started { task_id } => QueueAction::Start(ParseTask {
                    task_id,
                    user_id,
                    chat_id,
                    source_message_id,
                    share_url,
                    status_message_id: None,
                    cancellation,
                }),
                Admission::Queued {
                    task_id,
                    waiting_count,
                } => {
                    state.waiting_tasks.insert(
                        task_id,
                        ParseTask {
                            task_id,
                            user_id,
                            chat_id,
                            source_message_id,
                            share_url,
                            status_message_id: None,
                            cancellation,
                        },
                    );
                    QueueAction::Wait {
                        task_id,
                        waiting_count,
                    }
                }
                Admission::Rejected(reason) => QueueAction::Reject(reason),
            }
        };

        match action {
            QueueAction::Start(task) => self.spawn_parse_task(task),
            QueueAction::Wait {
                task_id,
                waiting_count,
            } => {
                let text = format_queue_status(waiting_count);
                let status = tokio::select! {
                    _ = shutdown.cancelled() => {
                        self.withdraw_waiting_task(task_id).await;
                        return Err(AppError::Cancelled);
                    }
                    result = self.telegram.send_message_reply_html(
                        chat_id,
                        &text,
                        source_message_id,
                        None,
                    ) => match result {
                        Ok(message) => message,
                        Err(error) => {
                            self.withdraw_waiting_task(task_id).await;
                            return Err(error.into());
                        }
                    },
                };

                let (found, dispatched) = {
                    let mut state = self.scheduler.lock().await;
                    let found = if let Some(task) = state.waiting_tasks.get_mut(&task_id) {
                        task.status_message_id = Some(status.message_id);
                        true
                    } else {
                        false
                    };
                    let dispatched = if found {
                        let task_ids = state.scheduler.mark_ready(task_id);
                        state.take_dispatched(task_ids)
                    } else {
                        Vec::new()
                    };
                    (found, dispatched)
                };
                if !found {
                    let _ = tokio::time::timeout(
                        Duration::from_secs(1),
                        self.telegram.delete_message(chat_id, status.message_id),
                    )
                    .await;
                    return Err(AppError::Cancelled);
                }
                self.spawn_parse_tasks(dispatched);
            }
            QueueAction::Reject(QueueRejection::UserQueueFull) => {
                tokio::select! {
                    _ = shutdown.cancelled() => return Err(AppError::Cancelled),
                    result = self.telegram.send_message_reply(
                        chat_id,
                        "你的等待队列已满（最多 3 个任务），请等待当前任务完成后再发送。",
                        source_message_id,
                        None,
                    ) => result?,
                };
            }
            QueueAction::Reject(QueueRejection::GlobalQueueFull) => {
                tokio::select! {
                    _ = shutdown.cancelled() => return Err(AppError::Cancelled),
                    result = self.telegram.send_message_reply(
                        chat_id,
                        "当前等待队列已满（最多 100 个任务），请稍后再发送链接。",
                        source_message_id,
                        None,
                    ) => result?,
                };
            }
            QueueAction::Reject(QueueRejection::ShuttingDown) => {
                return Err(AppError::Cancelled);
            }
        }
        Ok(())
    }

    async fn withdraw_waiting_task(&self, task_id: u64) {
        let dispatched = {
            let mut state = self.scheduler.lock().await;
            state.waiting_tasks.remove(&task_id);
            let task_ids = state.scheduler.cancel_waiting(task_id);
            state.take_dispatched(task_ids)
        };
        self.spawn_parse_tasks(dispatched);
    }

    fn spawn_parse_tasks(&self, tasks: Vec<ParseTask>) {
        for task in tasks {
            self.spawn_parse_task(task);
        }
    }

    fn spawn_parse_task(&self, task: ParseTask) {
        let service = self.clone();
        let task_id = task.task_id;
        let user_id = task.user_id;
        let handle = self.background_tasks.spawn(async move {
            let result = AssertUnwindSafe(service.process_parse_task(&task))
                .catch_unwind()
                .await;
            let dispatched = {
                let mut state = service.scheduler.lock().await;
                let task_ids = state.scheduler.complete(task_id, user_id);
                state.take_dispatched(task_ids)
            };

            match result {
                Ok(Err(error)) if !matches!(error, AppError::Cancelled) => {
                    error!(task_id, user_id, error = %error, "视频任务失败");
                }
                Err(_) => {
                    error!(task_id, user_id, "视频任务异常终止，调度名额已释放");
                }
                _ => {}
            }
            service.spawn_parse_tasks(dispatched);
        });
        let abort_handle = handle.abort_handle();
        drop(handle);
        let mut abort_handles = self
            .task_abort_handles
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        abort_handles.retain(|handle| !handle.is_finished());
        abort_handles.push(abort_handle);
    }

    async fn process_parse_task(&self, task: &ParseTask) -> Result<()> {
        let cancellation = &task.cancellation;
        let status_message_id = match task.status_message_id {
            Some(status_message_id) => {
                tokio::select! {
                    _ = cancellation.cancelled() => return Err(AppError::Cancelled),
                    result = self.telegram.edit_message_text_html(
                        task.chat_id,
                        status_message_id,
                        "<b>▎解 析 中...</b>",
                        None,
                    ) => {
                        if let Err(error) = result {
                            warn!(task_id = task.task_id, error = %error, "排队状态更新失败，任务继续执行");
                        }
                    }
                }
                status_message_id
            }
            None => {
                tokio::select! {
                    _ = cancellation.cancelled() => return Err(AppError::Cancelled),
                    result = self.telegram.send_message_reply_html(
                        task.chat_id,
                        "<b>▎解 析 中...</b>",
                        task.source_message_id,
                        None,
                    ) => result?.message_id,
                }
            }
        };

        let channel_allowed = self
            .ensure_required_channel(
                task.chat_id,
                task.user_id,
                task.source_message_id,
                cancellation,
            )
            .await;
        match channel_allowed {
            Ok(true) => {}
            Ok(false) => {
                tokio::select! {
                    _ = cancellation.cancelled() => {}
                    _ = self.telegram.delete_message(task.chat_id, status_message_id) => {}
                }
                return Ok(());
            }
            Err(error) => {
                self.edit_error_status(task.chat_id, status_message_id, &error, cancellation)
                    .await;
                return Err(error);
            }
        }

        let resolved = tokio::select! {
            _ = cancellation.cancelled() => Err(AppError::Cancelled),
            result = self.resolver.resolve_url(&task.share_url) => result,
        };

        let post = match resolved {
            Ok(post) => Arc::new(post),
            Err(error) => {
                self.edit_error_status(task.chat_id, status_message_id, &error, cancellation)
                    .await;
                return Err(error);
            }
        };
        let media_task = MediaTask {
            chat_id: task.chat_id,
            source_message_id: task.source_message_id,
            status_message_id,
            post,
        };

        let result = self.run_download(&media_task, cancellation).await;
        if let Err(error) = &result {
            self.edit_error_status(task.chat_id, status_message_id, error, cancellation)
                .await;
        }
        result
    }

    async fn edit_error_status(
        &self,
        chat_id: i64,
        status_message_id: i64,
        error: &AppError,
        cancellation: &CancellationToken,
    ) {
        tokio::select! {
            _ = cancellation.cancelled() => {}
            _ = self.telegram.edit_message_text(
                chat_id,
                status_message_id,
                error.user_message(),
                None,
            ) => {}
        }
    }

    async fn run_download(&self, task: &MediaTask, cancellation: &CancellationToken) -> Result<()> {
        if cancellation.is_cancelled() {
            return Err(AppError::Cancelled);
        }
        let caption = format_caption(&task.post);
        if let Some(cached) = self
            .cache
            .get(&task.post.platform, &task.post.post_id)
            .await?
        {
            self.update_status_html(task, "<b>▎发 送 中...</b>", cancellation)
                .await;
            let input = InputFile::file_id(cached.file_id.clone())?;
            let cached_send = tokio::select! {
                _ = cancellation.cancelled() => return Err(AppError::Cancelled),
                result = self.send_media(
                    task.chat_id,
                    cached.kind,
                    &input,
                    &caption,
                    task.source_message_id,
                    None,
                ) => result,
            };
            match cached_send {
                Ok(_) => {
                    tokio::select! {
                        _ = cancellation.cancelled() => {}
                        _ = self.telegram.delete_message(
                            task.chat_id,
                            task.status_message_id,
                        ) => {}
                    }
                    return Ok(());
                }
                Err(error) if error.error_code() == Some(400) => {
                    self.cache
                        .remove(&task.post.platform, &task.post.post_id)
                        .await?;
                }
                Err(error) => return Err(error.into()),
            }
        }

        let mut source = task.post.video.clone();
        let _permit = tokio::select! {
            _ = cancellation.cancelled() => return Err(AppError::Cancelled),
            permit = self.media_slots.acquire() => permit.map_err(|_| AppError::Cancelled)?,
        };
        self.update_status_html(task, "<b>▎下 载 中...</b>", cancellation)
            .await;
        let first_download = self.download_with_status(&source, task, cancellation).await;
        let downloaded = match first_download {
            Ok(downloaded) => downloaded,
            Err(AppError::Expired) => {
                self.update_status_html(task, "<b>▎解 析 中...</b>", cancellation)
                    .await;
                let refreshed = tokio::select! {
                    _ = cancellation.cancelled() => return Err(AppError::Cancelled),
                    result = self.resolver.resolve_url(&task.post.canonical_url) => result?,
                };
                source = refreshed.video;
                self.update_status_html(task, "<b>▎下 载 中...</b>", cancellation)
                    .await;
                self.download_with_status(&source, task, cancellation)
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
            self.update_status_html(task, "<b>▎上 传 中...</b>", cancellation)
                .await;

            let input = InputFile::local_path(&downloaded.path)?;
            let send_result = tokio::select! {
                _ = cancellation.cancelled() => return Err(AppError::Cancelled),
                result = self.send_media(
                    task.chat_id,
                    kind,
                    &input,
                    &caption,
                    task.source_message_id,
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
                            task.chat_id,
                            TelegramMediaKind::Document,
                            &input,
                            &caption,
                            task.source_message_id,
                            None,
                        ) => result?,
                    }
                }
                Err(error) => return Err(error.into()),
            };

            self.update_status_html(task, "<b>▎上 传 中... | 100%</b>", cancellation)
                .await;

            let ids = sent
                .media_file_ids()
                .ok_or_else(|| AppError::Telegram("发送成功但响应缺少 file_id".into()))?;
            if let Err(error) = self
                .cache
                .put(
                    &task.post.platform,
                    &task.post.post_id,
                    ids.kind,
                    ids.file_id,
                    Some(ids.file_unique_id),
                )
                .await
            {
                warn!(error = %error, "视频已发送，但 file_id 缓存写入失败");
            }

            tokio::select! {
                _ = cancellation.cancelled() => return Err(AppError::Cancelled),
                _ = tokio::time::sleep(Duration::from_millis(500)) => {}
            }
            tokio::select! {
                _ = cancellation.cancelled() => {}
                _ = self.telegram.delete_message(
                    task.chat_id,
                    task.status_message_id,
                ) => {}
            }
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
        task: &MediaTask,
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
                        self.update_download_status(task, progress.percent, cancellation).await;
                    }
                }
                result = &mut download => {
                    let downloaded = result?;
                    if last_percent < 100 {
                        self.update_download_status(task, 100, cancellation).await;
                    }
                    return Ok(downloaded);
                }
            }
        }
    }

    async fn update_download_status(
        &self,
        task: &MediaTask,
        percent: u8,
        cancellation: &CancellationToken,
    ) {
        let text = format_download_status(percent);
        self.update_status_html(task, &text, cancellation).await;
    }

    async fn update_status_html(
        &self,
        task: &MediaTask,
        text: &str,
        cancellation: &CancellationToken,
    ) {
        tokio::select! {
            _ = cancellation.cancelled() => {}
            result = self.telegram.edit_message_text_html(
                task.chat_id,
                task.status_message_id,
                text,
                None,
            ) => {
                if let Err(error) = result {
                    warn!(error = %error, "状态消息更新失败，媒体任务继续执行");
                }
            }
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
        cancellation: &CancellationToken,
    ) -> Result<bool> {
        let Some(channel_id) = self.required_channel_id.as_deref() else {
            return Ok(true);
        };

        let membership = tokio::select! {
            _ = cancellation.cancelled() => return Err(AppError::Cancelled),
            result = self.telegram.get_chat_member(channel_id, user_id) => result,
        };
        match membership {
            Ok(member) if member.has_joined() => Ok(true),
            Ok(_) => {
                let prompt = format_channel_requirement(channel_id);
                tokio::select! {
                    _ = cancellation.cancelled() => return Err(AppError::Cancelled),
                    result = self.telegram.send_message_reply_html(
                        chat_id,
                        &prompt,
                        source_message_id,
                        None,
                    ) => {
                        result?;
                    }
                }
                Ok(false)
            }
            Err(error) => {
                warn!(
                    error = %error,
                    channel_id,
                    user_id,
                    "频道成员状态验证失败"
                );
                tokio::select! {
                    _ = cancellation.cancelled() => return Err(AppError::Cancelled),
                    result = self.telegram.send_message_reply(
                        chat_id,
                        "暂时无法验证频道关注状态，请稍后重试。",
                        source_message_id,
                        None,
                    ) => {
                        result?;
                    }
                }
                Ok(false)
            }
        }
    }

    async fn shutdown_background_tasks(&self, shutdown: &CancellationToken) {
        shutdown.cancel();
        let waiting_tasks = {
            let mut state = self.scheduler.lock().await;
            let task_ids = state.scheduler.shutdown();
            state.take_dispatched(task_ids)
        };
        for task in waiting_tasks {
            task.cancellation.cancel();
        }
        self.background_tasks.close();

        if tokio::time::timeout(SHUTDOWN_GRACE, self.background_tasks.wait())
            .await
            .is_ok()
        {
            info!("所有后台视频任务已完成清理");
            return;
        }

        warn!(
            grace_seconds = SHUTDOWN_GRACE.as_secs(),
            "后台视频任务未在宽限期内退出，正在强制终止"
        );
        let abort_handles = self
            .task_abort_handles
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        for handle in abort_handles {
            if !handle.is_finished() {
                handle.abort();
            }
        }
        if tokio::time::timeout(FORCED_SHUTDOWN_GRACE, self.background_tasks.wait())
            .await
            .is_err()
        {
            warn!("部分后台任务在强制终止后仍未完成清理");
        }
    }
}

fn command_name(text: &str) -> Option<&str> {
    text.split_whitespace()
        .next()
        .and_then(|value| value.strip_prefix('/'))
        .map(|value| value.split('@').next().unwrap_or(value))
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
    let mut text = String::from("发送微信视频号链接或分享文案，我会自动下载并发送视频。");
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

fn format_queue_status(waiting_count: usize) -> String {
    format!("<b>▎排 队 中...</b>\n\n当前共有 {waiting_count} 个任务等待调度，轮到后会自动解析。")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recognizes_only_start_as_the_supported_command() {
        assert_eq!(command_name("/start"), Some("start"));
        assert_eq!(command_name("/start@parse_bot"), Some("start"));
        assert_ne!(command_name("/unknown"), Some("start"));
        assert_eq!(command_name("https://weixin.qq.com/sph/example"), None);
    }

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
                "标题 <视频> & 测试",
                "https://weixin.qq.com/sph/example?a=1&b=2"
            ),
            "标题 &lt;视频&gt; &amp; 测试\n\n<b>▎<a href=\"https://weixin.qq.com/sph/example?a=1&amp;b=2\">Source</a></b>"
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
    fn limits_active_workflows_to_ten() {
        assert_eq!(MEDIA_PIPELINE_CONCURRENCY, MAX_ACTIVE_TASKS);
        let mut scheduler = FairTaskScheduler::new(SchedulerLimits::default());
        for user_id in 1..=MAX_ACTIVE_TASKS as u64 {
            assert!(matches!(
                scheduler.admit(user_id),
                Admission::Started { .. }
            ));
        }
        assert_eq!(scheduler.running.len(), MAX_ACTIVE_TASKS);

        let Admission::Queued {
            task_id,
            waiting_count,
        } = scheduler.admit(11)
        else {
            panic!("the eleventh workflow must wait");
        };
        assert_eq!(waiting_count, 1);
        assert!(scheduler.mark_ready(task_id).is_empty());
        assert_eq!(scheduler.running.len(), MAX_ACTIVE_TASKS);
    }

    #[test]
    fn runs_only_one_task_per_user_and_ignores_stale_completions() {
        let mut scheduler = test_scheduler(2);
        let first = started_task_id(scheduler.admit(7));
        let second = queued_task_id(scheduler.admit(7));
        assert!(scheduler.mark_ready(second).is_empty());
        assert!(matches!(scheduler.admit(8), Admission::Started { .. }));

        assert_eq!(scheduler.complete(first, 7), vec![second]);
        assert!(scheduler.complete(first, 7).is_empty());
        assert_eq!(scheduler.running.get(&7), Some(&second));

        let third = queued_task_id(scheduler.admit(7));
        assert!(scheduler.mark_ready(third).is_empty());
        assert_eq!(scheduler.complete(second, 7), vec![third]);
    }

    #[test]
    fn rotates_fairly_between_users() {
        let mut scheduler = test_scheduler(1);
        let first_a = started_task_id(scheduler.admit(1));
        let second_a = queued_task_id(scheduler.admit(1));
        let third_a = queued_task_id(scheduler.admit(1));
        let first_b = queued_task_id(scheduler.admit(2));
        assert!(scheduler.mark_ready(second_a).is_empty());
        assert!(scheduler.mark_ready(third_a).is_empty());
        assert!(scheduler.mark_ready(first_b).is_empty());

        assert_eq!(scheduler.complete(first_a, 1), vec![first_b]);
        assert_eq!(scheduler.complete(first_b, 2), vec![second_a]);
        assert_eq!(scheduler.complete(second_a, 1), vec![third_a]);
    }

    #[test]
    fn limits_each_user_to_three_waiting_tasks() {
        let mut scheduler = test_scheduler(1);
        started_task_id(scheduler.admit(1));
        for _ in 0..MAX_WAITING_TASKS_PER_USER {
            assert!(matches!(scheduler.admit(1), Admission::Queued { .. }));
        }
        assert_eq!(
            scheduler.admit(1),
            Admission::Rejected(QueueRejection::UserQueueFull)
        );
        assert_eq!(scheduler.waiting_count, MAX_WAITING_TASKS_PER_USER);
    }

    #[test]
    fn limits_the_global_waiting_queue_to_one_hundred() {
        let mut scheduler = test_scheduler(1);
        started_task_id(scheduler.admit(1));
        for user_id in 2..=(MAX_WAITING_TASKS as u64 + 1) {
            assert!(matches!(scheduler.admit(user_id), Admission::Queued { .. }));
        }
        assert_eq!(scheduler.waiting_count, MAX_WAITING_TASKS);
        assert_eq!(
            scheduler.admit(MAX_WAITING_TASKS as u64 + 2),
            Admission::Rejected(QueueRejection::GlobalQueueFull)
        );
    }

    #[test]
    fn dispatches_ready_tasks_when_active_workflows_finish() {
        let mut scheduler = test_scheduler(2);
        let first = started_task_id(scheduler.admit(1));
        let second = started_task_id(scheduler.admit(2));
        let third = queued_task_id(scheduler.admit(3));
        let fourth = queued_task_id(scheduler.admit(4));
        assert!(scheduler.mark_ready(third).is_empty());
        assert!(scheduler.mark_ready(fourth).is_empty());

        assert_eq!(scheduler.complete(first, 1), vec![third]);
        assert_eq!(scheduler.complete(second, 2), vec![fourth]);
        assert_eq!(scheduler.running.len(), 2);
    }

    #[test]
    fn cancelling_an_unready_reservation_unblocks_the_queue() {
        let mut scheduler = test_scheduler(1);
        let running = started_task_id(scheduler.admit(1));
        let unready = queued_task_id(scheduler.admit(2));
        let ready = queued_task_id(scheduler.admit(3));
        assert!(scheduler.mark_ready(ready).is_empty());

        assert!(scheduler.cancel_waiting(unready).is_empty());
        assert_eq!(scheduler.complete(running, 1), vec![ready]);
    }

    #[test]
    fn shutdown_rejects_new_work_and_drains_waiting_tasks() {
        let mut scheduler = test_scheduler(2);
        let running = started_task_id(scheduler.admit(1));
        started_task_id(scheduler.admit(2));
        let third = queued_task_id(scheduler.admit(3));
        let fourth = queued_task_id(scheduler.admit(4));
        assert!(scheduler.mark_ready(third).is_empty());
        assert!(scheduler.mark_ready(fourth).is_empty());

        let mut drained = scheduler.shutdown();
        drained.sort_unstable();
        let mut expected = vec![third, fourth];
        expected.sort_unstable();
        assert_eq!(drained, expected);
        assert_eq!(scheduler.waiting_count, 0);
        assert!(scheduler.waiting_by_user.is_empty());
        assert!(scheduler.round_robin.is_empty());
        assert_eq!(
            scheduler.admit(5),
            Admission::Rejected(QueueRejection::ShuttingDown)
        );
        assert!(scheduler.complete(running, 1).is_empty());
    }

    #[test]
    fn formats_a_factual_waiting_queue_status() {
        assert_eq!(
            format_queue_status(12),
            "<b>▎排 队 中...</b>\n\n当前共有 12 个任务等待调度，轮到后会自动解析。"
        );
    }

    #[test]
    fn mentions_the_required_channel_only_when_configured() {
        let public_help = format_help_text(None);
        assert!(!public_help.contains("关注频道"));
        assert!(!public_help.contains("2000 MB"));
        assert!(!public_help.contains("telegram-bot-api"));

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

    fn test_scheduler(max_active: usize) -> FairTaskScheduler {
        FairTaskScheduler::new(SchedulerLimits {
            max_active,
            max_waiting_per_user: MAX_WAITING_TASKS_PER_USER,
            max_waiting: MAX_WAITING_TASKS,
        })
    }

    fn started_task_id(admission: Admission) -> u64 {
        let Admission::Started { task_id } = admission else {
            panic!("task must start immediately");
        };
        task_id
    }

    fn queued_task_id(admission: Admission) -> u64 {
        let Admission::Queued { task_id, .. } = admission else {
            panic!("task must be queued");
        };
        task_id
    }
}
