use parse_bot::{
    AppError, Result,
    config::Config,
    media::MediaDownloader,
    storage::MediaCache,
    telegram::{BotService, TdlibConfig, TelegramClient},
    wechat::WechatResolver,
};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

const BOT_SHUTDOWN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(40);

#[tokio::main]
async fn main() -> Result<()> {
    load_local_environment()?;
    init_tracing();

    let config = Config::from_env()?;

    let resolver = WechatResolver::new(
        config.wechat_yuanbao_cookie.clone(),
        config.wechat_resolve_timeout,
    )?;
    let downloader = MediaDownloader::with_options(
        config.data_paths.media.clone(),
        config
            .media_max_source_bytes
            .min(config.telegram_hard_limit_bytes),
        config.media_hosts,
        config.wechat_download_timeout,
    )?;
    let cache = MediaCache::open(&config.data_paths.state_database)?;
    let telegram = TelegramClient::connect(TdlibConfig {
        api_id: config.telegram_api_id,
        api_hash: config.telegram_api_hash,
        bot_token: config.telegram_bot_token,
        database_directory: config.data_paths.tdlib_database,
        files_directory: config.data_paths.tdlib_files,
        cover_downloader: downloader.clone(),
    })
    .await?;

    let bot = BotService::new(
        telegram.clone(),
        resolver,
        downloader,
        cache,
        config.required_channel_id,
        config.telegram_hard_limit_bytes,
    );

    let shutdown = CancellationToken::new();
    let run = bot.run(shutdown.clone());
    tokio::pin!(run);
    let run_result = tokio::select! {
        result = &mut run => result,
        signal = shutdown_signal() => {
            shutdown.cancel();
            match signal {
                Ok(()) => {
                    info!("收到退出信号");
                    match tokio::time::timeout(BOT_SHUTDOWN_TIMEOUT, &mut run).await {
                        Ok(result) => result,
                        Err(_) => {
                            warn!("Bot 清理超时，继续关闭 TDLib");
                            Err(AppError::Telegram("bot shutdown timed out".into()))
                        }
                    }
                }
                Err(error) => {
                    let _ = tokio::time::timeout(BOT_SHUTDOWN_TIMEOUT, &mut run).await;
                    Err(AppError::Io(error))
                }
            }
        }
    };

    let close_result = telegram.close().await.map_err(AppError::from);
    match (run_result, close_result) {
        (Ok(()), result) => result,
        (Err(error), Ok(())) => Err(error),
        (Err(error), Err(close_error)) => {
            warn!(error = %close_error, "关闭 TDLib 时发生额外错误");
            Err(error)
        }
    }
}

#[cfg(unix)]
async fn shutdown_signal() -> std::io::Result<()> {
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    tokio::select! {
        result = tokio::signal::ctrl_c() => result,
        _ = terminate.recv() => Ok(()),
    }
}

#[cfg(not(unix))]
async fn shutdown_signal() -> std::io::Result<()> {
    tokio::signal::ctrl_c().await
}

fn load_local_environment() -> Result<()> {
    for path in [".env.local", ".env"] {
        match dotenvy::from_path(path) {
            // `.env.local` is the preferred complete local configuration;
            // `.env` is only a fallback when it does not exist.
            Ok(()) => return Ok(()),
            Err(error) if error.not_found() => {}
            Err(dotenvy::Error::LineParse(_, index)) => {
                return Err(AppError::Config(format!(
                    "{path} 格式错误（出错字符位置 {index}）。包含空格或分号的值必须用单引号包住，例如 WECHAT_YUANBAO_COOKIE='完整 Cookie'"
                )));
            }
            Err(dotenvy::Error::Io(error)) => {
                return Err(AppError::Config(format!("无法读取 {path}：{error}")));
            }
            Err(dotenvy::Error::EnvVar(_)) => {
                return Err(AppError::Config(format!("无法加载 {path} 中的环境变量")));
            }
            Err(_) => {
                return Err(AppError::Config(format!("无法解析 {path}")));
            }
        }
    }
    Ok(())
}

fn init_tracing() {
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("parse_bot=info,warn"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .compact()
        .init();
}
