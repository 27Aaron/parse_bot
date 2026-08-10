use parse_bot::{
    AppError, Result,
    config::Config,
    media::MediaDownloader,
    storage::MediaCache,
    telegram::{BotService, TELEGRAM_FILE_LIMIT_BYTES, TdlibConfig, TelegramClient},
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
    let _data_directory_lock = config.data_paths.acquire_lock()?;
    let cleanup = config.data_paths.cleanup_stale_media()?;
    if cleanup.files > 0 {
        warn!(
            files = cleanup.files,
            bytes = cleanup.bytes,
            "已清理上次异常退出遗留的临时媒体"
        );
    }

    let resolver = WechatResolver::new(config.wechat_yuanbao_cookie.clone())?;
    let downloader =
        MediaDownloader::new(config.data_paths.media.clone(), TELEGRAM_FILE_LIMIT_BYTES)?;
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
        let Some(file) = open_local_environment_file(std::path::Path::new(path))? else {
            continue;
        };
        match dotenvy::from_read(file) {
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

fn open_local_environment_file(path: &std::path::Path) -> Result<Option<std::fs::File>> {
    let mut options = std::fs::OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;

        options.custom_flags(libc::O_NOFOLLOW);
    }
    let file = match options.open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(AppError::Io(error)),
    };
    let metadata = file.metadata()?;
    if !metadata.file_type().is_file() {
        return Err(AppError::Config(format!(
            "{} 必须是普通文件",
            path.display()
        )));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};

        // SAFETY: geteuid takes no pointers and has no preconditions.
        let effective_user_id = unsafe { libc::geteuid() };
        if metadata.uid() != effective_user_id {
            return Err(AppError::Config(format!(
                "{} 必须归当前运行用户所有",
                path.display()
            )));
        }
        if metadata.permissions().mode() & 0o077 != 0 {
            return Err(AppError::Config(format!(
                "{} 权限过宽，请执行 chmod 600 {}",
                path.display(),
                path.display()
            )));
        }
    }
    Ok(Some(file))
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

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(unix)]
    #[test]
    fn local_environment_file_must_be_private_and_not_a_symlink() {
        use std::os::unix::fs::{PermissionsExt, symlink};

        let directory = std::env::temp_dir().join(format!(
            "parse-bot-env-test-{}",
            uuid::Uuid::new_v4().hyphenated()
        ));
        std::fs::create_dir(&directory).unwrap();
        let environment = directory.join(".env.local");
        std::fs::write(&environment, b"EXAMPLE=value\n").unwrap();
        std::fs::set_permissions(&environment, std::fs::Permissions::from_mode(0o600)).unwrap();
        assert!(open_local_environment_file(&environment).unwrap().is_some());

        std::fs::set_permissions(&environment, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert!(open_local_environment_file(&environment).is_err());
        std::fs::set_permissions(&environment, std::fs::Permissions::from_mode(0o600)).unwrap();

        let link = directory.join("linked.env");
        symlink(&environment, &link).unwrap();
        assert!(open_local_environment_file(&link).is_err());
        assert!(
            open_local_environment_file(&directory.join("missing"))
                .unwrap()
                .is_none()
        );
        std::fs::remove_dir_all(directory).unwrap();
    }
}
