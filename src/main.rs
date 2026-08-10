use parse_bot::{
    AppError, Result,
    config::Config,
    media::MediaDownloader,
    storage::MediaCache,
    telegram::{BotService, TelegramClient},
    wechat::WechatResolver,
};
use tokio_util::sync::CancellationToken;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    load_local_environment()?;
    init_tracing();

    let mut config = Config::from_env()?;
    config.prepare_paths()?;

    let telegram = TelegramClient::new(
        config.telegram_api_url.clone(),
        config.telegram_bot_token.clone(),
    )?;
    let resolver = WechatResolver::new(
        config.wechat_yuanbao_cookie.clone(),
        config.wechat_resolve_timeout,
    )?;
    let downloader = MediaDownloader::with_options(
        config.media_shared_dir.clone(),
        config
            .media_max_source_bytes
            .min(config.telegram_hard_limit_bytes),
        config.media_hosts,
        config.wechat_download_timeout,
    )?;
    let cache = MediaCache::open(&config.database_path)?;

    let bot = BotService::new(
        telegram,
        resolver,
        downloader,
        cache,
        config.required_channel_id,
        config.telegram_hard_limit_bytes,
    );

    let shutdown = CancellationToken::new();
    let run = bot.run(shutdown.clone());
    tokio::pin!(run);
    tokio::select! {
        result = &mut run => result,
        signal = tokio::signal::ctrl_c() => {
            signal?;
            info!("收到退出信号");
            shutdown.cancel();
            run.await
        }
    }
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
