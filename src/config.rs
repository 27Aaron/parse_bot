use std::{
    collections::HashSet,
    env,
    path::{Path, PathBuf},
    time::Duration,
};

use crate::{AppError, Result, model::REVIEWED_WECHAT_MEDIA_HOSTS};

const DEFAULT_TDLIB_DATA_DIR: &str = "./data/tdlib";
const DEFAULT_MEDIA_MAX_BYTES: u64 = 2_000_000_000;
const DEFAULT_TELEGRAM_HARD_LIMIT: u64 = 2_000_000_000;

pub struct Config {
    pub telegram_api_id: i32,
    pub telegram_api_hash: String,
    pub telegram_bot_token: String,
    pub tdlib_database_dir: PathBuf,
    pub tdlib_files_dir: PathBuf,
    pub required_channel_id: Option<String>,
    pub wechat_yuanbao_cookie: String,
    pub wechat_resolve_timeout: Duration,
    pub wechat_download_timeout: Duration,
    pub media_shared_dir: PathBuf,
    pub media_max_source_bytes: u64,
    pub telegram_hard_limit_bytes: u64,
    pub database_path: PathBuf,
    pub media_hosts: HashSet<String>,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        let telegram_api_id = parse_telegram_api_id(&required("TELEGRAM_API_ID")?)?;
        let telegram_api_hash = parse_telegram_api_hash(required("TELEGRAM_API_HASH")?)?;
        let telegram_bot_token = required("TELEGRAM_BOT_TOKEN")?;
        let tdlib_data_dir = env::var_os("TDLIB_DATA_DIR")
            .filter(|value| !value.is_empty())
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(DEFAULT_TDLIB_DATA_DIR));
        let (tdlib_database_dir, tdlib_files_dir) = tdlib_directories(&tdlib_data_dir);

        let required_channel_id = match env::var("REQUIRED_CHANNEL_ID") {
            Ok(value) => parse_required_channel_id(Some(&value))?,
            Err(env::VarError::NotPresent) => None,
            Err(env::VarError::NotUnicode(_)) => {
                return Err(AppError::Config(
                    "REQUIRED_CHANNEL_ID 必须是有效的 UTF-8 文本".into(),
                ));
            }
        };

        let wechat_yuanbao_cookie = required("WECHAT_YUANBAO_COOKIE")?;
        let media_shared_dir = env::var_os("MEDIA_SHARED_DIR")
            .filter(|value| !value.is_empty())
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("./data/media"));

        let media_max_source_bytes = parse_u64("MEDIA_MAX_SOURCE_BYTES", DEFAULT_MEDIA_MAX_BYTES)?;
        let telegram_hard_limit_bytes =
            parse_u64("TELEGRAM_HARD_LIMIT_BYTES", DEFAULT_TELEGRAM_HARD_LIMIT)?;
        if telegram_hard_limit_bytes > 2_000_000_000 {
            return Err(AppError::Config(
                "TELEGRAM_HARD_LIMIT_BYTES 不能超过官方的 2000000000 字节".into(),
            ));
        }
        let database_path = parse_database_path(
            &env::var("DATABASE_URL").unwrap_or_else(|_| "sqlite:./data/state.db".into()),
        )?;

        let media_hosts = env::var("WECHAT_MEDIA_HOSTS")
            .unwrap_or_else(|_| REVIEWED_WECHAT_MEDIA_HOSTS.join(","))
            .split(',')
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(|value| value.to_ascii_lowercase())
            .collect::<HashSet<_>>();
        if media_hosts.is_empty() {
            return Err(AppError::Config("WECHAT_MEDIA_HOSTS 不能为空".into()));
        }

        let wechat_resolve_timeout_secs = parse_u64("WECHAT_RESOLVE_TIMEOUT_SECS", 30)?;
        let wechat_download_timeout_secs = parse_u64("WECHAT_DOWNLOAD_TIMEOUT_SECS", 7_200)?;
        if wechat_resolve_timeout_secs > 300 {
            return Err(AppError::Config(
                "WECHAT_RESOLVE_TIMEOUT_SECS 不能超过 300".into(),
            ));
        }
        if wechat_download_timeout_secs > 86_400 {
            return Err(AppError::Config(
                "WECHAT_DOWNLOAD_TIMEOUT_SECS 不能超过 86400".into(),
            ));
        }
        Ok(Self {
            telegram_api_id,
            telegram_api_hash,
            telegram_bot_token,
            tdlib_database_dir,
            tdlib_files_dir,
            required_channel_id,
            wechat_yuanbao_cookie,
            wechat_resolve_timeout: Duration::from_secs(wechat_resolve_timeout_secs),
            wechat_download_timeout: Duration::from_secs(wechat_download_timeout_secs),
            media_shared_dir,
            media_max_source_bytes,
            telegram_hard_limit_bytes,
            database_path,
            media_hosts,
        })
    }

    pub fn prepare_paths(&mut self) -> Result<()> {
        prepare_directory(&mut self.media_shared_dir)?;
        prepare_directory(&mut self.tdlib_database_dir)?;
        prepare_directory(&mut self.tdlib_files_dir)?;

        if let Some(parent) = self
            .database_path
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
        {
            std::fs::create_dir_all(parent)?;
        }
        Ok(())
    }
}

fn parse_telegram_api_id(value: &str) -> Result<i32> {
    value
        .trim()
        .parse::<i32>()
        .ok()
        .filter(|value| *value > 0)
        .ok_or_else(|| AppError::Config("TELEGRAM_API_ID 必须是大于 0 的整数".into()))
}

fn parse_telegram_api_hash(value: String) -> Result<String> {
    let value = value.trim();
    if value.len() != 32 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(AppError::Config(
            "TELEGRAM_API_HASH 必须是 32 位十六进制字符串".into(),
        ));
    }
    Ok(value.to_owned())
}

fn tdlib_directories(data_dir: &Path) -> (PathBuf, PathBuf) {
    (data_dir.join("database"), data_dir.join("files"))
}

fn prepare_directory(path: &mut PathBuf) -> Result<()> {
    std::fs::create_dir_all(&*path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        std::fs::set_permissions(&*path, std::fs::Permissions::from_mode(0o700))?;
    }
    *path = path
        .canonicalize()
        .map_err(|_| AppError::Storage(path.clone()))?;
    Ok(())
}

fn required(name: &str) -> Result<String> {
    env::var(name)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| AppError::Config(format!("缺少环境变量 {name}")))
}

fn parse_required_channel_id(value: Option<&str>) -> Result<Option<String>> {
    const MIN_USERNAME_LEN: usize = 5;
    const MAX_USERNAME_LEN: usize = 32;

    let Some(value) = value.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(None);
    };
    let Some(username) = value.strip_prefix('@') else {
        return Err(AppError::Config(
            "REQUIRED_CHANNEL_ID 必须是以 @ 开头的公开频道用户名".into(),
        ));
    };
    if !(MIN_USERNAME_LEN..=MAX_USERNAME_LEN).contains(&username.len())
        || !username
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
    {
        return Err(AppError::Config(
            "REQUIRED_CHANNEL_ID 的 @ 后必须是 5-32 个 ASCII 字母、数字或下划线".into(),
        ));
    }
    Ok(Some(value.to_owned()))
}

fn parse_u64(name: &str, default: u64) -> Result<u64> {
    match env::var(name) {
        Ok(value) if !value.trim().is_empty() => value
            .trim()
            .parse::<u64>()
            .map_err(|_| AppError::Config(format!("{name} 必须是正整数")))
            .and_then(|value| {
                (value > 0)
                    .then_some(value)
                    .ok_or_else(|| AppError::Config(format!("{name} 必须大于 0")))
            }),
        _ => Ok(default),
    }
}

fn parse_database_path(value: &str) -> Result<PathBuf> {
    let path = value
        .strip_prefix("sqlite:")
        .ok_or_else(|| AppError::Config("DATABASE_URL 当前只支持 sqlite: 路径".into()))?;
    if path.is_empty() || path == ":memory:" {
        return Err(AppError::Config(
            "DATABASE_URL 必须指向持久化 SQLite 文件".into(),
        ));
    }
    Ok(Path::new(path).to_path_buf())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_sqlite_path() {
        assert_eq!(
            parse_database_path("sqlite:./data/state.db").unwrap(),
            PathBuf::from("./data/state.db")
        );
    }

    #[test]
    fn rejects_non_sqlite_database() {
        assert!(parse_database_path("postgres://localhost/db").is_err());
    }

    #[test]
    fn parses_positive_telegram_api_id() {
        assert_eq!(parse_telegram_api_id(" 123456 ").unwrap(), 123_456);
    }

    #[test]
    fn rejects_invalid_telegram_api_id() {
        for value in ["", "0", "-1", "not-a-number", "2147483648"] {
            assert!(
                parse_telegram_api_id(value).is_err(),
                "unexpectedly accepted {value}"
            );
        }
    }

    #[test]
    fn validates_telegram_api_hash() {
        assert_eq!(
            parse_telegram_api_hash(" 0123456789abcdef0123456789ABCDEF ".into()).unwrap(),
            "0123456789abcdef0123456789ABCDEF"
        );
        for value in [
            "0123456789abcdef",
            "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
            "0123456789abcdef0123456789abcdef00",
        ] {
            assert!(parse_telegram_api_hash(value.into()).is_err());
        }
    }

    #[test]
    fn derives_separate_tdlib_directories() {
        assert_eq!(
            tdlib_directories(Path::new("./data/tdlib")),
            (
                PathBuf::from("./data/tdlib/database"),
                PathBuf::from("./data/tdlib/files")
            )
        );
    }

    #[test]
    fn prepares_and_canonicalizes_tdlib_directories() {
        let root =
            std::env::temp_dir().join(format!("parse-bot-config-test-{}", uuid::Uuid::new_v4()));
        let data_dir = root.join("parent").join("..").join("tdlib");
        let (mut database_dir, mut files_dir) = tdlib_directories(&data_dir);

        prepare_directory(&mut database_dir).unwrap();
        prepare_directory(&mut files_dir).unwrap();

        assert_eq!(
            database_dir,
            root.join("tdlib/database").canonicalize().unwrap()
        );
        assert_eq!(files_dir, root.join("tdlib/files").canonicalize().unwrap());

        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn missing_or_empty_required_channel_is_disabled() {
        assert_eq!(parse_required_channel_id(None).unwrap(), None);
        assert_eq!(parse_required_channel_id(Some("")).unwrap(), None);
        assert_eq!(parse_required_channel_id(Some("   ")).unwrap(), None);
    }

    #[test]
    fn accepts_public_channel_username() {
        assert_eq!(
            parse_required_channel_id(Some(" @Aaron_Channels ")).unwrap(),
            Some("@Aaron_Channels".into())
        );
    }

    #[test]
    fn rejects_invalid_public_channel_username() {
        for value in [
            "Aaron_Channels",
            "@abcd",
            "@abcdefghijklmnopqrstuvwxyz1234567",
            "@Aaron-Channels",
            "@频道Aaron",
        ] {
            assert!(
                parse_required_channel_id(Some(value)).is_err(),
                "unexpectedly accepted {value}"
            );
        }
    }
}
