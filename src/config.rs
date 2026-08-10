use std::{
    env,
    ffi::OsStr,
    path::{Path, PathBuf},
};

use crate::{AppError, Result};

const DEFAULT_DATA_DIR: &str = "./data";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DataPaths {
    pub root: PathBuf,
    pub state_database: PathBuf,
    pub media: PathBuf,
    pub tdlib_database: PathBuf,
    pub tdlib_files: PathBuf,
}

impl DataPaths {
    fn derive(root: PathBuf) -> Self {
        Self {
            state_database: root.join("state.db"),
            media: root.join("media"),
            tdlib_database: root.join("tdlib").join("database"),
            tdlib_files: root.join("tdlib").join("files"),
            root,
        }
    }

    fn prepare(configured_root: PathBuf) -> Result<Self> {
        let root = prepare_data_root(&configured_root)?;
        let mut paths = Self::derive(root.clone());

        // Prepare the common TDLib parent first so an escaping symlink is
        // rejected before either of its children can be created through it.
        prepare_subdirectory(&root, &root.join("tdlib"))?;
        paths.media = prepare_subdirectory(&root, &paths.media)?;
        paths.tdlib_database = prepare_subdirectory(&root, &paths.tdlib_database)?;
        paths.tdlib_files = prepare_subdirectory(&root, &paths.tdlib_files)?;
        paths.state_database = prepare_database_path(&root, &paths.state_database)?;
        validate_distinct_paths(&paths)?;

        Ok(paths)
    }
}

pub struct Config {
    pub telegram_api_id: i32,
    pub telegram_api_hash: String,
    pub telegram_bot_token: String,
    pub data_paths: DataPaths,
    pub required_channel_id: Option<String>,
    pub wechat_yuanbao_cookie: String,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        let telegram_api_id = parse_telegram_api_id(&required("TELEGRAM_API_ID")?)?;
        let telegram_api_hash = parse_telegram_api_hash(required("TELEGRAM_API_HASH")?)?;
        let telegram_bot_token = required("TELEGRAM_BOT_TOKEN")?;
        let data_dir = env::var_os("DATA_DIR");
        let data_dir = parse_data_dir(data_dir.as_deref())?;

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
        let data_paths = DataPaths::prepare(data_dir)?;
        Ok(Self {
            telegram_api_id,
            telegram_api_hash,
            telegram_bot_token,
            data_paths,
            required_channel_id,
            wechat_yuanbao_cookie,
        })
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

fn parse_data_dir(value: Option<&OsStr>) -> Result<PathBuf> {
    let Some(value) = value else {
        return Ok(PathBuf::from(DEFAULT_DATA_DIR));
    };
    if value.is_empty() || value.to_str().is_some_and(|value| value.trim().is_empty()) {
        return Err(AppError::Config("DATA_DIR 不能为空".into()));
    }
    Ok(PathBuf::from(value))
}

fn prepare_data_root(configured_root: &Path) -> Result<PathBuf> {
    std::fs::create_dir_all(configured_root)?;
    if !std::fs::metadata(configured_root)?.is_dir() {
        return Err(AppError::Config("DATA_DIR 必须指向目录".into()));
    }

    let root = configured_root
        .canonicalize()
        .map_err(|_| AppError::Storage(configured_root.to_path_buf()))?;
    if root.parent().is_none() {
        return Err(AppError::Config("DATA_DIR 不能是文件系统根目录".into()));
    }
    set_private_directory_permissions(&root)?;
    Ok(root)
}

fn prepare_subdirectory(root: &Path, path: &Path) -> Result<PathBuf> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            return Err(AppError::Config(format!(
                "数据目录不能是符号链接：{}",
                path.display()
            )));
        }
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    std::fs::create_dir_all(path)?;
    if !std::fs::metadata(path)?.is_dir() {
        return Err(AppError::Config(format!(
            "数据路径必须指向目录：{}",
            path.display()
        )));
    }

    let canonical = path
        .canonicalize()
        .map_err(|_| AppError::Storage(path.to_path_buf()))?;
    ensure_strictly_within(root, &canonical)?;
    set_private_directory_permissions(&canonical)?;
    Ok(canonical)
}

fn prepare_database_path(root: &Path, path: &Path) -> Result<PathBuf> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) => {
            if metadata.file_type().is_symlink() {
                return Err(AppError::Config(format!(
                    "SQLite 数据文件不能是符号链接：{}",
                    path.display()
                )));
            }
            let canonical = path
                .canonicalize()
                .map_err(|_| AppError::Storage(path.to_path_buf()))?;
            ensure_strictly_within(root, &canonical)?;
            if !std::fs::metadata(&canonical)?.is_file() {
                return Err(AppError::Config(format!(
                    "SQLite 数据路径必须指向文件：{}",
                    path.display()
                )));
            }
            set_private_file_permissions(&canonical)?;
            Ok(canonical)
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            create_private_database_file(path)?;
            Ok(path.to_path_buf())
        }
        Err(error) => Err(error.into()),
    }
}

fn create_private_database_file(path: &Path) -> Result<()> {
    let mut options = std::fs::OpenOptions::new();
    options.read(true).write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;

        options.mode(0o600);
    }
    options.open(path)?;
    Ok(())
}

fn ensure_strictly_within(root: &Path, path: &Path) -> Result<()> {
    if path == root || !path.starts_with(root) {
        return Err(AppError::Config(format!(
            "数据子路径不能位于 DATA_DIR 之外：{}",
            path.display()
        )));
    }
    Ok(())
}

fn validate_distinct_paths(paths: &DataPaths) -> Result<()> {
    let values = [
        &paths.state_database,
        &paths.media,
        &paths.tdlib_database,
        &paths.tdlib_files,
    ];
    for (index, left) in values.iter().enumerate() {
        for right in &values[index + 1..] {
            if left == right || left.starts_with(right) || right.starts_with(left) {
                return Err(AppError::Config("DATA_DIR 派生的数据路径不能重叠".into()));
            }
        }
    }
    Ok(())
}

fn set_private_directory_permissions(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))?;
    }
    #[cfg(not(unix))]
    let _ = path;
    Ok(())
}

fn set_private_file_permissions(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    }
    #[cfg(not(unix))]
    let _ = path;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_to_the_application_data_root() {
        assert_eq!(parse_data_dir(None).unwrap(), PathBuf::from("./data"));
    }

    #[test]
    fn rejects_an_explicitly_empty_data_root() {
        assert!(parse_data_dir(Some(OsStr::new(""))).is_err());
        assert!(parse_data_dir(Some(OsStr::new("   "))).is_err());
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
    fn derives_every_data_path_from_one_root() {
        let paths = DataPaths::derive(PathBuf::from("configured-data"));

        assert_eq!(paths.root, PathBuf::from("configured-data"));
        assert_eq!(
            paths.state_database,
            PathBuf::from("configured-data/state.db")
        );
        assert_eq!(paths.media, PathBuf::from("configured-data/media"));
        assert_eq!(
            paths.tdlib_database,
            PathBuf::from("configured-data/tdlib/database")
        );
        assert_eq!(
            paths.tdlib_files,
            PathBuf::from("configured-data/tdlib/files")
        );
    }

    #[test]
    fn prepares_and_canonicalizes_all_data_directories() {
        let sandbox = unique_test_path("prepare");
        let configured = sandbox.join("parent").join("..").join("data");
        let paths = DataPaths::prepare(configured).unwrap();
        let root = sandbox.join("data").canonicalize().unwrap();

        assert_eq!(paths, DataPaths::derive(root.clone()));
        for directory in [
            root.clone(),
            root.join("media"),
            root.join("tdlib"),
            root.join("tdlib/database"),
            root.join("tdlib/files"),
        ] {
            assert!(directory.is_dir(), "missing {}", directory.display());
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;

                assert_eq!(
                    directory.metadata().unwrap().permissions().mode() & 0o777,
                    0o700
                );
            }
        }
        assert!(paths.state_database.is_file());
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            assert_eq!(
                paths
                    .state_database
                    .metadata()
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o777,
                0o600
            );
        }

        std::fs::remove_dir_all(sandbox).unwrap();
    }

    #[test]
    fn preparing_twice_preserves_the_sqlite_database() {
        let sandbox = unique_test_path("idempotent");
        let configured = sandbox.join("data");
        let first = DataPaths::prepare(configured.clone()).unwrap();
        let connection = rusqlite::Connection::open(&first.state_database).unwrap();
        connection
            .execute_batch("CREATE TABLE marker (value TEXT); INSERT INTO marker VALUES ('ok');")
            .unwrap();
        drop(connection);

        let second = DataPaths::prepare(configured).unwrap();
        let connection = rusqlite::Connection::open(&second.state_database).unwrap();
        let marker: String = connection
            .query_row("SELECT value FROM marker", [], |row| row.get(0))
            .unwrap();

        assert_eq!(first, second);
        assert_eq!(marker, "ok");
        drop(connection);
        std::fs::remove_dir_all(sandbox).unwrap();
    }

    #[test]
    fn rejects_a_data_root_that_is_not_a_directory() {
        let sandbox = unique_test_path("file-root");
        std::fs::create_dir_all(&sandbox).unwrap();
        let file = sandbox.join("data");
        std::fs::write(&file, b"not a directory").unwrap();

        assert!(DataPaths::prepare(file).is_err());
        std::fs::remove_dir_all(sandbox).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn rejects_the_filesystem_root_as_data_root() {
        assert!(DataPaths::prepare(PathBuf::from("/")).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn rejects_a_subdirectory_symlink_that_escapes_the_data_root() {
        use std::os::unix::fs::symlink;

        let sandbox = unique_test_path("escaping-directory");
        let root = sandbox.join("data");
        let outside = sandbox.join("outside");
        std::fs::create_dir_all(&root).unwrap();
        std::fs::create_dir_all(&outside).unwrap();
        symlink(&outside, root.join("media")).unwrap();

        assert!(DataPaths::prepare(root).is_err());
        std::fs::remove_dir_all(sandbox).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn rejects_a_managed_subdirectory_symlink_within_the_data_root() {
        use std::os::unix::fs::symlink;

        let sandbox = unique_test_path("internal-directory-symlink");
        let root = sandbox.join("data");
        let alternate = root.join("alternate-media");
        std::fs::create_dir_all(&alternate).unwrap();
        symlink(&alternate, root.join("media")).unwrap();

        assert!(DataPaths::prepare(root).is_err());
        std::fs::remove_dir_all(sandbox).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn rejects_a_database_symlink_that_escapes_the_data_root() {
        use std::os::unix::fs::symlink;

        let sandbox = unique_test_path("escaping-database");
        let root = sandbox.join("data");
        let outside = sandbox.join("outside.db");
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(&outside, b"outside").unwrap();
        symlink(&outside, root.join("state.db")).unwrap();

        assert!(DataPaths::prepare(root).is_err());
        std::fs::remove_dir_all(sandbox).unwrap();
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

    fn unique_test_path(label: &str) -> PathBuf {
        std::env::temp_dir().join(format!("parse-bot-config-{label}-{}", uuid::Uuid::new_v4()))
    }
}
