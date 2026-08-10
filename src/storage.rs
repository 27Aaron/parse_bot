use std::{path::Path, str::FromStr, sync::Arc};

use chrono::{DateTime, TimeDelta, Utc};
use parking_lot::Mutex;
use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};

use crate::{AppError, Result, i18n::Language, model::TelegramMediaKind};

const CACHE_SCHEMA_VERSION: i64 = 7;
const VIDEO_VARIANT: &str = "video";
const CACHE_MAX_ENTRIES: usize = 10_000;
const CACHE_MAX_AGE_DAYS: i64 = 180;

#[derive(Clone)]
pub struct MediaCache {
    connection: Arc<Mutex<Connection>>,
}

#[derive(Debug, Clone)]
pub struct CachedTelegramMedia {
    pub file_id: String,
    pub file_unique_id: Option<String>,
    pub kind: TelegramMediaKind,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UserSettings {
    pub language: Language,
    pub show_source: bool,
    pub show_progress: bool,
    pub reply_to_source: bool,
    pub show_video_cover: bool,
}

impl Default for UserSettings {
    fn default() -> Self {
        Self {
            language: Language::Chinese,
            show_source: true,
            show_progress: true,
            reply_to_source: true,
            show_video_cover: true,
        }
    }
}

impl MediaCache {
    pub fn open(path: &Path) -> Result<Self> {
        let connection = Connection::open(path).map_err(db_error)?;
        initialize_connection(&connection)?;

        Ok(Self {
            connection: Arc::new(Mutex::new(connection)),
        })
    }

    pub async fn get(&self, platform: &str, post_id: &str) -> Result<Option<CachedTelegramMedia>> {
        let cache = self.clone();
        let platform = platform.to_owned();
        let post_id = post_id.to_owned();
        tokio::task::spawn_blocking(move || cache.get_sync(&platform, &post_id))
            .await
            .map_err(|_| AppError::Database("SQLite 读取任务异常退出".into()))?
    }

    fn get_sync(&self, platform: &str, post_id: &str) -> Result<Option<CachedTelegramMedia>> {
        let connection = self.connection.lock();
        let cutoff = cache_cutoff(CACHE_MAX_AGE_DAYS)?;
        let row = connection
            .query_row(
                "SELECT file_id, file_unique_id, media_kind, created_at
                 FROM telegram_media_cache
                 WHERE platform = ?1 AND post_id = ?2 AND variant = ?3
                   AND julianday(last_used_at) >= julianday(?4)",
                params![platform, post_id, VIDEO_VARIANT, cutoff],
                |row| {
                    let kind = TelegramMediaKind::from_str(row.get::<_, String>(2)?.as_str())
                        .map_err(|_| {
                            rusqlite::Error::InvalidColumnType(
                                2,
                                "media_kind".into(),
                                rusqlite::types::Type::Text,
                            )
                        })?;
                    let created = row.get::<_, String>(3)?;
                    let created_at = DateTime::parse_from_rfc3339(&created)
                        .map_err(|error| {
                            rusqlite::Error::FromSqlConversionFailure(
                                3,
                                rusqlite::types::Type::Text,
                                Box::new(error),
                            )
                        })?
                        .with_timezone(&Utc);
                    Ok(CachedTelegramMedia {
                        file_id: row.get(0)?,
                        file_unique_id: row.get(1)?,
                        kind,
                        created_at,
                    })
                },
            )
            .optional()
            .map_err(db_error)?;

        if row.is_some() {
            connection
                .execute(
                    "UPDATE telegram_media_cache SET last_used_at = ?4
                     WHERE platform = ?1 AND post_id = ?2 AND variant = ?3",
                    params![platform, post_id, VIDEO_VARIANT, Utc::now().to_rfc3339()],
                )
                .map_err(db_error)?;
        } else {
            // Remove an expired or malformed matching row so a later read
            // cannot revive it by refreshing `last_used_at`.
            connection
                .execute(
                    "DELETE FROM telegram_media_cache
                     WHERE platform = ?1 AND post_id = ?2 AND variant = ?3
                       AND (julianday(last_used_at) IS NULL
                            OR julianday(last_used_at) < julianday(?4))",
                    params![platform, post_id, VIDEO_VARIANT, cutoff],
                )
                .map_err(db_error)?;
        }
        Ok(row)
    }

    pub async fn put(
        &self,
        platform: &str,
        post_id: &str,
        kind: TelegramMediaKind,
        file_id: &str,
        file_unique_id: Option<&str>,
    ) -> Result<()> {
        let cache = self.clone();
        let platform = platform.to_owned();
        let post_id = post_id.to_owned();
        let file_id = file_id.to_owned();
        let file_unique_id = file_unique_id.map(str::to_owned);
        tokio::task::spawn_blocking(move || {
            cache.put_sync(
                &platform,
                &post_id,
                kind,
                &file_id,
                file_unique_id.as_deref(),
            )
        })
        .await
        .map_err(|_| AppError::Database("SQLite 写入任务异常退出".into()))?
    }

    fn put_sync(
        &self,
        platform: &str,
        post_id: &str,
        kind: TelegramMediaKind,
        file_id: &str,
        file_unique_id: Option<&str>,
    ) -> Result<()> {
        let now = Utc::now().to_rfc3339();
        let connection = self.connection.lock();
        connection
            .execute(
                "INSERT INTO telegram_media_cache (
                     platform, post_id, variant, media_kind,
                     file_id, file_unique_id, created_at, last_used_at
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?7)
                 ON CONFLICT(platform, post_id, variant) DO UPDATE SET
                     media_kind = excluded.media_kind,
                     file_id = excluded.file_id,
                     file_unique_id = excluded.file_unique_id,
                     last_used_at = excluded.last_used_at",
                params![
                    platform,
                    post_id,
                    VIDEO_VARIANT,
                    kind.as_str(),
                    file_id,
                    file_unique_id,
                    now
                ],
            )
            .map_err(db_error)?;
        prune_cache(&connection, CACHE_MAX_ENTRIES, CACHE_MAX_AGE_DAYS)?;
        Ok(())
    }

    pub async fn remove(&self, platform: &str, post_id: &str) -> Result<()> {
        let cache = self.clone();
        let platform = platform.to_owned();
        let post_id = post_id.to_owned();
        tokio::task::spawn_blocking(move || cache.remove_sync(&platform, &post_id))
            .await
            .map_err(|_| AppError::Database("SQLite 删除任务异常退出".into()))?
    }

    fn remove_sync(&self, platform: &str, post_id: &str) -> Result<()> {
        self.connection
            .lock()
            .execute(
                "DELETE FROM telegram_media_cache
                 WHERE platform = ?1 AND post_id = ?2 AND variant = ?3",
                params![platform, post_id, VIDEO_VARIANT],
            )
            .map_err(db_error)?;
        Ok(())
    }

    pub async fn remove_if_file_id(
        &self,
        platform: &str,
        post_id: &str,
        file_id: &str,
    ) -> Result<bool> {
        let cache = self.clone();
        let platform = platform.to_owned();
        let post_id = post_id.to_owned();
        let file_id = file_id.to_owned();
        tokio::task::spawn_blocking(move || {
            cache.remove_if_file_id_sync(&platform, &post_id, &file_id)
        })
        .await
        .map_err(|_| AppError::Database("SQLite 条件删除任务异常退出".into()))?
    }

    fn remove_if_file_id_sync(&self, platform: &str, post_id: &str, file_id: &str) -> Result<bool> {
        let deleted = self
            .connection
            .lock()
            .execute(
                "DELETE FROM telegram_media_cache
                 WHERE platform = ?1 AND post_id = ?2 AND variant = ?3 AND file_id = ?4",
                params![platform, post_id, VIDEO_VARIANT, file_id],
            )
            .map_err(db_error)?;
        Ok(deleted > 0)
    }

    pub async fn get_user_settings_with_default(
        &self,
        user_id: u64,
        default_language: Language,
    ) -> Result<UserSettings> {
        let cache = self.clone();
        tokio::task::spawn_blocking(move || cache.get_user_settings_sync(user_id, default_language))
            .await
            .map_err(|_| AppError::Database("SQLite 读取用户设置任务异常退出".into()))?
    }

    fn get_user_settings_sync(
        &self,
        user_id: u64,
        default_language: Language,
    ) -> Result<UserSettings> {
        let user_id = i64::try_from(user_id)
            .map_err(|_| AppError::Database("Telegram 用户 ID 超出 SQLite 支持范围".into()))?;
        let connection = self.connection.lock();
        let row = connection
            .query_row(
                "SELECT language, show_source, show_progress,
                        reply_to_source, show_video_cover
                 FROM telegram_user_settings WHERE user_id = ?1",
                params![user_id],
                |row| {
                    let language = row.get::<_, String>(0)?;
                    Ok(UserSettings {
                        language: Language::from_code(&language).unwrap_or_default(),
                        show_source: row.get::<_, i64>(1)? != 0,
                        show_progress: row.get::<_, i64>(2)? != 0,
                        reply_to_source: row.get::<_, i64>(3)? != 0,
                        show_video_cover: row.get::<_, i64>(4)? != 0,
                    })
                },
            )
            .optional()
            .map_err(db_error)?;
        Ok(row.unwrap_or(UserSettings {
            language: default_language,
            ..UserSettings::default()
        }))
    }

    pub async fn put_user_settings(&self, user_id: u64, settings: UserSettings) -> Result<()> {
        let cache = self.clone();
        tokio::task::spawn_blocking(move || cache.put_user_settings_sync(user_id, settings))
            .await
            .map_err(|_| AppError::Database("SQLite 写入用户设置任务异常退出".into()))?
    }

    fn put_user_settings_sync(&self, user_id: u64, settings: UserSettings) -> Result<()> {
        let user_id = i64::try_from(user_id)
            .map_err(|_| AppError::Database("Telegram 用户 ID 超出 SQLite 支持范围".into()))?;
        self.connection
            .lock()
            .execute(
                "INSERT INTO telegram_user_settings (
                     user_id, language, show_source, show_progress,
                     reply_to_source, show_video_cover, updated_at
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
                 ON CONFLICT(user_id) DO UPDATE SET
                     language = excluded.language,
                     show_source = excluded.show_source,
                     show_progress = excluded.show_progress,
                     reply_to_source = excluded.reply_to_source,
                     show_video_cover = excluded.show_video_cover,
                     updated_at = excluded.updated_at",
                params![
                    user_id,
                    settings.language.code(),
                    if settings.show_source { 1_i64 } else { 0_i64 },
                    if settings.show_progress { 1_i64 } else { 0_i64 },
                    if settings.reply_to_source {
                        1_i64
                    } else {
                        0_i64
                    },
                    if settings.show_video_cover {
                        1_i64
                    } else {
                        0_i64
                    },
                    Utc::now().to_rfc3339(),
                ],
            )
            .map_err(db_error)?;
        Ok(())
    }
}

fn initialize_connection(connection: &Connection) -> Result<()> {
    connection
        .execute_batch(
            "PRAGMA journal_mode = WAL;
             PRAGMA foreign_keys = ON;
             PRAGMA busy_timeout = 5000;
             CREATE TABLE IF NOT EXISTS telegram_media_cache (
                 platform TEXT NOT NULL,
                 post_id TEXT NOT NULL,
                 variant TEXT NOT NULL,
                 media_kind TEXT NOT NULL,
                 file_id TEXT NOT NULL,
                 file_unique_id TEXT,
                 created_at TEXT NOT NULL,
                 last_used_at TEXT NOT NULL,
                 PRIMARY KEY (platform, post_id, variant)
             );
             CREATE INDEX IF NOT EXISTS telegram_media_cache_last_used_idx
                 ON telegram_media_cache(last_used_at);
             CREATE TABLE IF NOT EXISTS telegram_user_settings (
                 user_id INTEGER PRIMARY KEY,
                 language TEXT NOT NULL,
                 show_source INTEGER NOT NULL CHECK (show_source IN (0, 1)),
                 show_progress INTEGER NOT NULL CHECK (show_progress IN (0, 1)),
                 reply_to_source INTEGER NOT NULL DEFAULT 1 CHECK (reply_to_source IN (0, 1)),
                 show_video_cover INTEGER NOT NULL DEFAULT 1 CHECK (show_video_cover IN (0, 1)),
                 updated_at TEXT NOT NULL
             );",
        )
        .map_err(db_error)?;

    let transaction =
        Transaction::new_unchecked(connection, TransactionBehavior::Immediate).map_err(db_error)?;
    let schema_version = transaction
        .query_row("PRAGMA user_version", [], |row| row.get::<_, i64>(0))
        .map_err(db_error)?;
    if schema_version > CACHE_SCHEMA_VERSION {
        return Err(AppError::Database(format!(
            "数据库版本 {schema_version} 高于程序支持的版本 {CACHE_SCHEMA_VERSION}"
        )));
    }
    if schema_version < 3 {
        // Versions before 3 did not store reliable preview metadata. Invalidate
        // the former single-quality cache once so Telegram can rebuild it.
        transaction
            .execute(
                "DELETE FROM telegram_media_cache
                 WHERE variant = 'original'",
                [],
            )
            .map_err(db_error)?;
    }
    if schema_version < 4 {
        // Version 4 removed the former low-quality cache.
        transaction
            .execute(
                "DELETE FROM telegram_media_cache
                 WHERE variant <> 'original'",
                [],
            )
            .map_err(db_error)?;
    }
    if schema_version < 5 {
        // Version 5 renames the remaining single-video cache key. Drop any
        // pre-release rows already using the new key before moving the trusted
        // version-4 rows, so the migration cannot hit a primary-key conflict.
        transaction
            .execute(
                "DELETE FROM telegram_media_cache WHERE variant = ?1",
                params![VIDEO_VARIANT],
            )
            .map_err(db_error)?;
        transaction
            .execute(
                "UPDATE telegram_media_cache
                 SET variant = ?1
                 WHERE variant = 'original'",
                params![VIDEO_VARIANT],
            )
            .map_err(db_error)?;
        transaction
            .execute(
                "DELETE FROM telegram_media_cache WHERE variant <> ?1",
                params![VIDEO_VARIANT],
            )
            .map_err(db_error)?;
        transaction
            .pragma_update(None, "user_version", 5)
            .map_err(db_error)?;
    }
    if schema_version < CACHE_SCHEMA_VERSION {
        ensure_user_settings_column(
            &transaction,
            "reply_to_source",
            "reply_to_source INTEGER NOT NULL DEFAULT 1 CHECK (reply_to_source IN (0, 1))",
        )?;
        ensure_user_settings_column(
            &transaction,
            "show_video_cover",
            "show_video_cover INTEGER NOT NULL DEFAULT 1 CHECK (show_video_cover IN (0, 1))",
        )?;
        transaction
            .pragma_update(None, "user_version", CACHE_SCHEMA_VERSION)
            .map_err(db_error)?;
    }
    prune_cache(&transaction, CACHE_MAX_ENTRIES, CACHE_MAX_AGE_DAYS)?;
    transaction.commit().map_err(db_error)?;
    Ok(())
}

fn ensure_user_settings_column(
    transaction: &Transaction<'_>,
    column_name: &str,
    definition: &str,
) -> Result<()> {
    let exists = transaction
        .prepare("SELECT 1 FROM pragma_table_info('telegram_user_settings') WHERE name = ?1")
        .map_err(db_error)?
        .query_row(params![column_name], |_| Ok(()))
        .optional()
        .map_err(db_error)?
        .is_some();
    if !exists {
        transaction
            .execute(
                &format!("ALTER TABLE telegram_user_settings ADD COLUMN {definition}"),
                [],
            )
            .map_err(db_error)?;
    }
    Ok(())
}

fn prune_cache(connection: &Connection, max_entries: usize, max_age_days: i64) -> Result<()> {
    let cutoff = cache_cutoff(max_age_days)?;
    connection
        .execute(
            "DELETE FROM telegram_media_cache
             WHERE julianday(last_used_at) IS NULL
                OR julianday(last_used_at) < julianday(?1)",
            params![cutoff],
        )
        .map_err(db_error)?;

    let offset = i64::try_from(max_entries)
        .map_err(|_| AppError::Database("缓存条目上限超出支持范围".into()))?;
    connection
        .execute(
            "DELETE FROM telegram_media_cache
             WHERE rowid IN (
                 SELECT rowid
                 FROM telegram_media_cache
                 ORDER BY last_used_at DESC, rowid DESC
                 LIMIT -1 OFFSET ?1
             )",
            params![offset],
        )
        .map_err(db_error)?;
    Ok(())
}

fn cache_cutoff(max_age_days: i64) -> Result<String> {
    Utc::now()
        .checked_sub_signed(TimeDelta::days(max_age_days))
        .map(|value| value.to_rfc3339())
        .ok_or_else(|| AppError::Database("缓存保留期限超出支持范围".into()))
}

fn db_error(error: rusqlite::Error) -> AppError {
    AppError::Database(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn cache_operations_only_target_current_video_variant() {
        let connection = Connection::open_in_memory().unwrap();
        initialize_connection(&connection).unwrap();
        insert_cache_row(&connection, "abc", "compatible", "low-file-id");
        let cache = MediaCache {
            connection: Arc::new(Mutex::new(connection)),
        };
        cache
            .put(
                "wechat_channels",
                "abc",
                TelegramMediaKind::Document,
                "video-file-id",
                Some("unique-id"),
            )
            .await
            .unwrap();

        let cached = cache.get("wechat_channels", "abc").await.unwrap().unwrap();
        assert_eq!(cached.file_id, "video-file-id");
        assert_eq!(cached.kind, TelegramMediaKind::Document);

        cache.remove("wechat_channels", "abc").await.unwrap();
        assert!(cache.get("wechat_channels", "abc").await.unwrap().is_none());
        let compatible_count: i64 = cache
            .connection
            .lock()
            .query_row(
                "SELECT COUNT(*) FROM telegram_media_cache
                 WHERE post_id = 'abc' AND variant = 'compatible'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(compatible_count, 1);
    }

    #[tokio::test]
    async fn stale_file_id_cannot_delete_a_newer_cache_entry() {
        let connection = Connection::open_in_memory().unwrap();
        initialize_connection(&connection).unwrap();
        let cache = MediaCache {
            connection: Arc::new(Mutex::new(connection)),
        };
        cache
            .put(
                "wechat_channels",
                "abc",
                TelegramMediaKind::Video,
                "new-file-id",
                Some("new-unique-id"),
            )
            .await
            .unwrap();

        assert!(
            !cache
                .remove_if_file_id("wechat_channels", "abc", "stale-file-id")
                .await
                .unwrap()
        );
        assert_eq!(
            cache
                .get("wechat_channels", "abc")
                .await
                .unwrap()
                .unwrap()
                .file_id,
            "new-file-id"
        );
        assert!(
            cache
                .remove_if_file_id("wechat_channels", "abc", "new-file-id")
                .await
                .unwrap()
        );
        assert!(cache.get("wechat_channels", "abc").await.unwrap().is_none());
    }

    #[test]
    fn direct_upgrade_from_before_v3_invalidates_all_legacy_variants() {
        let connection = Connection::open_in_memory().unwrap();
        initialize_connection(&connection).unwrap();
        connection.pragma_update(None, "user_version", 2).unwrap();
        insert_cache_row(&connection, "old-single", "original", "single-id");
        insert_cache_row(&connection, "old-compatible", "compatible", "low-id");

        initialize_connection(&connection).unwrap();
        assert!(cached_post_ids(&connection).is_empty());
        assert_eq!(schema_version(&connection), CACHE_SCHEMA_VERSION);
    }

    #[test]
    fn upgrade_from_v3_preserves_single_video_and_removes_low_quality_cache() {
        let connection = Connection::open_in_memory().unwrap();
        initialize_connection(&connection).unwrap();
        connection.pragma_update(None, "user_version", 3).unwrap();
        insert_cache_row(&connection, "valid-single", "original", "single-id");
        insert_cache_row(&connection, "old-compatible", "compatible", "low-id");

        initialize_connection(&connection).unwrap();
        assert_eq!(cached_post_ids(&connection), vec!["valid-single"]);
        assert_eq!(cached_variant(&connection, "valid-single"), "video");
        assert_eq!(schema_version(&connection), CACHE_SCHEMA_VERSION);

        initialize_connection(&connection).unwrap();
        assert_eq!(cached_post_ids(&connection), vec!["valid-single"]);
    }

    #[test]
    fn upgrade_from_v4_preserves_cached_media_fields_atomically() {
        let connection = Connection::open_in_memory().unwrap();
        initialize_connection(&connection).unwrap();
        connection.pragma_update(None, "user_version", 4).unwrap();
        let now = Utc::now().to_rfc3339();
        connection
            .execute(
                "INSERT INTO telegram_media_cache (
                     platform, post_id, variant, media_kind, file_id,
                     file_unique_id, created_at, last_used_at
                 ) VALUES ('wechat_channels', 'migrated', 'original', 'video',
                           'file-id', 'unique-id', ?1, ?1)",
                params![now],
            )
            .unwrap();

        initialize_connection(&connection).unwrap();
        let migrated: (String, String, String, Option<String>, String, String) = connection
            .query_row(
                "SELECT variant, media_kind, file_id, file_unique_id,
                        created_at, last_used_at
                 FROM telegram_media_cache WHERE post_id = 'migrated'",
                [],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                    ))
                },
            )
            .unwrap();
        assert_eq!(
            migrated,
            (
                "video".into(),
                "video".into(),
                "file-id".into(),
                Some("unique-id".into()),
                now.clone(),
                now,
            )
        );
    }

    #[test]
    fn rejects_a_database_created_by_a_newer_program() {
        let connection = Connection::open_in_memory().unwrap();
        initialize_connection(&connection).unwrap();
        connection
            .pragma_update(None, "user_version", CACHE_SCHEMA_VERSION + 1)
            .unwrap();

        assert!(matches!(
            initialize_connection(&connection),
            Err(AppError::Database(_))
        ));
    }

    #[tokio::test]
    async fn an_expired_entry_cannot_be_revived_by_a_read() {
        let connection = Connection::open_in_memory().unwrap();
        initialize_connection(&connection).unwrap();
        insert_cache_row_at(
            &connection,
            "expired",
            VIDEO_VARIANT,
            "expired-id",
            "2020-01-01T00:00:00Z",
        );
        let cache = MediaCache {
            connection: Arc::new(Mutex::new(connection)),
        };

        assert!(
            cache
                .get("wechat_channels", "expired")
                .await
                .unwrap()
                .is_none()
        );
        assert!(cached_post_ids(&cache.connection.lock()).is_empty());
    }

    #[tokio::test]
    async fn user_settings_default_and_round_trip() {
        let connection = Connection::open_in_memory().unwrap();
        initialize_connection(&connection).unwrap();
        let cache = MediaCache {
            connection: Arc::new(Mutex::new(connection)),
        };

        assert_eq!(
            cache
                .get_user_settings_with_default(42, Language::Chinese)
                .await
                .unwrap(),
            UserSettings::default()
        );
        assert_eq!(
            cache
                .get_user_settings_with_default(43, Language::Russian)
                .await
                .unwrap()
                .language,
            Language::Russian
        );
        let settings = UserSettings {
            language: Language::Japanese,
            show_source: false,
            show_progress: true,
            reply_to_source: false,
            show_video_cover: true,
        };
        cache.put_user_settings(42, settings).await.unwrap();
        assert_eq!(
            cache
                .get_user_settings_with_default(42, Language::Chinese)
                .await
                .unwrap(),
            settings
        );
    }

    #[test]
    fn pruning_removes_expired_and_least_recently_used_rows() {
        let connection = Connection::open_in_memory().unwrap();
        initialize_connection(&connection).unwrap();
        let older = (Utc::now() - TimeDelta::days(2)).to_rfc3339();
        let newer = (Utc::now() - TimeDelta::days(1)).to_rfc3339();
        insert_cache_row_at(
            &connection,
            "expired",
            VIDEO_VARIANT,
            "expired-id",
            "2020-01-01T00:00:00Z",
        );
        insert_cache_row_at(&connection, "older", VIDEO_VARIANT, "older-id", &older);
        insert_cache_row_at(&connection, "newer", VIDEO_VARIANT, "newer-id", &newer);

        prune_cache(&connection, 1, CACHE_MAX_AGE_DAYS).unwrap();
        assert_eq!(cached_post_ids(&connection), vec!["newer"]);
    }

    fn insert_cache_row(connection: &Connection, post_id: &str, variant: &str, file_id: &str) {
        let now = Utc::now().to_rfc3339();
        insert_cache_row_at(connection, post_id, variant, file_id, &now);
    }

    fn insert_cache_row_at(
        connection: &Connection,
        post_id: &str,
        variant: &str,
        file_id: &str,
        last_used_at: &str,
    ) {
        connection
            .execute(
                "INSERT INTO telegram_media_cache (
                     platform, post_id, variant, media_kind, file_id,
                     file_unique_id, created_at, last_used_at
                 ) VALUES ('wechat_channels', ?1, ?2, 'document', ?3, NULL,
                           ?4, ?4)",
                params![post_id, variant, file_id, last_used_at],
            )
            .unwrap();
    }

    fn cached_variant(connection: &Connection, post_id: &str) -> String {
        connection
            .query_row(
                "SELECT variant FROM telegram_media_cache WHERE post_id = ?1",
                params![post_id],
                |row| row.get(0),
            )
            .unwrap()
    }

    fn cached_post_ids(connection: &Connection) -> Vec<String> {
        let mut statement = connection
            .prepare("SELECT post_id FROM telegram_media_cache ORDER BY post_id")
            .unwrap();
        statement
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<std::result::Result<_, _>>()
            .unwrap()
    }

    fn schema_version(connection: &Connection) -> i64 {
        connection
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .unwrap()
    }
}
