use std::{path::Path, str::FromStr, sync::Arc};

use chrono::{DateTime, Utc};
use parking_lot::Mutex;
use rusqlite::{Connection, OptionalExtension, params};

use crate::{
    AppError, Result,
    model::{MediaVariant, TelegramMediaKind},
};

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

impl MediaCache {
    pub fn open(path: &Path) -> Result<Self> {
        let connection = Connection::open(path).map_err(db_error)?;
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
                 );",
            )
            .map_err(db_error)?;

        Ok(Self {
            connection: Arc::new(Mutex::new(connection)),
        })
    }

    pub async fn get(
        &self,
        platform: &str,
        post_id: &str,
        variant: MediaVariant,
    ) -> Result<Option<CachedTelegramMedia>> {
        let cache = self.clone();
        let platform = platform.to_owned();
        let post_id = post_id.to_owned();
        tokio::task::spawn_blocking(move || cache.get_sync(&platform, &post_id, variant))
            .await
            .map_err(|_| AppError::Database("SQLite 读取任务异常退出".into()))?
    }

    fn get_sync(
        &self,
        platform: &str,
        post_id: &str,
        variant: MediaVariant,
    ) -> Result<Option<CachedTelegramMedia>> {
        let connection = self.connection.lock();
        let row = connection
            .query_row(
                "SELECT file_id, file_unique_id, media_kind, created_at
                 FROM telegram_media_cache
                 WHERE platform = ?1 AND post_id = ?2 AND variant = ?3",
                params![platform, post_id, variant.as_str()],
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
                    params![platform, post_id, variant.as_str(), Utc::now().to_rfc3339()],
                )
                .map_err(db_error)?;
        }
        Ok(row)
    }

    pub async fn put(
        &self,
        platform: &str,
        post_id: &str,
        variant: MediaVariant,
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
                variant,
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
        variant: MediaVariant,
        kind: TelegramMediaKind,
        file_id: &str,
        file_unique_id: Option<&str>,
    ) -> Result<()> {
        let now = Utc::now().to_rfc3339();
        self.connection
            .lock()
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
                    variant.as_str(),
                    kind.as_str(),
                    file_id,
                    file_unique_id,
                    now
                ],
            )
            .map_err(db_error)?;
        Ok(())
    }

    pub async fn remove(&self, platform: &str, post_id: &str, variant: MediaVariant) -> Result<()> {
        let cache = self.clone();
        let platform = platform.to_owned();
        let post_id = post_id.to_owned();
        tokio::task::spawn_blocking(move || cache.remove_sync(&platform, &post_id, variant))
            .await
            .map_err(|_| AppError::Database("SQLite 删除任务异常退出".into()))?
    }

    fn remove_sync(&self, platform: &str, post_id: &str, variant: MediaVariant) -> Result<()> {
        self.connection
            .lock()
            .execute(
                "DELETE FROM telegram_media_cache
                 WHERE platform = ?1 AND post_id = ?2 AND variant = ?3",
                params![platform, post_id, variant.as_str()],
            )
            .map_err(db_error)?;
        Ok(())
    }
}

fn db_error(error: rusqlite::Error) -> AppError {
    AppError::Database(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn stores_and_reads_file_id() {
        let connection = Connection::open_in_memory().unwrap();
        connection
            .execute_batch(
                "CREATE TABLE telegram_media_cache (
                    platform TEXT NOT NULL,
                    post_id TEXT NOT NULL,
                    variant TEXT NOT NULL,
                    media_kind TEXT NOT NULL,
                    file_id TEXT NOT NULL,
                    file_unique_id TEXT,
                    created_at TEXT NOT NULL,
                    last_used_at TEXT NOT NULL,
                    PRIMARY KEY (platform, post_id, variant)
                );",
            )
            .unwrap();
        let cache = MediaCache {
            connection: Arc::new(Mutex::new(connection)),
        };
        cache
            .put(
                "wechat_channels",
                "abc",
                MediaVariant::Original,
                TelegramMediaKind::Document,
                "file-id",
                Some("unique-id"),
            )
            .await
            .unwrap();

        let cached = cache
            .get("wechat_channels", "abc", MediaVariant::Original)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(cached.file_id, "file-id");
        assert_eq!(cached.kind, TelegramMediaKind::Document);
    }
}
