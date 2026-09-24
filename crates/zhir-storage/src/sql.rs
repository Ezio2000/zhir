use crate::codec::storage_error;
use sqlx::{AnyPool, Column, Row, ValueRef, any::AnyPoolOptions};
use std::sync::Arc;
use zhir_core::{
    Result,
    error::Error,
    run::{Checkpoint, History},
    storage::{Commit, HistoryDelta},
    wire::CheckpointCore,
};

#[derive(Clone)]
pub(crate) struct SqlStore {
    pool: AnyPool,
    sqlite: bool,
}
impl SqlStore {
    pub async fn connect(url: &str, max_connections: u32) -> Result<Self> {
        if max_connections == 0 {
            return Err(Error::Invalid(
                "a SQL store requires at least one connection".into(),
            ));
        }
        sqlx::any::install_default_drivers();
        let sqlite = url.starts_with("sqlite:");
        let pool = AnyPoolOptions::new()
            .max_connections(max_connections)
            .acquire_timeout(std::time::Duration::from_secs(10))
            .after_connect(move |connection, _| {
                Box::pin(async move {
                    if sqlite {
                        // Readers in other processes proceed during a write, and a writer
                        // waits for the write lock instead of failing immediately.
                        sqlx::query("PRAGMA journal_mode=WAL")
                            .execute(&mut *connection)
                            .await?;
                        sqlx::query("PRAGMA busy_timeout=5000")
                            .execute(&mut *connection)
                            .await?;
                    }
                    Ok(())
                })
            })
            .connect(url)
            .await
            .map_err(storage_error)?;
        let format_query = if sqlite {
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='zhir_format'"
        } else {
            "SELECT COUNT(*) FROM information_schema.tables \
             WHERE table_schema=DATABASE() AND table_name='zhir_format'"
        };
        let existing: i64 = sqlx::query_scalar(format_query)
            .fetch_one(&pool)
            .await
            .map_err(storage_error)?;
        if existing == 0 {
            let unversioned_query = if sqlite {
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' \
                 AND name IN ('zhir_run_heads','zhir_commits','zhir_history')"
            } else {
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema=DATABASE() \
                 AND table_name IN ('zhir_run_heads','zhir_commits','zhir_history')"
            };
            let old: i64 = sqlx::query_scalar(unversioned_query)
                .fetch_one(&pool)
                .await
                .map_err(storage_error)?;
            if old != 0 {
                return Err(Error::Storage(
                    "unversioned zhir database; use a fresh database".into(),
                ));
            }
            sqlx::query(
                "CREATE TABLE IF NOT EXISTS zhir_format \
                 (id BIGINT PRIMARY KEY, version BIGINT NOT NULL)",
            )
            .execute(&pool)
            .await
            .map_err(storage_error)?;
            let inserted = sqlx::query(
                "INSERT INTO zhir_format(id,version) SELECT 1,5 \
                 WHERE NOT EXISTS (SELECT 1 FROM zhir_format WHERE id=1)",
            )
            .execute(&pool)
            .await;
            if inserted.is_err() {
                let version: i64 = sqlx::query_scalar("SELECT version FROM zhir_format WHERE id=1")
                    .fetch_one(&pool)
                    .await
                    .map_err(storage_error)?;
                if version != 5 {
                    return Err(Error::Storage("unsupported storage format".into()));
                }
            }
        }
        let version: i64 = sqlx::query_scalar("SELECT version FROM zhir_format WHERE id=1")
            .fetch_one(&pool)
            .await
            .map_err(storage_error)?;
        if version != 5 {
            return Err(Error::Storage(
                "unsupported storage format; use a fresh database".into(),
            ));
        }
        for ddl in [
            "CREATE TABLE IF NOT EXISTS zhir_run_heads (run_id VARCHAR(255) PRIMARY KEY, revision \
             BIGINT NOT NULL, checkpoint_id VARCHAR(255) NOT NULL, generation BIGINT NOT NULL, \
             core LONGTEXT NOT NULL)",
            "CREATE TABLE IF NOT EXISTS zhir_commits (run_id VARCHAR(255) NOT NULL, checkpoint_id \
             VARCHAR(255) NOT NULL, revision BIGINT NOT NULL, digest VARCHAR(64) NOT NULL, PRIMARY \
             KEY(run_id,checkpoint_id), UNIQUE(run_id,revision))",
            "CREATE TABLE IF NOT EXISTS zhir_history (run_id VARCHAR(255) NOT NULL, generation \
             BIGINT NOT NULL, revision BIGINT NOT NULL, payload LONGTEXT NOT NULL, PRIMARY \
             KEY(run_id,generation,revision))",
        ] {
            sqlx::query(ddl)
                .execute(&pool)
                .await
                .map_err(storage_error)?;
        }
        Ok(Self { pool, sqlite })
    }
    pub async fn close(&self) {
        self.pool.close().await;
    }
    /// A write transaction. SQLite takes the write lock up front: a deferred transaction
    /// that read before another process wrote cannot upgrade, and the busy timeout does
    /// not wait for that case.
    async fn begin(&self) -> sqlx::Result<sqlx::Transaction<'static, sqlx::Any>> {
        if self.sqlite {
            self.pool.begin_with("BEGIN IMMEDIATE").await
        } else {
            self.pool.begin().await
        }
    }
    pub async fn commit(&self, commit: Commit) -> Result<()> {
        // Own the transaction until it settles even when the public future is dropped.
        let this = self.clone();
        tokio::spawn(async move { this.commit_inner(commit).await })
            .await
            .map_err(storage_error)?
    }
    async fn commit_inner(&self, commit: Commit) -> Result<()> {
        let run = &commit.checkpoint().context.run_id;
        let id = &commit.checkpoint().id;
        let digest = commit.digest()?;
        let revision = i64::try_from(commit.checkpoint().revision).map_err(storage_error)?;
        // Waiting for a pooled connection is part of the commit budget.
        let begin = self.begin();
        let mut tx = match commit.deadline {
            Some(deadline) => {
                tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), begin)
                    .await
                    .map_err(|_| Error::Deadline)?
            }
            None => begin.await,
        }
        .map_err(storage_error)?;
        // A commit identity exists only while its run head does.
        let previous = sqlx::query(
            "SELECT h.core,h.generation,(SELECT c.digest FROM zhir_commits c \
             WHERE c.run_id=h.run_id AND c.checkpoint_id=?) AS digest \
             FROM zhir_run_heads h WHERE h.run_id=?",
        )
        .bind(id)
        .bind(run)
        .fetch_optional(&mut *tx)
        .await
        .map_err(storage_error)?;
        if let Some(existing) = previous
            .as_ref()
            .map(|row| read_optional_text(row, "digest"))
            .transpose()?
            .flatten()
        {
            return if existing == digest {
                Ok(())
            } else {
                Err(Error::Storage("checkpoint id reused".into()))
            };
        }
        let previous_core: Option<CheckpointCore> = previous
            .as_ref()
            .map(|r| -> Result<_> {
                let data: String = read_text(r, "core")?;
                serde_json::from_str(&data).map_err(storage_error)
            })
            .transpose()?;
        commit.check_deadline(std::time::Instant::now())?;
        commit.validate_against(previous_core.as_ref())?;
        let generation = if matches!(
            commit.history(),
            HistoryDelta::Initial(_) | HistoryDelta::Replace(_)
        ) {
            revision
        } else {
            previous
                .as_ref()
                .expect("validated previous head")
                .try_get::<i64, _>("generation")
                .map_err(storage_error)?
        };
        let core = serde_json::to_string(&commit.core()).map_err(storage_error)?;
        if let Some(previous) = previous_core {
            let result = sqlx::query(
                "UPDATE zhir_run_heads SET revision=?,checkpoint_id=?,generation=?,core=? \
                 WHERE run_id=? AND revision=?",
            )
            .bind(revision)
            .bind(id)
            .bind(generation)
            .bind(&core)
            .bind(run)
            .bind(i64::try_from(previous.revision).map_err(storage_error)?)
            .execute(&mut *tx)
            .await
            .map_err(storage_error)?;
            if result.rows_affected() != 1 {
                return Err(Error::Conflict {
                    expected: commit.expected_revision(),
                    actual: None,
                });
            }
        } else {
            sqlx::query(
                "INSERT INTO zhir_run_heads(run_id,revision,checkpoint_id,generation,core) \
                 VALUES(?,?,?,?,?)",
            )
            .bind(run)
            .bind(revision)
            .bind(id)
            .bind(generation)
            .bind(&core)
            .execute(&mut *tx)
            .await
            .map_err(|e| {
                // Another writer created this run first.
                if e.as_database_error()
                    .is_some_and(|d| d.is_unique_violation())
                {
                    Error::Conflict {
                        expected: commit.expected_revision(),
                        actual: None,
                    }
                } else {
                    storage_error(e)
                }
            })?;
        }
        sqlx::query(
            "INSERT INTO zhir_commits(run_id,checkpoint_id,revision,digest) VALUES(?,?,?,?)",
        )
        .bind(run)
        .bind(id)
        .bind(revision)
        .bind(&digest)
        .execute(&mut *tx)
        .await
        .map_err(storage_error)?;
        let messages = match commit.history() {
            HistoryDelta::Initial(m) | HistoryDelta::Append(m) | HistoryDelta::Replace(m) => {
                Some(m)
            }
            HistoryDelta::Unchanged => None,
        };
        if let Some(messages) = messages {
            let payload = serde_json::to_string(messages).map_err(storage_error)?;
            sqlx::query(
                "INSERT INTO zhir_history(run_id,generation,revision,payload) VALUES(?,?,?,?)",
            )
            .bind(run)
            .bind(generation)
            .bind(revision)
            .bind(payload)
            .execute(&mut *tx)
            .await
            .map_err(storage_error)?;
        }
        if matches!(
            commit.history(),
            HistoryDelta::Initial(_) | HistoryDelta::Replace(_)
        ) {
            // Only the current generation is reachable from the head.
            sqlx::query("DELETE FROM zhir_history WHERE run_id=? AND generation<?")
                .bind(run)
                .bind(generation)
                .execute(&mut *tx)
                .await
                .map_err(storage_error)?;
        }
        commit.check_deadline(std::time::Instant::now())?;
        tx.commit().await.map_err(storage_error)
    }
    pub async fn delete(&self, run: &str) -> Result<()> {
        let this = self.clone();
        let run = run.to_owned();
        tokio::spawn(async move {
            let mut tx = this.begin().await.map_err(storage_error)?;
            for statement in [
                "DELETE FROM zhir_history WHERE run_id=?",
                "DELETE FROM zhir_commits WHERE run_id=?",
                "DELETE FROM zhir_run_heads WHERE run_id=?",
            ] {
                sqlx::query(statement)
                    .bind(&run)
                    .execute(&mut *tx)
                    .await
                    .map_err(storage_error)?;
            }
            tx.commit().await.map_err(storage_error)
        })
        .await
        .map_err(storage_error)?
    }
    pub async fn load_head(&self, run: &str) -> Result<Option<Arc<Checkpoint>>> {
        let mut tx = self.pool.begin().await.map_err(storage_error)?;
        let Some(row) = sqlx::query("SELECT core,generation FROM zhir_run_heads WHERE run_id=?")
            .bind(run)
            .fetch_optional(&mut *tx)
            .await
            .map_err(storage_error)?
        else {
            return Ok(None);
        };
        let core: CheckpointCore =
            serde_json::from_str(&read_text(&row, "core")?).map_err(storage_error)?;
        let generation: i64 = row.try_get("generation").map_err(storage_error)?;
        let rows = sqlx::query(
            "SELECT payload FROM zhir_history \
             WHERE run_id=? AND generation=? AND revision<=? ORDER BY revision",
        )
        .bind(run)
        .bind(generation)
        .bind(i64::try_from(core.revision).map_err(storage_error)?)
        .fetch_all(&mut *tx)
        .await
        .map_err(storage_error)?;
        let mut history = History::default();
        for row in rows {
            let messages =
                serde_json::from_str(&read_text(&row, "payload")?).map_err(storage_error)?;
            history = history.append(messages)?;
        }
        tx.commit().await.map_err(storage_error)?;
        Ok(Some(Arc::new(core.with_history(history)?)))
    }
}

// SQLx Any exposes MySQL long text as a byte value, while SQLite exposes text.
fn read_text(row: &sqlx::any::AnyRow, column: &str) -> Result<String> {
    match row.column(column).type_info().kind() {
        sqlx::any::AnyTypeInfoKind::Blob => {
            String::from_utf8(row.try_get::<Vec<u8>, _>(column).map_err(storage_error)?)
                .map_err(storage_error)
        }
        _ => row.try_get(column).map_err(storage_error),
    }
}
fn read_optional_text(row: &sqlx::any::AnyRow, column: &str) -> Result<Option<String>> {
    if row.try_get_raw(column).map_err(storage_error)?.is_null() {
        Ok(None)
    } else {
        read_text(row, column).map(Some)
    }
}
