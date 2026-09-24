use crate::codec::storage_error;
use redis::{AsyncCommands, aio::MultiplexedConnection};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};
use zhir_core::{
    BoxFuture, Result,
    error::Error,
    run::{Checkpoint, History},
    storage::{Commit, HistoryDelta, RunStore},
    wire::CheckpointCore,
};
// An initial or rewritten history deletes the previous generation in the same script, so
// only the current generation is stored.
const COMMIT: &str = r#"
local old=redis.call('HGET',KEYS[2],ARGV[1])
if old then
 if old==ARGV[2] then return 'ok' else return 'identity_conflict' end
end
local revision=redis.call('HGET',KEYS[1],'revision')
if (revision or '')~=ARGV[3] then return 'revision_conflict' end
redis.call('HSET',KEYS[1],'revision',ARGV[4],'core',ARGV[5],'generation',ARGV[6])
redis.call('HSET',KEYS[2],ARGV[1],ARGV[2])
if ARGV[8]=='1' then redis.call('DEL',KEYS[3]) end
if ARGV[7]~='' then redis.call('HSET',KEYS[3],ARGV[6]..':'..ARGV[4],ARGV[7]) end
return 'ok'
"#;
const LOAD: &str = r#"
local core=redis.call('HGET',KEYS[1],'core')
if not core then return {} end
return {core,redis.call('HGET',KEYS[1],'generation'),redis.call('HGETALL',KEYS[2])}
"#;
#[derive(Clone)]
pub struct RedisRunStore {
    client: redis::Client,
    namespace: String,
    /// One multiplexed connection shared by every call, dropped after a Redis error and
    /// reopened by the next call.
    connection: Arc<Mutex<Option<MultiplexedConnection>>>,
}
impl RedisRunStore {
    pub async fn connect(url: &str, namespace: &str) -> Result<Self> {
        if namespace.is_empty()
            || !namespace
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"_:-".contains(&b))
        {
            return Err(Error::Invalid("invalid Redis namespace".into()));
        }
        let client = redis::Client::open(url).map_err(storage_error)?;
        let mut connection = client
            .get_multiplexed_async_connection()
            .await
            .map_err(storage_error)?;
        redis::cmd("PING")
            .query_async::<String>(&mut connection)
            .await
            .map_err(storage_error)?;
        let format: String = redis::Script::new(
            r#"
local version=redis.call('GET',KEYS[1])
if version then return version end
local cursor='0'
repeat
 local page=redis.call('SCAN',cursor,'MATCH',ARGV[1]..':*','COUNT',100)
 cursor=page[1]
 if #page[2]>0 then return 'unversioned' end
until cursor=='0'
redis.call('SET',KEYS[1],'5')
return '5'
"#,
        )
        .key(format!("{namespace}:format"))
        .arg(namespace)
        .invoke_async(&mut connection)
        .await
        .map_err(storage_error)?;
        if format != "5" {
            return Err(Error::Storage(
                "unsupported Redis format; use a fresh namespace".into(),
            ));
        }
        Ok(Self {
            client,
            namespace: namespace.into(),
            connection: Arc::new(Mutex::new(Some(connection))),
        })
    }
    fn keys(&self, run: &str) -> [String; 3] {
        let tag = run
            .as_bytes()
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>();
        [
            format!("{}:{{{tag}}}:head", self.namespace),
            format!("{}:{{{tag}}}:commits", self.namespace),
            format!("{}:{{{tag}}}:history", self.namespace),
        ]
    }
    async fn connection(&self) -> Result<MultiplexedConnection> {
        if let Some(connection) = self
            .connection
            .lock()
            .expect("redis connection lock")
            .clone()
        {
            return Ok(connection);
        }
        let connection = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(storage_error)?;
        *self.connection.lock().expect("redis connection lock") = Some(connection.clone());
        Ok(connection)
    }
    /// A failed command may have broken the shared connection; the next call reconnects.
    /// A failed commit stays uncertain and is never replayed here.
    fn failed(&self, error: redis::RedisError) -> Error {
        self.connection
            .lock()
            .expect("redis connection lock")
            .take();
        storage_error(error)
    }
    async fn commit_inner(&self, commit: Commit) -> Result<()> {
        let keys = self.keys(&commit.checkpoint().context.run_id);
        let mut conn = self.connection().await?;
        let digest = commit.digest()?;
        let existing: Option<String> = conn
            .hget(&keys[1], &commit.checkpoint().id)
            .await
            .map_err(|e| self.failed(e))?;
        if let Some(old) = existing {
            return if old == digest {
                Ok(())
            } else {
                Err(Error::Storage("checkpoint id reused".into()))
            };
        }
        let head: HashMap<String, String> =
            conn.hgetall(&keys[0]).await.map_err(|e| self.failed(e))?;
        let previous: Option<CheckpointCore> = head
            .get("core")
            .map(|s| serde_json::from_str(s).map_err(storage_error))
            .transpose()?;
        commit.check_deadline(std::time::Instant::now())?;
        commit.validate_against(previous.as_ref())?;
        let revision = commit.checkpoint().revision.to_string();
        let rewrite = matches!(
            commit.history(),
            HistoryDelta::Initial(_) | HistoryDelta::Replace(_)
        );
        let generation = if rewrite {
            revision.clone()
        } else {
            head.get("generation")
                .cloned()
                .ok_or_else(|| Error::Storage("missing history generation".into()))?
        };
        let payload = match commit.history() {
            HistoryDelta::Initial(m) | HistoryDelta::Append(m) | HistoryDelta::Replace(m) => {
                serde_json::to_string(m).map_err(storage_error)?
            }
            HistoryDelta::Unchanged => String::new(),
        };
        let core = serde_json::to_string(commit.core()).map_err(storage_error)?;
        // The script runs without a server clock check; the deadline is checked locally
        // before the write is sent.
        commit.check_deadline(std::time::Instant::now())?;
        let result: String = redis::Script::new(COMMIT)
            .key(&keys[0])
            .key(&keys[1])
            .key(&keys[2])
            .arg(&commit.checkpoint().id)
            .arg(digest)
            .arg(
                commit
                    .expected_revision()
                    .map(|r| r.to_string())
                    .unwrap_or_default(),
            )
            .arg(revision)
            .arg(core)
            .arg(generation)
            .arg(payload)
            .arg(if rewrite { "1" } else { "0" })
            .invoke_async(&mut conn)
            .await
            .map_err(|e| self.failed(e))?;
        match result.as_str() {
            "ok" => Ok(()),
            "revision_conflict" => Err(Error::Conflict {
                expected: commit.expected_revision(),
                actual: None,
            }),
            _ => Err(Error::Storage("checkpoint identity conflict".into())),
        }
    }
}
impl RunStore for RedisRunStore {
    fn commit(&self, commit: Commit) -> BoxFuture<'_, Result<()>> {
        let this = self.clone();
        Box::pin(async move {
            tokio::spawn(async move { this.commit_inner(commit).await })
                .await
                .map_err(storage_error)?
        })
    }
    fn load_head(&self, run_id: &str) -> BoxFuture<'_, Result<Option<Arc<Checkpoint>>>> {
        let keys = self.keys(run_id);
        Box::pin(async move {
            let mut conn = self.connection().await?;
            let value: redis::Value = redis::Script::new(LOAD)
                .key(&keys[0])
                .key(&keys[2])
                .invoke_async(&mut conn)
                .await
                .map_err(|e| self.failed(e))?;
            if let redis::Value::Array(v) = &value
                && v.is_empty()
            {
                return Ok(None);
            }
            let (core, generation, chunks): (String, String, HashMap<String, String>) =
                redis::from_redis_value(&value).map_err(storage_error)?;
            let core: CheckpointCore = serde_json::from_str(&core).map_err(storage_error)?;
            let mut ordered = Vec::new();
            for (field, payload) in chunks {
                if let Some((g, r)) = field.split_once(':')
                    && g == generation
                {
                    let revision = r.parse::<u64>().map_err(storage_error)?;
                    if revision <= core.revision {
                        ordered.push((revision, payload));
                    }
                }
            }
            ordered.sort_by_key(|(r, _)| *r);
            let mut history = History::default();
            for (_, payload) in ordered {
                history = history.append(serde_json::from_str(&payload).map_err(storage_error)?)?;
            }
            Ok(Some(Arc::new(core.with_history(history)?)))
        })
    }
    fn delete(&self, run_id: &str) -> BoxFuture<'_, Result<()>> {
        let keys = self.keys(run_id);
        Box::pin(async move {
            let mut conn = self.connection().await?;
            conn.del::<_, ()>(&keys).await.map_err(|e| self.failed(e))
        })
    }
}
