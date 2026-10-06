//! Job metadata. Payloads (ELFs, inputs, proofs) live in [`crate::blobs`].

use std::collections::HashMap;

use async_trait::async_trait;
use parking_lot::Mutex;
use sqlx::{postgres::PgPoolOptions, PgPool, Row};

use fleet_proto::fleet::v1::ProofKind;
use fleet_proto::stage::v1::Status as StageStatus;

use crate::streams::{Step, StepState, StreamRow};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobState {
    Queued,
    Running,
    Succeeded,
    /// Failed with the stage status the SDK will see.
    Failed(StageStatus),
}

impl JobState {
    pub fn is_terminal(self) -> bool {
        matches!(self, JobState::Succeeded | JobState::Failed(_))
    }

    fn to_db(self) -> (i32, i32) {
        match self {
            JobState::Queued => (0, 0),
            JobState::Running => (1, 0),
            JobState::Succeeded => (2, 0),
            JobState::Failed(s) => (3, s as i32),
        }
    }

    fn from_db(state: i32, status: i32) -> Self {
        match state {
            0 => JobState::Queued,
            1 => JobState::Running,
            2 => JobState::Succeeded,
            _ => JobState::Failed(
                StageStatus::try_from(status).unwrap_or(StageStatus::InternalError),
            ),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Job {
    /// The SDK's proof_id.
    pub id: String,
    pub tenant: String,
    pub elf_id: String,
    pub kind: ProofKind,
    pub state: JobState,
    pub error: String,
    /// Node name (stable across node restarts) the job runs or ran on.
    pub node: Option<String>,
    pub attempts: u32,
    /// Nodes that failed this job; a retry avoids them.
    pub failed_on: Vec<String>,
    pub cycles: u64,
    pub prove_ms: u64,
    pub created_ms: u64,
    pub started_ms: Option<u64>,
    pub finished_ms: Option<u64>,
}

/// A signing key registered for a tenant: the SDK's `ZKM_PRIVATE_KEY` maps to its address.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Credential {
    /// Lower-case hex address without 0x.
    pub address: String,
    pub tenant: String,
    pub label: String,
    pub created_ms: u64,
    pub revoked_ms: Option<u64>,
}

pub fn normalize_address(a: &str) -> String {
    a.trim()
        .trim_start_matches("0x")
        .trim_start_matches("0X")
        .to_ascii_lowercase()
}

#[async_trait]
pub trait Store: StreamStore + Send + Sync {
    async fn upsert(&self, job: &Job) -> anyhow::Result<()>;
    async fn get(&self, id: &str) -> anyhow::Result<Option<Job>>;
    /// Queued and running jobs, oldest first, for recovery after a restart.
    async fn incomplete(&self) -> anyhow::Result<Vec<Job>>;

    async fn add_credential(&self, c: &Credential) -> anyhow::Result<()>;
    /// Marks the credential revoked; returns false when the address is unknown.
    async fn revoke_credential(&self, address: &str, at_ms: u64) -> anyhow::Result<bool>;
    async fn credentials(&self) -> anyhow::Result<Vec<Credential>>;

}

/// Stream cursors and steps (see `streams.rs`); every `Store` provides it.
#[async_trait]
pub trait StreamStore: Send + Sync {
    async fn upsert_stream(&self, s: &StreamRow) -> anyhow::Result<()>;
    async fn streams(&self) -> anyhow::Result<Vec<StreamRow>>;
    async fn upsert_step(&self, s: &Step) -> anyhow::Result<()>;
    /// Steps not yet done, failed or skipped, lowest first.
    async fn open_steps(&self, stream: &str) -> anyhow::Result<Vec<Step>>;
    /// The newest steps of a stream, highest seq first.
    async fn recent_steps(&self, stream: &str, limit: usize) -> anyhow::Result<Vec<Step>>;
}

#[derive(Default)]
pub struct MemoryStore {
    streams: Mutex<HashMap<String, StreamRow>>,
    steps: Mutex<HashMap<(String, i64), Step>>,
    jobs: Mutex<HashMap<String, Job>>,
    creds: Mutex<HashMap<String, Credential>>,
}

#[async_trait]
impl Store for MemoryStore {
    async fn upsert(&self, job: &Job) -> anyhow::Result<()> {
        self.jobs.lock().insert(job.id.clone(), job.clone());
        Ok(())
    }

    async fn get(&self, id: &str) -> anyhow::Result<Option<Job>> {
        Ok(self.jobs.lock().get(id).cloned())
    }

    async fn incomplete(&self) -> anyhow::Result<Vec<Job>> {
        let mut v: Vec<Job> = self
            .jobs
            .lock()
            .values()
            .filter(|j| !j.state.is_terminal())
            .cloned()
            .collect();
        v.sort_by_key(|j| j.created_ms);
        Ok(v)
    }

    async fn add_credential(&self, c: &Credential) -> anyhow::Result<()> {
        let mut creds = self.creds.lock();
        anyhow::ensure!(
            !creds.contains_key(&c.address),
            "address already registered"
        );
        creds.insert(c.address.clone(), c.clone());
        Ok(())
    }

    async fn revoke_credential(&self, address: &str, at_ms: u64) -> anyhow::Result<bool> {
        Ok(self
            .creds
            .lock()
            .get_mut(&normalize_address(address))
            .map(|c| c.revoked_ms = Some(at_ms))
            .is_some())
    }

    async fn credentials(&self) -> anyhow::Result<Vec<Credential>> {
        let mut v: Vec<Credential> = self.creds.lock().values().cloned().collect();
        v.sort_by_key(|c| c.created_ms);
        Ok(v)
    }
}

#[async_trait]
impl StreamStore for MemoryStore {
    async fn upsert_stream(&self, s: &StreamRow) -> anyhow::Result<()> {
        self.streams.lock().insert(s.name.clone(), s.clone());
        Ok(())
    }
    async fn streams(&self) -> anyhow::Result<Vec<StreamRow>> {
        Ok(self.streams.lock().values().cloned().collect())
    }
    async fn upsert_step(&self, s: &Step) -> anyhow::Result<()> {
        self.steps.lock().insert((s.stream.clone(), s.seq), s.clone());
        Ok(())
    }
    async fn open_steps(&self, stream: &str) -> anyhow::Result<Vec<Step>> {
        let mut v: Vec<Step> = self
            .steps
            .lock()
            .values()
            .filter(|s| s.stream == stream && s.state.is_open())
            .cloned()
            .collect();
        v.sort_by_key(|s| s.seq);
        Ok(v)
    }
    async fn recent_steps(&self, stream: &str, limit: usize) -> anyhow::Result<Vec<Step>> {
        let mut v: Vec<Step> = self
            .steps
            .lock()
            .values()
            .filter(|s| s.stream == stream)
            .cloned()
            .collect();
        v.sort_by_key(|s| std::cmp::Reverse(s.seq));
        v.truncate(limit);
        Ok(v)
    }
}

pub struct PgStore {
    pool: PgPool,
}

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS fleet_jobs (
    id          TEXT PRIMARY KEY,
    tenant      TEXT NOT NULL,
    elf_id      TEXT NOT NULL,
    kind        INT  NOT NULL,
    state       INT  NOT NULL,
    status      INT  NOT NULL DEFAULT 0,
    error       TEXT NOT NULL DEFAULT '',
    node        TEXT,
    attempts    INT  NOT NULL DEFAULT 0,
    failed_on   TEXT NOT NULL DEFAULT '',
    cycles      BIGINT NOT NULL DEFAULT 0,
    prove_ms    BIGINT NOT NULL DEFAULT 0,
    created_ms  BIGINT NOT NULL,
    started_ms  BIGINT,
    finished_ms BIGINT
);
CREATE INDEX IF NOT EXISTS fleet_jobs_incomplete ON fleet_jobs (state) WHERE state < 2;
CREATE TABLE IF NOT EXISTS fleet_streams (
    name        TEXT PRIMARY KEY,
    next_seq    BIGINT,
    head        BIGINT,
    head_ms     BIGINT,
    paused      BOOLEAN NOT NULL DEFAULT FALSE,
    done        BIGINT NOT NULL DEFAULT 0,
    failed      BIGINT NOT NULL DEFAULT 0,
    skipped     BIGINT NOT NULL DEFAULT 0,
    updated_ms  BIGINT NOT NULL
);
CREATE TABLE IF NOT EXISTS fleet_stream_steps (
    stream        TEXT   NOT NULL,
    seq           BIGINT NOT NULL,
    state         INT    NOT NULL,
    attempts      INT    NOT NULL DEFAULT 0,
    worker        TEXT,
    leased_ms     BIGINT,
    not_before_ms BIGINT,
    created_ms    BIGINT NOT NULL,
    started_ms    BIGINT,
    finished_ms   BIGINT,
    wall_ms       BIGINT,
    error         TEXT   NOT NULL DEFAULT '',
    artifact      TEXT,
    job_id        TEXT,
    PRIMARY KEY (stream, seq)
);
CREATE INDEX IF NOT EXISTS fleet_stream_steps_open ON fleet_stream_steps (stream, state) WHERE state < 4;
CREATE TABLE IF NOT EXISTS fleet_credentials (
    address    TEXT PRIMARY KEY,
    tenant     TEXT NOT NULL,
    label      TEXT NOT NULL DEFAULT '',
    created_ms BIGINT NOT NULL,
    revoked_ms BIGINT
);
"#;

impl PgStore {
    pub async fn connect(url: &str) -> anyhow::Result<Self> {
        let pool = PgPoolOptions::new().max_connections(8).connect(url).await?;
        sqlx::raw_sql(SCHEMA).execute(&pool).await?;
        Ok(Self { pool })
    }

    fn row_to_job(row: &sqlx::postgres::PgRow) -> anyhow::Result<Job> {
        let failed_on: String = row.try_get("failed_on")?;
        Ok(Job {
            id: row.try_get("id")?,
            tenant: row.try_get("tenant")?,
            elf_id: row.try_get("elf_id")?,
            kind: ProofKind::try_from(row.try_get::<i32, _>("kind")?)
                .unwrap_or(ProofKind::Unspecified),
            state: JobState::from_db(row.try_get("state")?, row.try_get("status")?),
            error: row.try_get("error")?,
            node: row.try_get("node")?,
            attempts: row.try_get::<i32, _>("attempts")? as u32,
            failed_on: failed_on
                .split(',')
                .filter(|s| !s.is_empty())
                .map(String::from)
                .collect(),
            cycles: row.try_get::<i64, _>("cycles")? as u64,
            prove_ms: row.try_get::<i64, _>("prove_ms")? as u64,
            created_ms: row.try_get::<i64, _>("created_ms")? as u64,
            started_ms: row
                .try_get::<Option<i64>, _>("started_ms")?
                .map(|v| v as u64),
            finished_ms: row
                .try_get::<Option<i64>, _>("finished_ms")?
                .map(|v| v as u64),
        })
    }
}

#[async_trait]
impl Store for PgStore {
    async fn upsert(&self, j: &Job) -> anyhow::Result<()> {
        let (state, status) = j.state.to_db();
        sqlx::query(
            "INSERT INTO fleet_jobs (id, tenant, elf_id, kind, state, status, error, node, attempts, failed_on, \
             cycles, prove_ms, created_ms, started_ms, finished_ms) \
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15) \
             ON CONFLICT (id) DO UPDATE SET state=$5, status=$6, error=$7, node=$8, attempts=$9, failed_on=$10, \
             cycles=$11, prove_ms=$12, started_ms=$14, finished_ms=$15",
        )
        .bind(&j.id)
        .bind(&j.tenant)
        .bind(&j.elf_id)
        .bind(j.kind as i32)
        .bind(state)
        .bind(status)
        .bind(&j.error)
        .bind(&j.node)
        .bind(j.attempts as i32)
        .bind(j.failed_on.join(","))
        .bind(j.cycles as i64)
        .bind(j.prove_ms as i64)
        .bind(j.created_ms as i64)
        .bind(j.started_ms.map(|v| v as i64))
        .bind(j.finished_ms.map(|v| v as i64))
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn get(&self, id: &str) -> anyhow::Result<Option<Job>> {
        let row = sqlx::query("SELECT * FROM fleet_jobs WHERE id = $1")
            .bind(id)
            .fetch_optional(&self.pool)
            .await?;
        row.as_ref().map(Self::row_to_job).transpose()
    }

    async fn incomplete(&self) -> anyhow::Result<Vec<Job>> {
        let rows = sqlx::query("SELECT * FROM fleet_jobs WHERE state < 2 ORDER BY created_ms")
            .fetch_all(&self.pool)
            .await?;
        rows.iter().map(Self::row_to_job).collect()
    }

    async fn add_credential(&self, c: &Credential) -> anyhow::Result<()> {
        sqlx::query("INSERT INTO fleet_credentials (address, tenant, label, created_ms) VALUES ($1,$2,$3,$4)")
            .bind(&c.address)
            .bind(&c.tenant)
            .bind(&c.label)
            .bind(c.created_ms as i64)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn revoke_credential(&self, address: &str, at_ms: u64) -> anyhow::Result<bool> {
        let r = sqlx::query("UPDATE fleet_credentials SET revoked_ms = $2 WHERE address = $1")
            .bind(normalize_address(address))
            .bind(at_ms as i64)
            .execute(&self.pool)
            .await?;
        Ok(r.rows_affected() > 0)
    }

    async fn credentials(&self) -> anyhow::Result<Vec<Credential>> {
        let rows = sqlx::query("SELECT * FROM fleet_credentials ORDER BY created_ms")
            .fetch_all(&self.pool)
            .await?;
        rows.iter()
            .map(|r| {
                Ok(Credential {
                    address: r.try_get("address")?,
                    tenant: r.try_get("tenant")?,
                    label: r.try_get("label")?,
                    created_ms: r.try_get::<i64, _>("created_ms")? as u64,
                    revoked_ms: r.try_get::<Option<i64>, _>("revoked_ms")?.map(|v| v as u64),
                })
            })
            .collect()
    }
}

fn opt_u64(row: &sqlx::postgres::PgRow, col: &str) -> anyhow::Result<Option<u64>> {
    Ok(row.try_get::<Option<i64>, _>(col)?.map(|v| v as u64))
}

fn row_to_step(row: &sqlx::postgres::PgRow) -> anyhow::Result<Step> {
    Ok(Step {
        stream: row.try_get("stream")?,
        seq: row.try_get("seq")?,
        state: StepState::from_db(row.try_get("state")?),
        attempts: row.try_get::<i32, _>("attempts")? as u32,
        worker: row.try_get("worker")?,
        leased_ms: opt_u64(row, "leased_ms")?,
        not_before_ms: opt_u64(row, "not_before_ms")?,
        created_ms: row.try_get::<i64, _>("created_ms")? as u64,
        started_ms: opt_u64(row, "started_ms")?,
        finished_ms: opt_u64(row, "finished_ms")?,
        wall_ms: opt_u64(row, "wall_ms")?,
        error: row.try_get("error")?,
        artifact: row.try_get("artifact")?,
        job_id: row.try_get("job_id")?,
    })
}

#[async_trait]
impl StreamStore for PgStore {
    async fn upsert_stream(&self, s: &StreamRow) -> anyhow::Result<()> {
        sqlx::query(
            "INSERT INTO fleet_streams (name, next_seq, head, head_ms, paused, done, failed, skipped, updated_ms)
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
             ON CONFLICT (name) DO UPDATE SET next_seq = $2, head = $3, head_ms = $4, paused = $5, done = $6,
             failed = $7, skipped = $8, updated_ms = $9",
        )
        .bind(&s.name)
        .bind(s.next_seq)
        .bind(s.head)
        .bind(s.head_ms.map(|v| v as i64))
        .bind(s.paused)
        .bind(s.done as i64)
        .bind(s.failed as i64)
        .bind(s.skipped as i64)
        .bind(s.updated_ms as i64)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn streams(&self) -> anyhow::Result<Vec<StreamRow>> {
        let rows = sqlx::query("SELECT * FROM fleet_streams ORDER BY name")
            .fetch_all(&self.pool)
            .await?;
        rows.iter()
            .map(|row| {
                Ok(StreamRow {
                    name: row.try_get("name")?,
                    next_seq: row.try_get("next_seq")?,
                    head: row.try_get("head")?,
                    head_ms: opt_u64(row, "head_ms")?,
                    paused: row.try_get("paused")?,
                    done: row.try_get::<i64, _>("done")? as u64,
                    failed: row.try_get::<i64, _>("failed")? as u64,
                    skipped: row.try_get::<i64, _>("skipped")? as u64,
                    updated_ms: row.try_get::<i64, _>("updated_ms")? as u64,
                })
            })
            .collect()
    }

    async fn upsert_step(&self, s: &Step) -> anyhow::Result<()> {
        sqlx::query(
            "INSERT INTO fleet_stream_steps (stream, seq, state, attempts, worker, leased_ms, not_before_ms, created_ms,
             started_ms, finished_ms, wall_ms, error, artifact, job_id)
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
             ON CONFLICT (stream, seq) DO UPDATE SET state = $3, attempts = $4, worker = $5, leased_ms = $6,
             not_before_ms = $7, started_ms = $9, finished_ms = $10, wall_ms = $11, error = $12, artifact = $13,
             job_id = $14",
        )
        .bind(&s.stream)
        .bind(s.seq)
        .bind(s.state as i32)
        .bind(s.attempts as i32)
        .bind(&s.worker)
        .bind(s.leased_ms.map(|v| v as i64))
        .bind(s.not_before_ms.map(|v| v as i64))
        .bind(s.created_ms as i64)
        .bind(s.started_ms.map(|v| v as i64))
        .bind(s.finished_ms.map(|v| v as i64))
        .bind(s.wall_ms.map(|v| v as i64))
        .bind(&s.error)
        .bind(&s.artifact)
        .bind(&s.job_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn open_steps(&self, stream: &str) -> anyhow::Result<Vec<Step>> {
        let rows = sqlx::query("SELECT * FROM fleet_stream_steps WHERE stream = $1 AND state < 4 ORDER BY seq")
            .bind(stream)
            .fetch_all(&self.pool)
            .await?;
        rows.iter().map(row_to_step).collect()
    }

    async fn recent_steps(&self, stream: &str, limit: usize) -> anyhow::Result<Vec<Step>> {
        let rows = sqlx::query("SELECT * FROM fleet_stream_steps WHERE stream = $1 ORDER BY seq DESC LIMIT $2")
            .bind(stream)
            .bind(limit as i64)
            .fetch_all(&self.pool)
            .await?;
        rows.iter().map(row_to_step).collect()
    }
}
