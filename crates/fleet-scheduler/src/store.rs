//! Job metadata. Payloads (ELFs, inputs, proofs) live in [`crate::blobs`].

use std::collections::HashMap;

use async_trait::async_trait;
use parking_lot::Mutex;
use sqlx::{postgres::PgPoolOptions, PgPool, Row};

use fleet_proto::fleet::v1::ProofKind;
use fleet_proto::stage::v1::Status as StageStatus;

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

/// A job as the admin API shows it: the record itself, its kind and state by name.
#[derive(Debug, Clone, serde::Serialize)]
pub struct Job {
    /// The SDK's proof_id.
    pub id: String,
    pub tenant: String,
    pub elf_id: String,
    #[serde(serialize_with = "by_debug_name")]
    pub kind: ProofKind,
    #[serde(serialize_with = "by_debug_name")]
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

fn by_debug_name<T: std::fmt::Debug, S: serde::Serializer>(v: &T, s: S) -> Result<S::Ok, S::Error> {
    s.serialize_str(&format!("{v:?}"))
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
pub trait Store: Send + Sync {
    async fn upsert(&self, job: &Job) -> anyhow::Result<()>;
    async fn get(&self, id: &str) -> anyhow::Result<Option<Job>>;
    /// Queued and running jobs, oldest first, for recovery after a restart.
    async fn incomplete(&self) -> anyhow::Result<Vec<Job>>;
    /// The newest jobs, of one tenant or all.
    async fn recent(&self, tenant: Option<&str>, limit: usize) -> anyhow::Result<Vec<Job>>;

    async fn add_credential(&self, c: &Credential) -> anyhow::Result<()>;
    /// Marks the credential revoked; returns false when the address is unknown.
    async fn revoke_credential(&self, address: &str, at_ms: u64) -> anyhow::Result<bool>;
    async fn credentials(&self) -> anyhow::Result<Vec<Credential>>;
}

#[derive(Default)]
pub struct MemoryStore {
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

    async fn recent(&self, tenant: Option<&str>, limit: usize) -> anyhow::Result<Vec<Job>> {
        let mut v: Vec<Job> = self
            .jobs
            .lock()
            .values()
            .filter(|j| tenant.is_none_or(|t| j.tenant == t))
            .cloned()
            .collect();
        v.sort_by_key(|j| std::cmp::Reverse(j.created_ms));
        v.truncate(limit);
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

    async fn recent(&self, tenant: Option<&str>, limit: usize) -> anyhow::Result<Vec<Job>> {
        let rows = sqlx::query(
            "SELECT * FROM fleet_jobs WHERE ($1::text IS NULL OR tenant = $1) ORDER BY created_ms DESC LIMIT $2",
        )
        .bind(tenant)
        .bind(limit as i64)
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
