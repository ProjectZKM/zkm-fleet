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

    /// Its name, as the database and the admin API have it.
    pub fn name(self) -> &'static str {
        match self {
            JobState::Queued => "queued",
            JobState::Running => "running",
            JobState::Succeeded => "succeeded",
            JobState::Failed(_) => "failed",
        }
    }

    /// The stage status a failed job ended with, by name.
    pub fn fail_status(self) -> Option<String> {
        match self {
            JobState::Failed(s) => Some(status_name(s)),
            _ => None,
        }
    }

    fn from_names(state: &str, fail_status: Option<&str>) -> anyhow::Result<Self> {
        Ok(match state {
            "queued" => JobState::Queued,
            "running" => JobState::Running,
            "succeeded" => JobState::Succeeded,
            "failed" => JobState::Failed(
                fail_status
                    .and_then(|n| StageStatus::from_str_name(&n.to_ascii_uppercase()))
                    .unwrap_or(StageStatus::InternalError),
            ),
            other => anyhow::bail!("unknown job state {other:?}"),
        })
    }
}

const STATES: [&str; 4] = ["queued", "running", "succeeded", "failed"];

/// A stage status by name: `prove_error`.
pub fn status_name(s: StageStatus) -> String {
    s.as_str_name().to_ascii_lowercase()
}

/// A proof kind by name: `compressed`.
pub fn kind_name(k: ProofKind) -> String {
    k.as_str_name().trim_start_matches("PROOF_KIND_").to_ascii_lowercase()
}

fn kind_from_name(n: &str) -> ProofKind {
    ProofKind::from_str_name(&format!("PROOF_KIND_{}", n.to_ascii_uppercase())).unwrap_or(ProofKind::Unspecified)
}

/// Every value of a proto enum, by its number from 0 up.
fn all<T: TryFrom<i32>>() -> Vec<T> {
    (0..).map_while(|i| T::try_from(i).ok()).collect()
}

/// A proof job: an instance of a proof app (its tenant's program).
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

/// The admin API's job: the database row's fields and names (kind, state and
/// fail_status by name).
impl serde::Serialize for Job {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let mut m = s.serialize_struct("Job", 15)?;
        m.serialize_field("id", &self.id)?;
        m.serialize_field("tenant", &self.tenant)?;
        m.serialize_field("elf_id", &self.elf_id)?;
        m.serialize_field("kind", &kind_name(self.kind))?;
        m.serialize_field("state", self.state.name())?;
        m.serialize_field("fail_status", &self.state.fail_status())?;
        m.serialize_field("error", &self.error)?;
        m.serialize_field("node", &self.node)?;
        m.serialize_field("attempts", &self.attempts)?;
        m.serialize_field("failed_on", &self.failed_on)?;
        m.serialize_field("cycles", &self.cycles)?;
        m.serialize_field("prove_ms", &self.prove_ms)?;
        m.serialize_field("created_ms", &self.created_ms)?;
        m.serialize_field("started_ms", &self.started_ms)?;
        m.serialize_field("finished_ms", &self.finished_ms)?;
        m.end()
    }
}

/// A proof app: one tenant's program, and how its jobs (instances) went. Registered
/// before its first job (`created_ms`); a request refused before its app was known is
/// counted under the unregistered (tenant "", elf_id "") entry.
#[derive(Debug, Clone, serde::Serialize)]
pub struct ProofApp {
    pub tenant: String,
    pub elf_id: String,
    pub created_ms: u64,
    pub jobs: u64,
    pub active: u64,
    pub succeeded: u64,
    pub failed: u64,
    pub last_created_ms: u64,
    /// Mean proving time of its succeeded jobs.
    pub mean_prove_ms: Option<f64>,
    /// New jobs are refused (`fleetctl app disable`); the queued and running ones finish.
    pub disabled: bool,
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
    async fn recent(&self, f: &JobFilter, limit: usize) -> anyhow::Result<Vec<Job>>;
    /// Every (tenant, program) that has had a job, most recently used first.
    async fn apps(&self) -> anyhow::Result<Vec<ProofApp>>;
    /// Deletes an app and its finished jobs; returns the jobs' ids.
    async fn delete_app(&self, tenant: &str, elf_id: &str) -> anyhow::Result<Vec<String>>;
    /// Every registered app: (tenant, elf_id, disabled).
    async fn registered_apps(&self) -> anyhow::Result<Vec<(String, String, bool)>>;
    /// Registers an app; false when it already was.
    async fn add_app(&self, tenant: &str, elf_id: &str, at_ms: u64) -> anyhow::Result<bool>;
    async fn set_app_disabled(&self, tenant: &str, elf_id: &str, disabled: bool, at_ms: u64) -> anyhow::Result<()>;

    async fn add_credential(&self, c: &Credential) -> anyhow::Result<()>;
    /// Marks the credential revoked; returns false when the address is unknown.
    async fn revoke_credential(&self, address: &str, at_ms: u64) -> anyhow::Result<bool>;
    async fn credentials(&self) -> anyhow::Result<Vec<Credential>>;
}

/// Which jobs a listing takes.
#[derive(Debug, Default, Clone)]
pub struct JobFilter {
    /// Only this tenant's.
    pub tenant: Option<String>,
    /// Only the jobs of programs whose id starts with this (an app name carries the first 8).
    pub elf: Option<String>,
    /// Only queued or running ones.
    pub active: bool,
}

impl JobFilter {
    pub fn matches(&self, j: &Job) -> bool {
        self.tenant.as_ref().is_none_or(|t| &j.tenant == t)
            && self.elf.as_ref().is_none_or(|e| j.elf_id.starts_with(e.as_str()))
            && (!self.active || !j.state.is_terminal())
    }
}

#[derive(Default)]
pub struct MemoryStore {
    jobs: Mutex<HashMap<String, Job>>,
    creds: Mutex<HashMap<String, Credential>>,
    /// (tenant, elf_id) -> (created_ms, disabled_ms).
    apps: Mutex<HashMap<(String, String), (u64, Option<u64>)>>,
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

    async fn recent(&self, f: &JobFilter, limit: usize) -> anyhow::Result<Vec<Job>> {
        let mut v: Vec<Job> = self.jobs.lock().values().filter(|j| f.matches(j)).cloned().collect();
        v.sort_by_key(|j| std::cmp::Reverse(j.created_ms));
        v.truncate(limit);
        Ok(v)
    }

    async fn apps(&self) -> anyhow::Result<Vec<ProofApp>> {
        let blank = |tenant: &str, elf_id: &str, created_ms, disabled| ProofApp {
            tenant: tenant.into(), elf_id: elf_id.into(), created_ms, jobs: 0, active: 0, succeeded: 0, failed: 0,
            last_created_ms: 0, mean_prove_ms: None, disabled,
        };
        let mut apps: HashMap<(String, String), (ProofApp, f64)> = self
            .apps
            .lock()
            .iter()
            .map(|((t, e), (created, disabled))| ((t.clone(), e.clone()), (blank(t, e, *created, disabled.is_some()), 0.0)))
            .collect();
        for j in self.jobs.lock().values() {
            let (a, prove_sum) = apps
                .entry((j.tenant.clone(), j.elf_id.clone()))
                .or_insert_with(|| (blank(&j.tenant, &j.elf_id, j.created_ms, false), 0.0));
            a.jobs += 1;
            a.created_ms = a.created_ms.min(j.created_ms);
            a.last_created_ms = a.last_created_ms.max(j.created_ms);
            match j.state {
                JobState::Queued | JobState::Running => a.active += 1,
                JobState::Succeeded => {
                    a.succeeded += 1;
                    *prove_sum += j.prove_ms as f64;
                }
                JobState::Failed(_) => a.failed += 1,
            }
        }
        let mut v: Vec<ProofApp> = apps
            .into_values()
            .map(|(mut a, sum)| {
                if a.succeeded > 0 {
                    a.mean_prove_ms = Some(sum / a.succeeded as f64);
                }
                a
            })
            .collect();
        v.sort_by_key(|a| std::cmp::Reverse((a.last_created_ms, a.created_ms)));
        Ok(v)
    }

    async fn delete_app(&self, tenant: &str, elf_id: &str) -> anyhow::Result<Vec<String>> {
        let mut jobs = self.jobs.lock();
        let ids: Vec<String> = jobs
            .values()
            .filter(|j| j.tenant == tenant && j.elf_id == elf_id && j.state.is_terminal())
            .map(|j| j.id.clone())
            .collect();
        for id in &ids {
            jobs.remove(id);
        }
        self.apps.lock().remove(&(tenant.to_string(), elf_id.to_string()));
        Ok(ids)
    }

    async fn registered_apps(&self) -> anyhow::Result<Vec<(String, String, bool)>> {
        let mut v: Vec<_> = self.apps.lock().iter().map(|((t, e), (_, d))| (t.clone(), e.clone(), d.is_some())).collect();
        v.sort();
        Ok(v)
    }

    async fn add_app(&self, tenant: &str, elf_id: &str, at_ms: u64) -> anyhow::Result<bool> {
        let mut apps = self.apps.lock();
        let key = (tenant.to_string(), elf_id.to_string());
        if apps.contains_key(&key) {
            return Ok(false);
        }
        apps.insert(key, (at_ms, None));
        Ok(true)
    }

    async fn set_app_disabled(&self, tenant: &str, elf_id: &str, disabled: bool, at_ms: u64) -> anyhow::Result<()> {
        if let Some((_, d)) = self.apps.lock().get_mut(&(tenant.to_string(), elf_id.to_string())) {
            *d = disabled.then_some(at_ms);
        }
        Ok(())
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

/// The schema this scheduler writes; `fleet_schema` records what a database has.
const SCHEMA_VERSION: i32 = 3;

fn quoted(names: impl IntoIterator<Item = String>) -> String {
    names.into_iter().map(|n| format!("'{n}'")).collect::<Vec<_>>().join(", ")
}

/// Every table, as a new database gets them.
fn schema() -> String {
    format!("{}{}", jobs_and_credentials(), apps())
}

/// Jobs and credentials (schema 1). Names, not numbers: a state, kind or stage status
/// reads the same in the database as in the admin API, and the CHECKs list what may be
/// there.
fn jobs_and_credentials() -> String {
    let kinds = quoted(all::<ProofKind>().into_iter().map(kind_name));
    let statuses = quoted(all::<StageStatus>().into_iter().map(status_name));
    let states = quoted(STATES.map(String::from));
    format!(
        r#"
CREATE TABLE fleet_jobs (
    id          TEXT    PRIMARY KEY,
    tenant      TEXT    NOT NULL,
    elf_id      TEXT    NOT NULL,
    kind        TEXT    NOT NULL CONSTRAINT fleet_jobs_kind_known CHECK (kind IN ({kinds})),
    state       TEXT    NOT NULL CONSTRAINT fleet_jobs_state_known CHECK (state IN ({states})),
    fail_status TEXT             CONSTRAINT fleet_jobs_fail_status_known CHECK (fail_status IN ({statuses})),
    error       TEXT    NOT NULL DEFAULT '',
    node        TEXT,
    attempts    INT     NOT NULL DEFAULT 0 CONSTRAINT fleet_jobs_attempts_counted CHECK (attempts >= 0),
    failed_on   TEXT[]  NOT NULL DEFAULT '{{}}',
    cycles      BIGINT  NOT NULL DEFAULT 0 CONSTRAINT fleet_jobs_cycles_counted CHECK (cycles >= 0),
    prove_ms    BIGINT  NOT NULL DEFAULT 0 CONSTRAINT fleet_jobs_prove_ms_counted CHECK (prove_ms >= 0),
    created_ms  BIGINT  NOT NULL,
    started_ms  BIGINT,
    finished_ms BIGINT,
    CONSTRAINT fleet_jobs_failed_has_status CHECK ((state = 'failed') = (fail_status IS NOT NULL)),
    CONSTRAINT fleet_jobs_ended_has_finish CHECK ((state IN ('succeeded', 'failed')) = (finished_ms IS NOT NULL))
);
CREATE INDEX fleet_jobs_created ON fleet_jobs (created_ms DESC);
CREATE INDEX fleet_jobs_app ON fleet_jobs (tenant, elf_id, created_ms DESC);
CREATE INDEX fleet_jobs_active ON fleet_jobs (created_ms) WHERE state IN ('queued', 'running');
COMMENT ON TABLE fleet_jobs IS 'Proof jobs: instances of a proof app (a tenant''s program). Payloads are files in the data dir.';
COMMENT ON COLUMN fleet_jobs.id IS 'The SDK''s proof_id.';
COMMENT ON COLUMN fleet_jobs.tenant IS 'Empty for a request refused before its tenant was known.';
COMMENT ON COLUMN fleet_jobs.elf_id IS 'Hex SHA-256 of the guest ELF; empty for a request refused before it was read.';
COMMENT ON COLUMN fleet_jobs.fail_status IS 'The stage status a failed job ended with (what GetStatus answers).';
COMMENT ON COLUMN fleet_jobs.node IS 'Name of the node it runs or ran on (stable across node restarts).';
COMMENT ON COLUMN fleet_jobs.failed_on IS 'Nodes an attempt failed on; a retry avoids them.';
COMMENT ON COLUMN fleet_jobs.started_ms IS 'Unix ms the current (or last) attempt started; cleared when it is queued again.';

CREATE TABLE fleet_credentials (
    address    TEXT   PRIMARY KEY CONSTRAINT fleet_credentials_address_hex CHECK (address ~ '^[0-9a-f]{{40}}$'),
    tenant     TEXT   NOT NULL CONSTRAINT fleet_credentials_tenant_named CHECK (tenant <> ''),
    label      TEXT   NOT NULL DEFAULT '',
    created_ms BIGINT NOT NULL,
    revoked_ms BIGINT
);
COMMENT ON TABLE fleet_credentials IS 'Tenant signing keys: the SDK''s ZKM_PRIVATE_KEY maps to its address.';
COMMENT ON COLUMN fleet_credentials.address IS 'Lower-case hex address without 0x.';
"#
    )
}

/// The proof apps (schema 3): a tenant's registered programs. A job runs only for one
/// of them; `disabled_ms` is set while the app takes no new jobs.
fn apps() -> String {
    r#"
CREATE TABLE fleet_apps (
    tenant      TEXT   NOT NULL CONSTRAINT fleet_apps_tenant_named CHECK (tenant <> ''),
    elf_id      TEXT   NOT NULL CONSTRAINT fleet_apps_elf_id_hex CHECK (elf_id ~ '^[0-9a-f]{64}$'),
    created_ms  BIGINT NOT NULL,
    disabled_ms BIGINT,
    PRIMARY KEY (tenant, elf_id)
);
COMMENT ON TABLE fleet_apps IS 'Proof apps: a tenant''s registered programs (sha256 of the ELF). A job runs only for one of them; disabled_ms is set while the app takes no new jobs.';
"#
    .to_string()
}

/// Schema 2's table: a row only while an app was disabled (kept for the 1 -> 2 step).
fn apps_v2() -> String {
    r#"
CREATE TABLE fleet_apps (
    tenant      TEXT   NOT NULL,
    elf_id      TEXT   NOT NULL,
    disabled_ms BIGINT NOT NULL,
    PRIMARY KEY (tenant, elf_id)
);
"#
    .to_string()
}

/// Schema 2 into 3: the apps table rebuilt; every program that has had a job is
/// registered (from its first job), a disabled one stays disabled.
fn migrate_2_to_3() -> String {
    format!(
        r#"
ALTER TABLE fleet_apps RENAME TO fleet_apps_v2;
ALTER INDEX fleet_apps_pkey RENAME TO fleet_apps_v2_pkey;
{apps}
INSERT INTO fleet_apps (tenant, elf_id, created_ms, disabled_ms)
SELECT j.tenant, j.elf_id, min(j.created_ms), max(v.disabled_ms)
FROM fleet_jobs j LEFT JOIN fleet_apps_v2 v ON v.tenant = j.tenant AND v.elf_id = j.elf_id
WHERE j.tenant <> '' AND j.elf_id ~ '^[0-9a-f]{{64}}$'
GROUP BY j.tenant, j.elf_id;
DROP TABLE fleet_apps_v2;
"#,
        apps = apps()
    )
}

/// A script that ends by recording the version it brought the database to.
fn applying(body: String, version: i32) -> String {
    format!(
        "{body}\nINSERT INTO fleet_schema (version, applied_ms) \
         VALUES ({version}, (extract(epoch FROM clock_timestamp()) * 1000)::bigint);\n"
    )
}

// the scripts, made once (a query's text must outlive the connection's future)
static CREATE: std::sync::LazyLock<String> = std::sync::LazyLock::new(|| applying(schema(), SCHEMA_VERSION));
/// MIGRATIONS[v] takes a schema-v database to v + 1; a fresh one gets `schema()` at once.
static MIGRATIONS: std::sync::LazyLock<Vec<String>> = std::sync::LazyLock::new(|| {
    vec![applying(migrate_0_to_1(), 1), applying(apps_v2(), 2), applying(migrate_2_to_3(), 3)]
});

/// Schema 0 (numbers for state, stage status and kind; failed_on comma-joined) into 1:
/// both tables rebuilt from the old ones, so a migrated database has the same tables,
/// constraints and indexes as a new one (tests/pg.rs checks that, through every step).
fn migrate_0_to_1() -> String {
    let case = |col: &str, pairs: Vec<(i32, String)>, default: &str| {
        let arms: String = pairs.iter().map(|(n, name)| format!(" WHEN {n} THEN '{name}'")).collect();
        format!("CASE {col}{arms} ELSE {default} END")
    };
    let kind = case("kind", all::<ProofKind>().into_iter().map(|k| (k as i32, kind_name(k))).collect(), "'unspecified'");
    let status = case("status", all::<StageStatus>().into_iter().map(|s| (s as i32, status_name(s))).collect(), "'internal_error'");
    format!(
        r#"
ALTER TABLE fleet_jobs RENAME TO fleet_jobs_v0;
ALTER INDEX fleet_jobs_pkey RENAME TO fleet_jobs_v0_pkey;
DROP INDEX IF EXISTS fleet_jobs_incomplete;
ALTER TABLE fleet_credentials RENAME TO fleet_credentials_v0;
ALTER INDEX fleet_credentials_pkey RENAME TO fleet_credentials_v0_pkey;
{schema}
INSERT INTO fleet_jobs (id, tenant, elf_id, kind, state, fail_status, error, node, attempts, failed_on, cycles,
                        prove_ms, created_ms, started_ms, finished_ms)
SELECT id, tenant, elf_id, {kind},
       CASE state WHEN 0 THEN 'queued' WHEN 1 THEN 'running' WHEN 2 THEN 'succeeded' ELSE 'failed' END,
       CASE WHEN state >= 3 THEN {status} END,
       error, node, attempts,
       CASE WHEN failed_on = '' THEN '{{}}'::text[] ELSE string_to_array(failed_on, ',') END,
       cycles, prove_ms, created_ms, started_ms, finished_ms
FROM fleet_jobs_v0;
INSERT INTO fleet_credentials SELECT address, tenant, label, created_ms, revoked_ms FROM fleet_credentials_v0;
DROP TABLE fleet_jobs_v0;
DROP TABLE fleet_credentials_v0;
"#,
        schema = jobs_and_credentials()
    )
}

impl PgStore {
    pub async fn connect(url: &str) -> anyhow::Result<Self> {
        let pool = PgPoolOptions::new().max_connections(8).connect(url).await?;
        Self::migrate(&pool).await?;
        Ok(Self { pool })
    }

    /// Brings the database to SCHEMA_VERSION: creates the tables in a new database, runs
    /// the migrations an older one lacks, one after another. Each script is one query,
    /// which Postgres runs as one transaction; fleet_schema's key refuses a second
    /// scheduler doing the same step at once.
    async fn migrate(pool: &PgPool) -> anyhow::Result<()> {
        sqlx::raw_sql(
            "CREATE TABLE IF NOT EXISTS fleet_schema (version INT PRIMARY KEY, applied_ms BIGINT NOT NULL);
             COMMENT ON TABLE fleet_schema IS 'Schema versions applied to this database (the scheduler migrates on start).';",
        )
        .execute(pool)
        .await?;
        let have: Option<i32> = sqlx::query_scalar("SELECT max(version) FROM fleet_schema").fetch_one(pool).await?;
        let jobs_table: bool = sqlx::query_scalar("SELECT to_regclass('fleet_jobs') IS NOT NULL").fetch_one(pool).await?;
        let mut v = match have {
            Some(v) if v > SCHEMA_VERSION => {
                anyhow::bail!("the database has schema {v}, newer than this scheduler's {SCHEMA_VERSION}")
            }
            Some(v) => v,
            None if jobs_table => 0,
            None => {
                sqlx::raw_sql(CREATE.as_str()).execute(pool).await?;
                tracing::info!(schema = SCHEMA_VERSION, "database created");
                return Ok(());
            }
        };
        let from = v;
        while v < SCHEMA_VERSION {
            sqlx::raw_sql(MIGRATIONS[v as usize].as_str()).execute(pool).await?;
            v += 1;
        }
        if from != v {
            tracing::info!(from, to = v, "database schema migrated");
        }
        Ok(())
    }

    fn row_to_job(row: &sqlx::postgres::PgRow) -> anyhow::Result<Job> {
        Ok(Job {
            id: row.try_get("id")?,
            tenant: row.try_get("tenant")?,
            elf_id: row.try_get("elf_id")?,
            kind: kind_from_name(row.try_get("kind")?),
            state: JobState::from_names(row.try_get("state")?, row.try_get("fail_status")?)?,
            error: row.try_get("error")?,
            node: row.try_get("node")?,
            attempts: row.try_get::<i32, _>("attempts")? as u32,
            failed_on: row.try_get("failed_on")?,
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
        sqlx::query(
            "INSERT INTO fleet_jobs (id, tenant, elf_id, kind, state, fail_status, error, node, attempts, failed_on, \
             cycles, prove_ms, created_ms, started_ms, finished_ms) \
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15) \
             ON CONFLICT (id) DO UPDATE SET state=$5, fail_status=$6, error=$7, node=$8, attempts=$9, failed_on=$10, \
             cycles=$11, prove_ms=$12, started_ms=$14, finished_ms=$15",
        )
        .bind(&j.id)
        .bind(&j.tenant)
        .bind(&j.elf_id)
        .bind(kind_name(j.kind))
        .bind(j.state.name())
        .bind(j.state.fail_status())
        .bind(&j.error)
        .bind(&j.node)
        .bind(j.attempts as i32)
        .bind(j.failed_on.clone())
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
        let rows = sqlx::query("SELECT * FROM fleet_jobs WHERE state IN ('queued', 'running') ORDER BY created_ms")
            .fetch_all(&self.pool)
            .await?;
        rows.iter().map(Self::row_to_job).collect()
    }

    async fn recent(&self, f: &JobFilter, limit: usize) -> anyhow::Result<Vec<Job>> {
        let rows = sqlx::query(
            "SELECT * FROM fleet_jobs WHERE ($1::text IS NULL OR tenant = $1)
               AND ($2::text IS NULL OR starts_with(elf_id, $2)) AND (NOT $3 OR state IN ('queued', 'running'))
             ORDER BY created_ms DESC LIMIT $4",
        )
        .bind(&f.tenant)
        .bind(&f.elf)
        .bind(f.active)
        .bind(limit as i64)
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(Self::row_to_job).collect()
    }

    async fn apps(&self) -> anyhow::Result<Vec<ProofApp>> {
        const COUNTS: &str = "count(*) AS jobs, count(*) FILTER (WHERE state IN ('queued', 'running')) AS active, \
             count(*) FILTER (WHERE state = 'succeeded') AS succeeded, count(*) FILTER (WHERE state = 'failed') AS failed, \
             max(created_ms) AS last_created_ms, avg(prove_ms) FILTER (WHERE state = 'succeeded')::float8 AS mean_prove_ms";
        let sql = format!(
            "SELECT a.tenant, a.elf_id, a.created_ms, a.disabled_ms IS NOT NULL AS disabled, coalesce(j.jobs, 0) AS jobs, \
                    coalesce(j.active, 0) AS active, coalesce(j.succeeded, 0) AS succeeded, coalesce(j.failed, 0) AS failed, \
                    j.last_created_ms, j.mean_prove_ms \
             FROM fleet_apps a LEFT JOIN (SELECT tenant, elf_id, {COUNTS} FROM fleet_jobs GROUP BY tenant, elf_id) j \
                  ON j.tenant = a.tenant AND j.elf_id = a.elf_id \
             UNION ALL \
             SELECT j.tenant, j.elf_id, min(created_ms), false, {COUNTS} \
             FROM fleet_jobs j WHERE NOT EXISTS (SELECT 1 FROM fleet_apps a WHERE a.tenant = j.tenant AND a.elf_id = j.elf_id) \
             GROUP BY j.tenant, j.elf_id \
             ORDER BY last_created_ms DESC NULLS LAST, created_ms DESC"
        );
        let rows = sqlx::query(&sql).fetch_all(&self.pool).await?;
        rows.iter()
            .map(|r| {
                Ok(ProofApp {
                    tenant: r.try_get("tenant")?,
                    elf_id: r.try_get("elf_id")?,
                    created_ms: r.try_get::<i64, _>("created_ms")? as u64,
                    jobs: r.try_get::<i64, _>("jobs")? as u64,
                    active: r.try_get::<i64, _>("active")? as u64,
                    succeeded: r.try_get::<i64, _>("succeeded")? as u64,
                    failed: r.try_get::<i64, _>("failed")? as u64,
                    last_created_ms: r.try_get::<Option<i64>, _>("last_created_ms")?.unwrap_or(0) as u64,
                    mean_prove_ms: r.try_get("mean_prove_ms")?,
                    disabled: r.try_get("disabled")?,
                })
            })
            .collect()
    }

    async fn delete_app(&self, tenant: &str, elf_id: &str) -> anyhow::Result<Vec<String>> {
        let rows = sqlx::query(
            "WITH gone AS (DELETE FROM fleet_jobs WHERE tenant = $1 AND elf_id = $2 AND state IN ('succeeded', 'failed') RETURNING id), \
                  app AS (DELETE FROM fleet_apps WHERE tenant = $1 AND elf_id = $2) \
             SELECT id FROM gone",
        )
            .bind(tenant)
            .bind(elf_id)
            .fetch_all(&self.pool)
            .await?;
        rows.iter().map(|r| Ok(r.try_get("id")?)).collect()
    }

    async fn registered_apps(&self) -> anyhow::Result<Vec<(String, String, bool)>> {
        let rows = sqlx::query("SELECT tenant, elf_id, disabled_ms IS NOT NULL AS disabled FROM fleet_apps ORDER BY tenant, elf_id")
            .fetch_all(&self.pool)
            .await?;
        rows.iter().map(|r| Ok((r.try_get("tenant")?, r.try_get("elf_id")?, r.try_get("disabled")?))).collect()
    }

    async fn add_app(&self, tenant: &str, elf_id: &str, at_ms: u64) -> anyhow::Result<bool> {
        let r = sqlx::query("INSERT INTO fleet_apps (tenant, elf_id, created_ms) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING")
            .bind(tenant)
            .bind(elf_id)
            .bind(at_ms as i64)
            .execute(&self.pool)
            .await?;
        Ok(r.rows_affected() > 0)
    }

    async fn set_app_disabled(&self, tenant: &str, elf_id: &str, disabled: bool, at_ms: u64) -> anyhow::Result<()> {
        sqlx::query("UPDATE fleet_apps SET disabled_ms = CASE WHEN $3 THEN $4 ELSE NULL END WHERE tenant = $1 AND elf_id = $2")
            .bind(tenant)
            .bind(elf_id)
            .bind(disabled)
            .bind(at_ms as i64)
            .execute(&self.pool)
            .await?;
        Ok(())
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
