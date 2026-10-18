//! ProofJob metadata. New proof jobs reference app-owned storage; legacy payloads use blobs.

use std::collections::HashMap;

use async_trait::async_trait;
use parking_lot::Mutex;
use sqlx::{pool::PoolConnection, postgres::PgPoolOptions, PgPool, Postgres, Row};

use crate::quota::{Allocation, Quota};
use fleet_proto::fleet::v1::ProofKind;
use fleet_proto::stage::v1::Status as StageStatus;

pub use fleet_proto::model::v1::{ProofJob, ProofJobState, Namespace, App, Token};
pub use fleet_proto::models::{kind_name, status_name};

fn kind_from_name(n: &str) -> ProofKind {
    ProofKind::from_str_name(&format!("PROOF_KIND_{}", n.to_ascii_uppercase())).unwrap_or(ProofKind::Unspecified)
}

/// Every value of a proto enum, by its number from 0 up.
fn all<T: TryFrom<i32>>() -> Vec<T> {
    (0..).map_while(|i| T::try_from(i).ok()).collect()
}

pub fn normalize_address(a: &str) -> String {
    a.trim()
        .trim_start_matches("0x")
        .trim_start_matches("0X")
        .to_ascii_lowercase()
}

#[async_trait]
pub trait Store: Send + Sync {
    async fn upsert(&self, job: &ProofJob) -> anyhow::Result<()>;
    async fn get(&self, id: &str) -> anyhow::Result<Option<ProofJob>>;
    /// Queued and running jobs, oldest first, for recovery after a restart.
    async fn incomplete(&self) -> anyhow::Result<Vec<ProofJob>>;
    /// The newest jobs, of one tenant or all.
    async fn recent(&self, f: &ProofJobFilter, limit: usize) -> anyhow::Result<Vec<ProofJob>>;
    /// Every (tenant, program) that has had a job, most recently used first.
    async fn apps(&self) -> anyhow::Result<Vec<App>>;
    /// Deletes an app and its finished jobs; returns the jobs' ids.
    async fn delete_app(&self, namespace: &str, tenant: &str, elf_id: &str,
    ) -> anyhow::Result<Vec<String>>;
    /// Every registered app: (namespace, tenant, elf_id, disabled).
    async fn registered_apps(&self) -> anyhow::Result<Vec<(String, String, String, bool)>>;
    /// Registers an app; false when it already was.
    async fn add_app(&self, namespace: &str, tenant: &str, elf_id: &str, at_ms: u64, priority: i32,
    ) -> anyhow::Result<bool>;
    async fn set_app_priority(&self, namespace: &str, tenant: &str, elf_id: &str, priority: i32,
    ) -> anyhow::Result<()>;
    async fn set_app_disabled(&self, namespace: &str, tenant: &str, elf_id: &str, disabled: bool, at_ms: u64,
    ) -> anyhow::Result<()>;

    async fn add_token(&self, c: &Token) -> anyhow::Result<()>;
    /// Marks the token revoked; returns false when the address is unknown.
    async fn revoke_token(&self, id: &str, at_ms: u64) -> anyhow::Result<bool>;
    async fn tokens(&self) -> anyhow::Result<Vec<Token>>;
    async fn namespaces(&self) -> anyhow::Result<Vec<Namespace>>;
    async fn add_namespace(&self, namespace: &Namespace) -> anyhow::Result<()>;
    async fn suspend_namespace(&self, id: &str, at_ms: Option<u64>) -> anyhow::Result<()>;
    async fn namespace_job(&self, namespace: &str, tag: Option<&str>, client_id: &str,
    ) -> anyhow::Result<Option<ProofJob>>;
    async fn set_quota(&self, id: &str, quota: &Quota) -> anyhow::Result<()>;
    async fn set_tag_quota(&self, namespace: &str, tag: &str, quota: &Quota) -> anyhow::Result<()>;
    async fn allocations(&self) -> anyhow::Result<Vec<Allocation>>;
    async fn reserve(&self, allocation: &Allocation) -> anyhow::Result<()>;
    async fn release(&self, id: &str) -> anyhow::Result<()>;
}

pub use fleet_proto::model::v1::ProofJobFilter;

#[derive(Default)]
pub struct MemoryStore {
    allocations: Mutex<HashMap<String, Allocation>>,
    namespaces: Mutex<HashMap<String, Namespace>>,
    jobs: Mutex<HashMap<String, ProofJob>>,
    tokens: Mutex<HashMap<String, Token>>,
    /// (namespace, tenant, elf_id) -> (created_ms, disabled_ms, priority).
    apps: Mutex<HashMap<(String, String, String), (u64, Option<u64>, i32)>>,
}

#[async_trait]
impl Store for MemoryStore {
    async fn upsert(&self, job: &ProofJob) -> anyhow::Result<()> {
        job.validate()?;
        let mut jobs = self.jobs.lock();
        let mut saved = job.clone();
        if let Some(existing) = jobs.get(&job.id) {
            saved.owner = existing.owner.clone();
            saved.namespace = existing.namespace.clone();
            saved.client_id = existing.client_id.clone();
            saved.tag = existing.tag.clone();
            saved.tenant = existing.tenant.clone();
            saved.elf_id = existing.elf_id.clone();
            saved.kind = existing.kind;
            saved.priority = existing.priority;
            saved.created_ms = existing.created_ms;
            saved.inputs = existing.inputs.clone();
        }
        anyhow::ensure!(!jobs.values().any(|j| j.id != saved.id && j.namespace == saved.namespace
            && j.tag == saved.tag && j.client_id == saved.client_id), "proof_id already exists in this namespace and tag");
        jobs.insert(job.id.clone(), saved);
        Ok(())
    }

    async fn get(&self, id: &str) -> anyhow::Result<Option<ProofJob>> {
        Ok(self.jobs.lock().get(id).cloned())
    }

    async fn incomplete(&self) -> anyhow::Result<Vec<ProofJob>> {
        let mut v: Vec<ProofJob> = self
            .jobs
            .lock()
            .values()
            .filter(|j| !j.state().is_terminal())
            .cloned()
            .collect();
        v.sort_by_key(|j| j.created_ms);
        Ok(v)
    }

    async fn recent(&self, f: &ProofJobFilter, limit: usize) -> anyhow::Result<Vec<ProofJob>> {
        let mut v: Vec<ProofJob> = self.jobs.lock().values().filter(|j| f.matches(j)).cloned().collect();
        v.sort_by_key(|j| std::cmp::Reverse(j.created_ms));
        v.truncate(limit);
        Ok(v)
    }

    async fn apps(&self) -> anyhow::Result<Vec<App>> {
        let blank = |namespace: &str, tenant: &str, elf_id: &str, created_ms, disabled, priority| {
                App {
            namespace: namespace.into(), tenant: tenant.into(), elf_id: elf_id.into(), created_ms, jobs: 0, active: 0, succeeded: 0, failed: 0,
            last_created_ms: None, mean_prove_ms: None, disabled, priority,
        }
            };
        let mut apps: HashMap<(String, String, String), (App, f64)> = self
            .apps
            .lock()
            .iter()
            .map(|((n, t, e), (created, disabled, priority))| {
                ((n.clone(), t.clone(), e.clone()), (blank(n, t, e, *created, disabled.is_some(), *priority), 0.0),
                )
            })
            .collect();
        for j in self.jobs.lock().values() {
            let Some((a, prove_sum)) = apps.get_mut(&(j.namespace.clone(), j.tenant.clone(), j.elf_id.clone())) else {
                continue;
            };
            a.jobs += 1;
            a.last_created_ms = Some(a.last_created_ms.map_or(j.created_ms, |t| t.max(j.created_ms)),
            );
            match j.state() {
                ProofJobState::Queued | ProofJobState::Running => a.active += 1,
                ProofJobState::Succeeded => {
                    a.succeeded += 1;
                    *prove_sum += j.prove_ms as f64;
                }
                ProofJobState::Failed => a.failed += 1,
            }
        }
        let mut v: Vec<App> = apps
            .into_values()
            .map(|(mut a, sum)| {
                if a.succeeded > 0 {
                    a.mean_prove_ms = Some(sum / a.succeeded as f64);
                }
                a
            })
            .collect();
        v.sort_by_key(|a| std::cmp::Reverse((a.last_created_ms.unwrap_or(0), a.created_ms)));
        Ok(v)
    }

    async fn delete_app(&self, namespace: &str, tenant: &str, elf_id: &str,
    ) -> anyhow::Result<Vec<String>> {
        let mut jobs = self.jobs.lock();
        let ids: Vec<String> = jobs
            .values()
            .filter(|j| {
                j.namespace.as_str() == namespace && j.tenant == tenant && j.elf_id == elf_id && j.state().is_terminal()
            })
            .map(|j| j.id.clone())
            .collect();
        for id in &ids {
            jobs.remove(id);
        }
        self.apps.lock().remove(&(namespace.to_string(), tenant.to_string(), elf_id.to_string(),
        ));
        Ok(ids)
    }

    async fn registered_apps(&self) -> anyhow::Result<Vec<(String, String, String, bool)>> {
        let mut v: Vec<_> = self.apps.lock().iter().map(|((n, t, e), (_, d, _))| (n.clone(), t.clone(), e.clone(), d.is_some())).collect();
        v.sort();
        Ok(v)
    }

    async fn add_app(&self, namespace: &str, tenant: &str, elf_id: &str, at_ms: u64, priority: i32,
    ) -> anyhow::Result<bool> {
        let mut apps = self.apps.lock();
        let key = (namespace.to_string(), tenant.to_string(), elf_id.to_string(),
        );
        if apps.contains_key(&key) {
            return Ok(false);
        }
        apps.insert(key, (at_ms, None, priority));
        Ok(true)
    }

    async fn set_app_priority(&self, namespace: &str, tenant: &str, elf_id: &str, priority: i32,
    ) -> anyhow::Result<()> {
        if let Some((_, _, p)) = self.apps.lock().get_mut(&(namespace.into(), tenant.into(), elf_id.into())) { *p = priority; }
        Ok(())
    }

    async fn set_app_disabled(&self, namespace: &str, tenant: &str, elf_id: &str, disabled: bool, at_ms: u64,
    ) -> anyhow::Result<()> {
        if let Some((_, d, _)) = self.apps.lock().get_mut(&(namespace.to_string(), tenant.to_string(), elf_id.to_string(),
        )) {
            *d = disabled.then_some(at_ms);
        }
        Ok(())
    }

    async fn add_token(&self, c: &Token) -> anyhow::Result<()> {
        let mut tokens = self.tokens.lock();
        anyhow::ensure!(
            !tokens.contains_key(&c.address),
            "address already registered"
        );
        anyhow::ensure!(c.token_hash.as_ref().is_none_or(|hash| !tokens.values().any(|other| other.token_hash.as_ref() == Some(hash))), "token already registered");
        anyhow::ensure!(!tokens.values().any(|t| t.id == c.id), "token id already registered");
        tokens.insert(c.address.clone(), c.clone());
        Ok(())
    }

    async fn revoke_token(&self, id: &str, at_ms: u64) -> anyhow::Result<bool> {
        let mut tokens = self.tokens.lock();
        let token = tokens.values_mut().find(|t| t.id == id);
        Ok(token.map(|t| t.revoked_ms = Some(at_ms)).is_some())
    }

    async fn set_quota(&self, id: &str, quota: &Quota) -> anyhow::Result<()> {
        if let Some(n) = self.namespaces.lock().get_mut(id) { n.quota = quota.clone(); }
        Ok(())
    }

    async fn set_tag_quota(&self, namespace: &str, tag: &str, quota: &Quota) -> anyhow::Result<()> {
        if let Some(n) = self.namespaces.lock().get_mut(namespace) {
            n.tag_quotas.insert(tag.into(), quota.clone());
        }
        Ok(())
    }

    async fn allocations(&self) -> anyhow::Result<Vec<Allocation>> {
        Ok(self.allocations.lock().values().cloned().collect())
    }

    async fn reserve(&self, a: &Allocation) -> anyhow::Result<()> {
        let mut all = self.allocations.lock();
        anyhow::ensure!(all.get(&a.id).is_none_or(|old| old.namespace == a.namespace && old.tag == a.tag), "allocation belongs to another namespace");
        all.insert(a.id.clone(), a.clone());
        Ok(())
    }

    async fn release(&self, id: &str) -> anyhow::Result<()> {
        self.allocations.lock().remove(id);
        Ok(())
    }

    async fn namespaces(&self) -> anyhow::Result<Vec<Namespace>> {
        let mut namespaces = self.namespaces.lock();
        namespaces.entry(SYSTEM_NAMESPACE.into()).or_insert_with(|| Namespace { id: SYSTEM_NAMESPACE.into(), name: "system".into(), created_ms: 0, suspended_ms: None, quota: Default::default(), tag_quotas: Default::default(),
            });
        let mut all: Vec<_> = namespaces.values().cloned().collect();
        all.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(all)
    }

    async fn add_namespace(&self, n: &Namespace) -> anyhow::Result<()> {
        let mut all = self.namespaces.lock();
        anyhow::ensure!(!all.contains_key(&n.id) && !all.values().any(|v| v.name == n.name), "namespace already exists");
        all.insert(n.id.clone(), n.clone());
        Ok(())
    }

    async fn suspend_namespace(&self, id: &str, at_ms: Option<u64>) -> anyhow::Result<()> {
        if let Some(n) = self.namespaces.lock().get_mut(id) { n.suspended_ms = at_ms; }
        Ok(())
    }

    async fn namespace_job(&self, namespace: &str, tag: Option<&str>, client_id: &str,
    ) -> anyhow::Result<Option<ProofJob>> {
        Ok(self.jobs.lock().values().find(|j| {
                j.namespace == namespace
            && j.tag == tag.unwrap_or("") && j.client_id == client_id
            }).cloned())
    }

    async fn tokens(&self) -> anyhow::Result<Vec<Token>> {
        let mut v: Vec<Token> = self.tokens.lock().values().cloned().collect();
        v.sort_by_key(|c| c.created_ms);
        Ok(v)
    }
}

pub struct PgStore {
    pool: PgPool,
}

/// Holds a session lock for the lifetime of the active scheduler.
pub struct PgLeaderLock {
    _connection: PoolConnection<Postgres>,
}

/// The reserved namespace for operator-managed apps.
pub const SYSTEM_NAMESPACE: &str = "00000000000000000000000000000000";
/// The schema this scheduler writes; `fleet_schema` records what a database has.
const SCHEMA_VERSION: i32 = 14;
const TAG_PATTERN: &str = "^[A-Za-z][A-Za-z0-9_.-]{0,62}=[A-Za-z0-9_.-]{1,63}$";

fn quoted(names: impl IntoIterator<Item = String>) -> String {
    names.into_iter().map(|n| format!("'{n}'")).collect::<Vec<_>>().join(", ")
}

/// Create the current schema directly; development databases are reset, not backfilled.
fn schema() -> String {
    format!("{}{}{}", namespace_schema(), jobs_and_tokens(), apps())
}

fn namespace_schema() -> String {
    format!(r#"
CREATE TABLE fleet_namespaces (
    id text PRIMARY KEY CHECK(id ~ '^[0-9a-f]{{32}}$'),
    name text NOT NULL UNIQUE CHECK(name ~ '^[a-z][a-z0-9-]{{0,62}}$'),
    created_ms bigint NOT NULL, suspended_ms bigint,
    quota jsonb NOT NULL DEFAULT '{{"gpus":null,"cpus":null}}',
    tag_quotas jsonb NOT NULL DEFAULT '{{}}'
);
INSERT INTO fleet_namespaces(id,name,created_ms) VALUES ('{SYSTEM_NAMESPACE}','system',0);
CREATE TABLE fleet_namespace_allocations (
    id text PRIMARY KEY, namespace text NOT NULL REFERENCES fleet_namespaces(id),
    tag text CONSTRAINT fleet_namespace_allocations_tag_valid CHECK(tag IS NULL OR tag ~ '{TAG_PATTERN}'),
    gpus double precision NOT NULL CHECK(gpus >= 0 AND gpus < 'Infinity'::float8),
    cpus double precision NOT NULL CHECK(cpus >= 0 AND cpus < 'Infinity'::float8)
);
CREATE INDEX fleet_namespace_allocations_namespace ON fleet_namespace_allocations(namespace);
"#)
}

/// Jobs and tokens. Names, not numbers: a state, kind or stage status
/// reads the same in the database as in the admin API, and the CHECKs list what may be
/// there.
fn jobs_and_tokens() -> String {
    let kinds = quoted(all::<ProofKind>().into_iter().map(kind_name));
    let statuses = quoted(all::<StageStatus>().into_iter().map(status_name));
    let states = quoted(all::<ProofJobState>().into_iter().map(|s| s.name().to_owned()));
    format!(
        r#"
CREATE TABLE fleet_jobs (
    id          TEXT    PRIMARY KEY CONSTRAINT fleet_jobs_id_safe CHECK(id ~ '^[A-Za-z0-9_-]{{1,128}}$'),
    owner       TEXT    NOT NULL CONSTRAINT fleet_jobs_owner_named CHECK(owner ~ '^[0-9a-f]{{40}}$'),
    namespace   TEXT    NOT NULL REFERENCES fleet_namespaces(id),
    tag         TEXT    NOT NULL CONSTRAINT fleet_jobs_tag_scope CHECK(tag = '' OR tag ~ '{TAG_PATTERN}'),
    client_id   TEXT    NOT NULL CONSTRAINT fleet_jobs_client_id_safe CHECK(client_id ~ '^[A-Za-z0-9_-]{{1,128}}$'),
    priority    INT     NOT NULL DEFAULT 0,
    tenant      TEXT    NOT NULL,
    elf_id      TEXT    NOT NULL,
    inputs      JSONB,
    outputs     JSONB,
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
CREATE UNIQUE INDEX fleet_jobs_namespace_client ON fleet_jobs(namespace,tag,client_id);
CREATE INDEX fleet_jobs_namespace_created ON fleet_jobs(namespace,created_ms DESC);
CREATE INDEX fleet_jobs_owner ON fleet_jobs(owner,created_ms DESC);
CREATE INDEX fleet_jobs_created ON fleet_jobs (created_ms DESC);
CREATE INDEX fleet_jobs_app ON fleet_jobs (tenant, elf_id, created_ms DESC);
CREATE INDEX fleet_jobs_active ON fleet_jobs (created_ms) WHERE state IN ('queued', 'running');
COMMENT ON TABLE fleet_jobs IS 'Proof jobs: instances of a proof app (a tenant''s program). Payloads are files in the data dir.';
COMMENT ON COLUMN fleet_jobs.id IS 'Internal proof ID; namespaced requests retain the SDK ID in client_id.';
COMMENT ON COLUMN fleet_jobs.owner IS 'Authenticated submitting address; immutable audit identity.';
COMMENT ON COLUMN fleet_jobs.namespace IS 'Immutable namespace ID; resource accounting and access scope.';
COMMENT ON COLUMN fleet_jobs.tag IS 'Immutable optional isolation label; empty string means untagged.';
COMMENT ON COLUMN fleet_jobs.client_id IS 'Required SDK proof_id; immutable within namespace and tag.';
COMMENT ON COLUMN fleet_jobs.tenant IS 'Empty for a request refused before its tenant was known.';
COMMENT ON COLUMN fleet_jobs.elf_id IS 'Hex SHA-256 of the guest ELF; empty for a request refused before it was read.';
COMMENT ON COLUMN fleet_jobs.fail_status IS 'The stage status a failed job ended with (what GetStatus answers).';
COMMENT ON COLUMN fleet_jobs.node IS 'Name of the node it jobs or ran on (stable across node restarts).';
COMMENT ON COLUMN fleet_jobs.failed_on IS 'Nodes an attempt failed on; a retry avoids them.';
COMMENT ON COLUMN fleet_jobs.started_ms IS 'Unix ms the current (or last) attempt started; cleared when it is queued again.';

CREATE TABLE fleet_tokens (
    id         TEXT   PRIMARY KEY CONSTRAINT fleet_tokens_id_hex CHECK (id ~ '^[0-9a-f]{{32}}$'),
    address    TEXT   NOT NULL UNIQUE CONSTRAINT fleet_tokens_address_hex CHECK (address ~ '^[0-9a-f]{{40}}$'),
    tenant     TEXT   NOT NULL CONSTRAINT fleet_tokens_tenant_named CHECK (tenant <> ''),
    label      TEXT   NOT NULL DEFAULT '',
    created_ms BIGINT NOT NULL,
    revoked_ms BIGINT,
    namespace  TEXT REFERENCES fleet_namespaces(id),
    tag        TEXT CONSTRAINT fleet_tokens_tag_scope CHECK(tag IS NULL OR (namespace IS NOT NULL AND tag ~ '{TAG_PATTERN}')),
    apps       TEXT[],
    token_hash TEXT UNIQUE,
    manage_apps BOOLEAN NOT NULL DEFAULT FALSE
);
COMMENT ON TABLE fleet_tokens IS 'Administrator-managed API tokens with optional namespace, tag and app scope; the address binds the token to the SDK proof signer.';
COMMENT ON COLUMN fleet_tokens.id IS 'Opaque token resource identifier; unrelated to proof signing identity.';
COMMENT ON COLUMN fleet_tokens.address IS 'Internal SDK proof signer binding; not exposed by token APIs.';
"#
    )
}

/// The proof apps: a tenant's registered programs. A job executes only for one
/// of them; `disabled_ms` is set while the app takes no new jobs.
fn apps() -> String {
    r#"
CREATE TABLE fleet_apps (
    priority    INT    NOT NULL DEFAULT 0,
    namespace   TEXT   NOT NULL REFERENCES fleet_namespaces(id),
    tenant      TEXT   NOT NULL CONSTRAINT fleet_apps_tenant_named CHECK (tenant <> ''),
    elf_id      TEXT   NOT NULL CONSTRAINT fleet_apps_elf_id_hex CHECK (elf_id ~ '^[0-9a-f]{64}$'),
    created_ms  BIGINT NOT NULL,
    disabled_ms BIGINT,
    PRIMARY KEY (namespace, tenant, elf_id)
);
COMMENT ON TABLE fleet_apps IS 'Proof apps: a tenant''s registered programs (sha256 of the ELF). A job executes only for one of them; disabled_ms is set while the app takes no new jobs.';
"#
    .to_string()
}

impl PgStore {
    pub async fn connect(url: &str) -> anyhow::Result<Self> {
        let pool = PgPoolOptions::new().max_connections(8).connect(url).await?;
        Self::initialize(&pool).await?;
        Ok(Self { pool })
    }

    pub async fn acquire_leader_lock(&self) -> anyhow::Result<PgLeaderLock> {
        const LOCK_KEY: i64 = 0x5a4b_4d46_4c45_4554;
        let mut connection = self.pool.acquire().await?;
        let acquired: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock($1)")
            .bind(LOCK_KEY).fetch_one(&mut *connection).await?;
        anyhow::ensure!(acquired, "another fleet-scheduler already holds the database leader lock");
        Ok(PgLeaderLock { _connection: connection,
        })
    }

    /// Initialize a fresh database or verify the current schema. Old development
    /// databases require an explicit reset; startup never invents job identities.
    async fn initialize(pool: &PgPool) -> anyhow::Result<()> {
        sqlx::raw_sql(
            "CREATE TABLE IF NOT EXISTS fleet_schema (version INT PRIMARY KEY, applied_ms BIGINT NOT NULL);
             COMMENT ON TABLE fleet_schema IS 'The current database schema version.';",
        ).execute(pool).await?;
        let have: Option<i32> = sqlx::query_scalar("SELECT max(version) FROM fleet_schema").fetch_one(pool).await?;
        let jobs_table: bool = sqlx::query_scalar("SELECT to_regclass('fleet_jobs') IS NOT NULL").fetch_one(pool).await?;
        match (have, jobs_table) {
            (Some(SCHEMA_VERSION), true) => Ok(()),
            (None, false) => {
                let create = format!("{}\nINSERT INTO fleet_schema (version, applied_ms) VALUES ({SCHEMA_VERSION}, (extract(epoch FROM clock_timestamp()) * 1000)::bigint);", schema());
                sqlx::raw_sql(&create).execute(pool).await?;
                tracing::info!(schema = SCHEMA_VERSION, "database created");
                Ok(())
            }
            _ => anyhow::bail!("unsupported database schema {have:?}; reset the database before starting this scheduler (schema {SCHEMA_VERSION})"),
        }
    }

    fn row_to_job(row: &sqlx::postgres::PgRow) -> anyhow::Result<ProofJob> {
        Ok(ProofJob {
            inputs: row.try_get::<Option<serde_json::Value>, _>("inputs")?.map(serde_json::from_value).transpose()?,
            outputs: row.try_get::<Option<serde_json::Value>, _>("outputs")?.map(serde_json::from_value).transpose()?,
            id: row.try_get("id")?,
            tenant: row.try_get("tenant")?,
            owner: row.try_get("owner")?,
            namespace: row.try_get("namespace")?,
            tag: row.try_get("tag")?,
            client_id: row.try_get("client_id")?,
            elf_id: row.try_get("elf_id")?,
            priority: row.try_get("priority")?,
            kind: kind_from_name(row.try_get("kind")?) as i32,
            state: ProofJobState::from_name(row.try_get("state")?)? as i32,
            fail_status: row.try_get::<Option<&str>, _>("fail_status")?.map(|name| {
                StageStatus::from_str_name(&name.to_ascii_uppercase())
                    .unwrap_or(StageStatus::InternalError) as i32
            }),
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
    async fn upsert(&self, j: &ProofJob) -> anyhow::Result<()> {
        j.validate()?;
        sqlx::query(
            "INSERT INTO fleet_jobs (id, tenant, elf_id, kind, state, fail_status, error, node, attempts, failed_on, \
             cycles, prove_ms, created_ms, started_ms, finished_ms, owner, namespace, client_id, tag, priority, inputs, outputs) \
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22) \
             ON CONFLICT (id) DO UPDATE SET state=$5, fail_status=$6, error=$7, node=$8, attempts=$9, failed_on=$10, \
             cycles=$11, prove_ms=$12, started_ms=$14, finished_ms=$15, outputs=$22",
        )
        .bind(&j.id)
        .bind(&j.tenant)
        .bind(&j.elf_id)
        .bind(kind_name(j.kind()))
        .bind(j.state().name())
        .bind(j.failure_name())
        .bind(&j.error)
        .bind(&j.node)
        .bind(j.attempts as i32)
        .bind(j.failed_on.clone())
        .bind(j.cycles as i64)
        .bind(j.prove_ms as i64)
        .bind(j.created_ms as i64)
        .bind(j.started_ms.map(|v| v as i64))
        .bind(j.finished_ms.map(|v| v as i64))
        .bind(&j.owner)
        .bind(&j.namespace)
        .bind(&j.client_id)
        .bind(&j.tag)
        .bind(j.priority)
        .bind(j.inputs.as_ref().map(serde_json::to_value).transpose()?)
        .bind(j.outputs.as_ref().map(serde_json::to_value).transpose()?)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn get(&self, id: &str) -> anyhow::Result<Option<ProofJob>> {
        let row = sqlx::query("SELECT * FROM fleet_jobs WHERE id = $1")
            .bind(id)
            .fetch_optional(&self.pool)
            .await?;
        row.as_ref().map(Self::row_to_job).transpose()
    }

    async fn incomplete(&self) -> anyhow::Result<Vec<ProofJob>> {
        let rows = sqlx::query("SELECT * FROM fleet_jobs WHERE state IN ('queued', 'running') ORDER BY created_ms",
        )
            .fetch_all(&self.pool)
            .await?;
        rows.iter().map(Self::row_to_job).collect()
    }

    async fn recent(&self, f: &ProofJobFilter, limit: usize) -> anyhow::Result<Vec<ProofJob>> {
        let rows = sqlx::query(
            "SELECT * FROM fleet_jobs WHERE ($1::text IS NULL OR tenant = $1)
               AND ($2::text IS NULL OR starts_with(elf_id, $2)) AND (NOT $3 OR state IN ('queued', 'running'))
             AND ($5::text IS NULL OR owner = $5) AND ($6::text IS NULL OR namespace = $6) AND ($7::text IS NULL OR tag = $7) ORDER BY created_ms DESC LIMIT $4",
        )
        .bind(&f.tenant)
        .bind(&f.elf)
        .bind(f.active)
        .bind(i64::try_from(limit).unwrap_or(i64::MAX))
        .bind(&f.owner)
        .bind(&f.namespace)
        .bind(&f.tag)
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(Self::row_to_job).collect()
    }

    async fn apps(&self) -> anyhow::Result<Vec<App>> {
        const COUNTS: &str = "count(*) AS jobs, count(*) FILTER (WHERE state IN ('queued', 'running')) AS active, \
             count(*) FILTER (WHERE state = 'succeeded') AS succeeded, count(*) FILTER (WHERE state = 'failed') AS failed, \
             max(created_ms) AS last_created_ms, avg(prove_ms) FILTER (WHERE state = 'succeeded')::float8 AS mean_prove_ms";
        let sql = format!(
            "SELECT a.namespace, a.tenant, a.elf_id, a.created_ms, a.priority, a.disabled_ms IS NOT NULL AS disabled, coalesce(j.jobs, 0) AS jobs, \
                    coalesce(j.active, 0) AS active, coalesce(j.succeeded, 0) AS succeeded, coalesce(j.failed, 0) AS failed, \
                    j.last_created_ms, j.mean_prove_ms \
             FROM fleet_apps a LEFT JOIN (SELECT namespace, tenant, elf_id, {COUNTS} FROM fleet_jobs GROUP BY namespace, tenant, elf_id) j \
                  ON j.namespace = a.namespace AND j.tenant = a.tenant AND j.elf_id = a.elf_id \
             ORDER BY last_created_ms DESC NULLS LAST, created_ms DESC"
        );
        let rows = sqlx::query(&sql).fetch_all(&self.pool).await?;
        rows.iter()
            .map(|r| {
                Ok(App {
                    priority: r.try_get("priority")?,
                    namespace: r.try_get("namespace")?,
                    tenant: r.try_get("tenant")?,
                    elf_id: r.try_get("elf_id")?,
                    created_ms: r.try_get::<i64, _>("created_ms")? as u64,
                    jobs: r.try_get::<i64, _>("jobs")? as u64,
                    active: r.try_get::<i64, _>("active")? as u64,
                    succeeded: r.try_get::<i64, _>("succeeded")? as u64,
                    failed: r.try_get::<i64, _>("failed")? as u64,
                    last_created_ms: r.try_get::<Option<i64>, _>("last_created_ms")?.map(|v| v as u64),
                    mean_prove_ms: r.try_get("mean_prove_ms")?,
                    disabled: r.try_get("disabled")?,
                })
            })
            .collect()
    }

    async fn delete_app(&self, namespace: &str, tenant: &str, elf_id: &str,
    ) -> anyhow::Result<Vec<String>> {
        let rows = sqlx::query(
            "WITH gone AS (DELETE FROM fleet_jobs WHERE tenant = $1 AND elf_id = $2 AND namespace = $3 AND state IN ('succeeded', 'failed') RETURNING id), \
                  app AS (DELETE FROM fleet_apps WHERE tenant = $1 AND elf_id = $2 AND namespace = $3) \
             SELECT id FROM gone",
        )
            .bind(tenant)
            .bind(elf_id)
            .bind(namespace)
            .fetch_all(&self.pool)
            .await?;
        rows.iter().map(|r| Ok(r.try_get("id")?)).collect()
    }

    async fn registered_apps(&self) -> anyhow::Result<Vec<(String, String, String, bool)>> {
        let rows = sqlx::query("SELECT namespace, tenant, elf_id, disabled_ms IS NOT NULL AS disabled FROM fleet_apps ORDER BY namespace, tenant, elf_id")
            .fetch_all(&self.pool)
            .await?;
        rows.iter().map(|r| {
                Ok((r.try_get("namespace")?, r.try_get("tenant")?, r.try_get("elf_id")?, r.try_get("disabled")?,
                ))
            }).collect()
    }

    async fn add_app(&self, namespace: &str, tenant: &str, elf_id: &str, at_ms: u64, priority: i32,
    ) -> anyhow::Result<bool> {
        let r = sqlx::query("INSERT INTO fleet_apps (tenant, elf_id, created_ms, namespace, priority) VALUES ($1, $2, $3, $4, $5) ON CONFLICT DO NOTHING")
            .bind(tenant)
            .bind(elf_id)
            .bind(at_ms as i64)
            .bind(namespace)
            .bind(priority)
            .execute(&self.pool)
            .await?;
        Ok(r.rows_affected() > 0)
    }

    async fn set_app_priority(&self, namespace: &str, tenant: &str, elf_id: &str, priority: i32,
    ) -> anyhow::Result<()> {
        sqlx::query("UPDATE fleet_apps SET priority=$4 WHERE namespace=$1 AND tenant=$2 AND elf_id=$3",
        )
            .bind(namespace).bind(tenant).bind(elf_id).bind(priority).execute(&self.pool).await?;
        Ok(())
    }

    async fn set_app_disabled(&self, namespace: &str, tenant: &str, elf_id: &str, disabled: bool, at_ms: u64,
    ) -> anyhow::Result<()> {
        sqlx::query("UPDATE fleet_apps SET disabled_ms = CASE WHEN $3 THEN $4 ELSE NULL END WHERE tenant = $1 AND elf_id = $2 AND namespace = $5")
            .bind(tenant)
            .bind(elf_id)
            .bind(disabled)
            .bind(at_ms as i64)
            .bind(namespace)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn add_token(&self, c: &Token) -> anyhow::Result<()> {
        sqlx::query("INSERT INTO fleet_tokens (id, address, tenant, label, created_ms, apps, token_hash, namespace, tag, manage_apps) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)")
            .bind(&c.id)
            .bind(&c.address)
            .bind(&c.tenant)
            .bind(&c.label)
            .bind(c.created_ms as i64)
            .bind(c.apps.as_ref().map(|apps| &apps.values))
            .bind(&c.token_hash)
            .bind(&c.namespace)
            .bind(&c.tag)
            .bind(c.manage_apps)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn revoke_token(&self, id: &str, at_ms: u64) -> anyhow::Result<bool> {
        let r = sqlx::query("UPDATE fleet_tokens SET revoked_ms = $2 WHERE id = $1")
            .bind(id)
            .bind(at_ms as i64)
            .execute(&self.pool)
            .await?;
        Ok(r.rows_affected() > 0)
    }

    async fn set_quota(&self, id: &str, quota: &Quota) -> anyhow::Result<()> {
        sqlx::query("UPDATE fleet_namespaces SET quota = $2 WHERE id = $1")
            .bind(id).bind(serde_json::to_value(quota)?).execute(&self.pool).await?;
        Ok(())
    }

    async fn set_tag_quota(&self, namespace: &str, tag: &str, quota: &Quota) -> anyhow::Result<()> {
        sqlx::query("UPDATE fleet_namespaces SET tag_quotas=jsonb_set(tag_quotas,ARRAY[$2]::text[],$3) WHERE id=$1")
            .bind(namespace).bind(tag).bind(serde_json::to_value(quota)?).execute(&self.pool).await?;
        Ok(())
    }

    async fn allocations(&self) -> anyhow::Result<Vec<Allocation>> {
        let rows = sqlx::query("SELECT * FROM fleet_namespace_allocations").fetch_all(&self.pool).await?;
        rows.iter().map(|r| {
                Ok(Allocation { id: r.try_get("id")?, namespace: r.try_get("namespace")?, tag: r.try_get("tag")?,
            gpus: r.try_get("gpus")?, cpus: r.try_get("cpus")?,
                })
            }).collect()
    }

    async fn reserve(&self, a: &Allocation) -> anyhow::Result<()> {
        let r = sqlx::query("INSERT INTO fleet_namespace_allocations (id,namespace,gpus,cpus,tag) VALUES ($1,$2,$3,$4,$5)
            ON CONFLICT (id) DO UPDATE SET gpus=EXCLUDED.gpus, cpus=EXCLUDED.cpus
            WHERE fleet_namespace_allocations.namespace = EXCLUDED.namespace AND fleet_namespace_allocations.tag IS NOT DISTINCT FROM EXCLUDED.tag")
            .bind(&a.id).bind(&a.namespace).bind(a.gpus).bind(a.cpus).bind(&a.tag).execute(&self.pool).await?;
        anyhow::ensure!(r.rows_affected() == 1, "allocation belongs to another namespace");
        Ok(())
    }

    async fn release(&self, id: &str) -> anyhow::Result<()> {
        sqlx::query("DELETE FROM fleet_namespace_allocations WHERE id = $1").bind(id).execute(&self.pool).await?;
        Ok(())
    }

    async fn namespaces(&self) -> anyhow::Result<Vec<Namespace>> {
        let rows = sqlx::query("SELECT * FROM fleet_namespaces ORDER BY name").fetch_all(&self.pool).await?;
        rows.iter().map(|r| {
                Ok(Namespace { tag_quotas: serde_json::from_value(r.try_get("tag_quotas")?)?, quota: serde_json::from_value(r.try_get("quota")?)?, id: r.try_get("id")?, name: r.try_get("name")?,
            created_ms: r.try_get::<i64, _>("created_ms")? as u64,
            suspended_ms: r.try_get::<Option<i64>, _>("suspended_ms")?.map(|v| v as u64),
                })
            }).collect()
    }

    async fn add_namespace(&self, n: &Namespace) -> anyhow::Result<()> {
        sqlx::query("INSERT INTO fleet_namespaces (id,name,created_ms,suspended_ms,quota,tag_quotas) VALUES ($1,$2,$3,$4,$5,$6)")
            .bind(&n.id).bind(&n.name).bind(n.created_ms as i64).bind(n.suspended_ms.map(|v|v as i64))
            .bind(serde_json::to_value(&n.quota)?).bind(serde_json::to_value(&n.tag_quotas)?).execute(&self.pool).await?;
        Ok(())
    }

    async fn suspend_namespace(&self, id: &str, at_ms: Option<u64>) -> anyhow::Result<()> {
        sqlx::query("UPDATE fleet_namespaces SET suspended_ms = $2 WHERE id = $1")
            .bind(id).bind(at_ms.map(|v| v as i64)).execute(&self.pool).await?;
        Ok(())
    }

    async fn namespace_job(&self, namespace: &str, tag: Option<&str>, client_id: &str,
    ) -> anyhow::Result<Option<ProofJob>> {
        let row = sqlx::query("SELECT * FROM fleet_jobs WHERE namespace = $1 AND tag = $3 AND client_id = $2",
        )
            .bind(namespace).bind(client_id).bind(tag.unwrap_or("")).fetch_optional(&self.pool).await?;
        row.as_ref().map(Self::row_to_job).transpose()
    }

    async fn tokens(&self) -> anyhow::Result<Vec<Token>> {
        let rows = sqlx::query("SELECT * FROM fleet_tokens ORDER BY created_ms")
            .fetch_all(&self.pool)
            .await?;
        rows.iter()
            .map(|r| {
                Ok(Token {
                    id: r.try_get("id")?,
                    address: r.try_get("address")?,
                    tenant: r.try_get("tenant")?,
                    label: r.try_get("label")?,
                    created_ms: r.try_get::<i64, _>("created_ms")? as u64,
                    revoked_ms: r.try_get::<Option<i64>, _>("revoked_ms")?.map(|v| v as u64),
                    apps: r.try_get::<Option<Vec<String>>, _>("apps")?.map(Into::into),
                    token_hash: r.try_get("token_hash")?,
                    namespace: r.try_get("namespace")?,
                    tag: r.try_get("tag")?,
                    manage_apps: r.try_get("manage_apps")?,
                })
            })
            .collect()
    }
}
