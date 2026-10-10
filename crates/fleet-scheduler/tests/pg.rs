//! The Postgres store against a real database: FLEET_TEST_PG_URL (a database the test
//! may fill: it makes and drops the schemas `fresh` and `incompatible`). Skipped without it.

use fleet_scheduler::store::SYSTEM_NAMESPACE;
use fleet_proto::fleet::v1::ProofKind;
use fleet_proto::stage::v1::Status as StageStatus;
use fleet_scheduler::store::{Token, Job, JobFilter, JobState, Namespace, PgStore, Store};
use sqlx::{Executor, PgPool, Row};
use fleet_scheduler::quota::{Allocation, Quota};

/// An incomplete development schema must be reset, never assigned invented identities.
const INCOMPATIBLE_SCHEMA: &str = r#"
CREATE TABLE fleet_schema (version INT PRIMARY KEY, applied_ms BIGINT NOT NULL);
INSERT INTO fleet_schema VALUES(9, 0);
CREATE TABLE fleet_jobs (id TEXT PRIMARY KEY, owner TEXT);
INSERT INTO fleet_jobs VALUES('unowned', NULL);
"#;

fn url() -> Option<String> {
    std::env::var("FLEET_TEST_PG_URL").ok()
}

fn in_schema(url: &str, schema: &str) -> String {
    let sep = if url.contains('?') { '&' } else { '?' };
    format!("{url}{sep}options=-c%20search_path%3D{schema}")
}

async fn fresh_schema(admin: &PgPool, name: &str) {
    admin.execute(format!("DROP SCHEMA IF EXISTS {name} CASCADE; CREATE SCHEMA {name}").as_str()).await.unwrap();
}

/// Columns, constraints and indexes of a schema's tables, without the schema's name.
async fn ddl(admin: &PgPool, schema: &str) -> Vec<String> {
    let q = format!(
        "SELECT 'col ' || table_name || '.' || column_name || ' ' || data_type || ' ' || is_nullable || ' ' ||
                coalesce(column_default, '-') AS d
           FROM information_schema.columns WHERE table_schema = '{schema}'
         UNION ALL
         SELECT 'con ' || conrelid::regclass::text || ' ' || conname || ' ' || pg_get_constraintdef(oid)
           FROM pg_constraint WHERE connamespace = '{schema}'::regnamespace
         UNION ALL
         SELECT 'idx ' || indexname || ' ' || indexdef FROM pg_indexes WHERE schemaname = '{schema}'
         UNION ALL
         SELECT 'cmt ' || c.relname || '.' || coalesce(a.attname, '') || ' ' || d.description
           FROM pg_description d JOIN pg_class c ON c.oid = d.objoid
           LEFT JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = d.objsubid AND d.objsubid > 0
          WHERE c.relnamespace = '{schema}'::regnamespace AND c.relname <> 'fleet_schema'
         ORDER BY 1"
    );
    let rows = sqlx::query(&q).fetch_all(admin).await.unwrap();
    rows.iter()
        .map(|r| r.get::<String, _>("d").replace(&format!("{schema}."), ""))
        .collect()
}

fn job(id: &str, state: JobState) -> Job {
    Job {
                inputs: None, outputs: None,
        owner: "ab".repeat(20),
        namespace: SYSTEM_NAMESPACE.into(),
        tag: String::new(),
        client_id: id.into(),
        id: id.into(),
        tenant: "goat".into(),
        elf_id: "b".repeat(64),
        kind: ProofKind::Groth16,
        priority: 0,
        state,
        error: String::new(),
        node: Some("g0".into()),
        attempts: 1,
        failed_on: vec!["g1".into()],
        cycles: 3,
        prove_ms: 9,
        created_ms: 2000,
        started_ms: Some(2001),
        finished_ms: state.is_terminal().then_some(2002),
    }
}

async fn check_job_identity(store: &dyn Store) {
    let mut original = job("immutable", JobState::Running);
    let reference = fleet_proto::fleet::v1::ArtifactRef { uri:"file:///app/input".into(), sha256:"b".repeat(64), size:3 };
    original.inputs = Some(fleet_proto::fleet::v1::ProofInputs {input:Some(reference.clone()),elf:Some(reference.clone()),output_root:"file:///app/outputs".into()});
    store.upsert(&original).await.unwrap();
    assert!(store.recent(&JobFilter::default(),usize::MAX).await.unwrap().iter().any(|j|j.id==original.id),
        "unbounded drain queries must not produce a negative Postgres LIMIT");
    let mut changed = original.clone();
    changed.owner = "ff".repeat(20);
    changed.namespace = "1".repeat(32);
    changed.tag = "other".into();
    changed.client_id = "replacement".into();
    changed.tenant = "eth".into();
    changed.elf_id = "a".repeat(64);
    changed.kind = ProofKind::Core;
    changed.priority = 99;
    changed.created_ms += 1;
    changed.inputs.as_mut().unwrap().elf.as_mut().unwrap().sha256 = "a".repeat(64);
    changed.inputs.as_mut().unwrap().input.as_mut().unwrap().uri = "file:///replacement".into();
    changed.outputs = Some(fleet_proto::fleet::v1::ProofOutputs {machine: String::new(), proof:Some(reference.clone()),public_values:Some(reference)});
    changed.state = JobState::Succeeded;
    changed.finished_ms = Some(2003);
    store.upsert(&changed).await.unwrap();
    let restored = store.get(&original.id).await.unwrap().unwrap();
    assert_eq!((&restored.owner, &restored.namespace, &restored.tag, &restored.client_id),
        (&original.owner, &original.namespace, &original.tag, &original.client_id));
    assert_eq!((&restored.tenant, &restored.elf_id, restored.kind, restored.created_ms),
        (&original.tenant, &original.elf_id, original.kind, original.created_ms));
    assert_eq!(restored.inputs, original.inputs, "input references are immutable");
    assert_eq!(restored.outputs, changed.outputs, "completion metadata survives storage");
    assert_eq!(restored.priority, original.priority);
    assert_eq!(restored.state, JobState::Succeeded, "execution state can change");
    assert_eq!(restored.finished_ms, Some(2003));
    let mut duplicate = original.clone();
    duplicate.id = "duplicate-identity".into();
    assert!(store.upsert(&duplicate).await.is_err(), "client ID is unique within namespace and tag");
    for field in ["owner", "namespace", "client_id", "tag", "finished_ms"] {
        let mut invalid = job("invalid", JobState::Queued);
        match field {
            "owner" => invalid.owner.clear(),
            "namespace" => invalid.namespace.clear(),
            "client_id" => invalid.client_id.clear(),
            "tag" => invalid.tag = "Bad Tag".into(),
            _ => invalid.finished_ms = Some(1),
        }
        assert!(store.upsert(&invalid).await.is_err(), "accepted invalid {field}");
        assert!(store.get("invalid").await.unwrap().is_none());
    }
}

#[tokio::test]
async fn memory_store_preserves_job_identity_and_validates_fields() {
    let store = fleet_scheduler::store::MemoryStore::default();
    check_job_identity(&store).await;
    check_registered_app_inventory(&store).await;
}

async fn check_registered_app_inventory(store: &dyn Store) {
    let mut rejected = job("unregistered-attempt", JobState::Failed(StageStatus::InvalidParameter));
    rejected.tenant = "unregistered".into();
    store.upsert(&rejected).await.unwrap();
    assert!(!store.apps().await.unwrap().iter().any(|a| a.tenant == rejected.tenant));
    assert!(store.get(&rejected.id).await.unwrap().is_some(), "rejected requests remain in job history");
    store.add_app(SYSTEM_NAMESPACE, &rejected.tenant, &rejected.elf_id, 3000, 0).await.unwrap();
    let registered = store.apps().await.unwrap().into_iter().find(|a| a.tenant == rejected.tenant).unwrap();
    assert_eq!((registered.jobs, registered.failed, registered.created_ms), (1, 1, 3000));
    store.delete_app(SYSTEM_NAMESPACE, &rejected.tenant, &rejected.elf_id).await.unwrap();
    assert!(!store.apps().await.unwrap().iter().any(|a| a.tenant == rejected.tenant));
}

#[tokio::test]
async fn fresh_schema_requires_complete_job_identity() {
    let Some(url) = url() else {
        eprintln!("FLEET_TEST_PG_URL unset: skipped");
        return;
    };
    let admin = PgPool::connect(&url).await.unwrap();
    fresh_schema(&admin, "fresh").await;
    fresh_schema(&admin, "incompatible").await;
    let mig = PgPool::connect(&in_schema(&url, "incompatible")).await.unwrap();
    sqlx::raw_sql(INCOMPATIBLE_SCHEMA).execute(&mig).await.unwrap();

    let fresh = PgStore::connect(&in_schema(&url, "fresh")).await.unwrap();
    let error = PgStore::connect(&in_schema(&url, "incompatible")).await.err().expect("old ownerless jobs require a database reset");
    assert!(error.to_string().contains("reset the database"), "{error:#}");
    let owner: Option<String> = sqlx::query_scalar("SELECT owner FROM incompatible.fleet_jobs WHERE id='unowned'").fetch_one(&admin).await.unwrap();
    assert!(owner.is_none(), "startup does not backfill an invented owner");
    let before = ddl(&admin, "fresh").await;
    PgStore::connect(&in_schema(&url, "fresh")).await.unwrap();
    assert_eq!(before, ddl(&admin, "fresh").await, "restart preserves the schema");
    let versions: Vec<i32> = sqlx::query_scalar("SELECT version FROM fresh.fleet_schema").fetch_all(&admin).await.unwrap();
    assert_eq!(versions, vec![13]);

    let bb = "b".repeat(64);
    for store in [&fresh] {
        assert!(store.add_app(SYSTEM_NAMESPACE, "goat", &bb, 1999, 0).await.unwrap());
        assert!(!store.add_app(SYSTEM_NAMESPACE, "goat", &bb, 2000, 0).await.unwrap(), "registered once");
        let fresh_app = store.apps().await.unwrap().into_iter().find(|a| a.elf_id == bb).unwrap();
        assert_eq!((fresh_app.jobs, fresh_app.created_ms, fresh_app.last_created_ms), (0, 1999, None), "listed with no job yet");
        store.upsert(&job("g1", JobState::Running)).await.unwrap();
        store.upsert(&job("g2", JobState::Failed(StageStatus::SnarkError))).await.unwrap();
        store.upsert(&job("g2", JobState::Failed(StageStatus::SnarkError))).await.unwrap();
        let active = store
            .recent(&JobFilter { tag: None, namespace: None, owner: None, tenant: Some("goat".into()), elf: Some("b".into()), active: true }, 10)
            .await
            .unwrap();
        assert_eq!(active.iter().map(|j| j.id.as_str()).collect::<Vec<_>>(), vec!["g1"]);
        let apps = store.apps().await.unwrap();
        let goat = apps.iter().find(|a| a.tenant == "goat").unwrap();
        assert_eq!((goat.jobs, goat.active, goat.failed), (2, 1, 1));
        store.set_app_disabled(SYSTEM_NAMESPACE, "goat", &bb, true, 5).await.unwrap();
        assert!(store.registered_apps().await.unwrap().contains(&(SYSTEM_NAMESPACE.into(), "goat".to_string(), bb.clone(), true)));
        assert!(store.apps().await.unwrap().iter().find(|a| a.tenant == "goat").unwrap().disabled);
        store.set_app_disabled(SYSTEM_NAMESPACE, "goat", &bb, false, 7).await.unwrap();
        assert!(store.registered_apps().await.unwrap().contains(&(SYSTEM_NAMESPACE.into(), "goat".to_string(), bb.clone(), false)));
        assert_eq!(store.delete_app(SYSTEM_NAMESPACE, "goat", &bb).await.unwrap(), vec!["g2".to_string()], "the running one stays");
        assert!(!store.registered_apps().await.unwrap().iter().any(|(_, t, _, _)| t == "goat"), "the app is gone");
        assert_eq!(store.incomplete().await.unwrap().iter().filter(|j| j.id == "g1").count(), 1);
    }
    // what the constraints refuse
    let bad = [
        "INSERT INTO fresh.fleet_jobs (id, tenant, elf_id, kind, state, created_ms, owner, namespace, tag, client_id) VALUES ('b1', 't', 'e', 'core', 'done', 1, repeat('a',40), repeat('0',32), '', 'b1')",
        "INSERT INTO fresh.fleet_jobs (id, tenant, elf_id, kind, state, created_ms, finished_ms, owner, namespace, tag, client_id) VALUES ('b2', 't', 'e', 'core', 'failed', 1, 2, repeat('a',40), repeat('0',32), '', 'b2')",
        "INSERT INTO fresh.fleet_jobs (id, tenant, elf_id, kind, state, created_ms, owner, namespace, tag, client_id) VALUES ('b3', 't', 'e', 'core', 'succeeded', 1, repeat('a',40), repeat('0',32), '', 'b3')",
        "INSERT INTO fresh.fleet_tokens (address, tenant, created_ms) VALUES ('0xABC', 't', 1)",
        "INSERT INTO fresh.fleet_apps (tenant, elf_id, created_ms, namespace) VALUES ('', repeat('a', 64), 1, repeat('0',32))",
        "INSERT INTO fresh.fleet_apps (tenant, elf_id, created_ms, namespace) VALUES ('t', 'abc', 1, repeat('0',32))",
    ];
    for sql in bad {
        assert!(sqlx::query(sql).execute(&admin).await.is_err(), "accepted: {sql}");
    }
    let c = Token { id: "84".repeat(16), address: "84".repeat(20), tenant: "eth".into(), label: String::new(), created_ms: 1, revoked_ms: None, apps: None, token_hash: None, tag: None, namespace: None, manage_apps: false };
    fresh.add_token(&c).await.unwrap();

    let namespace = Namespace { tag_quotas: Default::default(), quota: Default::default(), id: "1".repeat(32), name: "alice".into(), created_ms: 1, suspended_ms: None };
    fresh.add_namespace(&namespace).await.unwrap();
    check_job_identity(&fresh).await;
    check_registered_app_inventory(&fresh).await;
    let shared_elf = "a".repeat(64);
    assert!(fresh.add_app("missing-namespace", "goat", &shared_elf, 1, 0).await.is_err());
    fresh.add_app(SYSTEM_NAMESPACE, "goat", &shared_elf, 1, 0).await.unwrap();
    fresh.add_app(&namespace.id, "goat", &shared_elf, 1, 7).await.unwrap();
    fresh.set_app_disabled(&namespace.id, "goat", &shared_elf, true, 2).await.unwrap();
    let app_priority = |apps: Vec<fleet_scheduler::store::ProofApp>| apps.into_iter()
        .find(|a| a.namespace == namespace.id && a.elf_id == shared_elf).unwrap().priority;
    assert_eq!(app_priority(fresh.apps().await.unwrap()), 7);
    fresh.set_app_priority(&namespace.id, "goat", &shared_elf, -5).await.unwrap();
    let reopened = PgStore::connect(&in_schema(&url, "fresh")).await.unwrap();
    assert_eq!(app_priority(reopened.apps().await.unwrap()), -5);

    let registered = fresh.registered_apps().await.unwrap();
    assert!(registered.contains(&(namespace.id.clone(), "goat".into(), shared_elf.clone(), true)));
    assert!(registered.contains(&(SYSTEM_NAMESPACE.into(), "goat".into(), shared_elf.clone(), false)));
    fresh.delete_app(&namespace.id, "goat", &shared_elf).await.unwrap();
    assert!(fresh.registered_apps().await.unwrap().contains(&(SYSTEM_NAMESPACE.into(), "goat".into(), shared_elf, false)));
    let mut owned = job("owned", JobState::Running);
    owned.namespace = namespace.id.clone();
    owned.tag = String::new();
    owned.client_id = "local-id".into();
    owned.owner = c.address.clone();
    fresh.upsert(&owned).await.unwrap();
    assert_eq!(fresh.get("owned").await.unwrap().unwrap().owner, c.address);
    owned.owner = "ff".repeat(20);
    fresh.upsert(&owned).await.unwrap();
    assert_eq!(fresh.get("owned").await.unwrap().unwrap().owner, c.address, "ownership is immutable on update");
    let filtered = fresh.recent(&JobFilter { tag: None, namespace: None, owner: Some(c.address.clone()), ..Default::default() }, 1).await.unwrap();
    assert_eq!(filtered.len(), 1);
    assert_eq!(filtered[0].id, "owned");
    let scoped = Token { id: "85".repeat(16), namespace: Some(namespace.id.clone()), tag: None, address: "85".repeat(20), apps: Some(vec!["training".into()]), token_hash: Some("ab".repeat(32)), ..c.clone() };
    fresh.add_token(&scoped).await.unwrap();
    let restored = fresh.tokens().await.unwrap().into_iter().find(|r| r.address == scoped.address).unwrap();
    assert_eq!(restored.apps, scoped.apps);
    assert_eq!(restored.token_hash, scoped.token_hash);
    assert!(serde_json::to_value(&restored).unwrap().get("token_hash").is_none());
    assert_eq!(fresh.namespace_job(&namespace.id, None, "local-id").await.unwrap().unwrap().id, "owned");
    let cap = Quota { gpus: Some(0.5), cpus: Some(4.0) };
    fresh.set_quota(&namespace.id, &cap).await.unwrap();
    let allocation = Allocation { id:"run:indefinite".into(), namespace:namespace.id.clone(), tag:None, gpus:0.5, cpus:4.0 };
    fresh.reserve(&allocation).await.unwrap();
    let restarted = PgStore::connect(&in_schema(&url, "fresh")).await.unwrap();
    assert_eq!(restarted.namespaces().await.unwrap()[0].quota.gpus, Some(0.5));
    assert_eq!(restarted.allocations().await.unwrap()[0].id, "run:indefinite", "no time expiry; allocation survives a restart");
    restarted.release("run:indefinite").await.unwrap();
    assert!(fresh.allocations().await.unwrap().is_empty());
    let mut research=owned.clone(); research.id="research-owned".into(); research.tag="research".into();
    fresh.upsert(&research).await.unwrap();
    assert_eq!(fresh.namespace_job(&namespace.id,Some("research"),"local-id").await.unwrap().unwrap().id,"research-owned");
    assert_eq!(fresh.recent(&JobFilter { namespace:Some(namespace.id.clone()),tag:Some("research".into()),..Default::default() },10).await.unwrap().len(),1);
    fresh.set_tag_quota(&namespace.id,"research",&cap).await.unwrap();
    let tag_store=PgStore::connect(&in_schema(&url,"fresh")).await.unwrap();
    assert_eq!(tag_store.namespaces().await.unwrap()[0].tag_quotas["research"].gpus,Some(0.5));

    fresh.suspend_namespace(&namespace.id, Some(99)).await.unwrap();
    assert_eq!(fresh.namespaces().await.unwrap()[0].suspended_ms, Some(99));

    // Direct SQL cannot bypass required identity, even when application validation is skipped.
    for column in ["owner", "namespace", "tag", "client_id"] {
        let sql = format!("UPDATE fresh.fleet_jobs SET {column}=NULL WHERE id='owned'");
        assert!(sqlx::query(&sql).execute(&admin).await.is_err(), "nullable {column}");
    }
    for (column, value) in [("owner", "''"), ("owner", "'0xABC'"), ("tag", "'Invalid Tag'"), ("client_id", "''")] {
        let sql = format!("UPDATE fresh.fleet_jobs SET {column}={value} WHERE id='owned'");
        assert!(sqlx::query(&sql).execute(&admin).await.is_err(), "accepted {column}={value}");
    }
    let mut duplicate = job("duplicate", JobState::Queued);
    duplicate.namespace = namespace.id.clone();
    duplicate.client_id = "local-id".into();
    assert!(fresh.upsert(&duplicate).await.is_err(), "untagged client IDs are unique within a namespace");
    mig.close().await;
    admin.execute("DROP SCHEMA fresh CASCADE; DROP SCHEMA incompatible CASCADE").await.unwrap();
}

#[tokio::test]
async fn postgres_allows_only_one_scheduler_leader_per_database() {
    let Some(url) = url() else {
        eprintln!("FLEET_TEST_PG_URL unset: skipped");
        return;
    };
    let admin = PgPool::connect(&url).await.unwrap();
    fresh_schema(&admin, "leader").await;
    let scoped = in_schema(&url, "leader");
    let first = PgStore::connect(&scoped).await.unwrap();
    let _lock = first.acquire_leader_lock().await.unwrap();
    let second = PgStore::connect(&scoped).await.unwrap();
    let error = match second.acquire_leader_lock().await {
        Ok(_) => panic!("a second scheduler acquired the same database lock"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("another fleet-scheduler"), "{error:#}");
}

#[tokio::test]
async fn schema_12_migrates_without_changing_existing_jobs() {
    let Some(url) = url() else { return; };
    let admin = PgPool::connect(&url).await.unwrap();
    fresh_schema(&admin, "metadata_migration").await;
    let db = in_schema(&url, "metadata_migration");
    let store = PgStore::connect(&db).await.unwrap();
    store.upsert(&job("pre-migration", JobState::Succeeded)).await.unwrap();
    let pool = PgPool::connect(&db).await.unwrap();
    sqlx::raw_sql("ALTER TABLE fleet_jobs DROP COLUMN inputs; ALTER TABLE fleet_jobs DROP COLUMN outputs; UPDATE fleet_schema SET version=12").execute(&pool).await.unwrap();
    let upgraded = PgStore::connect(&db).await.unwrap();
    let saved = upgraded.get("pre-migration").await.unwrap().unwrap();
    assert_eq!(saved.state, JobState::Succeeded);
    assert!(saved.inputs.is_none() && saved.outputs.is_none());
    let versions:Vec<i32> = sqlx::query_scalar("SELECT version FROM fleet_schema ORDER BY version").fetch_all(&pool).await.unwrap();
    assert_eq!(versions, vec![12,13]);
    PgStore::connect(&db).await.unwrap();
    assert_eq!(upgraded.get("pre-migration").await.unwrap().unwrap().owner, saved.owner);
}
