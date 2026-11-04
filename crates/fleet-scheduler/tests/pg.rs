//! The Postgres store against a real database: FLEET_TEST_PG_URL (a database the test
//! may fill: it makes and drops the schemas `fresh` and `incompatible`). Skipped without it.

use fleet_proto::fleet::v1::ProofKind;
use fleet_proto::stage::v1::Status as StageStatus;
use fleet_scheduler::quota::{Allocation, Quota};
use fleet_scheduler::store::SYSTEM_NAMESPACE;
use fleet_scheduler::store::{App, ProofJob, ProofJobFilter, ProofJobState, Namespace, PgStore, Store, Token};
use sqlx::{Executor, PgPool, Row};

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

/// A registration of `name` (program `elf_id`) with the default policy.
fn app(namespace: &str, name: &str, elf_id: &str, created_ms: u64, priority: i32) -> App {
    App {
        namespace: namespace.into(), name: name.into(), elf_id: elf_id.into(), created_ms, priority,
        class: "batch".into(), anchor: "none".into(), max_queued: 64, ..Default::default()
    }
}

fn job(id: &str, state: ProofJobState) -> ProofJob {
    ProofJob {
                inputs: None, outputs: None,
        owner: "ab".repeat(20),
        namespace: SYSTEM_NAMESPACE.into(),
        tag: String::new(),
        client_id: id.into(),
        id: id.into(),
        app: "goat".into(),
        elf_id: "b".repeat(64),
        kind: ProofKind::Groth16 as i32,
        priority: 0,
        state: state as i32,
        fail_status: (state == ProofJobState::Failed).then_some(StageStatus::InternalError as i32),
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
    let mut original = job("immutable", ProofJobState::Running);
    let reference = fleet_proto::fleet::v1::ArtifactRef { uri:"file:///app/input".into(), sha256:"b".repeat(64), size:3,
    };
    original.inputs = Some(fleet_proto::fleet::v1::ProofInputs {input:Some(reference.clone()),elf:Some(reference.clone()),output_root:"file:///app/outputs".into(),
    });
    store.upsert(&original).await.unwrap();
    assert!(store.recent(&ProofJobFilter::default(),usize::MAX).await.unwrap().iter().any(|j|j.id==original.id),
        "unbounded drain queries must not produce a negative Postgres LIMIT");
    let mut changed = original.clone();
    changed.owner = "ff".repeat(20);
    changed.namespace = "1".repeat(32);
    changed.tag = "scope=other".into();
    changed.client_id = "replacement".into();
    changed.app = "eth".into();
    changed.elf_id = "a".repeat(64);
    changed.kind = ProofKind::Core as i32;
    changed.priority = 99;
    changed.created_ms += 1;
    changed.inputs.as_mut().unwrap().elf.as_mut().unwrap().sha256 = "a".repeat(64);
    changed.inputs.as_mut().unwrap().input.as_mut().unwrap().uri = "file:///replacement".into();
    changed.outputs = Some(fleet_proto::fleet::v1::ProofOutputs {machine: String::new(), proof:Some(reference.clone()),public_values:Some(reference),
    });
    changed.state = ProofJobState::Succeeded as i32;
    changed.finished_ms = Some(2003);
    store.upsert(&changed).await.unwrap();
    let restored = store.get(&original.id).await.unwrap().unwrap();
    assert_eq!((&restored.owner, &restored.namespace, &restored.tag, &restored.client_id),
        (&original.owner, &original.namespace, &original.tag, &original.client_id));
    assert_eq!((&restored.app, &restored.elf_id, restored.kind, restored.created_ms),
        (&original.app, &original.elf_id, original.kind, original.created_ms));
    assert_eq!(restored.inputs, original.inputs, "input references are immutable");
    assert_eq!(restored.outputs, changed.outputs, "completion metadata survives storage");
    assert_eq!(restored.priority, original.priority);
    assert_eq!(restored.state(), ProofJobState::Succeeded, "execution state can change");
    assert_eq!(restored.finished_ms, Some(2003));
    let mut duplicate = original.clone();
    duplicate.id = "duplicate-identity".into();
    assert!(store.upsert(&duplicate).await.is_err(), "client ID is unique within namespace and tag");
    for field in ["owner", "namespace", "client_id", "tag", "finished_ms"] {
        let mut invalid = job("invalid", ProofJobState::Queued);
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
    let mut rejected = job("unregistered-attempt", ProofJobState::Failed);
    rejected.fail_status = Some(StageStatus::InvalidParameter as i32);
    rejected.app = "unregistered".into();
    rejected.elf_id = "c".repeat(64);
    store.upsert(&rejected).await.unwrap();
    assert!(!store.apps().await.unwrap().iter().any(|a| a.name == rejected.app));
    assert!(store.get(&rejected.id).await.unwrap().is_some(), "rejected requests remain in job history");
    assert!(store.add_app(&app(SYSTEM_NAMESPACE, &rejected.app, &rejected.elf_id, 3000, 0)).await.unwrap());
    let registered = store.apps().await.unwrap().into_iter().find(|a| a.name == rejected.app).unwrap();
    assert_eq!((registered.jobs, registered.failed, registered.created_ms), (1, 1, 3000));
    store.delete_app(SYSTEM_NAMESPACE, &rejected.app).await.unwrap();
    assert!(!store.apps().await.unwrap().iter().any(|a| a.name == rejected.app));
}

#[tokio::test]
async fn fresh_schema_requires_complete_job_identity() {
    let Some(url) = url() else {
        tracing::warn!("FLEET_TEST_PG_URL unset: skipped");
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
    assert_eq!(versions, vec![16]);

    let bb = "b".repeat(64);
    for store in [&fresh] {
        assert!(store.add_app(&app(SYSTEM_NAMESPACE, "goat", &bb, 1999, 0)).await.unwrap());
        assert!(!store.add_app(&app(SYSTEM_NAMESPACE, "goat", &"d".repeat(64), 2000, 0)).await.unwrap(), "a name is registered once");
        assert!(!store.add_app(&app(SYSTEM_NAMESPACE, "goat-again", &bb, 2000, 0)).await.unwrap(), "a program is one app of a namespace");
        let fresh_app = store.apps().await.unwrap().into_iter().find(|a| a.elf_id == bb).unwrap();
        assert_eq!((fresh_app.jobs, fresh_app.created_ms, fresh_app.last_created_ms), (0, 1999, None), "listed with no job yet");
        store.upsert(&job("g1", ProofJobState::Running)).await.unwrap();
        let mut failed = job("g2", ProofJobState::Failed);
        failed.fail_status = Some(StageStatus::SnarkError as i32);
        store.upsert(&failed).await.unwrap();
        store.upsert(&failed).await.unwrap();
        let active = store
            .recent(&ProofJobFilter { tag: None, namespace: None, owner: None, app: Some("goat".into()), active: true,
                }, 10,
            )
            .await
            .unwrap();
        assert_eq!(active.iter().map(|j| j.id.as_str()).collect::<Vec<_>>(), vec!["g1"]);
        let apps = store.apps().await.unwrap();
        let goat = apps.iter().find(|a| a.name == "goat").unwrap();
        assert_eq!((goat.jobs, goat.active, goat.failed), (2, 1, 1));
        assert_eq!((goat.class.as_str(), goat.anchor.as_str(), goat.max_queued), ("batch", "none", 64));
        store.set_app_disabled(SYSTEM_NAMESPACE, "goat", true, 5).await.unwrap();
        assert!(store.apps().await.unwrap().iter().find(|a| a.name == "goat").unwrap().disabled);
        store.set_app_disabled(SYSTEM_NAMESPACE, "goat", false, 7).await.unwrap();
        assert!(!store.apps().await.unwrap().iter().find(|a| a.name == "goat").unwrap().disabled);
        assert_eq!(store.delete_app(SYSTEM_NAMESPACE, "goat").await.unwrap(), vec!["g2".to_string()], "the running one stays");
        assert!(!store.apps().await.unwrap().iter().any(|a| a.name == "goat"), "the app is gone");
        assert_eq!(store.incomplete().await.unwrap().iter().filter(|j| j.id == "g1").count(), 1);
    }
    // what the constraints refuse
    let bad = [
        "INSERT INTO fresh.fleet_jobs (id, app, elf_id, kind, state, created_ms, owner, namespace, tag, client_id) VALUES ('b1', 't', 'e', 'core', 'done', 1, repeat('a',40), repeat('0',32), '', 'b1')",
        "INSERT INTO fresh.fleet_jobs (id, app, elf_id, kind, state, created_ms, finished_ms, owner, namespace, tag, client_id) VALUES ('b2', 't', 'e', 'core', 'failed', 1, 2, repeat('a',40), repeat('0',32), '', 'b2')",
        "INSERT INTO fresh.fleet_jobs (id, app, elf_id, kind, state, created_ms, owner, namespace, tag, client_id) VALUES ('b3', 't', 'e', 'core', 'succeeded', 1, repeat('a',40), repeat('0',32), '', 'b3')",
        "INSERT INTO fresh.fleet_tokens (id, address, created_ms) VALUES (repeat('1', 32), '0xABC', 1)",
        "INSERT INTO fresh.fleet_tokens (id, address, created_ms, permissions) VALUES (repeat('2', 32), repeat('2', 40), 1, '{read,root}')",
        "INSERT INTO fresh.fleet_tokens (id, address, created_ms) VALUES (repeat('3', 32), repeat('3', 40), 1)",
        "INSERT INTO fresh.fleet_tokens (id, address, created_ms, namespace, permissions) VALUES (repeat('4', 32), repeat('4', 40), 1, repeat('0',32), '{admin}')",
        "INSERT INTO fresh.fleet_apps (name, elf_id, created_ms, namespace) VALUES ('', repeat('a', 64), 1, repeat('0',32))",
        "INSERT INTO fresh.fleet_apps (name, elf_id, created_ms, namespace) VALUES ('t/e', repeat('a', 64), 1, repeat('0',32))",
        "INSERT INTO fresh.fleet_apps (name, elf_id, created_ms, namespace) VALUES ('t', 'abc', 1, repeat('0',32))",
        "INSERT INTO fresh.fleet_apps (name, elf_id, created_ms, namespace, class) VALUES ('t', repeat('a', 64), 1, repeat('0',32), 'fast')",
        "INSERT INTO fresh.fleet_apps (name, elf_id, created_ms, namespace, backends) VALUES ('t', repeat('a', 64), 1, repeat('0',32), '{tpu}')",
        "INSERT INTO fresh.fleet_apps (name, elf_id, created_ms, namespace, max_queued) VALUES ('t', repeat('a', 64), 1, repeat('0',32), 0)",
    ];
    for sql in bad {
        assert!(sqlx::query(sql).execute(&admin).await.is_err(), "accepted: {sql}");
    }
    let c = Token { id: "84".repeat(16), address: "84".repeat(20), label: String::new(), created_ms: 1, revoked_ms: None, apps: None, token_hash: Some("84".repeat(32)), tag: None, namespace: None, permissions: vec!["admin".into()],
    };
    fresh.add_token(&c).await.unwrap();

    let namespace = Namespace { tag_quotas: Default::default(), quota: Default::default(), id: "1".repeat(32), name: "alice".into(), created_ms: 1, suspended_ms: None,
    };
    fresh.add_namespace(&namespace).await.unwrap();
    check_job_identity(&fresh).await;
    check_registered_app_inventory(&fresh).await;
    let shared_elf = "a".repeat(64);
    assert!(fresh.add_app(&app("missing-namespace", "goat", &shared_elf, 1, 0)).await.is_err());
    fresh.add_app(&app(SYSTEM_NAMESPACE, "goat", &shared_elf, 1, 0)).await.unwrap();
    let mut policy = app(&namespace.id, "goat", &shared_elf, 1, 7);
    (policy.class, policy.anchor, policy.backends, policy.zkm_version, policy.max_queued) =
        ("latency".into(), "exclusive".into(), vec!["cuda".into()], Some("v1.2".into()), 3);
    fresh.add_app(&policy).await.unwrap();
    fresh.set_app_disabled(&namespace.id, "goat", true, 2).await.unwrap();
    let app_priority = |apps: Vec<fleet_scheduler::store::App>| {
        apps.into_iter()
        .find(|a| a.namespace == namespace.id && a.elf_id == shared_elf).unwrap().priority
    };
    assert_eq!(app_priority(fresh.apps().await.unwrap()), 7);
    fresh.set_app_priority(&namespace.id, "goat", -5).await.unwrap();
    let reopened = PgStore::connect(&in_schema(&url, "fresh")).await.unwrap();
    assert_eq!(app_priority(reopened.apps().await.unwrap()), -5);

    let registered = fresh.apps().await.unwrap();
    let mine = registered.iter().find(|a| a.namespace == namespace.id).unwrap();
    assert_eq!((mine.disabled, mine.class.as_str(), mine.anchor.as_str(), mine.backends.clone(), mine.zkm_version.as_deref(), mine.max_queued),
        (true, "latency", "exclusive", vec!["cuda".to_string()], Some("v1.2"), 3));
    assert!(registered.iter().any(|a| a.namespace == SYSTEM_NAMESPACE && a.name == "goat" && !a.disabled));
    fresh.delete_app(&namespace.id, "goat").await.unwrap();
    assert!(fresh.apps().await.unwrap().iter().any(|a| a.namespace == SYSTEM_NAMESPACE && a.name == "goat"));
    let mut owned = job("owned", ProofJobState::Running);
    owned.namespace = namespace.id.clone();
    owned.tag = String::new();
    owned.client_id = "local-id".into();
    owned.owner = c.address.clone();
    fresh.upsert(&owned).await.unwrap();
    assert_eq!(fresh.get("owned").await.unwrap().unwrap().owner, c.address);
    owned.owner = "ff".repeat(20);
    fresh.upsert(&owned).await.unwrap();
    assert_eq!(fresh.get("owned").await.unwrap().unwrap().owner, c.address, "ownership is immutable on update");
    let filtered = fresh.recent(&ProofJobFilter { tag: None, namespace: None, owner: Some(c.address.clone()), ..Default::default() }, 1,
        ).await.unwrap();
    assert_eq!(filtered.len(), 1);
    assert_eq!(filtered[0].id, "owned");
    let scoped = Token { id: "85".repeat(16), namespace: Some(namespace.id.clone()), tag: None, address: "85".repeat(20), apps: Some(vec!["training".into()].into()), token_hash: Some("ab".repeat(32)), permissions: vec!["read".into()], ..c.clone() };
    fresh.add_token(&scoped).await.unwrap();
    let restored = fresh.tokens().await.unwrap().into_iter().find(|r| r.address == scoped.address).unwrap();
    assert_eq!(restored.apps, scoped.apps);
    assert_eq!(restored.token_hash, scoped.token_hash);
    assert!(serde_json::to_value(&restored).unwrap().get("token_hash").is_none());
    assert_eq!(fresh.namespace_job(&namespace.id, None, "local-id").await.unwrap().unwrap().id, "owned");
    let cap = Quota { gpus: Some(0.5), cpus: Some(4.0),
    };
    fresh.set_quota(&namespace.id, &cap).await.unwrap();
    let allocation = Allocation { id:"job:indefinite".into(), namespace:namespace.id.clone(), tag:None, gpus:0.5, cpus:4.0,
    };
    fresh.reserve(&allocation).await.unwrap();
    let restarted = PgStore::connect(&in_schema(&url, "fresh")).await.unwrap();
    assert_eq!(restarted.namespaces().await.unwrap()[0].quota.gpus, Some(0.5));
    assert_eq!(restarted.allocations().await.unwrap()[0].id, "job:indefinite", "no time expiry; allocation survives a restart");
    restarted.release("job:indefinite").await.unwrap();
    assert!(fresh.allocations().await.unwrap().is_empty());
    let mut research=owned.clone(); research.id="research-owned".into(); research.tag="scope=research".into();
    fresh.upsert(&research).await.unwrap();
    assert_eq!(fresh.namespace_job(&namespace.id,Some("scope=research"),"local-id").await.unwrap().unwrap().id,"research-owned");
    assert_eq!(fresh.recent(&ProofJobFilter { namespace:Some(namespace.id.clone()),tag:Some("scope=research".into()),..Default::default() },10).await.unwrap().len(),1);
    fresh.set_tag_quota(&namespace.id,"scope=research",&cap).await.unwrap();
    let tag_store=PgStore::connect(&in_schema(&url,"fresh")).await.unwrap();
    assert_eq!(tag_store.namespaces().await.unwrap()[0].tag_quotas["scope=research"].gpus,Some(0.5));

    fresh.suspend_namespace(&namespace.id, Some(99)).await.unwrap();
    assert_eq!(fresh.namespaces().await.unwrap()[0].suspended_ms, Some(99));

    // Direct SQL cannot bypass required identity, even when application validation is skipped.
    for column in ["owner", "namespace", "tag", "client_id"] {
        let sql = format!("UPDATE fresh.fleet_jobs SET {column}=NULL WHERE id='owned'");
        assert!(sqlx::query(&sql).execute(&admin).await.is_err(), "nullable {column}");
    }
    for (column, value) in [("owner", "''"), ("owner", "'0xABC'"), ("tag", "'Invalid Tag'"), ("client_id", "''"),
    ] {
        let sql = format!("UPDATE fresh.fleet_jobs SET {column}={value} WHERE id='owned'");
        assert!(sqlx::query(&sql).execute(&admin).await.is_err(), "accepted {column}={value}");
    }
    let mut duplicate = job("duplicate", ProofJobState::Queued);
    duplicate.namespace = namespace.id.clone();
    duplicate.client_id = "local-id".into();
    assert!(fresh.upsert(&duplicate).await.is_err(), "untagged client IDs are unique within a namespace");
    mig.close().await;
    admin.execute("DROP SCHEMA fresh CASCADE; DROP SCHEMA incompatible CASCADE").await.unwrap();
}

#[tokio::test]
async fn postgres_allows_only_one_scheduler_leader_per_database() {
    let Some(url) = url() else {
        tracing::warn!("FLEET_TEST_PG_URL unset: skipped");
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
async fn incompatible_schema_requires_reset_without_changing_jobs() {
    let Some(url) = url() else { return; };
    let admin = PgPool::connect(&url).await.unwrap();
    for version in [12, 13] {
        let schema = format!("unsupported_{version}");
        fresh_schema(&admin, &schema).await;
        let db = in_schema(&url, &schema);
        let store = PgStore::connect(&db).await.unwrap();
        let original = job("saved-proof", ProofJobState::Succeeded);
        store.upsert(&original).await.unwrap();
        let pool = PgPool::connect(&db).await.unwrap();
        sqlx::query("UPDATE fleet_schema SET version=$1").bind(version).execute(&pool).await.unwrap();
        let before = ddl(&admin, &schema).await;
        assert!(PgStore::connect(&db).await.err().unwrap().to_string().contains("reset the database"));
        assert_eq!(before, ddl(&admin, &schema).await);
        assert_eq!(store.get(&original.id).await.unwrap().unwrap().owner, original.owner);
        let saved_version: i32 = sqlx::query_scalar("SELECT max(version) FROM fleet_schema").fetch_one(&pool).await.unwrap();
        assert_eq!(saved_version, version);
    }
}

#[tokio::test]
async fn postgres_validates_key_value_tags_for_jobs_tokens_and_allocations() {
    let Some(url) = url() else { return; };
    let admin = PgPool::connect(&url).await.unwrap();
    fresh_schema(&admin, "tags").await;
    let db = in_schema(&url, "tags");
    let store = PgStore::connect(&db).await.unwrap();
    let pool = PgPool::connect(&db).await.unwrap();
    for (index, tag) in ["scope=default", "network=Ethereum-mainnet", "Team_Name=release.2",
    ].into_iter().enumerate() {
        let mut tagged = job(&format!("tagged-{index}"), ProofJobState::Succeeded);
        tagged.tag = tag.into();
        store.upsert(&tagged).await.unwrap();
        let token = Token { id: format!("{index:032x}"), address: format!("{index:040x}"), label: tag.into(), created_ms: 1, revoked_ms: None,
            namespace: Some(SYSTEM_NAMESPACE.into()), tag: Some(tag.into()), apps: None, token_hash: None, permissions: Vec::new(),
        };
        store.add_token(&token).await.unwrap();
        store.reserve(&Allocation { id: format!("allocation-{index}"), namespace: SYSTEM_NAMESPACE.into(), tag: Some(tag.into()), gpus: 1.0, cpus: 4.0,
            }).await.unwrap();
    }
    for tag in ["default", "team=", "=research", "team=one=two", "Invalid Tag", "team=ümlaut",
    ] {
        for (table, column, id) in [("fleet_jobs", "id", "tagged-0"), ("fleet_tokens", "label", "scope=default"), ("fleet_namespace_allocations", "id", "allocation-0"),
        ] {
            let query = format!("UPDATE {table} SET tag=$1 WHERE {column}=$2");
            assert!(sqlx::query(&query).bind(tag).bind(id).execute(&pool).await.is_err(), "accepted {table} tag {tag}");
        }
    }
    PgStore::connect(&db).await.unwrap();
    assert_eq!(store.get("tagged-0").await.unwrap().unwrap().tag, "scope=default");
}

/// Schema 14, as a production database had it.
const SCHEMA_14: &str = include_str!("fixtures/schema-14.sql");

#[tokio::test]
async fn schema_14_migrates_to_the_current_schema_and_keeps_its_records() {
    let Some(url) = url() else { return; };
    let admin = PgPool::connect(&url).await.unwrap();
    fresh_schema(&admin, "v14").await;
    fresh_schema(&admin, "current").await;
    let db = in_schema(&url, "v14");
    let old = PgPool::connect(&db).await.unwrap();
    let (zkm, eth, goat, other) = ("5".repeat(32), "6f".repeat(32), "ab".repeat(32), "cd".repeat(32));
    sqlx::raw_sql(&format!("{SCHEMA_14}
        INSERT INTO fleet_schema VALUES (14, 1);
        INSERT INTO fleet_namespaces (id, name, created_ms) VALUES ('{SYSTEM_NAMESPACE}', 'system', 0), ('{zkm}', 'zkm', 1);
        INSERT INTO fleet_apps (namespace, tenant, elf_id, created_ms, priority, disabled_ms) VALUES
            ('{zkm}', 'ethproofs', '{eth}', 1, 3, NULL), ('{zkm}', 'goat', '{goat}', 2, 0, 9), ('{zkm}', 'goat', '{other}', 3, 0, NULL);
        INSERT INTO fleet_tokens (id, address, tenant, created_ms, namespace, tag, apps, token_hash, manage_apps) VALUES
            (repeat('1', 32), repeat('1', 40), 'goat', 1, '{zkm}', 'scope=default', NULL, repeat('1', 64), true),
            (repeat('2', 32), repeat('2', 40), 'ethproofs', 1, '{zkm}', 'scope=default', ARRAY['ethproofs/{eth}', 'ethproofs/{goat}', 'training'], repeat('2', 64), false),
            (repeat('3', 32), repeat('3', 40), 'ethproofs', 1, NULL, NULL, NULL, NULL, false);
        INSERT INTO fleet_jobs (id, owner, namespace, tag, client_id, tenant, elf_id, kind, state, created_ms) VALUES
            ('eth-job', repeat('2', 40), '{zkm}', 'scope=default', 'eth-job', 'ethproofs', '{eth}', 'groth16', 'queued', 5),
            ('lost-job', repeat('2', 40), '{zkm}', 'scope=default', 'lost-job', 'gone', '{eth}', 'groth16', 'queued', 6);
    ")).execute(&old).await.unwrap();
    let store = PgStore::connect(&db).await.unwrap();
    PgStore::connect(&in_schema(&url, "current")).await.unwrap();
    assert_eq!(ddl(&admin, "v14").await, ddl(&admin, "current").await, "a migrated database has the schema a new one has");
    let versions: Vec<i32> = sqlx::query_scalar("SELECT version FROM v14.fleet_schema ORDER BY 1").fetch_all(&admin).await.unwrap();
    assert_eq!(versions, vec![14, 16]);

    let apps = store.apps().await.unwrap();
    let named = |n: &str| apps.iter().find(|a| a.name == n).unwrap_or_else(|| panic!("no app {n}: {apps:?}"));
    let ethproofs = named("ethproofs");
    assert_eq!((ethproofs.elf_id.as_str(), ethproofs.priority, ethproofs.disabled, ethproofs.jobs), (eth.as_str(), 3, false, 1));
    assert_eq!((ethproofs.class.as_str(), ethproofs.anchor.as_str(), ethproofs.backends.len(), ethproofs.max_queued), ("batch", "none", 0, 64));
    assert!(named(&format!("goat-{}", &goat[..8])).disabled, "a tenant's several apps are told apart by their program");
    assert!(!named(&format!("goat-{}", &other[..8])).disabled);
    assert_eq!(store.get("eth-job").await.unwrap().unwrap().app, "ethproofs");
    assert_eq!(store.get("lost-job").await.unwrap().unwrap().app, "", "a job of no registered app keeps no app");

    let tokens = store.tokens().await.unwrap();
    assert_eq!(tokens.len(), 2, "a token without a namespace (a tenant's signer) goes");
    let token = |id: char| tokens.iter().find(|t| t.id == id.to_string().repeat(32)).unwrap();
    assert_eq!(token('1').permissions, vec!["read", "submit", "manage"]);
    assert_eq!(token('2').permissions, vec!["read", "submit"]);
    assert_eq!(token('2').apps.as_ref().unwrap().values, vec!["ethproofs", "training"], "a grant names its app; one of no app goes");
    // Only an administrator's token has no namespace from now on.
    assert!(sqlx::query("INSERT INTO v14.fleet_tokens (id, address, created_ms) VALUES (repeat('4', 32), repeat('4', 40), 1)").execute(&admin).await.is_err());
    assert!(sqlx::query("INSERT INTO v14.fleet_tokens (id, address, created_ms, permissions) VALUES (repeat('4', 32), repeat('4', 40), 1, '{admin}')").execute(&admin).await.is_ok());
    old.close().await;
    admin.execute("DROP SCHEMA v14 CASCADE; DROP SCHEMA current CASCADE").await.unwrap();
}

#[tokio::test]
async fn admin_issue_writes_the_first_administrators_token_into_the_database() {
    use sha2::Digest;
    use std::os::unix::fs::PermissionsExt;
    let Some(url) = url() else { return; };
    let admin = PgPool::connect(&url).await.unwrap();
    fresh_schema(&admin, "issue").await;
    let db = in_schema(&url, "issue");
    let dir = tempfile::tempdir().unwrap();
    let cfg: fleet_scheduler::config::SchedulerConfig = serde_yaml::from_str(&format!(
        "data_dir: {}\ncluster_token_file: unused\ndatabase_url: \"{db}\"\n", dir.path().display())).unwrap();
    let out = dir.path().join("admin.token");
    let token = fleet_scheduler::issue_admin_token(&cfg, "ops", &out).await.unwrap();
    let secret = std::fs::read_to_string(&out).unwrap();
    assert_eq!(std::fs::metadata(&out).unwrap().permissions().mode() & 0o777, 0o600);
    let stored = PgStore::connect(&db).await.unwrap().tokens().await.unwrap();
    assert_eq!(stored.len(), 1);
    assert_eq!((stored[0].id.as_str(), stored[0].label.as_str(), stored[0].permissions.clone(), stored[0].namespace.clone()),
        (token.id.as_str(), "ops", vec!["admin".to_string()], None));
    assert_eq!(stored[0].token_hash.as_deref(), Some(hex::encode(sha2::Sha256::digest(secret.trim().as_bytes()))).as_deref());
    assert!(fleet_scheduler::issue_admin_token(&cfg, "again", &out).await.is_err(), "an existing file is never overwritten");
    assert_eq!(PgStore::connect(&db).await.unwrap().tokens().await.unwrap().len(), 1, "and no token is added");
    admin.execute("DROP SCHEMA issue CASCADE").await.unwrap();
}
