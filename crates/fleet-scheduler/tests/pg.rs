//! The Postgres store against a real database: FLEET_TEST_PG_URL (a database the test
//! may fill: it makes and drops the schemas `fresh` and `migrated`). Skipped without it.

use fleet_proto::fleet::v1::ProofKind;
use fleet_proto::stage::v1::Status as StageStatus;
use fleet_scheduler::store::{Credential, Job, JobFilter, JobState, PgStore, Store};
use sqlx::{Executor, PgPool, Row};

/// The tables as schema 0 made them.
const SCHEMA_0: &str = r#"
CREATE TABLE fleet_jobs (
    id TEXT PRIMARY KEY, tenant TEXT NOT NULL, elf_id TEXT NOT NULL, kind INT NOT NULL, state INT NOT NULL,
    status INT NOT NULL DEFAULT 0, error TEXT NOT NULL DEFAULT '', node TEXT, attempts INT NOT NULL DEFAULT 0,
    failed_on TEXT NOT NULL DEFAULT '', cycles BIGINT NOT NULL DEFAULT 0, prove_ms BIGINT NOT NULL DEFAULT 0,
    created_ms BIGINT NOT NULL, started_ms BIGINT, finished_ms BIGINT
);
CREATE INDEX fleet_jobs_incomplete ON fleet_jobs (state) WHERE state < 2;
CREATE TABLE fleet_credentials (
    address TEXT PRIMARY KEY, tenant TEXT NOT NULL, label TEXT NOT NULL DEFAULT '', created_ms BIGINT NOT NULL,
    revoked_ms BIGINT
);
INSERT INTO fleet_jobs (id, tenant, elf_id, kind, state, status, error, node, attempts, failed_on, cycles, prove_ms,
                        created_ms, started_ms, finished_ms) VALUES
  ('q', 'eth', repeat('a', 64), 2, 0, 0, '', NULL, 0, '', 0, 0, 1000, NULL, NULL),
  ('r', 'eth', repeat('a', 64), 2, 1, 0, '', 'n1', 1, '', 0, 0, 1001, 1100, NULL),
  ('s', 'eth', repeat('a', 64), 2, 2, 0, '', 'n1', 1, '', 7, 50, 1002, 1100, 1200),
  ('f', 'eth', repeat('a', 64), 2, 3, 6, 'boom', 'n2', 2, 'n1,n2', 0, 0, 1003, 1100, 1300),
  ('x', '', '', 0, 3, 3, 'bad request', NULL, 0, '', 0, 0, 1004, NULL, 1004);
INSERT INTO fleet_credentials VALUES ('8468000000000000000000000000000000000000', 'eth', 'poller', 5, NULL);
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
        id: id.into(),
        tenant: "goat".into(),
        elf_id: "b".repeat(64),
        kind: ProofKind::Groth16,
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

#[tokio::test]
async fn schema_0_migrates_to_what_a_new_database_has() {
    let Some(url) = url() else {
        eprintln!("FLEET_TEST_PG_URL unset: skipped");
        return;
    };
    let admin = PgPool::connect(&url).await.unwrap();
    fresh_schema(&admin, "fresh").await;
    fresh_schema(&admin, "migrated").await;
    let mig = PgPool::connect(&in_schema(&url, "migrated")).await.unwrap();
    sqlx::raw_sql(SCHEMA_0).execute(&mig).await.unwrap();

    let fresh = PgStore::connect(&in_schema(&url, "fresh")).await.unwrap();
    let migrated = PgStore::connect(&in_schema(&url, "migrated")).await.unwrap();
    assert_eq!(ddl(&admin, "fresh").await, ddl(&admin, "migrated").await, "the same tables either way");
    // a second start finds schema 1 and leaves it
    PgStore::connect(&in_schema(&url, "migrated")).await.unwrap();
    let versions: Vec<i32> = sqlx::query_scalar("SELECT version FROM migrated.fleet_schema").fetch_all(&admin).await.unwrap();
    assert_eq!(versions, vec![1, 2, 3], "every step is recorded");
    // 2 -> 3 registered the program that had jobs, from its first one; the refused request's entry is no app
    assert_eq!(migrated.registered_apps().await.unwrap(), vec![("eth".to_string(), "a".repeat(64), false)]);
    let listed = migrated.apps().await.unwrap();
    let eth = listed.iter().find(|a| a.tenant == "eth").unwrap();
    assert_eq!((eth.created_ms, eth.jobs, eth.active, eth.succeeded, eth.failed), (1000, 4, 2, 1, 1));
    let rejected = listed.iter().find(|a| a.tenant.is_empty()).unwrap();
    assert_eq!((rejected.jobs, rejected.failed, rejected.created_ms), (1, 1, 1004));

    let get = |id: &'static str| {
        let m = &migrated;
        async move { m.get(id).await.unwrap().unwrap() }
    };
    assert_eq!(get("q").await.state, JobState::Queued);
    assert_eq!(get("r").await.state, JobState::Running);
    let s = get("s").await;
    assert_eq!((s.state, s.kind, s.cycles, s.prove_ms), (JobState::Succeeded, ProofKind::Compressed, 7, 50));
    let f = get("f").await;
    assert_eq!(f.state, JobState::Failed(StageStatus::ProveError));
    assert_eq!(f.failed_on, vec!["n1".to_string(), "n2".to_string()]);
    assert_eq!(get("x").await.state, JobState::Failed(StageStatus::InvalidParameter));
    let row = sqlx::query("SELECT state, fail_status, kind FROM migrated.fleet_jobs WHERE id = 'f'")
        .fetch_one(&admin)
        .await
        .unwrap();
    let names: (String, Option<String>, String) = (row.get(0), row.get(1), row.get(2));
    assert_eq!(names, ("failed".into(), Some("prove_error".into()), "compressed".into()));
    assert_eq!(migrated.credentials().await.unwrap().len(), 1);

    let bb = "b".repeat(64);
    for store in [&fresh, &migrated] {
        assert!(store.add_app("goat", &bb, 1999).await.unwrap());
        assert!(!store.add_app("goat", &bb, 2000).await.unwrap(), "registered once");
        let fresh_app = store.apps().await.unwrap().into_iter().find(|a| a.elf_id == bb).unwrap();
        assert_eq!((fresh_app.jobs, fresh_app.created_ms, fresh_app.last_created_ms), (0, 1999, 0), "listed with no job yet");
        store.upsert(&job("g1", JobState::Running)).await.unwrap();
        store.upsert(&job("g2", JobState::Failed(StageStatus::SnarkError))).await.unwrap();
        store.upsert(&job("g2", JobState::Failed(StageStatus::SnarkError))).await.unwrap();
        let active = store
            .recent(&JobFilter { tenant: Some("goat".into()), elf: Some("b".into()), active: true }, 10)
            .await
            .unwrap();
        assert_eq!(active.iter().map(|j| j.id.as_str()).collect::<Vec<_>>(), vec!["g1"]);
        let apps = store.apps().await.unwrap();
        let goat = apps.iter().find(|a| a.tenant == "goat").unwrap();
        assert_eq!((goat.jobs, goat.active, goat.failed), (2, 1, 1));
        store.set_app_disabled("goat", &bb, true, 5).await.unwrap();
        assert!(store.registered_apps().await.unwrap().contains(&("goat".to_string(), bb.clone(), true)));
        assert!(store.apps().await.unwrap().iter().find(|a| a.tenant == "goat").unwrap().disabled);
        store.set_app_disabled("goat", &bb, false, 7).await.unwrap();
        assert!(store.registered_apps().await.unwrap().contains(&("goat".to_string(), bb.clone(), false)));
        assert_eq!(store.delete_app("goat", &bb).await.unwrap(), vec!["g2".to_string()], "the running one stays");
        assert!(!store.registered_apps().await.unwrap().iter().any(|(t, _, _)| t == "goat"), "the app is gone");
        assert_eq!(store.incomplete().await.unwrap().iter().filter(|j| j.id == "g1").count(), 1);
    }
    // what the constraints refuse
    let bad = [
        "INSERT INTO fresh.fleet_jobs (id, tenant, elf_id, kind, state, created_ms) VALUES ('b1', 't', 'e', 'core', 'done', 1)",
        "INSERT INTO fresh.fleet_jobs (id, tenant, elf_id, kind, state, created_ms, finished_ms) VALUES ('b2', 't', 'e', 'core', 'failed', 1, 2)",
        "INSERT INTO fresh.fleet_jobs (id, tenant, elf_id, kind, state, created_ms) VALUES ('b3', 't', 'e', 'core', 'succeeded', 1)",
        "INSERT INTO fresh.fleet_credentials (address, tenant, created_ms) VALUES ('0xABC', 't', 1)",
        "INSERT INTO fresh.fleet_apps (tenant, elf_id, created_ms) VALUES ('', repeat('a', 64), 1)",
        "INSERT INTO fresh.fleet_apps (tenant, elf_id, created_ms) VALUES ('t', 'abc', 1)",
    ];
    for sql in bad {
        assert!(sqlx::query(sql).execute(&admin).await.is_err(), "accepted: {sql}");
    }
    let c = Credential { address: "84".repeat(20), tenant: "eth".into(), label: String::new(), created_ms: 1, revoked_ms: None };
    fresh.add_credential(&c).await.unwrap();

    admin.execute("DROP SCHEMA fresh CASCADE; DROP SCHEMA migrated CASCADE").await.unwrap();
}
