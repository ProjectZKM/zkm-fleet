-- The schema 14 a production database had (pg_dump --schema-only), unqualified: the
-- migration test loads it into a scratch schema and migrates it.

CREATE TABLE fleet_apps (
    priority integer DEFAULT 0 NOT NULL,
    namespace text NOT NULL,
    tenant text NOT NULL,
    elf_id text NOT NULL,
    created_ms bigint NOT NULL,
    disabled_ms bigint,
    CONSTRAINT fleet_apps_elf_id_hex CHECK ((elf_id ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT fleet_apps_tenant_named CHECK ((tenant <> ''::text))
);

COMMENT ON TABLE fleet_apps IS 'Proof apps: a tenant''s registered programs (sha256 of the ELF). A job runs only for one of them; disabled_ms is set while the app takes no new jobs.';

CREATE TABLE fleet_jobs (
    id text NOT NULL,
    owner text NOT NULL,
    namespace text NOT NULL,
    tag text NOT NULL,
    client_id text NOT NULL,
    priority integer DEFAULT 0 NOT NULL,
    tenant text NOT NULL,
    elf_id text NOT NULL,
    inputs jsonb,
    outputs jsonb,
    kind text NOT NULL,
    state text NOT NULL,
    fail_status text,
    error text DEFAULT ''::text NOT NULL,
    node text,
    attempts integer DEFAULT 0 NOT NULL,
    failed_on text[] DEFAULT '{}'::text[] NOT NULL,
    cycles bigint DEFAULT 0 NOT NULL,
    prove_ms bigint DEFAULT 0 NOT NULL,
    created_ms bigint NOT NULL,
    started_ms bigint,
    finished_ms bigint,
    CONSTRAINT fleet_jobs_attempts_counted CHECK ((attempts >= 0)),
    CONSTRAINT fleet_jobs_client_id_safe CHECK ((client_id ~ '^[A-Za-z0-9_-]{1,128}$'::text)),
    CONSTRAINT fleet_jobs_cycles_counted CHECK ((cycles >= 0)),
    CONSTRAINT fleet_jobs_ended_has_finish CHECK (((state = ANY (ARRAY['succeeded'::text, 'failed'::text])) = (finished_ms IS NOT NULL))),
    CONSTRAINT fleet_jobs_fail_status_known CHECK ((fail_status = ANY (ARRAY['success'::text, 'unspecified'::text, 'computing'::text, 'invalid_parameter'::text, 'internal_error'::text, 'split_error'::text, 'prove_error'::text, 'agg_error'::text, 'snark_error'::text]))),
    CONSTRAINT fleet_jobs_failed_has_status CHECK (((state = 'failed'::text) = (fail_status IS NOT NULL))),
    CONSTRAINT fleet_jobs_id_safe CHECK ((id ~ '^[A-Za-z0-9_-]{1,128}$'::text)),
    CONSTRAINT fleet_jobs_kind_known CHECK ((kind = ANY (ARRAY['unspecified'::text, 'core'::text, 'compressed'::text, 'groth16'::text, 'plonk'::text, 'compress_to_groth16'::text]))),
    CONSTRAINT fleet_jobs_owner_named CHECK ((owner ~ '^[0-9a-f]{40}$'::text)),
    CONSTRAINT fleet_jobs_prove_ms_counted CHECK ((prove_ms >= 0)),
    CONSTRAINT fleet_jobs_state_known CHECK ((state = ANY (ARRAY['queued'::text, 'running'::text, 'succeeded'::text, 'failed'::text]))),
    CONSTRAINT fleet_jobs_tag_scope CHECK (((tag = ''::text) OR (tag ~ '^[A-Za-z][A-Za-z0-9_.-]{0,62}=[A-Za-z0-9_.-]{1,63}$'::text)))
);

COMMENT ON TABLE fleet_jobs IS 'Proof jobs: instances of a proof app (a tenant''s program). Payloads are files in the data dir.';

COMMENT ON COLUMN fleet_jobs.id IS 'Internal proof ID; namespaced requests retain the SDK ID in client_id.';

COMMENT ON COLUMN fleet_jobs.owner IS 'Authenticated submitting address; immutable audit identity.';

COMMENT ON COLUMN fleet_jobs.namespace IS 'Immutable namespace ID; resource accounting and access scope.';

COMMENT ON COLUMN fleet_jobs.tag IS 'Immutable optional isolation label; empty string means untagged.';

COMMENT ON COLUMN fleet_jobs.client_id IS 'Required SDK proof_id; immutable within namespace and tag.';

COMMENT ON COLUMN fleet_jobs.tenant IS 'Empty for a request refused before its tenant was known.';

COMMENT ON COLUMN fleet_jobs.elf_id IS 'Hex SHA-256 of the guest ELF; empty for a request refused before it was read.';

COMMENT ON COLUMN fleet_jobs.fail_status IS 'The stage status a failed job ended with (what GetStatus answers).';

COMMENT ON COLUMN fleet_jobs.node IS 'Name of the node it runs or ran on (stable across node restarts).';

COMMENT ON COLUMN fleet_jobs.failed_on IS 'Nodes an attempt failed on; a retry avoids them.';

COMMENT ON COLUMN fleet_jobs.started_ms IS 'Unix ms the current (or last) attempt started; cleared when it is queued again.';

CREATE TABLE fleet_namespace_allocations (
    id text NOT NULL,
    namespace text NOT NULL,
    tag text,
    gpus double precision NOT NULL,
    cpus double precision NOT NULL,
    CONSTRAINT fleet_namespace_allocations_cpus_check CHECK (((cpus >= (0)::double precision) AND (cpus < 'Infinity'::double precision))),
    CONSTRAINT fleet_namespace_allocations_gpus_check CHECK (((gpus >= (0)::double precision) AND (gpus < 'Infinity'::double precision))),
    CONSTRAINT fleet_namespace_allocations_tag_valid CHECK (((tag IS NULL) OR (tag ~ '^[A-Za-z][A-Za-z0-9_.-]{0,62}=[A-Za-z0-9_.-]{1,63}$'::text)))
);

CREATE TABLE fleet_namespaces (
    id text NOT NULL,
    name text NOT NULL,
    created_ms bigint NOT NULL,
    suspended_ms bigint,
    quota jsonb DEFAULT '{"cpus": null, "gpus": null}'::jsonb NOT NULL,
    tag_quotas jsonb DEFAULT '{}'::jsonb NOT NULL,
    CONSTRAINT fleet_namespaces_id_check CHECK ((id ~ '^[0-9a-f]{32}$'::text)),
    CONSTRAINT fleet_namespaces_name_check CHECK ((name ~ '^[a-z][a-z0-9-]{0,62}$'::text))
);

CREATE TABLE fleet_schema (
    version integer NOT NULL,
    applied_ms bigint NOT NULL
);

COMMENT ON TABLE fleet_schema IS 'The current database schema version.';

CREATE TABLE fleet_tokens (
    id text NOT NULL,
    address text NOT NULL,
    tenant text NOT NULL,
    label text DEFAULT ''::text NOT NULL,
    created_ms bigint NOT NULL,
    revoked_ms bigint,
    namespace text,
    tag text,
    apps text[],
    token_hash text,
    manage_apps boolean DEFAULT false NOT NULL,
    CONSTRAINT fleet_tokens_address_hex CHECK ((address ~ '^[0-9a-f]{40}$'::text)),
    CONSTRAINT fleet_tokens_id_hex CHECK ((id ~ '^[0-9a-f]{32}$'::text)),
    CONSTRAINT fleet_tokens_tag_scope CHECK (((tag IS NULL) OR ((namespace IS NOT NULL) AND (tag ~ '^[A-Za-z][A-Za-z0-9_.-]{0,62}=[A-Za-z0-9_.-]{1,63}$'::text)))),
    CONSTRAINT fleet_tokens_tenant_named CHECK ((tenant <> ''::text))
);

COMMENT ON TABLE fleet_tokens IS 'Administrator-managed API tokens with optional namespace, tag and app scope; the address binds the token to the SDK proof signer.';

COMMENT ON COLUMN fleet_tokens.id IS 'Opaque token resource identifier; unrelated to proof signing identity.';

COMMENT ON COLUMN fleet_tokens.address IS 'Internal SDK proof signer binding; not exposed by token APIs.';

ALTER TABLE ONLY fleet_apps
    ADD CONSTRAINT fleet_apps_pkey PRIMARY KEY (namespace, tenant, elf_id);

ALTER TABLE ONLY fleet_jobs
    ADD CONSTRAINT fleet_jobs_pkey PRIMARY KEY (id);

ALTER TABLE ONLY fleet_namespace_allocations
    ADD CONSTRAINT fleet_namespace_allocations_pkey PRIMARY KEY (id);

ALTER TABLE ONLY fleet_namespaces
    ADD CONSTRAINT fleet_namespaces_name_key UNIQUE (name);

ALTER TABLE ONLY fleet_namespaces
    ADD CONSTRAINT fleet_namespaces_pkey PRIMARY KEY (id);

ALTER TABLE ONLY fleet_schema
    ADD CONSTRAINT fleet_schema_pkey PRIMARY KEY (version);

ALTER TABLE ONLY fleet_tokens
    ADD CONSTRAINT fleet_tokens_address_key UNIQUE (address);

ALTER TABLE ONLY fleet_tokens
    ADD CONSTRAINT fleet_tokens_pkey PRIMARY KEY (id);

ALTER TABLE ONLY fleet_tokens
    ADD CONSTRAINT fleet_tokens_token_hash_key UNIQUE (token_hash);

CREATE INDEX fleet_jobs_active ON fleet_jobs USING btree (created_ms) WHERE (state = ANY (ARRAY['queued'::text, 'running'::text]));

CREATE INDEX fleet_jobs_app ON fleet_jobs USING btree (tenant, elf_id, created_ms DESC);

CREATE INDEX fleet_jobs_created ON fleet_jobs USING btree (created_ms DESC);

CREATE UNIQUE INDEX fleet_jobs_namespace_client ON fleet_jobs USING btree (namespace, tag, client_id);

CREATE INDEX fleet_jobs_namespace_created ON fleet_jobs USING btree (namespace, created_ms DESC);

CREATE INDEX fleet_jobs_owner ON fleet_jobs USING btree (owner, created_ms DESC);

CREATE INDEX fleet_namespace_allocations_namespace ON fleet_namespace_allocations USING btree (namespace);

ALTER TABLE ONLY fleet_apps
    ADD CONSTRAINT fleet_apps_namespace_fkey FOREIGN KEY (namespace) REFERENCES fleet_namespaces(id);

ALTER TABLE ONLY fleet_jobs
    ADD CONSTRAINT fleet_jobs_namespace_fkey FOREIGN KEY (namespace) REFERENCES fleet_namespaces(id);

ALTER TABLE ONLY fleet_namespace_allocations
    ADD CONSTRAINT fleet_namespace_allocations_namespace_fkey FOREIGN KEY (namespace) REFERENCES fleet_namespaces(id);

ALTER TABLE ONLY fleet_tokens
    ADD CONSTRAINT fleet_tokens_namespace_fkey FOREIGN KEY (namespace) REFERENCES fleet_namespaces(id);

