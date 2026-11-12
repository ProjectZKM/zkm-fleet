//! The JSON shapes the scheduler and the nodes exchange about workloads. Records keep them as
//! JSON, as a node configured or reported them, so keys the scheduler does not know pass
//! through untouched; these views read and write the keys it does know, each in one place.
use crate::fleet::v1::NodeOperation;
use crate::model::v1::{AppDeployment, CommandRecord, Job, JobReport, NodeReport, ScaleStatus, WorkloadTemplate};
use serde_json::{json, Value};

fn text<'a>(v: &'a Value, key: &str) -> &'a str {
    v[key].as_str().unwrap_or("")
}

/// A node's `apps.yaml` entry for one app, as a deployment, an app's template, a change's
/// target or a start carries it. In a node's report it also says how the app runs there.
#[derive(Clone, Copy, Debug)]
pub struct Entry<'a>(pub &'a Value);

impl<'a> Entry<'a> {
    /// What a scale changes: the GPUs and CPUs of each replica, and how many replicas run.
    pub const RESOURCES: [&'static str; 3] = ["gpus", "cpus", "replicas"];
    /// The node's own configuration of an entry: the app it runs, its program and how its
    /// worker launches. No change the scheduler sends replaces them.
    pub const NODE_OWNED: [&'static str; 8] = ["app", "elf_id", "worker_command", "worker_env_file",
        "worker_workdir", "worker_stop_timeout_s", "machine", "proof_storage_root"];
    /// What a report says about an entry beside its configuration, and the namespace reference
    /// the scheduler keeps from it.
    pub const REPORTED: [&'static str; 4] = ["ready", "logs", "cleaned", "namespace_ref"];

    /// The entry's own name, unique on its node.
    pub fn name(self) -> &'a str {
        text(self.0, "name")
    }
    /// The app the entry runs.
    pub fn app(self) -> &'a str {
        text(self.0, "app")
    }
    /// The namespace's id once the scheduler has resolved it.
    pub fn namespace(self) -> &'a str {
        text(self.0, "namespace")
    }
    /// How the node's configuration names the namespace, by id or by name.
    pub fn namespace_reference(self) -> Option<&'a str> {
        self.0["namespace_ref"].as_str().or_else(|| self.0["namespace"].as_str())
    }
    pub fn tag(self) -> Option<&'a str> {
        self.0["tag"].as_str()
    }
    /// `app` for a container, `ziren-gpu` or `ziren-cpu` for a prover.
    pub fn kind(self) -> &'a str {
        text(self.0, "kind")
    }
    pub fn image(self) -> &'a str {
        text(self.0, "image")
    }
    /// The machine the node's configuration anchors the entry to.
    pub fn machine(self) -> &'a str {
        text(self.0, "machine")
    }
    pub fn priority(self) -> i64 {
        self.0["priority"].as_i64().unwrap_or(0)
    }
    pub fn disabled(self) -> bool {
        self.0["disabled"].as_bool() == Some(true)
    }
    /// In a report: every replica runs and is healthy.
    pub fn ready(self) -> bool {
        self.0["ready"].as_bool() == Some(true)
    }
    /// In a report: nothing of the app is left running on the node.
    pub fn cleaned(self) -> bool {
        self.0["cleaned"].as_bool() == Some(true)
    }
    /// The GPUs of each replica, when the entry gives them.
    pub fn gpus(self) -> Option<f64> {
        self.0["gpus"].as_f64()
    }
    /// The GPUs of each replica: one where the entry gives none, none for a CPU prover.
    pub fn gpus_or_default(self) -> f64 {
        self.gpus().unwrap_or(if self.kind() == "ziren-cpu" { 0.0 } else { 1.0 })
    }
    /// The CPUs of each replica, one where the entry gives none.
    pub fn cpus(self) -> f64 {
        self.0["cpus"].as_f64().unwrap_or(1.0)
    }
    /// How many replicas run, one where the entry gives none.
    pub fn replicas(self) -> u64 {
        self.0["replicas"].as_u64().unwrap_or(1)
    }
    fn replica_count(self) -> f64 {
        self.0["replicas"].as_f64().unwrap_or(1.0)
    }
    /// The GPUs and CPUs of all its replicas; a replica counts `gpus` GPUs where the entry
    /// gives none.
    pub fn total(self, gpus: f64) -> (f64, f64) {
        let n = self.replica_count();
        (self.gpus().unwrap_or(gpus) * n, self.cpus() * n)
    }
    /// Whether `other` gives it the same resources. A missing setting is one, and no GPU when
    /// `other` is a CPU prover.
    pub fn same_resources(self, other: Entry) -> bool {
        let gpus = if other.kind() == "ziren-cpu" { 0.0 } else { 1.0 };
        (self.gpus().unwrap_or(gpus) - other.gpus().unwrap_or(gpus)).abs() < 1e-9
            && (self.cpus() - other.cpus()).abs() < 1e-9
            && (self.replica_count() - other.replica_count()).abs() < 1e-9
    }
    /// How long its replicas take to become ready before a start fails.
    pub fn ready_timeout_s(self) -> f64 {
        self.0["ready_timeout_s"].as_f64().unwrap_or(600.0)
    }
    pub fn is_container(self) -> bool {
        self.kind() == "app"
    }
    pub fn is_prover(self) -> bool {
        self.kind().starts_with("ziren-")
    }
    /// A GPU prover whose node also runs the app's own worker against it, on one replica.
    pub fn integrated_worker(self) -> bool {
        self.kind() == "ziren-gpu" && self.0["worker_command"].as_array().is_some_and(|command| !command.is_empty())
    }
    /// Whether the deployment holds its namespace's quota for all its replicas: a container,
    /// or an integrated worker. A plain prover is charged per proof instead.
    pub fn holds_allocation(self) -> bool {
        self.is_container() || self.integrated_worker()
    }
}

/// An entry being changed.
pub struct EntryMut<'a>(pub &'a mut Value);

impl EntryMut<'_> {
    pub fn set_disabled(&mut self, disabled: bool) {
        self.0["disabled"] = json!(disabled);
    }
    pub fn set_priority(&mut self, priority: i64) {
        self.0["priority"] = json!(priority);
    }
    /// Gives the entry priority 0 where it has none.
    pub fn default_priority(&mut self) {
        if self.0["priority"].is_null() {
            self.set_priority(0);
        }
    }
    /// Sets the resources a scale gives, keeping the others.
    pub fn set_resources(&mut self, gpus: Option<f64>, cpus: Option<u32>, replicas: Option<u32>) {
        if let Some(v) = gpus {
            self.0["gpus"] = json!(v);
        }
        if let Some(v) = cpus {
            self.0["cpus"] = json!(v);
        }
        if let Some(v) = replicas {
            self.0["replicas"] = json!(v);
        }
    }
    /// Keeps how the node names the namespace, and the namespace's id once it is known.
    pub fn set_namespace(&mut self, reference: &str, id: Option<&str>) {
        self.0["namespace_ref"] = json!(reference);
        if let Some(id) = id {
            self.0["namespace"] = json!(id);
        }
    }
    /// Drops what a report said about the entry, keeping its configuration.
    pub fn forget_report(&mut self) {
        if let Some(fields) = self.0.as_object_mut() {
            for key in Entry::REPORTED {
                fields.remove(key);
            }
        }
    }
}

impl AppDeployment {
    /// Its entry as the scheduler last configured it.
    pub fn entry(&self) -> Entry<'_> {
        Entry(&self.spec)
    }
}

impl ScaleStatus {
    /// The entry the change ends with.
    pub fn target_entry(&self) -> Entry<'_> {
        Entry(&self.target)
    }
}

impl NodeReport {
    /// The entries the node configures, with how each runs there.
    pub fn entries(&self) -> impl Iterator<Item = Entry<'_>> {
        self.apps.iter().map(|v| Entry(v))
    }
    /// Its entry named `name`.
    pub fn entry(&self, name: &str) -> Option<Entry<'_>> {
        self.entries().find(|e| e.name() == name)
    }
}

impl WorkloadTemplate {
    /// An app's entry as the scheduler creates it on a node; a batch's settings.
    pub fn entry(&self) -> Entry<'_> {
        Entry(&self.spec)
    }
    /// The tag it belongs to: the one it was created for, or the one its node configures.
    pub fn fixed_tag(&self) -> Option<&str> {
        self.tag.as_deref().or_else(|| self.spec["tag"].as_str())
    }
    pub fn disabled(&self) -> bool {
        self.spec["disabled"].as_bool() == Some(true)
    }
    pub fn set_disabled(&mut self, disabled: bool) {
        self.spec["disabled"] = json!(disabled);
    }
    pub fn priority(&self) -> &Value {
        &self.spec["priority"]
    }
    pub fn set_priority(&mut self, priority: i64) {
        self.spec["priority"] = json!(priority);
    }
}

/// A batch's settings, as its template keeps them and each of its jobs keeps them as its policy.
#[derive(Clone, Copy, Debug)]
pub struct Batch<'a>(pub &'a Value);

impl<'a> Batch<'a> {
    /// The most GPUs one job may ask for.
    pub fn max_gpus(self) -> f64 {
        self.0["max_gpus"].as_f64().unwrap_or(8.0)
    }
    /// The VRAM limit of a job that gives none.
    pub fn default_gpu_mem_gb(self) -> u64 {
        self.0["default_gpu_mem_gb"].as_u64().unwrap_or(0)
    }
    /// The host paths its jobs may mount.
    pub fn allow(self) -> &'a [Value] {
        self.0["allow"].as_array().map(Vec::as_slice).unwrap_or_default()
    }
    /// The user its jobs run as.
    pub fn user(self) -> Option<&'a str> {
        self.0["user"].as_str()
    }
    pub fn priority(self) -> i64 {
        self.0["priority"].as_i64().unwrap_or(0)
    }
    /// Whether a higher-priority job may stop its running jobs.
    pub fn preemptible(self) -> bool {
        self.0["preemptible"].as_bool().unwrap_or(true)
    }
    /// How long a job runs before it may be preempted.
    pub fn min_runtime_s(self) -> u64 {
        self.0["min_runtime_s"].as_u64().unwrap_or(300)
    }
}

impl WorkloadTemplate {
    /// A batch's settings.
    pub fn batch(&self) -> Batch<'_> {
        Batch(&self.spec)
    }
}

/// A batch job's submission, as its owner sent it and its record keeps it once checked.
#[derive(Clone, Copy, Debug)]
pub struct Submission<'a>(pub &'a Value);

impl<'a> Submission<'a> {
    /// The name its job's id starts with.
    pub fn name(self) -> &'a str {
        text(self.0, "name")
    }
    /// The GPUs it asks for: one where it gives none.
    pub fn gpus(self) -> f64 {
        self.0["gpus"].as_f64().unwrap_or(1.0)
    }
    /// The CPUs it asks for: eight where it gives none.
    pub fn cpus(self) -> u64 {
        self.0["cpus"].as_u64().unwrap_or(8)
    }
    /// The machines it runs on: one where it gives none.
    pub fn nodes(self) -> u64 {
        self.0["nodes"].as_u64().unwrap_or(1)
    }
    /// Its VRAM limit, when it gives one.
    pub fn gpu_mem(self) -> Option<u64> {
        self.0["gpu_mem"].as_u64()
    }
    /// Its command, when it is a non-empty list of strings.
    pub fn command(self) -> Option<&'a [Value]> {
        self.0["command"].as_array().filter(|cmd| !cmd.is_empty() && cmd.iter().all(Value::is_string)).map(Vec::as_slice)
    }
    /// The host paths it mounts, each `PATH[:MODE]`.
    pub fn mounts(self) -> &'a [Value] {
        self.0["mount"].as_array().map(Vec::as_slice).unwrap_or_default()
    }
    /// The machine it is pinned to.
    pub fn machine(self) -> Option<&'a str> {
        self.0["machine"].as_array().and_then(|m| m.first()).and_then(Value::as_str)
    }
    /// The job it waits for.
    pub fn after(self) -> Option<&'a str> {
        self.0["after"].as_str()
    }
    /// Whether it names its own log file.
    pub fn names_log(self) -> bool {
        self.0["log"].is_string()
    }
}

/// A submission being completed with what its batch and the scheduler decide.
pub struct SubmissionMut<'a>(pub &'a mut Value);

impl SubmissionMut<'_> {
    /// Records the resources it gets, on one machine.
    pub fn set_resources(&mut self, gpus: f64, cpus: u64, gpu_mem: u64) {
        self.0["gpus"] = json!(gpus);
        self.0["cpus"] = json!(cpus);
        self.0["gpu_mem"] = json!(gpu_mem);
        self.0["nodes"] = json!(1);
    }
}

impl Job {
    /// The record as listings show it: its status with who and where.
    pub fn value(&self) -> Value {
        let mut v = self.status.0.clone();
        for (k, x) in [
            ("type", json!("batch")),
            ("id", json!(self.id)),
            ("batch", json!(self.batch)),
            ("namespace", json!(self.namespace)),
            ("tag", json!(self.tag)),
            ("owner", json!(self.owner)),
            ("priority", self.policy["priority"].clone()),
            ("worker", json!(self.worker)),
            ("node", json!(self.worker)),
        ] {
            v[k] = x;
        }
        v
    }
    pub fn state(&self) -> &str {
        self.status["state"].as_str().unwrap_or("")
    }
    pub fn gpus(&self) -> f64 {
        self.request["gpus"].as_f64().unwrap_or(0.0)
    }
    pub fn cpus(&self) -> f64 {
        self.request["cpus"].as_f64().unwrap_or(1.0)
    }
    /// What it was submitted with.
    pub fn submission(&self) -> Submission<'_> {
        Submission(&self.request)
    }
    /// Its batch's settings when it was submitted.
    pub fn policy(&self) -> Batch<'_> {
        Batch(&self.policy)
    }
    pub fn submitted_at(&self) -> Option<f64> {
        self.status["submitted_at"].as_f64()
    }
    pub fn started_at(&self) -> Option<f64> {
        self.status["started_at"].as_f64()
    }
    pub fn finished_at(&self) -> Option<f64> {
        self.status["finished_at"].as_f64()
    }
    pub fn set_state(&mut self, state: &str) {
        self.status["state"] = json!(state);
    }
    pub fn set_detail(&mut self, detail: &str) {
        self.status["detail"] = json!(detail);
    }
    /// Ends it in `state` at `at`.
    pub fn finish(&mut self, state: &str, at: f64) {
        self.set_state(state);
        self.status["finished_at"] = json!(at);
    }
    /// Ends it failed with `error` at `at`.
    pub fn fail(&mut self, error: impl serde::Serialize, at: f64) {
        self.set_state("failed");
        self.status["error"] = json!(error);
        self.status["finished_at"] = json!(at);
    }
    /// Puts it back in the queue, unfinished.
    pub fn requeue(&mut self) {
        self.set_state("waiting_quota");
        self.status["finished_at"] = Value::Null;
    }
}

impl JobReport {
    pub fn state(&self) -> &str {
        text(&self.status, "state")
    }
    /// The batch of a job the node runs.
    pub fn batch(&self) -> &str {
        text(&self.spec, "batch")
    }
    pub fn owner(&self) -> &str {
        self.spec["owner"].as_str().unwrap_or("admin")
    }
    pub fn tag(&self) -> Option<&str> {
        self.spec["tag"].as_str()
    }
    pub fn policy(&self) -> &Value {
        &self.spec["policy"]
    }
    /// The submission of a job the node runs that the scheduler has no record of: its command
    /// and resources, on one machine.
    pub fn submission(&self) -> Value {
        let mut request = json!({"command": self.spec["command"], "gpus": self.spec["gpus"], "cpus": self.spec["cpus"], "nodes": 1});
        if request["gpus"].is_null() {
            request["gpus"] = json!(0);
        }
        request
    }
}

impl CommandRecord {
    pub fn is(&self, operation: NodeOperation) -> bool {
        self.operation == operation as i32
    }
    /// A job command's job.
    pub fn job_id(&self) -> Option<&str> {
        self.parameters["id"].as_str()
    }
    /// A job command's execution lease.
    pub fn lease(&self) -> &str {
        text(&self.parameters, "lease")
    }
    /// A start or stop's entry.
    pub fn entry(&self) -> Entry<'_> {
        Entry(&self.parameters["app"])
    }
    /// Whether it starts its entry.
    pub fn starts(&self) -> bool {
        self.parameters["disabled"].as_bool() == Some(false)
    }
    /// Whether it stops its entry.
    pub fn stops(&self) -> bool {
        self.parameters["disabled"].as_bool() == Some(true)
    }
    /// The change a start or stop is a step of.
    pub fn scale_id(&self) -> Option<&str> {
        self.parameters["scale_id"].as_str()
    }
    /// Whether a stop cleans up after a failed change.
    pub fn scale_cleanup(&self) -> bool {
        self.parameters["scale_cleanup"].as_bool() == Some(true)
    }
    /// Whether it is a start or stop of entry `name` on `worker` that its node has not run yet.
    pub fn changes_entry(&self, worker: &str, name: &str) -> bool {
        !self.done && self.worker == worker && self.is(NodeOperation::SetApp) && self.entry().name() == name
    }
    /// The entry an app message creates on its node.
    pub fn created(&self) -> Option<Entry<'_>> {
        self.parameters.get("create").map(Entry)
    }
    /// The entry whose settings an app message changes.
    pub fn changed_entry(&self) -> &str {
        text(&self.parameters["entry"], "name")
    }
    /// The namespace and name of the app whose entries an app message removes.
    pub fn removal(&self) -> Option<(&str, &str)> {
        let app = &self.parameters["app"];
        (self.is(NodeOperation::App) && self.parameters["remove"] == json!(true))
            .then(|| (text(app, "namespace"), text(app, "name")))
    }
    /// Whether it is an app message removing the entries that run `app`.
    pub fn removes_app(&self, app: &str) -> bool {
        self.removal().is_some_and(|(_, name)| name == app)
    }
    /// The app or batch an app message removes, by name.
    pub fn removes(&self) -> Option<&str> {
        let p = &self.parameters;
        (self.is(NodeOperation::App) && p["remove"] == json!(true))
            .then(|| p["app"]["name"].as_str().or(p["batch"]["name"].as_str()))
            .flatten()
    }
    /// The app or batch an app message deletes, by name: it is removed with what the node
    /// holds of it. A removal that only takes it off one node keeps that.
    pub fn deletes(&self) -> Option<&str> {
        self.removes().filter(|_| self.parameters.get("delete").is_some())
    }
    /// In its result: nothing of what it stopped is left running.
    pub fn cleaned(&self) -> bool {
        self.result["cleaned"].as_bool() == Some(true)
    }
    /// In an app removal's result: the entries the node removed.
    pub fn removed(&self) -> impl Iterator<Item = &str> {
        self.result["removed"].as_array().into_iter().flatten().filter_map(Value::as_str)
    }
    /// In an app change's result: whether the node wrote entry `name`.
    pub fn updated(&self, name: &str) -> bool {
        self.result["updated"].as_array().is_some_and(|names| names.iter().any(|n| n.as_str() == Some(name)))
    }
}

/// The parameters of the commands the scheduler sends a node.
pub mod command {
    use super::*;

    /// Starts an entry, or stops it when `disabled` (NODE_OPERATION_SET_APP).
    pub fn set_app(entry: &Value, disabled: bool) -> Value {
        json!({"app": entry, "disabled": disabled})
    }
    /// Stops an entry whose change failed, cleaning up what is left of it.
    pub fn clean_up(entry: &Value) -> Value {
        json!({"app": entry, "disabled": true, "scale_cleanup": true})
    }
    /// A step of change `scale_id`: `phase` is `stop` or `apply`.
    pub fn scale_step(entry: &Value, disabled: bool, scale_id: &str, phase: &str) -> Value {
        json!({"app": entry, "disabled": disabled, "scale_id": scale_id, "scale_phase": phase})
    }
    /// Runs `job`'s execution lease on the node it is assigned to.
    pub fn launch(job: &Job) -> Value {
        json!({"id": job.id, "lease": job.lease, "namespace": job.namespace, "tag": job.tag, "owner": job.owner,
            "batch": job.batch, "policy": job.policy, "request": job.request, "submitted_at": job.status["submitted_at"]})
    }
    /// Cancels or preempts a job's execution `lease`.
    pub fn job(id: &str, lease: &str) -> Value {
        json!({"id": id, "lease": lease})
    }
    /// An app message's address: the entries that run app `name`. A node's configuration may
    /// name the namespace by id or by name, so both go along.
    pub fn app(namespace: &str, namespace_name: &str, name: &str) -> Value {
        json!({"app": {"namespace": namespace, "namespace_name": namespace_name, "name": name}})
    }
    /// An app message's address: the entry `name`.
    pub fn entry(name: &str) -> Value {
        json!({"entry": {"name": name}})
    }
    /// An app message's address: the batch `name`.
    pub fn batch(name: &str) -> Value {
        json!({"batch": {"name": name}})
    }
    /// Removes what the message addresses.
    pub fn remove() -> Value {
        json!({"remove": true})
    }
    /// Changes these settings of what the message addresses.
    pub fn set(settings: impl serde::Serialize) -> Value {
        json!({"set": settings})
    }
    /// Creates `entry` on the node.
    pub fn create(entry: &Value) -> Value {
        json!({"create": entry})
    }
    /// `address` with `change`: an app message.
    pub fn with(mut address: Value, change: &Value) -> Value {
        for (key, value) in change.as_object().into_iter().flatten() {
            address[key] = value.clone();
        }
        address
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_entry_without_resources_counts_one_cpu_and_replica_and_the_gpus_its_reader_assumes() {
        let bare = json!({"name": "model", "kind": "app"});
        let e = Entry(&bare);
        assert_eq!((e.gpus(), e.cpus(), e.replicas()), (None, 1.0, 1));
        assert_eq!(e.total(0.0), (0.0, 1.0), "reserving and admitting count no GPU");
        assert_eq!(e.total(1.0), (1.0, 1.0), "placing counts one");
        assert_eq!(e.gpus_or_default(), 1.0);
        assert_eq!(Entry(&json!({"kind": "ziren-cpu"})).gpus_or_default(), 0.0);
        let sized = json!({"gpus": 2, "cpus": 8, "replicas": 3});
        assert_eq!(Entry(&sized).total(0.0), (6.0, 24.0));
    }

    #[test]
    fn same_resources_takes_the_missing_gpus_from_the_other_entrys_kind() {
        let missing = json!({"cpus": 1, "replicas": 1});
        assert!(Entry(&missing).same_resources(Entry(&json!({"gpus": 1, "kind": "app"}))));
        assert!(Entry(&missing).same_resources(Entry(&json!({"gpus": 0, "kind": "ziren-cpu"}))));
        assert!(!Entry(&missing).same_resources(Entry(&json!({"gpus": 2}))));
    }

    #[test]
    fn integrated_workers_and_containers_hold_their_allocation() {
        let worker = json!({"kind": "ziren-gpu", "worker_command": ["run"]});
        assert!(Entry(&worker).integrated_worker() && Entry(&worker).holds_allocation());
        assert!(!Entry(&json!({"kind": "ziren-gpu", "worker_command": []})).holds_allocation());
        assert!(Entry(&json!({"kind": "app"})).holds_allocation());
    }

    #[test]
    fn a_submission_and_a_batch_without_settings_take_the_documented_defaults() {
        let bare = json!({"command": ["true"]});
        let sub = Submission(&bare);
        assert_eq!((sub.gpus(), sub.cpus(), sub.nodes(), sub.gpu_mem()), (1.0, 8, 1, None));
        assert_eq!((sub.machine(), sub.after(), sub.names_log(), sub.mounts().len()), (None, None, false, 0));
        assert!(sub.command().is_some());
        assert!(Submission(&json!({"command": []})).command().is_none());
        assert!(Submission(&json!({"command": ["a", 1]})).command().is_none());
        let batch = Batch(&json!({}));
        assert_eq!((batch.max_gpus(), batch.default_gpu_mem_gb(), batch.priority()), (8.0, 0, 0));
        assert_eq!((batch.preemptible(), batch.min_runtime_s(), batch.user(), batch.allow().len()), (true, 300, None, 0));
    }

    #[test]
    fn ending_and_requeueing_a_job_write_its_state_error_and_finish_time() {
        let mut job = Job { status: json!({"state": "running", "submitted_at": 1.0}).into(), ..Default::default() };
        job.fail("out of memory", 5.0);
        assert_eq!(job.status.0, json!({"state": "failed", "submitted_at": 1.0, "error": "out of memory", "finished_at": 5.0}));
        job.requeue();
        assert_eq!((job.state(), job.finished_at()), ("waiting_quota", None));
        job.finish("cancelled", 7.0);
        assert_eq!((job.state(), job.finished_at(), job.submitted_at()), ("cancelled", Some(7.0), Some(1.0)));
    }

    #[test]
    fn commands_read_back_what_their_builders_wrote() {
        let entry = json!({"name": "model-a", "app": "model"});
        let record = |operation: NodeOperation, parameters: Value, result: Value| CommandRecord {
            id: "c".into(), worker: "gpu".into(), operation: operation as i32, parameters: parameters.into(),
            done: false, result: result.into(), error: None, admitted: true,
        };
        let start = record(NodeOperation::SetApp, command::scale_step(&entry, false, "s1", "apply"), json!(null));
        assert!(start.starts() && !start.stops() && start.scale_id() == Some("s1") && !start.scale_cleanup());
        assert!(start.changes_entry("gpu", "model-a") && !start.changes_entry("cpu", "model-a"));
        let stop = record(NodeOperation::SetApp, command::clean_up(&entry), json!({"cleaned": true}));
        assert!(stop.stops() && stop.scale_cleanup() && stop.cleaned());
        let remove = record(NodeOperation::App, command::with(command::app("ns", "team", "model"), &command::remove()),
            json!({"removed": ["model-a"]}));
        assert_eq!(remove.removal(), Some(("ns", "model")));
        assert!(remove.removes_app("model") && !remove.removes_app("other"));
        assert_eq!(remove.removed().collect::<Vec<_>>(), ["model-a"]);
        let create = record(NodeOperation::App, command::with(command::app("ns", "team", "model"), &command::create(&entry)), json!(null));
        assert_eq!(create.created().map(Entry::name), Some("model-a"));
        assert_eq!(create.removal(), None);
        let set = record(NodeOperation::App, command::with(command::entry("model-a"), &command::set(json!({"disabled": true}))),
            json!({"updated": ["model-a"]}));
        assert_eq!(set.changed_entry(), "model-a");
        assert!(set.updated("model-a") && !set.updated("model-b"));
        let job = record(NodeOperation::CancelJob, command::job("j1", "l1"), json!(null));
        assert_eq!((job.job_id(), job.lease()), (Some("j1"), "l1"));
    }
}
