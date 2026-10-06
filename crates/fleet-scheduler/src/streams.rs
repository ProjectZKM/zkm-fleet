//! Streams: a sequence of proofs (one per block, or one per chain step) that the
//! scheduler drives from a cursor, and stream workers execute.
//!
//! The scheduler owns the policy: where the cursor is, which steps are planned,
//! fetched, proving or done, how far behind the head the stream may fall before an
//! independent stream skips ahead (`max_lag`), how many steps prove at once
//! (`parallel`; one when `chained`, and then step n+1 only after n is done, with n's
//! proof handed to it), how many inputs are fetched ahead (`prefetch`), and how a
//! failed step is retried. A worker (the bridge's `kind: stream` actor) knows only
//! how to run the provider's commands: it leases a task (`head`, `fetch seq` or
//! `prove seq`), runs it, and reports how it went. A lease that is not reported in
//! time goes back to the pool: a worker that dies loses nothing but time.
//!
//! State lives in the store (`fleet_streams`, `fleet_stream_steps`) and is reloaded
//! at start, so the cursor survives a scheduler restart.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

use crate::config::StreamConfig;
use crate::fleet::now_ms;
use crate::store::Store;

/// A step's progress. The numbers are what the store keeps.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StepState {
    Planned = 0,
    Fetching = 1,
    Fetched = 2,
    Proving = 3,
    Done = 4,
    Failed = 5,
    Skipped = 6,
}

impl StepState {
    pub fn from_db(v: i32) -> Self {
        match v {
            0 => Self::Planned,
            1 => Self::Fetching,
            2 => Self::Fetched,
            3 => Self::Proving,
            4 => Self::Done,
            5 => Self::Failed,
            _ => Self::Skipped,
        }
    }

    pub fn is_open(self) -> bool {
        (self as i32) < 4
    }

    fn leased(self) -> bool {
        matches!(self, Self::Fetching | Self::Proving)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Task {
    Head,
    Fetch,
    Prove,
    None,
}

/// One sequence number of a stream and what happened to it.
#[derive(Debug, Clone, Serialize)]
pub struct Step {
    pub stream: String,
    pub seq: i64,
    pub state: StepState,
    pub attempts: u32,
    pub worker: Option<String>,
    pub leased_ms: Option<u64>,
    pub not_before_ms: Option<u64>,
    pub created_ms: u64,
    pub started_ms: Option<u64>,
    pub finished_ms: Option<u64>,
    pub wall_ms: Option<u64>,
    pub error: String,
    /// What the prove step left behind (a path the provider reported).
    pub artifact: Option<String>,
    /// The scheduler job the provider's proof went through, when it said so.
    pub job_id: Option<String>,
}

/// A stream's cursor and counters.
#[derive(Debug, Clone, Serialize)]
pub struct StreamRow {
    pub name: String,
    /// The next sequence number to plan; None until the head is first seen.
    pub next_seq: Option<i64>,
    pub head: Option<i64>,
    pub head_ms: Option<u64>,
    pub paused: bool,
    pub done: u64,
    pub failed: u64,
    pub skipped: u64,
    pub updated_ms: u64,
}

#[derive(Debug, Deserialize)]
pub struct LeaseRequest {
    pub worker: String,
    /// The worker has a fetch command: inputs are fetched ahead, proofs take fetched steps.
    #[serde(default)]
    pub can_fetch: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct Lease {
    pub task: Task,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub seq: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prev_seq: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prev_artifact: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prev_job_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout_s: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retry_after_s: Option<u64>,
}

impl Lease {
    fn none(retry_after_s: u64) -> Self {
        Self {
            task: Task::None,
            seq: None,
            prev_seq: None,
            prev_artifact: None,
            prev_job_id: None,
            timeout_s: None,
            retry_after_s: Some(retry_after_s),
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct Report {
    pub worker: String,
    pub task: Task,
    #[serde(default)]
    pub seq: Option<i64>,
    pub ok: bool,
    #[serde(default)]
    pub error: String,
    #[serde(default)]
    pub wall_ms: Option<u64>,
    #[serde(default)]
    pub artifact: Option<String>,
    #[serde(default)]
    pub job_id: Option<String>,
    /// A head task's answer.
    #[serde(default)]
    pub head: Option<i64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct StreamStatus {
    pub name: String,
    pub chained: bool,
    pub paused: bool,
    pub head: Option<i64>,
    pub head_age_s: Option<u64>,
    pub next_seq: Option<i64>,
    pub last_done: Option<i64>,
    /// Steps between the last proof and the head's target, in the stream's step unit.
    pub lag: Option<i64>,
    pub open: BTreeMap<String, usize>,
    pub done: u64,
    pub failed: u64,
    pub skipped: u64,
    pub workers: Vec<WorkerSeen>,
}

#[derive(Debug, Clone, Serialize)]
pub struct WorkerSeen {
    pub name: String,
    pub seen_age_s: u64,
}

struct StreamState {
    cfg: StreamConfig,
    row: StreamRow,
    /// Open steps and the most recent closed ones (the store has them all).
    steps: BTreeMap<i64, Step>,
    head_lease_ms: Option<u64>,
    workers: HashMap<String, u64>,
}

const HEAD_LEASE_S: u64 = 60;
const LEASE_GRACE_S: u64 = 30;
const KEEP_CLOSED: usize = 200;

impl StreamState {
    fn open_steps(&self) -> impl Iterator<Item = &Step> {
        self.steps.values().filter(|s| s.state.is_open())
    }

    fn count(&self, state: StepState) -> usize {
        self.steps.values().filter(|s| s.state == state).count()
    }

    fn last_done(&self) -> Option<&Step> {
        self.steps
            .values()
            .rev()
            .find(|s| s.state == StepState::Done)
    }

    fn timeout_s(&self, task: Task) -> u64 {
        match task {
            Task::Fetch => self.cfg.fetch_timeout_s,
            _ => self.cfg.prove_timeout_s,
        }
    }

    /// Steps whose lease ran out go back to the pool as a failed attempt.
    fn expire_leases(&mut self, now: u64, changed: &mut Vec<Step>) {
        let (attempts, chained, backoff) = (
            self.cfg.attempts,
            self.cfg.chained,
            self.cfg.retry_backoff_s,
        );
        for step in self.steps.values_mut() {
            if !step.state.leased() {
                continue;
            }
            let task = if step.state == StepState::Fetching {
                Task::Fetch
            } else {
                Task::Prove
            };
            let timeout = match task {
                Task::Fetch => self.cfg.fetch_timeout_s,
                _ => self.cfg.prove_timeout_s,
            };
            let deadline = step.leased_ms.unwrap_or(0) + (timeout + LEASE_GRACE_S) * 1000;
            if now > deadline {
                tracing::warn!(stream = %step.stream, seq = step.seq, ?task, worker = ?step.worker, "lease expired");
                fail_attempt(
                    step,
                    task,
                    "lease expired: the worker did not report in time",
                    now,
                    attempts,
                    chained,
                    backoff,
                );
                changed.push(step.clone());
            }
        }
        if let Some(t) = self.head_lease_ms {
            if now > t + HEAD_LEASE_S * 1000 {
                self.head_lease_ms = None;
            }
        }
    }

    /// Plans sequence numbers up to the head's target, skipping ahead when an
    /// independent stream is too far behind.
    fn plan(&mut self, now: u64, changed: &mut Vec<Step>) {
        let Some(head) = self.row.head else {
            return;
        };
        let cfg = &self.cfg;
        let target = head - cfg.behind;
        let mut next = match self.row.next_seq {
            Some(n) => n,
            None => cfg.start.unwrap_or(target),
        };
        if !cfg.chained && cfg.max_lag > 0 && (target - next) / cfg.step > cfg.max_lag {
            // Independent proofs of old blocks are worth less than the head's: jump.
            let skip_to = target - (target - next) % cfg.step;
            let skipped = ((skip_to - next) / cfg.step) as u64;
            tracing::info!(stream = %cfg.name, from = next, to = skip_to, skipped, "behind by more than max_lag: skipping ahead");
            self.row.skipped += skipped;
            next = skip_to;
        }
        let window = cfg.parallel + cfg.prefetch + 1;
        while next <= target && self.open_steps().count() < window {
            if !self.steps.contains_key(&next) {
                let step = Step {
                    stream: cfg.name.clone(),
                    seq: next,
                    state: StepState::Planned,
                    attempts: 0,
                    worker: None,
                    leased_ms: None,
                    not_before_ms: None,
                    created_ms: now,
                    started_ms: None,
                    finished_ms: None,
                    wall_ms: None,
                    error: String::new(),
                    artifact: None,
                    job_id: None,
                };
                changed.push(step.clone());
                self.steps.insert(next, step);
            }
            next += cfg.step;
        }
        self.row.next_seq = Some(next);
    }

    /// The task a worker gets now, if any.
    fn lease(&mut self, req: &LeaseRequest, now: u64, changed: &mut Vec<Step>) -> Lease {
        self.workers.insert(req.worker.clone(), now);
        if self.row.paused {
            return Lease::none(10);
        }
        self.expire_leases(now, changed);
        let head_stale = match self.row.head_ms {
            None => true,
            Some(t) => now >= t + self.cfg.period_s * 1000,
        };
        if head_stale && self.head_lease_ms.is_none() {
            self.head_lease_ms = Some(now);
            return Lease {
                task: Task::Head,
                seq: None,
                prev_seq: None,
                prev_artifact: None,
                prev_job_id: None,
                timeout_s: Some(HEAD_LEASE_S),
                retry_after_s: None,
            };
        }
        self.plan(now, changed);
        let ready = |s: &Step| s.not_before_ms.map_or(true, |t| now >= t);
        let provable = |s: &Step| {
            s.state == StepState::Fetched || (!req.can_fetch && s.state == StepState::Planned)
        };
        // prove: chained streams one at a time and in order, the rest up to `parallel`
        let proving = self.count(StepState::Proving);
        let limit = if self.cfg.chained { 1 } else { self.cfg.parallel };
        if proving < limit {
            let lowest_open = self.open_steps().map(|s| s.seq).next();
            let candidate = self
                .steps
                .values()
                .find(|s| provable(s) && ready(s) && (!self.cfg.chained || Some(s.seq) == lowest_open))
                .map(|s| s.seq);
            if let Some(seq) = candidate {
                let prev = if self.cfg.chained {
                    self.steps.get(&(seq - self.cfg.step)).cloned()
                } else {
                    None
                };
                let timeout = self.timeout_s(Task::Prove);
                let step = self.steps.get_mut(&seq).expect("candidate exists");
                step.state = StepState::Proving;
                step.worker = Some(req.worker.clone());
                step.leased_ms = Some(now);
                step.started_ms = Some(now);
                step.error.clear();
                changed.push(step.clone());
                return Lease {
                    task: Task::Prove,
                    seq: Some(seq),
                    prev_seq: prev.as_ref().map(|p| p.seq),
                    prev_artifact: prev.as_ref().and_then(|p| p.artifact.clone()),
                    prev_job_id: prev.and_then(|p| p.job_id),
                    timeout_s: Some(timeout),
                    retry_after_s: None,
                };
            }
        }
        // fetch: inputs ahead of the proofs
        if req.can_fetch {
            let ahead = self.count(StepState::Fetching) + self.count(StepState::Fetched);
            if ahead < self.cfg.prefetch.max(1) {
                let candidate = self
                    .steps
                    .values()
                    .find(|s| s.state == StepState::Planned && ready(s))
                    .map(|s| s.seq);
                if let Some(seq) = candidate {
                    let timeout = self.timeout_s(Task::Fetch);
                    let step = self.steps.get_mut(&seq).expect("candidate exists");
                    step.state = StepState::Fetching;
                    step.worker = Some(req.worker.clone());
                    step.leased_ms = Some(now);
                    step.error.clear();
                    changed.push(step.clone());
                    return Lease {
                        task: Task::Fetch,
                        seq: Some(seq),
                        prev_seq: None,
                        prev_artifact: None,
                        prev_job_id: None,
                        timeout_s: Some(timeout),
                        retry_after_s: None,
                    };
                }
            }
        }
        Lease::none(2)
    }

    fn report(&mut self, r: &Report, now: u64, changed: &mut Vec<Step>) -> anyhow::Result<()> {
        self.workers.insert(r.worker.clone(), now);
        if r.task == Task::Head {
            self.head_lease_ms = None;
            if r.ok {
                if let Some(h) = r.head {
                    self.row.head = Some(h);
                    self.row.head_ms = Some(now);
                }
            } else {
                tracing::warn!(stream = %self.cfg.name, worker = %r.worker, error = %r.error, "head probe failed");
            }
            return Ok(());
        }
        let seq = r.seq.ok_or_else(|| anyhow::anyhow!("report without seq"))?;
        let (attempts, chained, backoff) = (
            self.cfg.attempts,
            self.cfg.chained,
            self.cfg.retry_backoff_s,
        );
        let step = self
            .steps
            .get_mut(&seq)
            .ok_or_else(|| anyhow::anyhow!("no step {seq} in stream {}", self.cfg.name))?;
        let expected = match r.task {
            Task::Fetch => StepState::Fetching,
            Task::Prove => StepState::Proving,
            _ => anyhow::bail!("a report names a fetch or a prove"),
        };
        if step.state != expected {
            // a late report for a lease that expired: say so, change nothing
            anyhow::bail!("step {seq} is {:?}, not {:?}", step.state, expected);
        }
        if r.ok {
            match r.task {
                Task::Fetch => step.state = StepState::Fetched,
                _ => {
                    step.state = StepState::Done;
                    step.finished_ms = Some(now);
                    step.artifact = r.artifact.clone();
                    step.job_id = r.job_id.clone();
                    self.row.done += 1;
                }
            }
            step.wall_ms = r.wall_ms;
            step.leased_ms = None;
            step.not_before_ms = None;
        } else {
            let before = step.state;
            fail_attempt(step, r.task, &r.error, now, attempts, chained, backoff);
            if before != StepState::Failed && step.state == StepState::Failed {
                self.row.failed += 1;
            }
        }
        changed.push(step.clone());
        self.trim();
        Ok(())
    }

    /// Keeps memory bounded: closed steps beyond the most recent ones leave the map.
    fn trim(&mut self) {
        let closed: Vec<i64> = self
            .steps
            .values()
            .filter(|s| !s.state.is_open())
            .map(|s| s.seq)
            .collect();
        if closed.len() > KEEP_CLOSED {
            for seq in &closed[..closed.len() - KEEP_CLOSED] {
                self.steps.remove(seq);
            }
        }
    }

    fn status(&self, now: u64) -> StreamStatus {
        let mut open = BTreeMap::new();
        for s in self.open_steps() {
            *open
                .entry(format!("{:?}", s.state).to_lowercase())
                .or_insert(0) += 1;
        }
        let last_done = self.last_done().map(|s| s.seq);
        let lag = match (self.row.head, last_done) {
            (Some(h), Some(d)) => Some((h - self.cfg.behind - d) / self.cfg.step),
            (Some(h), None) => self.row.next_seq.map(|n| (h - self.cfg.behind - n) / self.cfg.step),
            _ => None,
        };
        let mut workers: Vec<WorkerSeen> = self
            .workers
            .iter()
            .map(|(n, t)| WorkerSeen {
                name: n.clone(),
                seen_age_s: now.saturating_sub(*t) / 1000,
            })
            .collect();
        workers.sort_by(|a, b| a.name.cmp(&b.name));
        StreamStatus {
            name: self.cfg.name.clone(),
            chained: self.cfg.chained,
            paused: self.row.paused,
            head: self.row.head,
            head_age_s: self.row.head_ms.map(|t| now.saturating_sub(t) / 1000),
            next_seq: self.row.next_seq,
            last_done,
            lag,
            open,
            done: self.row.done,
            failed: self.row.failed,
            skipped: self.row.skipped,
            workers,
        }
    }
}

/// A failed attempt: back to the pool after a backoff, or failed for good (never for
/// a chained stream, whose next step needs this one).
fn fail_attempt(
    step: &mut Step,
    task: Task,
    error: &str,
    now: u64,
    attempts: u32,
    chained: bool,
    backoff_s: u64,
) {
    step.attempts += 1;
    step.error = error.chars().take(500).collect();
    step.worker = None;
    step.leased_ms = None;
    if !chained && step.attempts >= attempts {
        step.state = StepState::Failed;
        step.finished_ms = Some(now);
        return;
    }
    // a failed proof refetches its input: the input may be what was wrong
    step.state = StepState::Planned;
    let _ = task;
    step.not_before_ms = Some(now + backoff_s * 1000 * u64::from(step.attempts.min(10)));
}

pub struct Streams {
    inner: Mutex<BTreeMap<String, StreamState>>,
    store: Arc<dyn Store>,
}

impl Streams {
    pub fn new(configs: &[StreamConfig], store: Arc<dyn Store>) -> Arc<Self> {
        let now = now_ms();
        let inner = configs
            .iter()
            .map(|cfg| {
                let state = StreamState {
                    cfg: cfg.clone(),
                    row: StreamRow {
                        name: cfg.name.clone(),
                        next_seq: None,
                        head: None,
                        head_ms: None,
                        paused: false,
                        done: 0,
                        failed: 0,
                        skipped: 0,
                        updated_ms: now,
                    },
                    steps: BTreeMap::new(),
                    head_lease_ms: None,
                    workers: HashMap::new(),
                };
                (cfg.name.clone(), state)
            })
            .collect();
        Arc::new(Self {
            inner: Mutex::new(inner),
            store,
        })
    }

    /// Cursors and open steps from the store; a lease that was out when the scheduler
    /// stopped is retried (the worker reports into the void, then leases again).
    pub async fn load(&self) -> anyhow::Result<usize> {
        let rows = self.store.streams().await?;
        let mut loaded = 0;
        for row in rows {
            let steps = self.store.open_steps(&row.name).await?;
            let recent = self.store.recent_steps(&row.name, KEEP_CLOSED).await?;
            let mut inner = self.inner.lock();
            if let Some(s) = inner.get_mut(&row.name) {
                s.row = row;
                s.row.head = None; // probe again: the head moved while we were away
                s.row.head_ms = None;
                for mut step in recent.into_iter().chain(steps) {
                    if step.state.leased() {
                        step.state = if step.state == StepState::Proving && step.artifact.is_none() {
                            StepState::Planned
                        } else {
                            StepState::Planned
                        };
                        step.worker = None;
                        step.leased_ms = None;
                    }
                    s.steps.insert(step.seq, step);
                }
                loaded += 1;
            } else {
                tracing::warn!(stream = %row.name, "stream in the store is not configured; left alone");
            }
        }
        Ok(loaded)
    }

    async fn persist(&self, name: &str, row: Option<StreamRow>, steps: Vec<Step>) {
        if let Some(row) = row {
            if let Err(e) = self.store.upsert_stream(&row).await {
                tracing::error!(stream = name, "persisting the stream: {e:#}");
            }
        }
        for step in steps {
            if let Err(e) = self.store.upsert_step(&step).await {
                tracing::error!(stream = name, seq = step.seq, "persisting a step: {e:#}");
            }
        }
    }

    pub async fn lease(&self, name: &str, req: &LeaseRequest) -> anyhow::Result<Lease> {
        let now = now_ms();
        let mut changed = Vec::new();
        let (lease, row) = {
            let mut inner = self.inner.lock();
            let s = inner
                .get_mut(name)
                .ok_or_else(|| anyhow::anyhow!("no stream {name}"))?;
            let lease = s.lease(req, now, &mut changed);
            s.row.updated_ms = now;
            (lease, s.row.clone())
        };
        self.persist(name, Some(row), changed).await;
        Ok(lease)
    }

    pub async fn report(&self, name: &str, r: &Report) -> anyhow::Result<()> {
        let now = now_ms();
        let mut changed = Vec::new();
        let (res, row) = {
            let mut inner = self.inner.lock();
            let s = inner
                .get_mut(name)
                .ok_or_else(|| anyhow::anyhow!("no stream {name}"))?;
            let res = s.report(r, now, &mut changed);
            s.row.updated_ms = now;
            (res, s.row.clone())
        };
        self.persist(name, Some(row), changed).await;
        res
    }

    pub fn status(&self) -> Vec<StreamStatus> {
        let now = now_ms();
        self.inner.lock().values().map(|s| s.status(now)).collect()
    }

    pub fn steps(&self, name: &str, limit: usize) -> Option<Vec<Step>> {
        let inner = self.inner.lock();
        let s = inner.get(name)?;
        Some(s.steps.values().rev().take(limit).cloned().collect())
    }

    /// Every step the store has, newest first (the memory keeps only recent ones).
    pub async fn stored_steps(&self, name: &str, limit: usize) -> anyhow::Result<Vec<Step>> {
        self.store.recent_steps(name, limit).await
    }

    pub async fn set_paused(&self, name: &str, paused: bool) -> anyhow::Result<()> {
        let row = {
            let mut inner = self.inner.lock();
            let s = inner
                .get_mut(name)
                .ok_or_else(|| anyhow::anyhow!("no stream {name}"))?;
            s.row.paused = paused;
            s.row.updated_ms = now_ms();
            s.row.clone()
        };
        self.persist(name, Some(row), Vec::new()).await;
        Ok(())
    }

    /// Moves the cursor: planned steps not yet leased are dropped, leased ones finish.
    pub async fn seek(&self, name: &str, seq: i64) -> anyhow::Result<()> {
        let now = now_ms();
        let (row, dropped) = {
            let mut inner = self.inner.lock();
            let s = inner
                .get_mut(name)
                .ok_or_else(|| anyhow::anyhow!("no stream {name}"))?;
            let mut dropped = Vec::new();
            for step in s.steps.values_mut() {
                if matches!(step.state, StepState::Planned | StepState::Fetched) {
                    step.state = StepState::Skipped;
                    step.finished_ms = Some(now);
                    dropped.push(step.clone());
                }
            }
            s.row.next_seq = Some(seq);
            s.row.updated_ms = now;
            (s.row.clone(), dropped)
        };
        self.persist(name, Some(row), dropped).await;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::MemoryStore;

    fn cfg(chained: bool) -> StreamConfig {
        StreamConfig {
            name: "s".into(),
            chained,
            step: 2,
            behind: 2,
            max_lag: 10,
            parallel: 3,
            prefetch: 2,
            period_s: 60,
            attempts: 2,
            retry_backoff_s: 0,
            fetch_timeout_s: 10,
            prove_timeout_s: 10,
            start: None,
        }
    }

    fn req(worker: &str, can_fetch: bool) -> LeaseRequest {
        LeaseRequest {
            worker: worker.into(),
            can_fetch,
        }
    }

    fn report(task: Task, seq: i64, ok: bool) -> Report {
        Report {
            worker: "w".into(),
            task,
            seq: Some(seq),
            ok,
            error: if ok { String::new() } else { "boom".into() },
            wall_ms: Some(1),
            artifact: ok.then(|| format!("/p/{seq}")),
            job_id: None,
            head: None,
        }
    }

    fn head(h: i64) -> Report {
        Report {
            worker: "w".into(),
            task: Task::Head,
            seq: None,
            ok: true,
            error: String::new(),
            wall_ms: None,
            artifact: None,
            job_id: None,
            head: Some(h),
        }
    }

    #[tokio::test]
    async fn independent_streams_fetch_ahead_and_prove_in_parallel() {
        let mut c = cfg(false);
        c.start = Some(98);
        let s = Streams::new(&[c], Arc::new(MemoryStore::default()));
        assert_eq!(s.lease("s", &req("w", true)).await.unwrap().task, Task::Head);
        s.report("s", &head(104)).await.unwrap();
        // target 102, cursor 98: fetches first (nothing fetched yet), prefetch 2 ahead
        let l1 = s.lease("s", &req("w", true)).await.unwrap();
        assert_eq!((l1.task, l1.seq), (Task::Fetch, Some(98)));
        let l2 = s.lease("s", &req("w", true)).await.unwrap();
        assert_eq!((l2.task, l2.seq), (Task::Fetch, Some(100)));
        assert_eq!(s.lease("s", &req("w", true)).await.unwrap().task, Task::None);
        s.report("s", &report(Task::Fetch, 98, true)).await.unwrap();
        // a fetched step proves, and the freed prefetch slot fetches the next
        let l3 = s.lease("s", &req("w", true)).await.unwrap();
        assert_eq!((l3.task, l3.seq), (Task::Prove, Some(98)));
        let l4 = s.lease("s", &req("w", true)).await.unwrap();
        assert_eq!((l4.task, l4.seq), (Task::Fetch, Some(102)));
        s.report("s", &report(Task::Prove, 98, true)).await.unwrap();
        let st = &s.status()[0];
        assert_eq!((st.last_done, st.done, st.next_seq), (Some(98), 1, Some(104)));
    }

    #[tokio::test]
    async fn chained_streams_hand_the_previous_proof_to_the_next_step() {
        let s = Streams::new(&[cfg(true)], Arc::new(MemoryStore::default()));
        s.lease("s", &req("w", false)).await.unwrap();
        s.report("s", &head(10)).await.unwrap();
        let l = s.lease("s", &req("w", false)).await.unwrap();
        assert_eq!((l.task, l.seq, l.prev_seq), (Task::Prove, Some(8), None));
        // one in flight: nothing else while 8 proves
        assert_eq!(s.lease("s", &req("w2", false)).await.unwrap().task, Task::None);
        s.report("s", &report(Task::Prove, 8, true)).await.unwrap();
        s.report("s", &head(20)).await.unwrap();
        let l = s.lease("s", &req("w", false)).await.unwrap();
        assert_eq!((l.task, l.seq, l.prev_seq), (Task::Prove, Some(10), Some(8)));
        assert_eq!(l.prev_artifact.as_deref(), Some("/p/8"));
        assert_eq!(s.lease("s", &req("w2", false)).await.unwrap().task, Task::None);
    }

    #[tokio::test]
    async fn a_failed_attempt_comes_back_and_a_chained_step_never_fails_for_good() {
        let s = Streams::new(&[cfg(true)], Arc::new(MemoryStore::default()));
        s.lease("s", &req("w", false)).await.unwrap();
        s.report("s", &head(10)).await.unwrap();
        for _ in 0..3 {
            let l = s.lease("s", &req("w", false)).await.unwrap();
            assert_eq!((l.task, l.seq), (Task::Prove, Some(8)));
            s.report("s", &report(Task::Prove, 8, false)).await.unwrap();
        }
        let st = &s.status()[0];
        assert_eq!((st.failed, st.open.get("planned")), (0, Some(&1)));
    }

    #[tokio::test]
    async fn an_independent_step_fails_after_its_attempts_and_the_stream_skips_past_max_lag() {
        let s = Streams::new(&[cfg(false)], Arc::new(MemoryStore::default()));
        s.lease("s", &req("w", false)).await.unwrap();
        s.report("s", &head(10)).await.unwrap();
        for _ in 0..2 {
            let l = s.lease("s", &req("w", false)).await.unwrap();
            assert_eq!((l.task, l.seq), (Task::Prove, Some(8)));
            s.report("s", &report(Task::Prove, 8, false)).await.unwrap();
        }
        assert_eq!(s.status()[0].failed, 1);
        // the head jumps far ahead: the cursor follows, skipping
        {
            let mut inner = s.inner.lock();
            inner.get_mut("s").unwrap().row.head_ms = None;
        }
        s.lease("s", &req("w", false)).await.unwrap();
        s.report("s", &head(1000)).await.unwrap();
        let l = s.lease("s", &req("w", false)).await.unwrap();
        assert_eq!(l.task, Task::Prove);
        assert!(l.seq.unwrap() >= 998 - 2 * 10, "{l:?}");
        assert!(s.status()[0].skipped > 0);
    }
}
