//! The controller's job and command records in memory. Nearly all of them are history:
//! finished jobs and done commands. Scheduling passes only look at the open ones, which
//! are indexed, so a pass costs what is in flight rather than everything that ever ran.

use std::collections::{btree_map, BTreeMap, BTreeSet, HashMap};

pub(super) trait Record {
    /// Whether a scheduling pass still has to look at the record.
    fn open(&self) -> bool;
    /// The job the record is about, when it is about one. A job's records are found by it.
    fn subject(&self) -> Option<&str> {
        None
    }
}

/// Records by id, with the open ones and each job's records indexed. A record changes
/// only through `insert` or `update`, so the indexes always match it.
#[derive(Clone, Debug)]
pub(super) struct Records<T> {
    all: BTreeMap<String, T>,
    open: BTreeSet<String>,
    about: HashMap<String, BTreeSet<String>>,
}

impl<T> Default for Records<T> {
    fn default() -> Self {
        Self { all: BTreeMap::new(), open: BTreeSet::new(), about: HashMap::new() }
    }
}

fn index<T: Record>(open: &mut BTreeSet<String>, about: &mut HashMap<String, BTreeSet<String>>, id: &str, v: &T) {
    if v.open() {
        open.insert(id.to_owned());
    }
    if let Some(s) = v.subject() {
        about.entry(s.to_owned()).or_default().insert(id.to_owned());
    }
}

fn unindex<T: Record>(open: &mut BTreeSet<String>, about: &mut HashMap<String, BTreeSet<String>>, id: &str, v: &T) {
    open.remove(id);
    if let Some(s) = v.subject() {
        if let Some(ids) = about.get_mut(s) {
            ids.remove(id);
            if ids.is_empty() {
                about.remove(s);
            }
        }
    }
}

impl<T: Record> Records<T> {
    pub(super) fn insert(&mut self, id: String, value: T) -> Option<T> {
        let old = self.all.insert(id.clone(), value);
        if let Some(old) = &old {
            unindex(&mut self.open, &mut self.about, &id, old);
        }
        index(&mut self.open, &mut self.about, &id, &self.all[&id]);
        old
    }

    /// Changes one record in place; the indexes follow the change.
    pub(super) fn update<R>(&mut self, id: &str, f: impl FnOnce(&mut T) -> R) -> Option<R> {
        let v = self.all.get_mut(id)?;
        unindex(&mut self.open, &mut self.about, id, v);
        let r = f(v);
        index(&mut self.open, &mut self.about, id, v);
        Some(r)
    }

    pub(super) fn get(&self, id: &str) -> Option<&T> {
        self.all.get(id)
    }

    pub(super) fn remove(&mut self, id: &str) -> Option<T> {
        let old = self.all.remove(id)?;
        unindex(&mut self.open, &mut self.about, id, &old);
        Some(old)
    }

    /// Every record, history included, in id order.
    pub(super) fn values(&self) -> btree_map::Values<'_, String, T> {
        self.all.values()
    }

    /// The open records, in id order.
    pub(super) fn open(&self) -> impl Iterator<Item = &T> + '_ {
        self.open.iter().filter_map(|id| self.all.get(id))
    }

    /// The records about job `id`, open or not, in id order.
    pub(super) fn about<'a>(&'a self, id: &str) -> impl Iterator<Item = &'a T> + 'a {
        self.about.get(id).into_iter().flatten().filter_map(|id| self.all.get(id))
    }

    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.all.len()
    }

    #[cfg(test)]
    pub(super) fn is_empty(&self) -> bool {
        self.all.is_empty()
    }

    /// Changes every record that `select` picks; the indexes follow.
    #[cfg(test)]
    pub(super) fn update_where(&mut self, select: impl Fn(&T) -> bool, mut f: impl FnMut(&mut T)) {
        let ids: Vec<String> = self.all.iter().filter(|(_, v)| select(v)).map(|(id, _)| id.clone()).collect();
        for id in ids {
            self.update(&id, &mut f);
        }
    }
}

impl<T, Q: Ord + ?Sized> std::ops::Index<&Q> for Records<T>
where
    String: std::borrow::Borrow<Q>,
{
    type Output = T;
    fn index(&self, id: &Q) -> &T {
        &self.all[id]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone, Debug, PartialEq)]
    struct R {
        done: bool,
        job: Option<&'static str>,
    }
    impl Record for R {
        fn open(&self) -> bool {
            !self.done
        }
        fn subject(&self) -> Option<&str> {
            self.job
        }
    }

    #[test]
    fn the_indexes_follow_every_change() {
        let mut r = Records::default();
        r.insert("a".into(), R { done: false, job: Some("j1") });
        r.insert("b".into(), R { done: true, job: Some("j1") });
        r.insert("c".into(), R { done: false, job: None });
        assert_eq!(r.open().count(), 2);
        assert_eq!(r.about("j1").count(), 2);
        assert_eq!(r.values().count(), 3);

        r.update("a", |v| v.done = true);
        assert_eq!(r.open().collect::<Vec<_>>(), vec![&R { done: false, job: None }]);
        r.insert("c".into(), R { done: true, job: Some("j2") });
        assert_eq!(r.open().count(), 0);
        assert_eq!(r.about("j2").count(), 1);
        r.insert("c".into(), R { done: false, job: None });
        assert_eq!(r.about("j2").count(), 0);
        assert_eq!(r.open().count(), 1);
        assert!(r.update("missing", |v| v.done = true).is_none());
        r.update_where(|v| v.job == Some("j1"), |v| v.done = false);
        assert_eq!(r.open().count(), 3);
    }
}
