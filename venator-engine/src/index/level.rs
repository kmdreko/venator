use serde::{Deserialize, Serialize};

use crate::filter::ValueComparison;
use crate::models::SimpleLevel;
use crate::util::{BoundSearch, IndexExt};
use crate::Timestamp;

#[derive(Serialize, Deserialize)]
pub(crate) struct LevelIndex([Vec<Timestamp>; 6]);

impl LevelIndex {
    pub(crate) fn new() -> LevelIndex {
        LevelIndex([
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
        ])
    }

    pub(crate) fn add_entry(&mut self, level: SimpleLevel, key: Timestamp) {
        let level_index = &mut self.0[level as usize];
        let idx = level_index.upper_bound_via_expansion(&key);
        level_index.insert(idx, key);
    }

    pub(crate) fn remove_entries(&mut self, keys: &[Timestamp]) {
        for index in &mut self.0 {
            index.remove_list_sorted(keys);
        }
    }

    /// This returns a set of indexed filters that when OR'd together will yield
    /// all the values for the provided operator and value.
    pub(crate) fn make_indexed_filter(
        &self,
        filter: ValueComparison<SimpleLevel>,
    ) -> Vec<&[Timestamp]> {
        let mut indexes = vec![];

        if filter.matches(&SimpleLevel::Trace) {
            indexes.push(&*self.0[0]);
        }
        if filter.matches(&SimpleLevel::Debug) {
            indexes.push(&*self.0[1]);
        }
        if filter.matches(&SimpleLevel::Info) {
            indexes.push(&*self.0[2]);
        }
        if filter.matches(&SimpleLevel::Warn) {
            indexes.push(&*self.0[3]);
        }
        if filter.matches(&SimpleLevel::Error) {
            indexes.push(&*self.0[4]);
        }
        if filter.matches(&SimpleLevel::Fatal) {
            indexes.push(&*self.0[5]);
        }

        indexes
    }
}
