use std::collections::BTreeMap;

use crate::filter::attribute::{ValueComparison, ValueFilter, ValueStringComparison};
use crate::filter::BoundSearch;
use crate::models::ValueOperator;
use crate::{Timestamp, Value};

use super::util::IndexExt;

/// This is an index for a single attribute name.

// Since the values of an attribute can be varied, this keeps separate typed
// indexes. It is unlikely that an attribute has values with multiple types, but
// it needs to be accounted for regardless.
pub(crate) struct AttributeIndex {
    strings: AttributeStringIndex,
    f64s: AttributeF64Index,
    i64s: AttributeI64Index,
    u64s: AttributeU64Index,
    i128s: AttributeI128Index,
    u128s: AttributeU128Index,
    bools: AttributeBoolIndex,
}

impl AttributeIndex {
    #[allow(unused)]
    pub(crate) fn new() -> AttributeIndex {
        AttributeIndex {
            strings: AttributeStringIndex::new(),
            f64s: AttributeF64Index::new(),
            i64s: AttributeI64Index::new(),
            u64s: AttributeU64Index::new(),
            i128s: AttributeI128Index::new(),
            u128s: AttributeU128Index::new(),
            bools: AttributeBoolIndex::new(),
        }
    }

    pub(crate) fn add_entry(&mut self, key: Timestamp, value: &Value) {
        match value {
            Value::F64(_) => {
                let idx = self.f64s.index.upper_bound_via_expansion(&key);
                self.f64s.index.insert(idx, key);
            }
            Value::I64(_) => {
                let idx = self.i64s.index.upper_bound_via_expansion(&key);
                self.i64s.index.insert(idx, key);
            }
            Value::U64(_) => {
                let idx = self.u64s.index.upper_bound_via_expansion(&key);
                self.u64s.index.insert(idx, key);
            }
            Value::I128(_) => {
                let idx = self.i128s.index.upper_bound_via_expansion(&key);
                self.i128s.index.insert(idx, key);
            }
            Value::U128(_) => {
                let idx = self.u128s.index.upper_bound_via_expansion(&key);
                self.u128s.index.insert(idx, key);
            }
            Value::Bool(_) => {
                let idx = self.f64s.index.upper_bound_via_expansion(&key);
                self.f64s.index.insert(idx, key);
            }
            Value::Str(value) => {
                let idx = self.strings.total.upper_bound_via_expansion(&key);
                self.strings.total.insert(idx, key);

                let value_index = self
                    .strings
                    .value_indexes
                    .entry(value.to_owned())
                    .or_default();

                let idx = value_index.upper_bound_via_expansion(&key);
                value_index.insert(idx, key);
            }
        }
    }

    pub(crate) fn remove_entry(&mut self, key: Timestamp, value: &Value) {
        match value {
            Value::F64(_) => {
                let idx = self.f64s.index.lower_bound(&key);
                self.f64s.index.remove(idx);
            }
            Value::I64(_) => {
                let idx = self.i64s.index.lower_bound(&key);
                self.i64s.index.remove(idx);
            }
            Value::U64(_) => {
                let idx = self.u64s.index.lower_bound(&key);
                self.u64s.index.remove(idx);
            }
            Value::I128(_) => {
                let idx = self.i128s.index.lower_bound(&key);
                self.i128s.index.remove(idx);
            }
            Value::U128(_) => {
                let idx = self.u128s.index.lower_bound(&key);
                self.u128s.index.remove(idx);
            }
            Value::Bool(_) => {
                let idx = self.f64s.index.lower_bound(&key);
                self.f64s.index.remove(idx);
            }
            Value::Str(value) => {
                let idx = self.strings.total.lower_bound(&key);
                self.strings.total.remove(idx);

                let value_index = self
                    .strings
                    .value_indexes
                    .entry(value.to_owned())
                    .or_default();

                let idx = value_index.lower_bound(&key);
                value_index.remove(idx);
            }
        }
    }

    pub(crate) fn remove_entries(&mut self, keys: &[Timestamp]) {
        self.strings.remove_entries(keys);
        self.f64s.remove_entries(keys);
        self.i64s.remove_entries(keys);
        self.u64s.remove_entries(keys);
        self.i128s.remove_entries(keys);
        self.u128s.remove_entries(keys);
        self.bools.remove_entries(keys);
    }

    /// This returns a set of indexed filters that when OR'd together will yield
    /// all the values for the provided operator and value.
    pub(crate) fn make_indexed_filter(
        &self,
        filter: ValueFilter,
    ) -> Vec<(&[Timestamp], Option<ValueFilter>)> {
        let mut filters: Vec<(&[Timestamp], _)> = vec![];

        match &filter.f64s {
            ValueComparison::None => {}
            ValueComparison::Compare(_, _) => {
                filters.push((&self.f64s.index, Some(filter.clone())));
            }
            ValueComparison::All => filters.push((&self.f64s.index, None)),
        }

        match &filter.i64s {
            ValueComparison::None => {}
            ValueComparison::Compare(_, _) => {
                filters.push((&self.i64s.index, Some(filter.clone())));
            }
            ValueComparison::All => filters.push((&self.i64s.index, None)),
        }

        match &filter.u64s {
            ValueComparison::None => {}
            ValueComparison::Compare(_, _) => {
                filters.push((&self.u64s.index, Some(filter.clone())));
            }
            ValueComparison::All => filters.push((&self.u64s.index, None)),
        }

        match &filter.i128s {
            ValueComparison::None => {}
            ValueComparison::Compare(_, _) => {
                filters.push((&self.i128s.index, Some(filter.clone())));
            }
            ValueComparison::All => filters.push((&self.i128s.index, None)),
        }

        match &filter.u128s {
            ValueComparison::None => {}
            ValueComparison::Compare(_, _) => {
                filters.push((&self.u128s.index, Some(filter.clone())));
            }
            ValueComparison::All => filters.push((&self.u128s.index, None)),
        }

        match &filter.bools {
            ValueComparison::None => {}
            ValueComparison::Compare(ValueOperator::Eq, true) => {
                filters.push((&self.bools.trues, None));
            }
            ValueComparison::Compare(ValueOperator::Eq, false) => {
                filters.push((&self.bools.falses, None));
            }
            ValueComparison::Compare(_, _) => {
                filters.push((&self.bools.trues, Some(filter.clone())));
                filters.push((&self.bools.falses, Some(filter.clone())));
            }
            ValueComparison::All => {
                filters.push((&self.bools.trues, None));
                filters.push((&self.bools.falses, None));
            }
        }

        match &filter.strings {
            ValueStringComparison::None => {}
            ValueStringComparison::Compare(ValueOperator::Eq, value) => {
                filters.push((self.strings.value_index(value), None));
            }
            ValueStringComparison::Compare(_, _) => {
                filters.push((&self.strings.total, Some(filter.clone())));
            }
            ValueStringComparison::Wildcard(_) => {
                filters.push((&self.strings.total, Some(filter.clone())));
            }
            ValueStringComparison::Regex(_) => {
                filters.push((&self.strings.total, Some(filter.clone())));
            }
            ValueStringComparison::All => filters.push((&self.strings.total, None)),
        }

        filters
    }
}

// This is a sub-index for string values of an attribute index. It keeps a
// "total" list as well for queries on strings but can't reasonably use the
// invididual values.
struct AttributeStringIndex {
    total: Vec<Timestamp>,
    value_indexes: BTreeMap<String, Vec<Timestamp>>,
}

impl AttributeStringIndex {
    fn new() -> AttributeStringIndex {
        AttributeStringIndex {
            total: Vec::new(),
            value_indexes: BTreeMap::new(),
        }
    }

    fn value_index<'a>(&'a self, value: &str) -> &'a [Timestamp] {
        self.value_indexes
            .get(value)
            .map(Vec::as_slice)
            .unwrap_or_default()
    }

    fn remove_entries(&mut self, keys: &[Timestamp]) {
        self.total.remove_list_sorted(keys);
        for value_index in self.value_indexes.values_mut() {
            value_index.remove_list_sorted(keys);
        }
    }
}

struct AttributeF64Index {
    // TODO: figure out how best to do categorical & numerical indexing
    index: Vec<Timestamp>,
}

impl AttributeF64Index {
    fn new() -> AttributeF64Index {
        AttributeF64Index { index: Vec::new() }
    }

    fn remove_entries(&mut self, keys: &[Timestamp]) {
        self.index.remove_list_sorted(keys);
    }
}

struct AttributeI64Index {
    // TODO: figure out how best to do categorical & numerical indexing
    index: Vec<Timestamp>,
}

impl AttributeI64Index {
    fn new() -> AttributeI64Index {
        AttributeI64Index { index: Vec::new() }
    }

    fn remove_entries(&mut self, keys: &[Timestamp]) {
        self.index.remove_list_sorted(keys);
    }
}

struct AttributeU64Index {
    // TODO: figure out how best to do categorical & numerical indexing
    index: Vec<Timestamp>,
}

impl AttributeU64Index {
    fn new() -> AttributeU64Index {
        AttributeU64Index { index: Vec::new() }
    }

    fn remove_entries(&mut self, keys: &[Timestamp]) {
        self.index.remove_list_sorted(keys);
    }
}

struct AttributeI128Index {
    // TODO: figure out how best to do categorical & numerical indexing
    index: Vec<Timestamp>,
}

impl AttributeI128Index {
    fn new() -> AttributeI128Index {
        AttributeI128Index { index: Vec::new() }
    }

    fn remove_entries(&mut self, keys: &[Timestamp]) {
        self.index.remove_list_sorted(keys);
    }
}

struct AttributeU128Index {
    // TODO: figure out how best to do categorical & numerical indexing
    index: Vec<Timestamp>,
}

impl AttributeU128Index {
    fn new() -> AttributeU128Index {
        AttributeU128Index { index: Vec::new() }
    }

    fn remove_entries(&mut self, keys: &[Timestamp]) {
        self.index.remove_list_sorted(keys);
    }
}

struct AttributeBoolIndex {
    trues: Vec<Timestamp>,
    falses: Vec<Timestamp>,
}

impl AttributeBoolIndex {
    fn new() -> AttributeBoolIndex {
        AttributeBoolIndex {
            trues: Vec::new(),
            falses: Vec::new(),
        }
    }

    fn remove_entries(&mut self, keys: &[Timestamp]) {
        self.trues.remove_list_sorted(keys);
        self.falses.remove_list_sorted(keys);
    }
}
