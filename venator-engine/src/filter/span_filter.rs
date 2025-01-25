use std::collections::HashMap;
use std::ops::Range;

use regex::Regex;
use serde::Deserialize;
use wildcard::WildcardBuilder;

use crate::context::SpanContext;
use crate::engine::SyncEngine;
use crate::index::{SpanDurationIndex, SpanIndexes};
use crate::models::{FullSpanId, Level, SimpleLevel, SpanKey, Timestamp, TraceRoot, ValueOperator};
use crate::storage::Storage;

use super::input::{FilterPredicate, FilterPredicateSingle, FilterPropertyKind, ValuePredicate};
use super::value::{ValueFilter, ValueStringComparison};
use super::{
    validate_value_predicate, BoundSearch, FallibleFilterPredicate, FileFilter, InputError, Order,
    Query,
};

pub(crate) enum IndexedSpanFilter<'i> {
    Single(&'i [Timestamp], Option<NonIndexedSpanFilter>),
    Stratified(&'i [Timestamp], Range<u64>, Option<NonIndexedSpanFilter>),
    Not(&'i [Timestamp], Box<IndexedSpanFilter<'i>>),
    And(Vec<IndexedSpanFilter<'i>>),
    Or(Vec<IndexedSpanFilter<'i>>),
}

impl IndexedSpanFilter<'_> {
    pub fn build<'a, S: Storage>(
        filter: Option<BasicSpanFilter>,
        span_indexes: &'a SpanIndexes,
        storage: &S,
    ) -> IndexedSpanFilter<'a> {
        let Some(filter) = filter else {
            return IndexedSpanFilter::Single(&span_indexes.all, None);
        };

        match filter {
            BasicSpanFilter::Level(level) => {
                IndexedSpanFilter::Single(&span_indexes.levels[level as usize], None)
            }
            BasicSpanFilter::Duration(duration_filter) => {
                let filters = span_indexes.durations.to_stratified_indexes();
                let filters = filters
                    .into_iter()
                    .filter_map(|(index, range)| {
                        match duration_filter.matches_duration_range(&range) {
                            Some(true) => Some(IndexedSpanFilter::Stratified(index, range, None)),
                            None => Some(IndexedSpanFilter::Stratified(
                                index,
                                range,
                                Some(NonIndexedSpanFilter::Duration(duration_filter.clone())),
                            )),
                            Some(false) => None,
                        }
                    })
                    .collect();

                IndexedSpanFilter::Or(filters)
            }
            BasicSpanFilter::Created(op, value) => match op {
                ValueOperator::Gt => {
                    let idx = span_indexes.all.upper_bound(&value);
                    IndexedSpanFilter::Single(&span_indexes.all[idx..], None)
                }
                ValueOperator::Gte => {
                    let idx = span_indexes.all.lower_bound(&value);
                    IndexedSpanFilter::Single(&span_indexes.all[idx..], None)
                }
                ValueOperator::Eq => {
                    let start = span_indexes.all.lower_bound(&value);
                    let end = span_indexes.all.upper_bound(&value);
                    IndexedSpanFilter::Single(&span_indexes.all[start..end], None)
                }
                ValueOperator::Lte => {
                    let idx = span_indexes.all.upper_bound(&value);
                    IndexedSpanFilter::Single(&span_indexes.all[..idx], None)
                }
                ValueOperator::Lt => {
                    let idx = span_indexes.all.lower_bound(&value);
                    IndexedSpanFilter::Single(&span_indexes.all[..idx], None)
                }
            },
            BasicSpanFilter::Closed(op, value) => {
                let filters = span_indexes.durations.to_stratified_indexes();
                let filters = filters
                    .into_iter()
                    .map(|(index, range)| {
                        match op {
                            ValueOperator::Gt => {
                                let v = value.get().saturating_sub(range.end - 1); // use the max of range
                                let v = Timestamp::new(v).unwrap_or(Timestamp::MIN);
                                let idx = index.upper_bound(&v);
                                IndexedSpanFilter::Single(
                                    &index[idx..],
                                    Some(NonIndexedSpanFilter::Closed(op, value)),
                                )
                            }
                            ValueOperator::Gte => {
                                let v = value.get().saturating_sub(range.end - 1); // use the max of range
                                let v = Timestamp::new(v).unwrap_or(Timestamp::MIN);
                                let idx = index.lower_bound(&v);
                                IndexedSpanFilter::Single(
                                    &index[idx..],
                                    Some(NonIndexedSpanFilter::Closed(op, value)),
                                )
                            }
                            ValueOperator::Eq => {
                                let vstart = value.get().saturating_sub(range.end - 1); // use the max of range
                                let vstart = Timestamp::new(vstart).unwrap_or(Timestamp::MIN);
                                let vend = value.get().saturating_sub(range.start); // use the min of range
                                let vend = Timestamp::new(vend).unwrap_or(Timestamp::MIN);
                                let start = index.lower_bound(&vstart);
                                let end = index.upper_bound(&vend);
                                IndexedSpanFilter::Single(
                                    &index[start..end],
                                    Some(NonIndexedSpanFilter::Closed(op, value)),
                                )
                            }
                            ValueOperator::Lt => {
                                let v = value.get().saturating_sub(range.start); // use the min of range
                                let v = Timestamp::new(v).unwrap_or(Timestamp::MIN);
                                let idx = index.lower_bound(&v);
                                IndexedSpanFilter::Single(
                                    &index[..idx],
                                    Some(NonIndexedSpanFilter::Closed(op, value)),
                                )
                            }
                            ValueOperator::Lte => {
                                let v = value.get().saturating_sub(range.start); // use the min of range
                                let v = Timestamp::new(v).unwrap_or(Timestamp::MIN);
                                let idx = index.upper_bound(&v);
                                IndexedSpanFilter::Single(
                                    &index[..idx],
                                    Some(NonIndexedSpanFilter::Closed(op, value)),
                                )
                            }
                        }
                    })
                    .collect();

                IndexedSpanFilter::Or(filters)
            }
            BasicSpanFilter::Name(filter) => match filter {
                ValueStringComparison::None => IndexedSpanFilter::Single(&[], None),
                ValueStringComparison::Compare(ValueOperator::Eq, value) => {
                    let name_index = span_indexes
                        .names
                        .get(&value)
                        .map(Vec::as_slice)
                        .unwrap_or_default();

                    IndexedSpanFilter::Single(name_index, None)
                }
                ValueStringComparison::Compare(_, _) => IndexedSpanFilter::Single(
                    &span_indexes.all,
                    Some(NonIndexedSpanFilter::Name(filter)),
                ),
                ValueStringComparison::Wildcard(_) => IndexedSpanFilter::Single(
                    &span_indexes.all,
                    Some(NonIndexedSpanFilter::Name(filter)),
                ),
                ValueStringComparison::Regex(_) => IndexedSpanFilter::Single(
                    &span_indexes.all,
                    Some(NonIndexedSpanFilter::Name(filter)),
                ),
                ValueStringComparison::All => IndexedSpanFilter::Single(
                    &span_indexes.all,
                    Some(NonIndexedSpanFilter::Name(filter)),
                ),
            },
            BasicSpanFilter::Namespace(filter) => match filter {
                ValueStringComparison::None => IndexedSpanFilter::Single(&[], None),
                ValueStringComparison::Compare(ValueOperator::Eq, value) => {
                    let namespace_index = span_indexes
                        .namespaces
                        .get(&value)
                        .map(Vec::as_slice)
                        .unwrap_or_default();

                    IndexedSpanFilter::Single(namespace_index, None)
                }
                ValueStringComparison::Compare(_, _) => IndexedSpanFilter::Single(
                    &span_indexes.all,
                    Some(NonIndexedSpanFilter::Namespace(filter)),
                ),
                ValueStringComparison::Wildcard(_) => IndexedSpanFilter::Single(
                    &span_indexes.all,
                    Some(NonIndexedSpanFilter::Namespace(filter)),
                ),
                ValueStringComparison::Regex(_) => IndexedSpanFilter::Single(
                    &span_indexes.all,
                    Some(NonIndexedSpanFilter::Namespace(filter)),
                ),
                ValueStringComparison::All => IndexedSpanFilter::Single(
                    &span_indexes.all,
                    Some(NonIndexedSpanFilter::Namespace(filter)),
                ),
            },
            BasicSpanFilter::File(filter) => match &filter.name {
                ValueStringComparison::None => IndexedSpanFilter::Single(&[], None),
                ValueStringComparison::Compare(ValueOperator::Eq, value) => {
                    let filename_index = span_indexes
                        .filenames
                        .get(value)
                        .map(Vec::as_slice)
                        .unwrap_or_default();

                    IndexedSpanFilter::Single(
                        filename_index,
                        filter.line.map(|_| NonIndexedSpanFilter::File(filter)),
                    )
                }
                ValueStringComparison::Compare(_, _) => IndexedSpanFilter::Single(
                    &span_indexes.all,
                    Some(NonIndexedSpanFilter::File(filter)),
                ),
                ValueStringComparison::Wildcard(_) => IndexedSpanFilter::Single(
                    &span_indexes.all,
                    Some(NonIndexedSpanFilter::File(filter)),
                ),
                ValueStringComparison::Regex(_) => IndexedSpanFilter::Single(
                    &span_indexes.all,
                    Some(NonIndexedSpanFilter::File(filter)),
                ),
                ValueStringComparison::All => IndexedSpanFilter::Single(
                    &span_indexes.all,
                    Some(NonIndexedSpanFilter::File(filter)),
                ),
            },
            BasicSpanFilter::Root => IndexedSpanFilter::Single(&span_indexes.roots, None),
            BasicSpanFilter::Trace(trace) => {
                let index = span_indexes
                    .traces
                    .get(&trace)
                    .map(Vec::as_slice)
                    .unwrap_or_default();

                IndexedSpanFilter::Single(index, None)
            }
            BasicSpanFilter::Parent(parent_key) => {
                let parent = SpanContext::new(parent_key, storage);

                let index = span_indexes
                    .traces
                    .get(&parent.trace_root())
                    .map(Vec::as_slice)
                    .unwrap_or_default();

                IndexedSpanFilter::Single(index, Some(NonIndexedSpanFilter::Parent(parent_key)))
            }
            BasicSpanFilter::Attribute(attribute, value_filter) => {
                if let Some(attr_index) = span_indexes.attributes.get(&attribute) {
                    let filters = attr_index
                        .make_indexed_filter(value_filter)
                        .into_iter()
                        .map(|(i, f)| {
                            IndexedSpanFilter::Single(
                                i,
                                f.map(|f| NonIndexedSpanFilter::Attribute(attribute.clone(), f)),
                            )
                        })
                        .collect();

                    IndexedSpanFilter::Or(filters)
                } else {
                    // we are creating indexes for all attributes, so if one
                    // doesn't exist, then there are no entities with that attribute
                    IndexedSpanFilter::Single(&[], None)
                }
            }
            BasicSpanFilter::Not(filter) => IndexedSpanFilter::Not(
                &span_indexes.all,
                Box::new(IndexedSpanFilter::build(
                    Some(*filter),
                    span_indexes,
                    storage,
                )),
            ),
            BasicSpanFilter::And(filters) => IndexedSpanFilter::And(
                filters
                    .into_iter()
                    .map(|f| IndexedSpanFilter::build(Some(f), span_indexes, storage))
                    .collect(),
            ),
            BasicSpanFilter::Or(filters) => IndexedSpanFilter::Or(
                filters
                    .into_iter()
                    .map(|f| IndexedSpanFilter::build(Some(f), span_indexes, storage))
                    .collect(),
            ),
        }
    }

    // This basically checks if the filter can be trimmed by timeframe. Only
    // Stratified filters can be trimmed, so this checks if those are already
    // considered in the filter or not.
    fn is_stratified(&self) -> bool {
        match self {
            IndexedSpanFilter::Single(_, _) => false,
            IndexedSpanFilter::Stratified(_, _, _) => true,
            IndexedSpanFilter::Not(_, _) => false,
            IndexedSpanFilter::And(filters) => filters.iter().any(|f| f.is_stratified()),
            IndexedSpanFilter::Or(filters) => filters.iter().all(|f| f.is_stratified()),
        }
    }

    // This searches for an entry equal to or beyond the provided entry
    #[allow(clippy::too_many_arguments)]
    pub fn search<S: Storage>(
        &mut self,
        storage: &S,
        mut entry: Timestamp, // this is the current lower bound for span keys
        order: Order,
        bound: Timestamp, // this is the current upper bound for span keys
        start: Timestamp, // this is the original search start time
                          // end: Timestamp,   // this is the original search end time
    ) -> Option<Timestamp> {
        match self {
            IndexedSpanFilter::Single(entries, filter) => match order {
                Order::Asc => loop {
                    let idx = entries.lower_bound(&entry);
                    *entries = &entries[idx..];
                    let found_entry = entries.first().cloned();

                    let found_entry = found_entry?;
                    if found_entry > bound {
                        return None;
                    }

                    if found_entry < start {
                        let span = storage.get_span(found_entry).unwrap();
                        if let Some(closed_at) = span.closed_at {
                            if closed_at <= start {
                                entry = found_entry.saturating_add(1);
                                continue;
                            }
                        }
                    }

                    if let Some(filter) = filter {
                        if filter.matches(&SpanContext::new(found_entry, storage)) {
                            return Some(found_entry);
                        } else {
                            entry = found_entry.saturating_add(1);
                        }
                    } else {
                        return Some(found_entry);
                    }
                },
                Order::Desc => loop {
                    let idx = entries.upper_bound(&entry);
                    *entries = &entries[..idx];
                    let found_entry = entries.last().cloned();

                    let found_entry = found_entry?;
                    if found_entry < bound {
                        return None;
                    }

                    if found_entry < start {
                        let span = storage.get_span(found_entry).unwrap();
                        if let Some(closed_at) = span.closed_at {
                            if closed_at <= start {
                                entry = Timestamp::new(found_entry.get() - 1).unwrap();
                                continue;
                            }
                        }
                    }

                    if let Some(filter) = filter {
                        if filter.matches(&SpanContext::new(found_entry, storage)) {
                            return Some(found_entry);
                        } else {
                            entry = Timestamp::new(found_entry.get() - 1).unwrap();
                        }
                    } else {
                        return Some(found_entry);
                    }
                },
            },
            IndexedSpanFilter::Stratified(entries, _, filter) => match order {
                Order::Asc => loop {
                    let idx = entries.lower_bound(&entry);
                    *entries = &entries[idx..];
                    let found_entry = entries.first().cloned();

                    let found_entry = found_entry?;
                    if found_entry > bound {
                        return None;
                    }

                    if found_entry < start {
                        let span = storage.get_span(found_entry).unwrap();
                        if let Some(closed_at) = span.closed_at {
                            if closed_at <= start {
                                entry = found_entry.saturating_add(1);
                                continue;
                            }
                        }
                    }

                    if let Some(filter) = filter {
                        if filter.matches(&SpanContext::new(found_entry, storage)) {
                            return Some(found_entry);
                        } else {
                            entry = found_entry.saturating_add(1);
                        }
                    } else {
                        return Some(found_entry);
                    }
                },
                Order::Desc => loop {
                    let idx = entries.upper_bound(&entry);
                    *entries = &entries[..idx];
                    let found_entry = entries.last().cloned();

                    let found_entry = found_entry?;
                    if found_entry < bound {
                        return None;
                    }

                    if found_entry < start {
                        let span = storage.get_span(found_entry).unwrap();
                        if let Some(closed_at) = span.closed_at {
                            if closed_at <= start {
                                entry = Timestamp::new(found_entry.get() - 1).unwrap();
                                continue;
                            }
                        }
                    }

                    if let Some(filter) = filter {
                        if filter.matches(&SpanContext::new(found_entry, storage)) {
                            return Some(found_entry);
                        } else {
                            entry = Timestamp::new(found_entry.get() - 1).unwrap();
                        }
                    } else {
                        return Some(found_entry);
                    }
                },
            },
            IndexedSpanFilter::Not(entries, filter) => match order {
                Order::Asc => loop {
                    let idx = entries.lower_bound(&entry);
                    *entries = &entries[idx..];
                    let found_entry = entries.first().cloned();

                    let found_entry = found_entry?;
                    if found_entry > bound {
                        return None;
                    }

                    if found_entry < start {
                        let span = storage.get_span(found_entry).unwrap();
                        if let Some(closed_at) = span.closed_at {
                            if closed_at <= start {
                                entry = found_entry.saturating_add(1);
                                continue;
                            }
                        }
                    }

                    let nested_entry =
                        filter.search(storage, found_entry, order, found_entry, start);

                    if nested_entry != Some(found_entry) {
                        return Some(found_entry);
                    } else {
                        entry = found_entry.saturating_add(1);
                    }
                },
                Order::Desc => loop {
                    let idx = entries.upper_bound(&entry);
                    *entries = &entries[..idx];
                    let found_entry = entries.last().cloned();

                    let found_entry = found_entry?;
                    if found_entry < bound {
                        return None;
                    }

                    // even if we're negating the filter, the span needs to be
                    // in range
                    if found_entry < start {
                        let span = storage.get_span(found_entry).unwrap();
                        if let Some(closed_at) = span.closed_at {
                            if closed_at <= start {
                                entry = Timestamp::new(found_entry.get() - 1).unwrap();
                                continue;
                            }
                        }
                    }

                    let nested_entry =
                        filter.search(storage, found_entry, order, found_entry, start);

                    if nested_entry != Some(found_entry) {
                        return Some(found_entry);
                    } else {
                        entry = Timestamp::new(found_entry.get() - 1).unwrap();
                    }
                },
            },
            IndexedSpanFilter::And(indexed_filters) => {
                let mut current = entry;
                'outer: loop {
                    current = indexed_filters[0].search(storage, current, order, bound, start)?;
                    for indexed_filter in &mut indexed_filters[1..] {
                        match indexed_filter.search(storage, current, order, current, start) {
                            Some(found_entry) if found_entry != current => {
                                current = found_entry;
                                continue 'outer;
                            }
                            Some(_) => { /* continue */ }
                            None => {
                                match order {
                                    Order::Asc => current = current.saturating_add(1),
                                    Order::Desc => {
                                        current = Timestamp::new(current.get() - 1).unwrap()
                                    }
                                }
                                continue 'outer;
                            }
                        }
                    }

                    break Some(current);
                }
            }
            IndexedSpanFilter::Or(indexed_filters) => {
                let mut next_entry = indexed_filters[0].search(storage, entry, order, bound, start);
                for indexed_filter in &mut indexed_filters[1..] {
                    let bound = next_entry.unwrap_or(bound);
                    if let Some(found_entry) =
                        indexed_filter.search(storage, entry, order, bound, start)
                    {
                        if let Some(next_entry) = &mut next_entry {
                            match order {
                                Order::Asc if *next_entry > found_entry => {
                                    *next_entry = found_entry;
                                }
                                Order::Desc if *next_entry < found_entry => {
                                    *next_entry = found_entry;
                                }
                                _ => { /* continue */ }
                            }
                        } else {
                            next_entry = Some(found_entry);
                        }
                    }
                }

                next_entry
            }
        }
    }

    // This gives an estimate of the number of elements the filter may select.
    // It doesn't use any heuristics but rather returns the theoretical maximum.
    fn estimate_count(&self) -> usize {
        match self {
            IndexedSpanFilter::Single(index, _) => {
                // we don't look at the basic filter because we can't really
                // guess how many elements it will select
                index.len()
            }
            IndexedSpanFilter::Stratified(index, _, _) => {
                // we don't look at the range since we can't really guess how
                // many elements it will select
                index.len()
            }
            IndexedSpanFilter::Not(index, _) => {
                // there may be a better solution, but this assumes that the
                // filter never matches
                index.len()
            }
            IndexedSpanFilter::And(filters) => {
                // since an element must pass all filters, we can only select
                // the minimum from a single filter
                filters.iter().map(Self::estimate_count).min().unwrap_or(0)
            }
            IndexedSpanFilter::Or(filters) => {
                // since OR filters can be completely disjoint, we can possibly
                // yield the sum of all filters
                filters.iter().map(Self::estimate_count).sum()
            }
        }
    }

    pub fn optimize(&mut self) {
        match self {
            IndexedSpanFilter::Single(_, _) => { /* nothing to do */ }
            IndexedSpanFilter::Stratified(_, _, _) => { /* TODO: convert to AND and sort */ }
            IndexedSpanFilter::Not(_, inner_filter) => {
                inner_filter.optimize();
            }
            IndexedSpanFilter::And(filters) => filters.sort_by_key(Self::estimate_count),
            IndexedSpanFilter::Or(filters) => filters.sort_by_key(Self::estimate_count),
        }
    }

    pub fn trim_to_timeframe(&mut self, start: Timestamp, end: Timestamp) {
        match self {
            IndexedSpanFilter::Single(index, _) => {
                // we can trim the end
                let trim_end = end;

                let end_idx = index.upper_bound(&trim_end);

                *index = &index[..end_idx];
            }
            IndexedSpanFilter::Stratified(index, duration_range, _) => {
                // we can trim to "max duration" before `start`
                let trim_start = Timestamp::new(start.get().saturating_sub(duration_range.end))
                    .unwrap_or(Timestamp::MIN);

                // we can trim by the end
                let trim_end = end;

                let start_idx = index.lower_bound(&trim_start);
                let end_idx = index.upper_bound(&trim_end);

                *index = &index[start_idx..end_idx];
            }
            IndexedSpanFilter::Not(index, inner_filter) => {
                // we can trim the end
                let trim_end = end;

                let end_idx = index.upper_bound(&trim_end);

                *index = &index[..end_idx];

                inner_filter.trim_to_timeframe(start, end);
            }
            IndexedSpanFilter::And(filters) => filters
                .iter_mut()
                .for_each(|f| f.trim_to_timeframe(start, end)),
            IndexedSpanFilter::Or(filters) => filters
                .iter_mut()
                .for_each(|f| f.trim_to_timeframe(start, end)),
        }
    }
}

impl<'a> IndexedSpanFilter<'a> {
    // This basically ensures that the filter can be trimmed by timeframe. Only
    // `Stratified` filters can be trimmed. If there are no stratified filters
    // or the filter is constructed in a way that not all filters are covered,
    // this will add the necessary `Stratified` filters to the root.
    pub fn ensure_stratified(&mut self, duration_index: &'a SpanDurationIndex) {
        if self.is_stratified() {
            return;
        }

        if let IndexedSpanFilter::And(filters) = self {
            let dfilters = duration_index.to_stratified_indexes();
            let dfilters = dfilters
                .into_iter()
                .map(|(index, range)| IndexedSpanFilter::Stratified(index, range, None))
                .collect();
            let dfilter = IndexedSpanFilter::Or(dfilters);

            filters.push(dfilter);
        } else {
            let this = std::mem::replace(self, IndexedSpanFilter::Single(&[], None));

            let dfilters = duration_index.to_stratified_indexes();
            let dfilters = dfilters
                .into_iter()
                .map(|(index, range)| IndexedSpanFilter::Stratified(index, range, None))
                .collect();
            let dfilter = IndexedSpanFilter::Or(dfilters);

            *self = IndexedSpanFilter::And(vec![this, dfilter])
        }
    }
}

pub(crate) enum BasicSpanFilter {
    Level(SimpleLevel),
    Duration(DurationFilter),
    Created(ValueOperator, Timestamp),
    Closed(ValueOperator, Timestamp),
    Name(ValueStringComparison),
    Namespace(ValueStringComparison),
    File(FileFilter),
    Root,
    Trace(TraceRoot),
    Parent(SpanKey),
    Attribute(String, ValueFilter),
    Not(Box<BasicSpanFilter>),
    And(Vec<BasicSpanFilter>),
    Or(Vec<BasicSpanFilter>),
}

impl BasicSpanFilter {
    pub fn simplify(&mut self) {
        match self {
            BasicSpanFilter::Level(_) => {}
            BasicSpanFilter::Duration(_) => {}
            BasicSpanFilter::Created(_, _) => {}
            BasicSpanFilter::Closed(_, _) => {}
            BasicSpanFilter::Name(_) => {}
            BasicSpanFilter::Namespace(_) => {}
            BasicSpanFilter::File(_) => {}
            BasicSpanFilter::Root => {}
            BasicSpanFilter::Trace(_) => {}
            BasicSpanFilter::Parent(_) => {}
            BasicSpanFilter::Attribute(_, _) => {}
            BasicSpanFilter::Not(_) => {}
            BasicSpanFilter::And(filters) => {
                for filter in &mut *filters {
                    filter.simplify()
                }

                if filters.len() == 1 {
                    let mut filters = std::mem::take(filters);
                    let filter = filters.pop().unwrap();
                    *self = filter;
                }
            }
            BasicSpanFilter::Or(filters) => {
                for filter in &mut *filters {
                    filter.simplify()
                }

                if filters.len() == 1 {
                    let mut filters = std::mem::take(filters);
                    let filter = filters.pop().unwrap();
                    *self = filter;
                }
            }
        }
    }

    pub fn matches<S: Storage>(&self, context: &SpanContext<'_, S>) -> bool {
        let span = context.span();
        match self {
            BasicSpanFilter::Level(level) => span.level.into_simple_level() == *level,
            BasicSpanFilter::Duration(filter) => filter.matches(span.duration()),
            BasicSpanFilter::Created(op, value) => op.compare(span.created_at, *value),
            BasicSpanFilter::Closed(op, value) => {
                let Some(closed_at) = span.closed_at else {
                    return false; // never match an open span
                };

                op.compare(closed_at, *value)
            }
            BasicSpanFilter::Name(filter) => filter.matches(&span.name),
            BasicSpanFilter::Namespace(filter) => filter.matches_opt(span.namespace.as_deref()),
            BasicSpanFilter::File(filter) => {
                filter.matches(span.file_name.as_deref(), span.file_line)
            }
            BasicSpanFilter::Root => span.parent_key.is_none(),
            BasicSpanFilter::Trace(trace) => context.trace_root() == *trace,
            BasicSpanFilter::Parent(parent_key) => span.parent_key == Some(*parent_key),
            BasicSpanFilter::Attribute(attribute, value_filter) => context
                .attribute(attribute)
                .map(|v| value_filter.matches(v))
                .unwrap_or(false),
            BasicSpanFilter::Not(inner_filter) => !inner_filter.matches(context),
            BasicSpanFilter::And(filters) => filters.iter().all(|f| f.matches(context)),
            BasicSpanFilter::Or(filters) => filters.iter().any(|f| f.matches(context)),
        }
    }

    pub fn validate(predicate: FilterPredicate) -> Result<FallibleFilterPredicate, InputError> {
        use FilterPropertyKind::*;
        use ValueOperator::*;

        let predicate = match predicate {
            FilterPredicate::Single(single) => single,
            FilterPredicate::And(predicates) => {
                return Ok(FallibleFilterPredicate::And(
                    predicates
                        .into_iter()
                        .map(|p| Self::validate(p.clone()).map_err(|e| (e, p.to_string())))
                        .collect(),
                ));
            }
            FilterPredicate::Or(predicates) => {
                return Ok(FallibleFilterPredicate::Or(
                    predicates
                        .into_iter()
                        .map(|p| Self::validate(p.clone()).map_err(|e| (e, p.to_string())))
                        .collect(),
                ));
            }
        };

        let property_kind = predicate
            .property_kind
            .unwrap_or(match predicate.property.as_str() {
                "level" | "duration" | "name" | "namespace" | "file" | "parent" | "created"
                | "closed" | "trace" => Inherent,
                _ => Attribute,
            });

        match (property_kind, predicate.property.as_str()) {
            (Inherent, "level") => {
                let (op, value) = match &predicate.value {
                    ValuePredicate::Comparison(op, value) => (op, value),
                    _ => return Err(InputError::InvalidLevelValue),
                };

                let _level = match value.as_str() {
                    "TRACE" => Level::Trace,
                    "DEBUG" => Level::Debug,
                    "INFO" => Level::Info,
                    "WARN" => Level::Warn,
                    "ERROR" => Level::Error,
                    _ => return Err(InputError::InvalidLevelValue),
                };

                let _above = match op {
                    Eq => false,
                    Gte => true,
                    _ => return Err(InputError::InvalidLevelOperator),
                };
            }
            (Inherent, "duration") => validate_value_predicate(
                &predicate.value,
                |op, value| {
                    DurationFilter::from_input(*op, value)?;
                    Ok(())
                },
                |_| Err(InputError::InvalidDurationValue),
                |_| Err(InputError::InvalidDurationValue),
            )?,
            (Inherent, "name") => validate_value_predicate(
                &predicate.value,
                |_op, _value| Ok(()),
                |wildcard| {
                    WildcardBuilder::new(wildcard.as_bytes())
                        .without_one_metasymbol()
                        .build()
                        .map_err(|_| InputError::InvalidWildcardValue)?;
                    Ok(())
                },
                |regex| {
                    Regex::new(regex).map_err(|_| InputError::InvalidRegexValue)?;
                    Ok(())
                },
            )?,
            (Inherent, "namespace") => validate_value_predicate(
                &predicate.value,
                |_op, _value| Ok(()),
                |wildcard| {
                    WildcardBuilder::new(wildcard.as_bytes())
                        .without_one_metasymbol()
                        .build()
                        .map_err(|_| InputError::InvalidWildcardValue)?;
                    Ok(())
                },
                |regex| {
                    Regex::new(regex).map_err(|_| InputError::InvalidRegexValue)?;
                    Ok(())
                },
            )?,
            (Inherent, "file") => validate_value_predicate(
                &predicate.value,
                |op, value| {
                    if *op != ValueOperator::Eq {
                        return Err(InputError::InvalidFileOperator);
                    }

                    if let Some((_name, line)) = value.rsplit_once(':') {
                        let _: u32 = line.parse().map_err(|_| InputError::InvalidFileValue)?;
                    }

                    Ok(())
                },
                |wildcard| {
                    if let Some((name, line)) = wildcard.rsplit_once(':') {
                        let _: u32 = line.parse().map_err(|_| InputError::InvalidFileValue)?;

                        WildcardBuilder::new(name.as_bytes())
                            .without_one_metasymbol()
                            .build()
                            .map_err(|_| InputError::InvalidWildcardValue)?;
                    } else {
                        WildcardBuilder::new(wildcard.as_bytes())
                            .without_one_metasymbol()
                            .build()
                            .map_err(|_| InputError::InvalidWildcardValue)?;
                    }

                    Ok(())
                },
                |regex| {
                    Regex::new(regex).map_err(|_| InputError::InvalidRegexValue)?;
                    Ok(())
                },
            )?,
            (Inherent, "created") => {
                validate_value_predicate(
                    &predicate.value,
                    |_op, value| {
                        let _: Timestamp =
                            value.parse().map_err(|_| InputError::InvalidCreatedValue)?;

                        Ok(())
                    },
                    |_| Err(InputError::InvalidCreatedValue),
                    |_| Err(InputError::InvalidCreatedValue),
                )?;
            }
            (Inherent, "closed") => {
                validate_value_predicate(
                    &predicate.value,
                    |_op, value| {
                        let _: Timestamp =
                            value.parse().map_err(|_| InputError::InvalidClosedValue)?;

                        Ok(())
                    },
                    |_| Err(InputError::InvalidClosedValue),
                    |_| Err(InputError::InvalidClosedValue),
                )?;
            }
            (Inherent, "parent") => {
                validate_value_predicate(
                    &predicate.value,
                    |op, value| {
                        if *op != ValueOperator::Eq {
                            return Err(InputError::InvalidParentOperator);
                        }

                        if value != "none" {
                            let _: FullSpanId =
                                value.parse().map_err(|_| InputError::InvalidParentValue)?;
                        }

                        Ok(())
                    },
                    |_| Err(InputError::InvalidParentValue),
                    |_| Err(InputError::InvalidParentValue),
                )?;
            }
            (Inherent, "trace") => {
                validate_value_predicate(
                    &predicate.value,
                    |op, value| {
                        if *op != ValueOperator::Eq {
                            return Err(InputError::InvalidTraceOperator);
                        }

                        let _: TraceRoot =
                            value.parse().map_err(|_| InputError::InvalidTraceValue)?;

                        Ok(())
                    },
                    |_| Err(InputError::InvalidTraceValue),
                    |_| Err(InputError::InvalidTraceValue),
                )?;
            }
            (Inherent, _) => {
                return Err(InputError::InvalidInherentProperty);
            }
            (Attribute, _) => {
                validate_value_predicate(
                    &predicate.value,
                    |_op, _value| Ok(()),
                    |wildcard| {
                        WildcardBuilder::new(wildcard.as_bytes())
                            .without_one_metasymbol()
                            .build()
                            .map_err(|_| InputError::InvalidWildcardValue)?;
                        Ok(())
                    },
                    |regex| {
                        Regex::new(regex).map_err(|_| InputError::InvalidRegexValue)?;
                        Ok(())
                    },
                )?;
            }
        }

        Ok(FallibleFilterPredicate::Single(FilterPredicateSingle {
            property_kind: Some(property_kind),
            ..predicate
        }))
    }

    pub fn from_predicate(
        predicate: FilterPredicate,
        span_key_map: &HashMap<FullSpanId, SpanKey>,
    ) -> Result<BasicSpanFilter, InputError> {
        use FilterPropertyKind::*;
        use ValueOperator::*;

        let predicate = match predicate {
            FilterPredicate::Single(single) => single,
            FilterPredicate::And(predicates) => {
                return predicates
                    .into_iter()
                    .map(|p| Self::from_predicate(p, span_key_map))
                    .collect::<Result<_, _>>()
                    .map(BasicSpanFilter::And)
            }
            FilterPredicate::Or(predicates) => {
                return predicates
                    .into_iter()
                    .map(|p| Self::from_predicate(p, span_key_map))
                    .collect::<Result<_, _>>()
                    .map(BasicSpanFilter::Or)
            }
        };

        let property_kind = predicate
            .property_kind
            .unwrap_or(match predicate.property.as_str() {
                "level" | "duration" | "name" | "namespace" | "file" | "parent" | "created"
                | "closed" | "trace" => Inherent,
                _ => Attribute,
            });

        let filter = match (property_kind, predicate.property.as_str()) {
            (Inherent, "level") => {
                let (op, value) = match &predicate.value {
                    ValuePredicate::Comparison(op, value) => (op, value),
                    _ => return Err(InputError::InvalidLevelValue),
                };

                let level = match value.as_str() {
                    "TRACE" => SimpleLevel::Trace,
                    "DEBUG" => SimpleLevel::Debug,
                    "INFO" => SimpleLevel::Info,
                    "WARN" => SimpleLevel::Warn,
                    "ERROR" => SimpleLevel::Error,
                    "FATAL" => SimpleLevel::Fatal,
                    _ => return Err(InputError::InvalidLevelValue),
                };

                let above = match op {
                    Gte => true,
                    Eq => false,
                    _ => return Err(InputError::InvalidLevelOperator),
                };

                if above {
                    BasicSpanFilter::Or(level.iter_gte().map(BasicSpanFilter::Level).collect())
                } else {
                    BasicSpanFilter::Level(level)
                }
            }
            (Inherent, "duration") => filterify_span_filter(
                predicate.value,
                |op, value| {
                    Ok(BasicSpanFilter::Duration(DurationFilter::from_input(
                        op, &value,
                    )?))
                },
                |_| Err(InputError::InvalidDurationValue),
                |_| Err(InputError::InvalidDurationValue),
            )?,
            (Inherent, "name") => filterify_span_filter(
                predicate.value,
                |op, value| {
                    let filter = ValueStringComparison::Compare(op, value);
                    Ok(BasicSpanFilter::Name(filter))
                },
                |wildcard| {
                    let wildcard = WildcardBuilder::from_owned(wildcard.into_bytes())
                        .without_one_metasymbol()
                        .build()
                        .map_err(|_| InputError::InvalidWildcardValue)?;

                    let filter = ValueStringComparison::Wildcard(wildcard);
                    Ok(BasicSpanFilter::Name(filter))
                },
                |regex| {
                    let regex = Regex::new(&regex).map_err(|_| InputError::InvalidWildcardValue)?;

                    let filter = ValueStringComparison::Regex(regex);
                    Ok(BasicSpanFilter::Name(filter))
                },
            )?,
            (Inherent, "namespace") => filterify_span_filter(
                predicate.value,
                |op, value| {
                    let filter = ValueStringComparison::Compare(op, value);
                    Ok(BasicSpanFilter::Namespace(filter))
                },
                |wildcard| {
                    let wildcard = WildcardBuilder::from_owned(wildcard.into_bytes())
                        .without_one_metasymbol()
                        .build()
                        .map_err(|_| InputError::InvalidWildcardValue)?;

                    let filter = ValueStringComparison::Wildcard(wildcard);
                    Ok(BasicSpanFilter::Namespace(filter))
                },
                |regex| {
                    let regex = Regex::new(&regex).map_err(|_| InputError::InvalidWildcardValue)?;

                    let filter = ValueStringComparison::Regex(regex);
                    Ok(BasicSpanFilter::Namespace(filter))
                },
            )?,
            (Inherent, "file") => filterify_span_filter(
                predicate.value,
                |op, value| {
                    if op != ValueOperator::Eq {
                        return Err(InputError::InvalidFileOperator);
                    }

                    let filter = if let Some((name, line)) = value.rsplit_once(':') {
                        let line: u32 = line.parse().map_err(|_| InputError::InvalidFileValue)?;

                        FileFilter {
                            name: ValueStringComparison::Compare(
                                ValueOperator::Eq,
                                name.to_owned(),
                            ),
                            line: Some(line),
                        }
                    } else {
                        FileFilter {
                            name: ValueStringComparison::Compare(
                                ValueOperator::Eq,
                                value.to_owned(),
                            ),
                            line: None,
                        }
                    };

                    Ok(BasicSpanFilter::File(filter))
                },
                |wildcard| {
                    let filter = if let Some((name, line)) = wildcard.rsplit_once(':') {
                        let line: u32 = line.parse().map_err(|_| InputError::InvalidFileValue)?;

                        let wildcard = WildcardBuilder::from_owned(name.to_owned().into_bytes())
                            .without_one_metasymbol()
                            .build()
                            .map_err(|_| InputError::InvalidWildcardValue)?;

                        FileFilter {
                            name: ValueStringComparison::Wildcard(wildcard),
                            line: Some(line),
                        }
                    } else {
                        let wildcard = WildcardBuilder::from_owned(wildcard.into_bytes())
                            .without_one_metasymbol()
                            .build()
                            .map_err(|_| InputError::InvalidWildcardValue)?;

                        FileFilter {
                            name: ValueStringComparison::Wildcard(wildcard),
                            line: None,
                        }
                    };

                    Ok(BasicSpanFilter::File(filter))
                },
                |regex| {
                    let regex = Regex::new(&regex).map_err(|_| InputError::InvalidWildcardValue)?;

                    Ok(BasicSpanFilter::File(FileFilter {
                        name: ValueStringComparison::Regex(regex),
                        line: None,
                    }))
                },
            )?,
            (Inherent, "created") => filterify_span_filter(
                predicate.value,
                |op, value| {
                    let at: Timestamp =
                        value.parse().map_err(|_| InputError::InvalidCreatedValue)?;

                    Ok(BasicSpanFilter::Created(op, at))
                },
                |_| Err(InputError::InvalidCreatedValue),
                |_| Err(InputError::InvalidCreatedValue),
            )?,
            (Inherent, "closed") => filterify_span_filter(
                predicate.value,
                |op, value| {
                    let at: Timestamp =
                        value.parse().map_err(|_| InputError::InvalidClosedValue)?;

                    Ok(BasicSpanFilter::Closed(op, at))
                },
                |_| Err(InputError::InvalidClosedValue),
                |_| Err(InputError::InvalidClosedValue),
            )?,
            (Inherent, "parent") => filterify_span_filter(
                predicate.value,
                |op, value| {
                    if op != ValueOperator::Eq {
                        return Err(InputError::InvalidParentOperator);
                    }

                    if value == "none" {
                        Ok(BasicSpanFilter::Root)
                    } else {
                        let parent_id: FullSpanId =
                            value.parse().map_err(|_| InputError::InvalidParentValue)?;

                        let parent_key = span_key_map
                            .get(&parent_id)
                            .copied()
                            .unwrap_or(SpanKey::MIN);

                        Ok(BasicSpanFilter::Parent(parent_key))
                    }
                },
                |_| Err(InputError::InvalidParentValue),
                |_| Err(InputError::InvalidParentValue),
            )?,
            (Inherent, "trace") => filterify_span_filter(
                predicate.value,
                |op, value| {
                    if op != ValueOperator::Eq {
                        return Err(InputError::InvalidTraceOperator);
                    }

                    let trace: TraceRoot =
                        value.parse().map_err(|_| InputError::InvalidTraceValue)?;

                    Ok(BasicSpanFilter::Trace(trace))
                },
                |_| Err(InputError::InvalidTraceValue),
                |_| Err(InputError::InvalidTraceValue),
            )?,
            (Inherent, _) => {
                return Err(InputError::InvalidInherentProperty);
            }
            (Attribute, name) => filterify_span_filter(
                predicate.value,
                |op, value| {
                    let value_filter = ValueFilter::from_input(op, &value);
                    Ok(BasicSpanFilter::Attribute(name.to_owned(), value_filter))
                },
                |wildcard| {
                    let value_filter = ValueFilter::from_wildcard(wildcard)?;
                    Ok(BasicSpanFilter::Attribute(name.to_owned(), value_filter))
                },
                |regex| {
                    let value_filter = ValueFilter::from_regex(regex)?;
                    Ok(BasicSpanFilter::Attribute(name.to_owned(), value_filter))
                },
            )?,
        };

        Ok(filter)
    }
}

pub(crate) enum NonIndexedSpanFilter {
    Duration(DurationFilter),
    Closed(ValueOperator, Timestamp),
    Name(ValueStringComparison),
    Namespace(ValueStringComparison),
    File(FileFilter),
    Parent(SpanKey),
    Attribute(String, ValueFilter),
}

impl NonIndexedSpanFilter {
    fn matches<S: Storage>(&self, context: &SpanContext<'_, S>) -> bool {
        let span = context.span();
        match self {
            NonIndexedSpanFilter::Duration(filter) => filter.matches(span.duration()),
            NonIndexedSpanFilter::Closed(op, value) => {
                let Some(closed_at) = span.closed_at else {
                    return false; // never match an open span
                };

                op.compare(closed_at, *value)
            }
            NonIndexedSpanFilter::Name(filter) => filter.matches(&span.name),
            NonIndexedSpanFilter::Namespace(filter) => {
                filter.matches_opt(span.namespace.as_deref())
            }
            // NonIndexedSpanFilter::Function(filter) => filter.matches_opt(span.function.as_deref()),
            NonIndexedSpanFilter::File(filter) => {
                filter.matches(span.file_name.as_deref(), span.file_line)
            }
            NonIndexedSpanFilter::Parent(parent_key) => span.parent_key == Some(*parent_key),
            NonIndexedSpanFilter::Attribute(attribute, value_filter) => context
                .attribute(attribute)
                .map(|v| value_filter.matches(v))
                .unwrap_or(false),
        }
    }
}

#[derive(Debug, PartialEq, Clone, Deserialize)]
pub(crate) struct DurationFilter {
    op: ValueOperator,
    measure: u64,
}

impl DurationFilter {
    pub fn from_input(op: ValueOperator, value: &str) -> Result<DurationFilter, InputError> {
        use nom::bytes::complete::{take_while, take_while1};
        use nom::combinator::{eof, opt};
        use nom::sequence::tuple;

        let (_, (number, maybe_units, _)) = tuple((
            take_while1(|c: char| c.is_ascii_digit() || c == '.'),
            opt(tuple((
                take_while(|c: char| c.is_whitespace()),
                take_while1(|c: char| c.is_alphabetic()),
            ))),
            eof,
        ))(value)
        .map_err(|_: nom::Err<nom::error::Error<_>>| InputError::InvalidDurationValue)?;

        let measure: f64 = number
            .parse()
            .map_err(|_| InputError::InvalidDurationValue)?;

        let unit_scale = match maybe_units.map(|(_, unit)| unit) {
            Some("μs" | "us" | "microsecond" | "microseconds") => 1.0,
            Some("ms" | "millisecond" | "milliseconds") => 1000.0,
            Some("s" | "second" | "seconds") => 1000000.0,
            Some("m" | "min" | "minute" | "minutes") => 60.0 * 1000000.0,
            Some("h" | "hour" | "hours") => 60.0 * 60.0 * 1000000.0,
            Some("d" | "day" | "days") => 24.0 * 60.0 * 60.0 * 1000000.0,
            Some(_other) => return Err(InputError::InvalidDurationValue),
            None => 1.0,
        };

        let measure = (measure * unit_scale) as u64;

        Ok(DurationFilter { op, measure })
    }

    pub fn matches(&self, duration: Option<u64>) -> bool {
        let Some(duration) = duration else {
            return false; // never match an incomplete duration
        };

        self.op.compare(&duration, &self.measure)
    }

    pub fn matches_duration_range(&self, range: &Range<u64>) -> Option<bool> {
        match self.op {
            // --y--[ p ]--n--
            ValueOperator::Gt if self.measure <= range.start => Some(true),
            ValueOperator::Gt if self.measure >= range.end => Some(false),
            ValueOperator::Gt => None,
            ValueOperator::Gte if self.measure < range.start => Some(true),
            ValueOperator::Gte if self.measure > range.end => Some(false),
            ValueOperator::Gte => None,

            ValueOperator::Eq => Some(range.contains(&self.measure)),

            // --n--[ p ]--y--
            ValueOperator::Lt if self.measure >= range.end => Some(true),
            ValueOperator::Lt if self.measure <= range.start => Some(false),
            ValueOperator::Lt => None,
            ValueOperator::Lte if self.measure > range.end => Some(true),
            ValueOperator::Lte if self.measure < range.start => Some(false),
            ValueOperator::Lte => None,
        }
    }
}

pub(crate) struct IndexedSpanFilterIterator<'i, S> {
    filter: IndexedSpanFilter<'i>,
    order: Order,
    curr_key: Timestamp,
    start_key: Timestamp,
    end_key: Timestamp,
    storage: &'i S,
}

impl<'i, S: Storage> IndexedSpanFilterIterator<'i, S> {
    pub fn new(query: Query, engine: &'i SyncEngine<S>) -> IndexedSpanFilterIterator<'i, S> {
        let mut filter = BasicSpanFilter::And(
            query
                .filter
                .into_iter()
                .map(|p| BasicSpanFilter::from_predicate(p, &engine.span_indexes.ids).unwrap())
                .collect(),
        );
        filter.simplify();

        let mut filter =
            IndexedSpanFilter::build(Some(filter), &engine.span_indexes, &engine.storage);

        let curr;
        let mut start = query.start;
        let mut end = query.end;

        // if order is asc
        // - if previous & greater than or = start, then start = previous + 1, curr = start
        // - if previous & less than start, then start = start, curr = previous + 1
        // - if no previous, then start = start, curr = MIN
        // if order is desc
        // - if previous & greater than start, then end = previous - 1, curr = end
        // - if previous & less than or = start, then end = start, curr = previous - 1
        // - if no previous, then end = end, curr = end

        match (query.order, query.previous) {
            (Order::Asc, Some(prev)) if prev >= query.start => {
                start = prev.saturating_add(1);
                curr = start;
            }
            (Order::Asc, Some(prev)) => {
                curr = prev.saturating_add(1);
            }
            (Order::Asc, None) => {
                curr = Timestamp::MIN;
            }
            (Order::Desc, Some(prev)) if prev > query.start => {
                end = Timestamp::new(prev.get() - 1).unwrap();
                curr = end;
            }
            (Order::Desc, Some(prev)) => {
                end = start;
                curr = Timestamp::new(prev.get() - 1).unwrap();
            }
            (Order::Desc, None) => {
                curr = end;
            }
        }

        filter.ensure_stratified(&engine.span_indexes.durations);
        filter.trim_to_timeframe(start, end);
        filter.optimize();

        let (start_key, end_key) = match query.order {
            Order::Asc => (start, end),
            Order::Desc => (start, Timestamp::MIN),
        };

        IndexedSpanFilterIterator {
            filter,
            order: query.order,
            curr_key: curr,
            end_key,
            start_key,
            storage: &engine.storage,
        }
    }

    pub fn new_internal(
        filter: IndexedSpanFilter<'i>,
        engine: &'i SyncEngine<S>,
    ) -> IndexedSpanFilterIterator<'i, S> {
        IndexedSpanFilterIterator {
            filter,
            order: Order::Asc,
            curr_key: Timestamp::MIN,
            end_key: Timestamp::MAX,
            start_key: Timestamp::MIN,
            storage: &engine.storage,
        }
    }
}

impl<S> Iterator for IndexedSpanFilterIterator<'_, S>
where
    S: Storage,
{
    type Item = SpanKey;

    fn next(&mut self) -> Option<SpanKey> {
        let span_key = self.filter.search(
            self.storage,
            self.curr_key,
            self.order,
            self.end_key,
            self.start_key,
        )?;

        match self.order {
            Order::Asc => self.curr_key = span_key.saturating_add(1),
            Order::Desc => self.curr_key = Timestamp::new(span_key.get() - 1).unwrap(),
        };

        Some(span_key)
    }

    // fn size_hint(&self) -> (usize, Option<usize>) {
    //     self.filter.size_hint()
    // }
}

fn filterify_span_filter(
    value: ValuePredicate,
    comparison_filterifier: impl Fn(ValueOperator, String) -> Result<BasicSpanFilter, InputError>
        + Clone,
    wildcard_filterifier: impl Fn(String) -> Result<BasicSpanFilter, InputError> + Clone,
    regex_filterifier: impl Fn(String) -> Result<BasicSpanFilter, InputError> + Clone,
) -> Result<BasicSpanFilter, InputError> {
    match value {
        ValuePredicate::Not(predicate) => {
            Ok(BasicSpanFilter::Not(Box::new(filterify_span_filter(
                *predicate,
                comparison_filterifier,
                wildcard_filterifier,
                regex_filterifier,
            )?)))
        }
        ValuePredicate::Comparison(op, value) => comparison_filterifier(op, value),
        ValuePredicate::Wildcard(wildcard) => wildcard_filterifier(wildcard),
        ValuePredicate::Regex(regex) => regex_filterifier(regex),
        ValuePredicate::And(predicates) => Ok(BasicSpanFilter::And(
            predicates
                .into_iter()
                .map(|p| {
                    filterify_span_filter(
                        p,
                        comparison_filterifier.clone(),
                        wildcard_filterifier.clone(),
                        regex_filterifier.clone(),
                    )
                })
                .collect::<Result<_, _>>()?,
        )),
        ValuePredicate::Or(predicates) => Ok(BasicSpanFilter::Or(
            predicates
                .into_iter()
                .map(|p| {
                    filterify_span_filter(
                        p,
                        comparison_filterifier.clone(),
                        wildcard_filterifier.clone(),
                        regex_filterifier.clone(),
                    )
                })
                .collect::<Result<_, _>>()?,
        )),
    }
}

pub fn validate_span_filter(
    predicate: FilterPredicate,
) -> Result<FallibleFilterPredicate, InputError> {
    BasicSpanFilter::validate(predicate)
}
