use std::cmp::Ordering;
use std::collections::HashMap;
use std::ops::{Add, Range};

use attribute::ValueFilter;
use ghost_cell::GhostToken;
use input::{FilterPredicate, FilterPropertyKind, ValuePredicate};
use regex::Regex;
use serde::Deserialize;
use wildcard::WildcardBuilder;

use crate::index::{EventIndexes, SpanDurationIndex, SpanIndexes};
use crate::models::{parse_full_span_id, EventKey, Level, SpanKey, Timestamp, ValueOperator};
use crate::storage::Storage;
use crate::{Ancestors, Event, InstanceId, InstanceKey, RawEngine, SpanId};

pub mod attribute;
pub mod input;

#[derive(Deserialize)]
pub struct Query {
    pub filter: Vec<FilterPredicate>,
    pub order: Order,
    pub limit: usize,
    pub start: Timestamp,
    pub end: Timestamp,
    // when paginating, this is the last key of the previous call
    pub previous: Option<Timestamp>,
}

pub enum IndexedEventFilter<'i> {
    Single(&'i [Timestamp], Option<NonIndexedEventFilter>),
    Not(&'i [Timestamp], Box<IndexedEventFilter<'i>>),
    And(Vec<IndexedEventFilter<'i>>),
    Or(Vec<IndexedEventFilter<'i>>),
}

impl IndexedEventFilter<'_> {
    pub fn build(
        filter: Option<BasicEventFilter>,
        event_indexes: &EventIndexes,
    ) -> IndexedEventFilter<'_> {
        let Some(filter) = filter else {
            return IndexedEventFilter::Single(&event_indexes.all, None);
        };

        match filter {
            BasicEventFilter::Level(level) => {
                IndexedEventFilter::Single(&event_indexes.levels[level as usize], None)
            }
            BasicEventFilter::Instance(instance_key) => {
                let instance_index = event_indexes
                    .instances
                    .get(&instance_key)
                    .map(Vec::as_slice)
                    .unwrap_or_default();

                IndexedEventFilter::Single(instance_index, None)
            }
            BasicEventFilter::Ancestor(ancestor_key) => {
                let index = &event_indexes.descendents[&ancestor_key];

                IndexedEventFilter::Single(index, None)
            }
            BasicEventFilter::Root => IndexedEventFilter::Single(&event_indexes.roots, None),
            BasicEventFilter::Parent(parent_key) => {
                let index = &event_indexes.descendents[&parent_key];

                IndexedEventFilter::Single(index, Some(NonIndexedEventFilter::Parent(parent_key)))
            }
            BasicEventFilter::Attribute(attribute, value_filter) => {
                if let Some(attr_index) = event_indexes.attributes.get(&attribute) {
                    let filters = attr_index
                        .make_indexed_filter(value_filter)
                        .into_iter()
                        .map(|(i, f)| {
                            IndexedEventFilter::Single(
                                i,
                                f.map(|f| {
                                    NonIndexedEventFilter::Attribute(attribute.clone(), Box::new(f))
                                }),
                            )
                        })
                        .collect();

                    IndexedEventFilter::Or(filters)
                } else {
                    IndexedEventFilter::Single(
                        &event_indexes.all,
                        Some(NonIndexedEventFilter::Attribute(
                            attribute,
                            Box::new(value_filter),
                        )),
                    )
                }
            }
            BasicEventFilter::Not(filter) => IndexedEventFilter::Not(
                &event_indexes.all,
                Box::new(IndexedEventFilter::build(Some(*filter), event_indexes)),
            ),
            BasicEventFilter::And(filters) => IndexedEventFilter::And(
                filters
                    .into_iter()
                    .map(|f| IndexedEventFilter::build(Some(f), event_indexes))
                    .collect(),
            ),
            BasicEventFilter::Or(filters) => IndexedEventFilter::Or(
                filters
                    .into_iter()
                    .map(|f| IndexedEventFilter::build(Some(f), event_indexes))
                    .collect(),
            ),
        }
    }

    // This searches for an entry equal to or beyond the provided entry
    pub fn search<'b, S: Storage>(
        &mut self,
        token: &GhostToken<'b>,
        storage: &S,
        event_ancestors: &HashMap<Timestamp, Ancestors<'b>>,
        mut entry: Timestamp,
        order: Order,
        bound: Timestamp,
    ) -> Option<Timestamp> {
        match self {
            IndexedEventFilter::Single(entries, filter) => match order {
                Order::Asc => loop {
                    let idx = entries.lower_bound(&entry);
                    *entries = &entries[idx..];
                    let found_entry = entries.first().cloned();

                    let found_entry = found_entry?;
                    if found_entry > bound {
                        return None;
                    }

                    if let Some(filter) = filter {
                        if filter.matches(token, storage, event_ancestors, found_entry) {
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

                    if let Some(filter) = filter {
                        if filter.matches(token, storage, event_ancestors, found_entry) {
                            return Some(found_entry);
                        } else {
                            entry = Timestamp::new(found_entry.get() - 1).unwrap();
                        }
                    } else {
                        return Some(found_entry);
                    }
                },
            },
            IndexedEventFilter::Not(entries, filter) => match order {
                Order::Asc => loop {
                    let idx = entries.lower_bound(&entry);
                    *entries = &entries[idx..];
                    let found_entry = entries.first().cloned();

                    let found_entry = found_entry?;
                    if found_entry > bound {
                        return None;
                    }

                    let nested_entry = filter.search(
                        token,
                        storage,
                        event_ancestors,
                        found_entry,
                        order,
                        found_entry,
                    );

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

                    let nested_entry = filter.search(
                        token,
                        storage,
                        event_ancestors,
                        found_entry,
                        order,
                        found_entry,
                    );

                    if nested_entry != Some(found_entry) {
                        return Some(found_entry);
                    } else {
                        entry = Timestamp::new(found_entry.get() - 1).unwrap();
                    }
                },
            },
            IndexedEventFilter::And(indexed_filters) => {
                let mut current = entry;
                'outer: loop {
                    current = indexed_filters[0].search(
                        token,
                        storage,
                        event_ancestors,
                        current,
                        order,
                        bound,
                    )?;

                    for indexed_filter in &mut indexed_filters[1..] {
                        match indexed_filter.search(
                            token,
                            storage,
                            event_ancestors,
                            current,
                            order,
                            current,
                        ) {
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
            IndexedEventFilter::Or(indexed_filters) => {
                let mut next_entry =
                    indexed_filters[0].search(token, storage, event_ancestors, entry, order, bound);
                for indexed_filter in &mut indexed_filters[1..] {
                    let bound = next_entry.unwrap_or(bound);
                    if let Some(found_entry) =
                        indexed_filter.search(token, storage, event_ancestors, entry, order, bound)
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
            IndexedEventFilter::Single(index, _) => {
                // we don't look at the basic filter because we can't really
                // guess how many elements it will select
                index.len()
            }
            IndexedEventFilter::Not(index, _) => {
                // there may be a better solution, but this assumes that the
                // filter never matches
                index.len()
            }
            IndexedEventFilter::And(filters) => {
                // since an element must pass all filters, we can only select
                // the minimum from a single filter
                filters.iter().map(Self::estimate_count).min().unwrap_or(0)
            }
            IndexedEventFilter::Or(filters) => {
                // since OR filters can be completely disjoint, we can possibly
                // yield the sum of all filters
                filters.iter().map(Self::estimate_count).sum()
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match self {
            IndexedEventFilter::Single(index, Some(_)) => {
                // The non-indexed filter may filter-out all elements or none of
                // them. So the full range is possible.
                (0, Some(index.len()))
            }
            IndexedEventFilter::Single(index, None) => {
                // Without a non-indexed filter, this will always yield the
                // number of elements it contains.
                (index.len(), Some(index.len()))
            }
            IndexedEventFilter::Not(index, _) => {
                // The fill range is possible
                (0, Some(index.len()))
            }
            IndexedEventFilter::And(filters) => match filters.len() {
                0 => (0, Some(0)),
                1 => filters[0].size_hint(),
                _ => {
                    // With multiple filters AND-ed together, the potential min
                    // is zero (where none agree) and potential max is the
                    // smallest maximum.
                    let max = filters.iter().fold(None, |max, filter| {
                        merge(max, filter.size_hint().1, usize::min)
                    });

                    (0, max)
                }
            },
            IndexedEventFilter::Or(filters) => match filters.len() {
                0 => (0, Some(0)),
                1 => filters[0].size_hint(),
                _ => {
                    // With multiple filters OR-ed together, the potential min
                    // is the largest minimum and potential max is the sum of
                    // maximums.
                    filters.iter().fold((0, None), |(a_min, a_max), filter| {
                        let (min, max) = filter.size_hint();
                        (usize::max(a_min, min), merge(a_max, max, Add::add))
                    })
                }
            },
        }
    }

    pub fn trim_to_timeframe(&mut self, start: Timestamp, end: Timestamp) {
        match self {
            IndexedEventFilter::Single(index, _) => {
                let start_idx = index.lower_bound(&start);
                let end_idx = index.upper_bound(&end);

                *index = &index[start_idx..end_idx];
            }
            IndexedEventFilter::Not(index, inner_filter) => {
                let start_idx = index.lower_bound(&start);
                let end_idx = index.upper_bound(&end);

                *index = &index[start_idx..end_idx];

                inner_filter.trim_to_timeframe(start, end);
            }
            IndexedEventFilter::And(filters) => filters
                .iter_mut()
                .for_each(|f| f.trim_to_timeframe(start, end)),
            IndexedEventFilter::Or(filters) => filters
                .iter_mut()
                .for_each(|f| f.trim_to_timeframe(start, end)),
        }
    }

    pub fn optimize(&mut self) {
        match self {
            IndexedEventFilter::Single(_, _) => { /* nothing to do */ }
            IndexedEventFilter::Not(_, _) => { /* nothing to do */ }
            IndexedEventFilter::And(filters) => filters.sort_by_key(Self::estimate_count),
            IndexedEventFilter::Or(filters) => filters.sort_by_key(Self::estimate_count),
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum InputError {
    InvalidLevelValue,
    InvalidLevelOperator,
    InvalidNameValue,
    InvalidNameOperator,
    InvalidInstanceValue,
    InvalidInstanceOperator,
    InvalidAttributeValue,
    InvalidInherentProperty,
    InvalidDurationValue,
    MissingDurationOperator,
    InvalidDurationOperator,
    InvalidCreatedValue,
    MissingCreatedOperator,
    InvalidParentValue,
    InvalidParentOperator,
    InvalidStackValue,
    InvalidStackOperator,
    InvalidConnectedValue,
    MissingConnectedOperator,
    InvalidDisconnectedValue,
    MissingDisconnectedOperator,
    InvalidWildcardValue,
    InvalidRegexValue,
}

pub enum BasicEventFilter {
    Level(Level),
    Instance(InstanceKey),
    Ancestor(SpanKey),
    Root,
    Parent(SpanKey),
    Attribute(String, ValueFilter),
    Not(Box<BasicEventFilter>),
    And(Vec<BasicEventFilter>),
    Or(Vec<BasicEventFilter>),
}

impl BasicEventFilter {
    pub fn simplify(&mut self) {
        match self {
            BasicEventFilter::Level(_) => {}
            BasicEventFilter::Instance(_) => {}
            BasicEventFilter::Ancestor(_) => {}
            BasicEventFilter::Root => {}
            BasicEventFilter::Parent(_) => {}
            BasicEventFilter::Attribute(_, _) => {}
            BasicEventFilter::Not(_) => {}
            BasicEventFilter::And(filters) => {
                for filter in &mut *filters {
                    filter.simplify()
                }

                if filters.len() == 1 {
                    let mut filters = std::mem::take(filters);
                    let filter = filters.pop().unwrap();
                    *self = filter;
                }
            }
            BasicEventFilter::Or(filters) => {
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

    pub fn validate(predicate: FilterPredicate) -> Result<FilterPredicate, InputError> {
        use FilterPropertyKind::*;
        use ValueOperator::*;

        let property_kind = predicate
            .property_kind
            .unwrap_or(match predicate.property.as_str() {
                "level" | "instance" | "parent" | "stack" => Inherent,
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
            (Inherent, "instance") => {
                validate_value_predicate(
                    &predicate.value,
                    |op, value| {
                        if *op != ValueOperator::Eq {
                            return Err(InputError::InvalidInstanceOperator);
                        }

                        let _: InstanceId = value
                            .parse()
                            .map_err(|_| InputError::InvalidInstanceValue)?;

                        Ok(())
                    },
                    |_| Err(InputError::InvalidInstanceValue),
                    |_| Err(InputError::InvalidInstanceValue),
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
                            let _ =
                                parse_full_span_id(value).ok_or(InputError::InvalidParentValue)?;
                        }

                        Ok(())
                    },
                    |_| Err(InputError::InvalidParentValue),
                    |_| Err(InputError::InvalidParentValue),
                )?;
            }
            (Inherent, "stack") => {
                let (op, value) = match &predicate.value {
                    ValuePredicate::Comparison(op, value) => (op, value),
                    _ => return Err(InputError::InvalidStackValue),
                };

                let _ = parse_full_span_id(value).ok_or(InputError::InvalidStackValue)?;

                if *op != ValueOperator::Eq {
                    return Err(InputError::InvalidStackOperator);
                }
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
        };

        Ok(FilterPredicate {
            property_kind: Some(property_kind),
            ..predicate
        })
    }

    pub fn from_predicate(
        predicate: FilterPredicate,
        instance_key_map: &HashMap<InstanceId, InstanceKey>,
        span_key_map: &HashMap<(InstanceKey, SpanId), SpanKey>,
    ) -> Result<BasicEventFilter, InputError> {
        use FilterPropertyKind::*;
        use ValueOperator::*;

        let property_kind = predicate
            .property_kind
            .unwrap_or(match predicate.property.as_str() {
                "level" | "instance" | "parent" | "stack" => Inherent,
                _ => Attribute,
            });

        let filter = match (property_kind, predicate.property.as_str()) {
            (Inherent, "level") => {
                let (op, value) = match &predicate.value {
                    ValuePredicate::Comparison(op, value) => (op, value),
                    _ => return Err(InputError::InvalidLevelValue),
                };

                let level = match value.as_str() {
                    "TRACE" => Level::Trace,
                    "DEBUG" => Level::Debug,
                    "INFO" => Level::Info,
                    "WARN" => Level::Warn,
                    "ERROR" => Level::Error,
                    _ => return Err(InputError::InvalidLevelValue),
                };

                let above = match op {
                    Eq => false,
                    Gte => true,
                    _ => return Err(InputError::InvalidLevelOperator),
                };

                if above {
                    BasicEventFilter::Or(
                        ((level as i32)..5)
                            .map(|l| BasicEventFilter::Level(l.try_into().unwrap()))
                            .collect(),
                    )
                } else {
                    BasicEventFilter::Level(level)
                }
            }
            (Inherent, "instance") => filterify_event_filter(
                predicate.value,
                |op, value| {
                    if op != ValueOperator::Eq {
                        return Err(InputError::InvalidInstanceOperator);
                    }

                    let instance_id: InstanceId = value
                        .parse()
                        .map_err(|_| InputError::InvalidInstanceValue)?;

                    let instance_key = instance_key_map
                        .get(&instance_id)
                        .copied()
                        .unwrap_or(InstanceKey::MIN);

                    Ok(BasicEventFilter::Instance(instance_key))
                },
                |_| Err(InputError::InvalidInstanceValue),
                |_| Err(InputError::InvalidInstanceValue),
            )?,
            (Inherent, "parent") => filterify_event_filter(
                predicate.value,
                |op, value| {
                    if op != ValueOperator::Eq {
                        return Err(InputError::InvalidParentOperator);
                    }

                    if value == "none" {
                        Ok(BasicEventFilter::Root)
                    } else {
                        let (instance_id, parent_id) =
                            parse_full_span_id(&value).ok_or(InputError::InvalidParentValue)?;

                        let instance_key = instance_key_map
                            .get(&instance_id)
                            .copied()
                            .unwrap_or(InstanceKey::MIN);

                        let parent_key = span_key_map
                            .get(&(instance_key, parent_id))
                            .copied()
                            .unwrap_or(SpanKey::MIN);

                        Ok(BasicEventFilter::Parent(parent_key))
                    }
                },
                |_| Err(InputError::InvalidInstanceValue),
                |_| Err(InputError::InvalidInstanceValue),
            )?,
            (Inherent, "stack") => {
                let (op, value) = match &predicate.value {
                    ValuePredicate::Comparison(op, value) => (op, value),
                    _ => return Err(InputError::InvalidStackValue),
                };

                let (instance_id, span_id) =
                    parse_full_span_id(value).ok_or(InputError::InvalidStackValue)?;

                if *op != ValueOperator::Eq {
                    return Err(InputError::InvalidStackOperator);
                }

                let instance_key = instance_key_map
                    .get(&instance_id)
                    .copied()
                    .unwrap_or(InstanceKey::MIN);
                let span_key = span_key_map
                    .get(&(instance_key, span_id))
                    .copied()
                    .unwrap_or(SpanKey::MIN);

                BasicEventFilter::Ancestor(span_key)
            }
            (Inherent, _) => {
                return Err(InputError::InvalidInherentProperty);
            }
            (Attribute, name) => filterify_event_filter(
                predicate.value,
                |op, value| {
                    let value_filter = ValueFilter::from_input(op, &value);
                    Ok(BasicEventFilter::Attribute(name.to_owned(), value_filter))
                },
                |wildcard| {
                    let value_filter = ValueFilter::from_wildcard(wildcard)?;
                    Ok(BasicEventFilter::Attribute(name.to_owned(), value_filter))
                },
                |regex| {
                    let value_filter = ValueFilter::from_regex(regex)?;
                    Ok(BasicEventFilter::Attribute(name.to_owned(), value_filter))
                },
            )?,
        };

        Ok(filter)
    }

    pub(crate) fn matches<'b>(
        &self,
        token: &GhostToken<'b>,
        event_ancestors: &HashMap<Timestamp, Ancestors<'b>>,
        event: &Event,
    ) -> bool {
        match self {
            BasicEventFilter::Level(level) => event.level == *level,
            BasicEventFilter::Instance(instance_key) => event.instance_key == *instance_key,
            BasicEventFilter::Ancestor(span_key) => {
                event_ancestors[&event.key()].has_parent(*span_key)
            }
            BasicEventFilter::Root => event.span_key.is_none(),
            BasicEventFilter::Parent(parent_key) => event.span_key == Some(*parent_key),
            BasicEventFilter::Attribute(attribute, value_filter) => event_ancestors[&event.key()]
                .get_value(attribute, token)
                .map(|v| value_filter.matches(v))
                .unwrap_or(false),
            BasicEventFilter::Not(inner_filter) => {
                !inner_filter.matches(token, event_ancestors, event)
            }
            BasicEventFilter::And(filters) => filters
                .iter()
                .all(|f| f.matches(token, event_ancestors, event)),
            BasicEventFilter::Or(filters) => filters
                .iter()
                .any(|f| f.matches(token, event_ancestors, event)),
        }
    }
}

pub enum NonIndexedEventFilter {
    Parent(SpanKey),
    Attribute(String, Box<ValueFilter>),
}

impl NonIndexedEventFilter {
    fn matches<'b, S: Storage>(
        &self,
        token: &GhostToken<'b>,
        storage: &S,
        event_ancestors: &HashMap<Timestamp, Ancestors<'b>>,
        entry: Timestamp,
    ) -> bool {
        let event = storage.get_event(entry).unwrap();
        match self {
            NonIndexedEventFilter::Parent(parent_key) => event.span_key == Some(*parent_key),
            NonIndexedEventFilter::Attribute(attribute, value_filter) => event_ancestors
                [&event.timestamp]
                .get_value(attribute, token)
                .map(|v| value_filter.matches(v))
                .unwrap_or(false),
        }
    }
}

pub struct IndexedEventFilterIterator<'i, 'b, S> {
    filter: IndexedEventFilter<'i>,
    order: Order,
    start_key: Timestamp,
    end_key: Timestamp,
    storage: &'i S,
    token: &'i GhostToken<'b>,
    ancestors: &'i HashMap<Timestamp, Ancestors<'b>>,
}

impl<'i, 'b, S> IndexedEventFilterIterator<'i, 'b, S> {
    pub fn new(
        query: Query,
        engine: &'i RawEngine<'b, S>,
    ) -> IndexedEventFilterIterator<'i, 'b, S> {
        let mut filter = BasicEventFilter::And(
            query
                .filter
                .into_iter()
                .map(|p| {
                    BasicEventFilter::from_predicate(
                        p,
                        &engine.instance_key_map,
                        &engine.span_key_map,
                    )
                    .unwrap()
                })
                .collect(),
        );
        filter.simplify();

        let mut filter = IndexedEventFilter::build(Some(filter), &engine.event_indexes);

        let mut start = query.start;
        let mut end = query.end;

        if let Some(prev) = query.previous {
            match query.order {
                Order::Asc => start = prev.saturating_add(1),
                Order::Desc => end = Timestamp::new(prev.get() - 1).unwrap(),
            }
        }

        filter.trim_to_timeframe(start, end);
        filter.optimize();

        let (start_key, end_key) = match query.order {
            Order::Asc => (start, end),
            Order::Desc => (end, start),
        };

        IndexedEventFilterIterator {
            filter,
            order: query.order,
            start_key,
            end_key,
            storage: &engine.storage,
            token: &engine.token,
            ancestors: &engine.event_ancestors,
        }
    }
}

impl<S> Iterator for IndexedEventFilterIterator<'_, '_, S>
where
    S: Storage,
{
    type Item = EventKey;

    fn next(&mut self) -> Option<EventKey> {
        let event_key = self.filter.search(
            self.token,
            self.storage,
            self.ancestors,
            self.start_key,
            self.order,
            self.end_key,
        )?;

        match self.order {
            Order::Asc => self.start_key = event_key.saturating_add(1),
            Order::Desc => self.start_key = Timestamp::new(event_key.get() - 1).unwrap(),
        };

        Some(event_key)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.filter.size_hint()
    }
}

pub enum IndexedSpanFilter<'i> {
    Single(&'i [Timestamp], Option<NonIndexedSpanFilter>),
    Stratified(&'i [Timestamp], Range<u64>, Option<NonIndexedSpanFilter>),
    Not(&'i [Timestamp], Box<IndexedSpanFilter<'i>>),
    And(Vec<IndexedSpanFilter<'i>>),
    Or(Vec<IndexedSpanFilter<'i>>),
}

impl IndexedSpanFilter<'_> {
    pub fn build(
        filter: Option<BasicSpanFilter>,
        span_indexes: &SpanIndexes,
    ) -> IndexedSpanFilter<'_> {
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
            BasicSpanFilter::Created(comparison) => {
                match comparison {
                    TimestampComparisonFilter::Gt(value) => {
                        let idx = span_indexes.all.upper_bound(&value);
                        IndexedSpanFilter::Single(&span_indexes.all[idx..], None)
                    }
                    TimestampComparisonFilter::Gte(value) => {
                        let idx = span_indexes.all.lower_bound(&value);
                        IndexedSpanFilter::Single(&span_indexes.all[idx..], None)
                    }
                    // TimestampComparisonFilter::Eq(value) => IndexedSpanFilter::Single(todo!(), None),
                    // TimestampComparisonFilter::Ne(value) => IndexedSpanFilter::Single(todo!(), None),
                    TimestampComparisonFilter::Lte(value) => {
                        let idx = span_indexes.all.upper_bound(&value);
                        IndexedSpanFilter::Single(&span_indexes.all[..idx], None)
                    }
                    TimestampComparisonFilter::Lt(value) => {
                        let idx = span_indexes.all.lower_bound(&value);
                        IndexedSpanFilter::Single(&span_indexes.all[..idx], None)
                    }
                }
            }
            BasicSpanFilter::Instance(instance_key) => {
                let instance_index = span_indexes
                    .instances
                    .get(&instance_key)
                    .map(Vec::as_slice)
                    .unwrap_or_default();

                IndexedSpanFilter::Single(instance_index, None)
            }
            BasicSpanFilter::Name(value) => {
                let name_index = span_indexes
                    .names
                    .get(&value)
                    .map(Vec::as_slice)
                    .unwrap_or_default();

                IndexedSpanFilter::Single(name_index, None)
            }
            BasicSpanFilter::Ancestor(ancestor_key) => {
                let index = &span_indexes.descendents[&ancestor_key];

                IndexedSpanFilter::Single(index, None)
            }
            BasicSpanFilter::Root => IndexedSpanFilter::Single(&span_indexes.roots, None),
            BasicSpanFilter::Parent(parent_key) => {
                let index = &span_indexes.descendents[&parent_key];

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
                    IndexedSpanFilter::Single(
                        &span_indexes.all,
                        Some(NonIndexedSpanFilter::Attribute(attribute, value_filter)),
                    )
                }
            }
            BasicSpanFilter::Not(filter) => IndexedSpanFilter::Not(
                &span_indexes.all,
                Box::new(IndexedSpanFilter::build(Some(*filter), span_indexes)),
            ),
            BasicSpanFilter::And(filters) => IndexedSpanFilter::And(
                filters
                    .into_iter()
                    .map(|f| IndexedSpanFilter::build(Some(f), span_indexes))
                    .collect(),
            ),
            BasicSpanFilter::Or(filters) => IndexedSpanFilter::Or(
                filters
                    .into_iter()
                    .map(|f| IndexedSpanFilter::build(Some(f), span_indexes))
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
    pub fn search<'b, S: Storage>(
        &mut self,
        token: &GhostToken<'b>,
        storage: &S,
        span_ancestors: &HashMap<Timestamp, Ancestors<'b>>,
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
                        if filter.matches(token, storage, span_ancestors, found_entry) {
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
                        if filter.matches(token, storage, span_ancestors, found_entry) {
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
                        if filter.matches(token, storage, span_ancestors, found_entry) {
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
                        if filter.matches(token, storage, span_ancestors, found_entry) {
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

                    let nested_entry = filter.search(
                        token,
                        storage,
                        span_ancestors,
                        found_entry,
                        order,
                        found_entry,
                        start,
                    );

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

                    let nested_entry = filter.search(
                        token,
                        storage,
                        span_ancestors,
                        found_entry,
                        order,
                        found_entry,
                        start,
                    );

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
                    current = indexed_filters[0].search(
                        token,
                        storage,
                        span_ancestors,
                        current,
                        order,
                        bound,
                        start,
                    )?;
                    for indexed_filter in &mut indexed_filters[1..] {
                        match indexed_filter.search(
                            token,
                            storage,
                            span_ancestors,
                            current,
                            order,
                            current,
                            start,
                        ) {
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
                let mut next_entry = indexed_filters[0].search(
                    token,
                    storage,
                    span_ancestors,
                    entry,
                    order,
                    bound,
                    start,
                );
                for indexed_filter in &mut indexed_filters[1..] {
                    let bound = next_entry.unwrap_or(bound);
                    if let Some(found_entry) = indexed_filter.search(
                        token,
                        storage,
                        span_ancestors,
                        entry,
                        order,
                        bound,
                        start,
                    ) {
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

#[derive(Debug, PartialEq, Deserialize)]
pub enum TimestampComparisonFilter {
    Gt(Timestamp),
    Gte(Timestamp),
    // Eq(Timestamp),
    // Ne(Timestamp),
    Lte(Timestamp),
    Lt(Timestamp),
}

pub enum BasicSpanFilter {
    Level(Level),
    Duration(DurationFilter),
    Created(TimestampComparisonFilter),
    Instance(InstanceKey),
    Name(String),
    Ancestor(SpanKey),
    Root,
    Parent(SpanKey),
    Attribute(String, ValueFilter),
    Not(Box<BasicSpanFilter>),
    And(Vec<BasicSpanFilter>),
    Or(Vec<BasicSpanFilter>),
}

impl BasicSpanFilter {
    fn simplify(&mut self) {
        match self {
            BasicSpanFilter::Level(_) => {}
            BasicSpanFilter::Duration(_) => {}
            BasicSpanFilter::Created(_) => {}
            BasicSpanFilter::Instance(_) => {}
            BasicSpanFilter::Name(_) => {}
            BasicSpanFilter::Ancestor(_) => {}
            BasicSpanFilter::Root => {}
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

    pub fn validate(predicate: FilterPredicate) -> Result<FilterPredicate, InputError> {
        use FilterPropertyKind::*;
        use ValueOperator::*;

        let property_kind = predicate
            .property_kind
            .unwrap_or(match predicate.property.as_str() {
                "level" | "instance" | "duration" | "name" | "parent" | "created" | "stack" => {
                    Inherent
                }
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
            (Inherent, "name") => {
                let (op, _value) = match &predicate.value {
                    ValuePredicate::Comparison(op, value) => (op, value),
                    _ => return Err(InputError::InvalidNameValue),
                };

                if *op != ValueOperator::Eq {
                    return Err(InputError::InvalidNameOperator);
                }
            }
            (Inherent, "instance") => {
                validate_value_predicate(
                    &predicate.value,
                    |op, value| {
                        if *op != ValueOperator::Eq {
                            return Err(InputError::InvalidInstanceOperator);
                        }

                        let _: InstanceId = value
                            .parse()
                            .map_err(|_| InputError::InvalidInstanceValue)?;

                        Ok(())
                    },
                    |_| Err(InputError::InvalidInstanceValue),
                    |_| Err(InputError::InvalidInstanceValue),
                )?;
            }
            (Inherent, "created") => {
                let (op, value) = match &predicate.value {
                    ValuePredicate::Comparison(op, value) => (op, value),
                    _ => return Err(InputError::InvalidCreatedValue),
                };

                let _: Timestamp = value.parse().map_err(|_| InputError::InvalidCreatedValue)?;

                match op {
                    Gt | Gte => {}
                    Lt | Lte => {}
                    Eq => return Err(InputError::MissingCreatedOperator),
                }
            }
            (Inherent, "parent") => {
                validate_value_predicate(
                    &predicate.value,
                    |op, value| {
                        if *op != ValueOperator::Eq {
                            return Err(InputError::InvalidParentOperator);
                        }

                        if value != "none" {
                            let _ =
                                parse_full_span_id(value).ok_or(InputError::InvalidParentValue)?;
                        }

                        Ok(())
                    },
                    |_| Err(InputError::InvalidParentValue),
                    |_| Err(InputError::InvalidParentValue),
                )?;
            }
            (Inherent, "stack") => {
                let (op, value) = match &predicate.value {
                    ValuePredicate::Comparison(op, value) => (op, value),
                    _ => return Err(InputError::InvalidStackValue),
                };

                let _ = parse_full_span_id(value).ok_or(InputError::InvalidStackValue)?;

                if *op != ValueOperator::Eq {
                    return Err(InputError::InvalidStackOperator);
                }
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

        Ok(FilterPredicate {
            property_kind: Some(property_kind),
            ..predicate
        })
    }

    pub fn from_predicate(
        predicate: FilterPredicate,
        instance_key_map: &HashMap<InstanceId, InstanceKey>,
        span_key_map: &HashMap<(InstanceKey, SpanId), SpanKey>,
    ) -> Result<BasicSpanFilter, InputError> {
        use FilterPropertyKind::*;
        use ValueOperator::*;

        let property_kind = predicate
            .property_kind
            .unwrap_or(match predicate.property.as_str() {
                "level" | "instance" | "duration" | "name" | "parent" | "created" | "stack" => {
                    Inherent
                }
                _ => Attribute,
            });

        let filter = match (property_kind, predicate.property.as_str()) {
            (Inherent, "level") => {
                let (op, value) = match &predicate.value {
                    ValuePredicate::Comparison(op, value) => (op, value),
                    _ => return Err(InputError::InvalidLevelValue),
                };

                let level = match value.as_str() {
                    "TRACE" => Level::Trace,
                    "DEBUG" => Level::Debug,
                    "INFO" => Level::Info,
                    "WARN" => Level::Warn,
                    "ERROR" => Level::Error,
                    _ => return Err(InputError::InvalidLevelValue),
                };

                let above = match op {
                    Gte => true,
                    Eq => false,
                    _ => return Err(InputError::InvalidLevelOperator),
                };

                if above {
                    BasicSpanFilter::Or(
                        ((level as i32)..5)
                            .map(|l| BasicSpanFilter::Level(l.try_into().unwrap()))
                            .collect(),
                    )
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
            (Inherent, "name") => {
                let (op, value) = match predicate.value {
                    ValuePredicate::Comparison(op, value) => (op, value),
                    _ => return Err(InputError::InvalidNameValue),
                };

                if op != ValueOperator::Eq {
                    return Err(InputError::InvalidNameOperator);
                }

                BasicSpanFilter::Name(value)
            }
            (Inherent, "instance") => filterify_span_filter(
                predicate.value,
                |op, value| {
                    if op != ValueOperator::Eq {
                        return Err(InputError::InvalidInstanceOperator);
                    }

                    let instance_id: InstanceId = value
                        .parse()
                        .map_err(|_| InputError::InvalidInstanceValue)?;

                    let instance_key = instance_key_map
                        .get(&instance_id)
                        .copied()
                        .unwrap_or(InstanceKey::MIN);

                    Ok(BasicSpanFilter::Instance(instance_key))
                },
                |_| Err(InputError::InvalidInstanceValue),
                |_| Err(InputError::InvalidInstanceValue),
            )?,
            (Inherent, "created") => {
                let (op, value) = match &predicate.value {
                    ValuePredicate::Comparison(op, value) => (op, value),
                    _ => return Err(InputError::InvalidCreatedValue),
                };

                let at: Timestamp = value.parse().map_err(|_| InputError::InvalidCreatedValue)?;

                let filter = match op {
                    Gte => TimestampComparisonFilter::Gte(at),
                    Gt => TimestampComparisonFilter::Gt(at),
                    Lte => TimestampComparisonFilter::Lte(at),
                    Lt => TimestampComparisonFilter::Lt(at),
                    Eq => return Err(InputError::MissingCreatedOperator),
                };

                BasicSpanFilter::Created(filter)
            }
            (Inherent, "parent") => filterify_span_filter(
                predicate.value,
                |op, value| {
                    if op != ValueOperator::Eq {
                        return Err(InputError::InvalidParentOperator);
                    }

                    if value == "none" {
                        Ok(BasicSpanFilter::Root)
                    } else {
                        let (instance_id, parent_id) =
                            parse_full_span_id(&value).ok_or(InputError::InvalidParentValue)?;

                        let instance_key = instance_key_map
                            .get(&instance_id)
                            .copied()
                            .unwrap_or(InstanceKey::MIN);

                        let parent_key = span_key_map
                            .get(&(instance_key, parent_id))
                            .copied()
                            .unwrap_or(SpanKey::MIN);

                        Ok(BasicSpanFilter::Parent(parent_key))
                    }
                },
                |_| Err(InputError::InvalidInstanceValue),
                |_| Err(InputError::InvalidInstanceValue),
            )?,
            (Inherent, "stack") => {
                let (op, value) = match &predicate.value {
                    ValuePredicate::Comparison(op, value) => (op, value),
                    _ => return Err(InputError::InvalidStackValue),
                };

                let (instance_id, span_id) =
                    parse_full_span_id(value).ok_or(InputError::InvalidStackValue)?;

                if *op != ValueOperator::Eq {
                    return Err(InputError::InvalidStackOperator);
                }

                let instance_key = instance_key_map
                    .get(&instance_id)
                    .copied()
                    .unwrap_or(InstanceKey::MIN);
                let span_key = span_key_map
                    .get(&(instance_key, span_id))
                    .copied()
                    .unwrap_or(SpanKey::MIN);

                BasicSpanFilter::Ancestor(span_key)
            }
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

pub enum NonIndexedSpanFilter {
    Duration(DurationFilter),
    Parent(SpanKey),
    Attribute(String, ValueFilter),
}

impl NonIndexedSpanFilter {
    fn matches<'b, S: Storage>(
        &self,
        token: &GhostToken<'b>,
        storage: &S,
        span_ancestors: &HashMap<Timestamp, Ancestors<'b>>,
        entry: Timestamp,
    ) -> bool {
        let span = storage.get_span(entry).unwrap();
        match self {
            NonIndexedSpanFilter::Duration(filter) => filter.matches(span.duration()),
            NonIndexedSpanFilter::Parent(parent_key) => span.parent_key == Some(*parent_key),
            NonIndexedSpanFilter::Attribute(attribute, value_filter) => span_ancestors
                [&span.created_at]
                .get_value(attribute, token)
                .map(|v| value_filter.matches(v))
                .unwrap_or(false),
        }
    }
}

#[derive(Debug, PartialEq, Clone, Deserialize)]
pub struct DurationFilter {
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

pub struct IndexedSpanFilterIterator<'i, 'b, S> {
    filter: IndexedSpanFilter<'i>,
    order: Order,
    curr_key: Timestamp,
    start_key: Timestamp,
    end_key: Timestamp,
    storage: &'i S,
    token: &'i GhostToken<'b>,
    ancestors: &'i HashMap<Timestamp, Ancestors<'b>>,
}

impl<'i, 'b, S> IndexedSpanFilterIterator<'i, 'b, S> {
    pub fn new(query: Query, engine: &'i RawEngine<'b, S>) -> IndexedSpanFilterIterator<'i, 'b, S> {
        let mut filter = BasicSpanFilter::And(
            query
                .filter
                .into_iter()
                .map(|p| {
                    BasicSpanFilter::from_predicate(
                        p,
                        &engine.instance_key_map,
                        &engine.span_key_map,
                    )
                    .unwrap()
                })
                .collect(),
        );
        filter.simplify();

        let mut filter = IndexedSpanFilter::build(Some(filter), &engine.span_indexes);

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
            token: &engine.token,
            ancestors: &engine.span_ancestors,
        }
    }

    pub fn new_internal(
        filter: IndexedSpanFilter<'i>,
        engine: &'i RawEngine<'b, S>,
    ) -> IndexedSpanFilterIterator<'i, 'b, S> {
        IndexedSpanFilterIterator {
            filter,
            order: Order::Asc,
            curr_key: Timestamp::MIN,
            end_key: Timestamp::MAX,
            start_key: Timestamp::MIN,
            storage: &engine.storage,
            token: &engine.token,
            ancestors: &engine.span_ancestors,
        }
    }
}

impl<S> Iterator for IndexedSpanFilterIterator<'_, '_, S>
where
    S: Storage,
{
    type Item = SpanKey;

    fn next(&mut self) -> Option<SpanKey> {
        let span_key = self.filter.search(
            self.token,
            self.storage,
            self.ancestors,
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

pub enum BasicInstanceFilter {
    Duration(DurationFilter),
    Connected(TimestampComparisonFilter),
    Disconnected(TimestampComparisonFilter),
    Attribute(String, ValueFilter),
    Not(Box<BasicInstanceFilter>),
    And(Vec<BasicInstanceFilter>),
    Or(Vec<BasicInstanceFilter>),
}

impl BasicInstanceFilter {
    pub fn simplify(&mut self) {
        match self {
            BasicInstanceFilter::Duration(_) => {}
            BasicInstanceFilter::Connected(_) => {}
            BasicInstanceFilter::Disconnected(_) => {}
            BasicInstanceFilter::Attribute(_, _) => {}
            BasicInstanceFilter::Not(_) => {}
            BasicInstanceFilter::And(filters) => {
                for filter in &mut *filters {
                    filter.simplify()
                }

                if filters.len() == 1 {
                    let mut filters = std::mem::take(filters);
                    let filter = filters.pop().unwrap();
                    *self = filter;
                }
            }
            BasicInstanceFilter::Or(filters) => {
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

    pub fn validate(predicate: FilterPredicate) -> Result<FilterPredicate, InputError> {
        use FilterPropertyKind::*;
        use ValueOperator::*;

        let property_kind = predicate
            .property_kind
            .unwrap_or(match predicate.property.as_str() {
                "duration" | "connected" | "disconnected" => Inherent,
                _ => Attribute,
            });

        match (property_kind, predicate.property.as_str()) {
            (Inherent, "duration") => validate_value_predicate(
                &predicate.value,
                |op, value| {
                    DurationFilter::from_input(*op, value)?;
                    Ok(())
                },
                |_| Err(InputError::InvalidDurationValue),
                |_| Err(InputError::InvalidDurationValue),
            )?,
            (Inherent, "connected") => {
                let (op, value) = match &predicate.value {
                    ValuePredicate::Comparison(op, value) => (op, value),
                    _ => return Err(InputError::InvalidConnectedValue),
                };

                let _: Timestamp = value
                    .parse()
                    .map_err(|_| InputError::InvalidConnectedValue)?;

                match op {
                    Gt | Gte => {}
                    Lt | Lte => {}
                    Eq => return Err(InputError::MissingConnectedOperator),
                }
            }
            (Inherent, "disconnected") => {
                let (op, value) = match &predicate.value {
                    ValuePredicate::Comparison(op, value) => (op, value),
                    _ => return Err(InputError::InvalidDisconnectedValue),
                };

                let _: Timestamp = value
                    .parse()
                    .map_err(|_| InputError::InvalidDisconnectedValue)?;

                match op {
                    Gt | Gte => {}
                    Lt | Lte => {}
                    Eq => return Err(InputError::MissingDisconnectedOperator),
                }
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

        Ok(FilterPredicate {
            property_kind: Some(property_kind),
            ..predicate
        })
    }

    pub fn from_predicate(predicate: FilterPredicate) -> Result<BasicInstanceFilter, InputError> {
        use FilterPropertyKind::*;
        use ValueOperator::*;

        let property_kind = predicate
            .property_kind
            .unwrap_or(match predicate.property.as_str() {
                "duration" | "connected" | "disconnected" => Inherent,
                _ => Attribute,
            });

        let filter = match (property_kind, predicate.property.as_str()) {
            (Inherent, "duration") => filterify_instance_filter(
                predicate.value,
                |op, value| {
                    Ok(BasicInstanceFilter::Duration(DurationFilter::from_input(
                        op, &value,
                    )?))
                },
                |_| Err(InputError::InvalidDurationValue),
                |_| Err(InputError::InvalidDurationValue),
            )?,
            (Inherent, "connected") => {
                let (op, value) = match &predicate.value {
                    ValuePredicate::Comparison(op, value) => (op, value),
                    _ => return Err(InputError::InvalidConnectedValue),
                };

                let at: Timestamp = value
                    .parse()
                    .map_err(|_| InputError::InvalidConnectedValue)?;

                let filter = match op {
                    Gte => TimestampComparisonFilter::Gte(at),
                    Gt => TimestampComparisonFilter::Gt(at),
                    Lte => TimestampComparisonFilter::Lte(at),
                    Lt => TimestampComparisonFilter::Lt(at),
                    Eq => return Err(InputError::MissingConnectedOperator),
                };

                BasicInstanceFilter::Connected(filter)
            }
            (Inherent, "disconnected") => {
                let (op, value) = match &predicate.value {
                    ValuePredicate::Comparison(op, value) => (op, value),
                    _ => return Err(InputError::InvalidDisconnectedValue),
                };

                let at: Timestamp = value
                    .parse()
                    .map_err(|_| InputError::InvalidDisconnectedValue)?;

                let filter = match op {
                    Gte => TimestampComparisonFilter::Gte(at),
                    Gt => TimestampComparisonFilter::Gt(at),
                    Lte => TimestampComparisonFilter::Lte(at),
                    Lt => TimestampComparisonFilter::Lt(at),
                    Eq => return Err(InputError::MissingDisconnectedOperator),
                };

                BasicInstanceFilter::Disconnected(filter)
            }
            (Inherent, _) => {
                return Err(InputError::InvalidInherentProperty);
            }
            (Attribute, name) => filterify_instance_filter(
                predicate.value,
                |op, value| {
                    let value_filter = ValueFilter::from_input(op, &value);
                    Ok(BasicInstanceFilter::Attribute(
                        name.to_owned(),
                        value_filter,
                    ))
                },
                |wildcard| {
                    let value_filter = ValueFilter::from_wildcard(wildcard)?;
                    Ok(BasicInstanceFilter::Attribute(
                        name.to_owned(),
                        value_filter,
                    ))
                },
                |regex| {
                    let value_filter = ValueFilter::from_regex(regex)?;
                    Ok(BasicInstanceFilter::Attribute(
                        name.to_owned(),
                        value_filter,
                    ))
                },
            )?,
        };

        Ok(filter)
    }

    pub fn matches<S: Storage>(&self, storage: &S, entry: Timestamp) -> bool {
        let instance = storage.get_instance(entry).unwrap();
        match self {
            BasicInstanceFilter::Duration(filter) => filter.matches(instance.duration()),
            BasicInstanceFilter::Connected(filter) => match filter {
                TimestampComparisonFilter::Gt(timestamp) => instance.connected_at > *timestamp,
                TimestampComparisonFilter::Gte(timestamp) => instance.connected_at >= *timestamp,
                TimestampComparisonFilter::Lte(timestamp) => instance.connected_at <= *timestamp,
                TimestampComparisonFilter::Lt(timestamp) => instance.connected_at < *timestamp,
            },
            BasicInstanceFilter::Disconnected(filter) => {
                let Some(disconnected_at) = instance.disconnected_at else {
                    return false;
                };

                match filter {
                    TimestampComparisonFilter::Gt(timestamp) => disconnected_at > *timestamp,
                    TimestampComparisonFilter::Gte(timestamp) => disconnected_at >= *timestamp,
                    TimestampComparisonFilter::Lte(timestamp) => disconnected_at <= *timestamp,
                    TimestampComparisonFilter::Lt(timestamp) => disconnected_at < *timestamp,
                }
            }
            BasicInstanceFilter::Attribute(attribute, value_filter) => instance
                .fields
                .get(attribute)
                .map(|v| value_filter.matches(v))
                .unwrap_or(false),
            BasicInstanceFilter::Not(inner_filter) => !inner_filter.matches(storage, entry),
            BasicInstanceFilter::And(filters) => filters.iter().all(|f| f.matches(storage, entry)),
            BasicInstanceFilter::Or(filters) => filters.iter().any(|f| f.matches(storage, entry)),
        }
    }
}

fn validate_value_predicate(
    value: &ValuePredicate,
    comparison_validator: impl Fn(&ValueOperator, &str) -> Result<(), InputError> + Clone,
    wildcard_validator: impl Fn(&str) -> Result<(), InputError> + Clone,
    regex_validator: impl Fn(&str) -> Result<(), InputError> + Clone,
) -> Result<(), InputError> {
    match value {
        ValuePredicate::Not(predicate) => validate_value_predicate(
            predicate,
            comparison_validator,
            wildcard_validator,
            regex_validator,
        ),
        ValuePredicate::Comparison(op, value) => comparison_validator(op, value),
        ValuePredicate::Wildcard(wildcard) => wildcard_validator(wildcard),
        ValuePredicate::Regex(regex) => regex_validator(regex),
        ValuePredicate::And(predicates) => predicates.iter().try_for_each(|p| {
            validate_value_predicate(
                p,
                comparison_validator.clone(),
                wildcard_validator.clone(),
                regex_validator.clone(),
            )
        }),
        ValuePredicate::Or(predicates) => predicates.iter().try_for_each(|p| {
            validate_value_predicate(
                p,
                comparison_validator.clone(),
                wildcard_validator.clone(),
                regex_validator.clone(),
            )
        }),
    }
}

fn filterify_event_filter(
    value: ValuePredicate,
    comparison_filterifier: impl Fn(ValueOperator, String) -> Result<BasicEventFilter, InputError>
        + Clone,
    wildcard_filterifier: impl Fn(String) -> Result<BasicEventFilter, InputError> + Clone,
    regex_filterifier: impl Fn(String) -> Result<BasicEventFilter, InputError> + Clone,
) -> Result<BasicEventFilter, InputError> {
    match value {
        ValuePredicate::Not(predicate) => {
            Ok(BasicEventFilter::Not(Box::new(filterify_event_filter(
                *predicate,
                comparison_filterifier,
                wildcard_filterifier,
                regex_filterifier,
            )?)))
        }
        ValuePredicate::Comparison(op, value) => comparison_filterifier(op, value),
        ValuePredicate::Wildcard(wildcard) => wildcard_filterifier(wildcard),
        ValuePredicate::Regex(regex) => regex_filterifier(regex),
        ValuePredicate::And(predicates) => Ok(BasicEventFilter::And(
            predicates
                .into_iter()
                .map(|p| {
                    filterify_event_filter(
                        p,
                        comparison_filterifier.clone(),
                        wildcard_filterifier.clone(),
                        regex_filterifier.clone(),
                    )
                })
                .collect::<Result<_, _>>()?,
        )),
        ValuePredicate::Or(predicates) => Ok(BasicEventFilter::Or(
            predicates
                .into_iter()
                .map(|p| {
                    filterify_event_filter(
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

fn filterify_instance_filter(
    value: ValuePredicate,
    comparison_filterifier: impl Fn(ValueOperator, String) -> Result<BasicInstanceFilter, InputError>
        + Clone,
    wildcard_filterifier: impl Fn(String) -> Result<BasicInstanceFilter, InputError> + Clone,
    regex_filterifier: impl Fn(String) -> Result<BasicInstanceFilter, InputError> + Clone,
) -> Result<BasicInstanceFilter, InputError> {
    match value {
        ValuePredicate::Not(predicate) => Ok(BasicInstanceFilter::Not(Box::new(
            filterify_instance_filter(
                *predicate,
                comparison_filterifier,
                wildcard_filterifier,
                regex_filterifier,
            )?,
        ))),
        ValuePredicate::Comparison(op, value) => comparison_filterifier(op, value),
        ValuePredicate::Wildcard(wildcard) => wildcard_filterifier(wildcard),
        ValuePredicate::Regex(regex) => regex_filterifier(regex),
        ValuePredicate::And(predicates) => Ok(BasicInstanceFilter::And(
            predicates
                .into_iter()
                .map(|p| {
                    filterify_instance_filter(
                        p,
                        comparison_filterifier.clone(),
                        wildcard_filterifier.clone(),
                        regex_filterifier.clone(),
                    )
                })
                .collect::<Result<_, _>>()?,
        )),
        ValuePredicate::Or(predicates) => Ok(BasicInstanceFilter::Or(
            predicates
                .into_iter()
                .map(|p| {
                    filterify_instance_filter(
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

#[derive(Copy, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Order {
    Asc,
    Desc,
}

#[allow(dead_code)]
pub trait BoundSearch<T> {
    // This finds the first index of an item that is not less than the provided
    // item. This works via a binary-search algorithm.
    //
    // NOTE: The result is only meaningful if the input is sorted.
    fn lower_bound(&self, item: &T) -> usize;

    // This finds the first index of an item that is greater than the provided
    // item. This works via a binary-search algorithm.
    //
    // NOTE: The result is only meaningful if the input is sorted.
    fn upper_bound(&self, item: &T) -> usize;

    // This finds the first index of an item that is not less than the provided
    // item. This works via a binary-expansion-search algorithm, i.e. it checks
    // indexes geometrically starting from the beginning and then uses binary
    // -search within those bounds. This method is good if the item is expected
    // near the beginning.
    //
    // NOTE: The result is only meaningful if the input is sorted.
    fn lower_bound_via_expansion(&self, item: &T) -> usize;

    // This finds the first index of an item that is greater than the provided
    // item. This works via a binary-expansion-search algorithm, i.e. it checks
    // indexes geometrically starting from the end and then uses binary-search
    // within those bounds. This method is good if the item is expected near the
    // end.
    //
    // NOTE: The result is only meaningful if the input is sorted.
    fn upper_bound_via_expansion(&self, item: &T) -> usize;
}

impl<T: Ord> BoundSearch<T> for [T] {
    fn lower_bound(&self, item: &T) -> usize {
        self.binary_search_by(|current_item| match current_item.cmp(item) {
            Ordering::Greater => Ordering::Greater,
            Ordering::Equal => Ordering::Greater,
            Ordering::Less => Ordering::Less,
        })
        .unwrap_or_else(|idx| idx)
    }

    fn upper_bound(&self, item: &T) -> usize {
        self.binary_search_by(|current_item| match current_item.cmp(item) {
            Ordering::Greater => Ordering::Greater,
            Ordering::Equal => Ordering::Less,
            Ordering::Less => Ordering::Less,
        })
        .unwrap_or_else(|idx| idx)
    }

    fn lower_bound_via_expansion(&self, item: &T) -> usize {
        let len = self.len();
        for (start, mut end) in std::iter::successors(Some((0, 1)), |&(_, j)| Some((j, j * 2))) {
            if end >= len {
                end = len
            } else if &self[end] < item {
                continue;
            }

            return self[start..end].lower_bound(item) + start;
        }

        unreachable!()
    }

    fn upper_bound_via_expansion(&self, item: &T) -> usize {
        let len = self.len();
        for (start, mut end) in std::iter::successors(Some((0, 1)), |&(_, j)| Some((j, j * 2))) {
            if end >= len {
                end = len
            } else if &self[len - end] > item {
                continue;
            }

            return self[len - end..len - start].upper_bound(item) + (len - end);
        }

        unreachable!()
    }
}

fn merge<T>(a: Option<T>, b: Option<T>, f: impl FnOnce(T, T) -> T) -> Option<T> {
    // I wish this was in the standard library

    match (a, b) {
        (Some(a), Some(b)) => Some(f(a, b)),
        (Some(a), None) => Some(a),
        (None, Some(b)) => Some(b),
        (None, None) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bounds_on_empty_slice() {
        assert_eq!([].lower_bound(&0), 0);
        assert_eq!([].upper_bound(&0), 0);
        assert_eq!([].lower_bound_via_expansion(&0), 0);
        assert_eq!([].upper_bound_via_expansion(&0), 0);
    }

    #[test]
    fn bounds_on_single_slice() {
        assert_eq!([1].lower_bound(&0), 0);
        assert_eq!([1].upper_bound(&0), 0);
        assert_eq!([1].lower_bound_via_expansion(&0), 0);
        assert_eq!([1].upper_bound_via_expansion(&0), 0);

        assert_eq!([1].lower_bound(&1), 0);
        assert_eq!([1].upper_bound(&1), 1);
        assert_eq!([1].lower_bound_via_expansion(&1), 0);
        assert_eq!([1].upper_bound_via_expansion(&1), 1);

        assert_eq!([1].lower_bound(&2), 1);
        assert_eq!([1].upper_bound(&2), 1);
        assert_eq!([1].lower_bound_via_expansion(&2), 1);
        assert_eq!([1].upper_bound_via_expansion(&2), 1);
    }

    #[test]
    fn bounds_for_duplicate_item() {
        assert_eq!([0, 0, 1, 1, 2, 2].lower_bound(&-1), 0);
        assert_eq!([0, 0, 1, 1, 2, 2].upper_bound(&-1), 0);
        assert_eq!([0, 0, 1, 1, 2, 2].lower_bound_via_expansion(&-1), 0);
        assert_eq!([0, 0, 1, 1, 2, 2].upper_bound_via_expansion(&-1), 0);

        assert_eq!([0, 0, 1, 1, 2, 2].lower_bound(&0), 0);
        assert_eq!([0, 0, 1, 1, 2, 2].upper_bound(&0), 2);
        assert_eq!([0, 0, 1, 1, 2, 2].lower_bound_via_expansion(&0), 0);
        assert_eq!([0, 0, 1, 1, 2, 2].upper_bound_via_expansion(&0), 2);

        assert_eq!([0, 0, 1, 1, 2, 2].lower_bound(&1), 2);
        assert_eq!([0, 0, 1, 1, 2, 2].upper_bound(&1), 4);
        assert_eq!([0, 0, 1, 1, 2, 2].lower_bound_via_expansion(&1), 2);
        assert_eq!([0, 0, 1, 1, 2, 2].upper_bound_via_expansion(&1), 4);

        assert_eq!([0, 0, 1, 1, 2, 2].lower_bound(&2), 4);
        assert_eq!([0, 0, 1, 1, 2, 2].upper_bound(&2), 6);
        assert_eq!([0, 0, 1, 1, 2, 2].lower_bound_via_expansion(&2), 4);
        assert_eq!([0, 0, 1, 1, 2, 2].upper_bound_via_expansion(&2), 6);

        assert_eq!([0, 0, 1, 1, 2, 2].lower_bound(&3), 6);
        assert_eq!([0, 0, 1, 1, 2, 2].upper_bound(&3), 6);
        assert_eq!([0, 0, 1, 1, 2, 2].lower_bound_via_expansion(&3), 6);
        assert_eq!([0, 0, 1, 1, 2, 2].upper_bound_via_expansion(&3), 6);
    }

    #[test]
    fn bounds_for_missing_item() {
        assert_eq!([0, 0, 2, 2].lower_bound(&1), 2);
        assert_eq!([0, 0, 2, 2].upper_bound(&1), 2);
        assert_eq!([0, 0, 2, 2].lower_bound_via_expansion(&1), 2);
        assert_eq!([0, 0, 2, 2].upper_bound_via_expansion(&1), 2);
    }

    // #[test]
    // fn parse_level_into_filter() {
    //     assert_eq!(
    //         BasicEventFilter::from_str("#level:TRACE").unwrap(),
    //         BasicEventFilter::Level(0),
    //     );
    //     assert_eq!(
    //         BasicEventFilter::from_str("#level:DEBUG").unwrap(),
    //         BasicEventFilter::Level(1),
    //     );
    //     assert_eq!(
    //         BasicEventFilter::from_str("#level:INFO").unwrap(),
    //         BasicEventFilter::Level(2),
    //     );
    //     assert_eq!(
    //         BasicEventFilter::from_str("#level:WARN").unwrap(),
    //         BasicEventFilter::Level(3),
    //     );
    //     assert_eq!(
    //         BasicEventFilter::from_str("#level:ERROR").unwrap(),
    //         BasicEventFilter::Level(4),
    //     );
    // }

    // #[test]
    // fn parse_level_plus_into_filter() {
    //     assert_eq!(
    //         BasicEventFilter::from_str("#level:TRACE+").unwrap(),
    //         BasicEventFilter::Or(vec![
    //             BasicEventFilter::Level(0),
    //             BasicEventFilter::Level(1),
    //             BasicEventFilter::Level(2),
    //             BasicEventFilter::Level(3),
    //             BasicEventFilter::Level(4),
    //         ])
    //     );
    //     assert_eq!(
    //         BasicEventFilter::from_str("#level:DEBUG+").unwrap(),
    //         BasicEventFilter::Or(vec![
    //             BasicEventFilter::Level(1),
    //             BasicEventFilter::Level(2),
    //             BasicEventFilter::Level(3),
    //             BasicEventFilter::Level(4),
    //         ])
    //     );
    //     assert_eq!(
    //         BasicEventFilter::from_str("#level:INFO+").unwrap(),
    //         BasicEventFilter::Or(vec![
    //             BasicEventFilter::Level(2),
    //             BasicEventFilter::Level(3),
    //             BasicEventFilter::Level(4),
    //         ])
    //     );
    //     assert_eq!(
    //         BasicEventFilter::from_str("#level:WARN+").unwrap(),
    //         BasicEventFilter::Or(vec![BasicEventFilter::Level(3), BasicEventFilter::Level(4),])
    //     );
    //     assert_eq!(
    //         BasicEventFilter::from_str("#level:ERROR+").unwrap(),
    //         BasicEventFilter::Level(4)
    //     );
    // }

    // #[test]
    // fn parse_attribute_into_filter() {
    //     assert_eq!(
    //         BasicEventFilter::from_str("@attr1:A").unwrap(),
    //         BasicEventFilter::Attribute("attr1".into(), "A".into()),
    //     );
    // }

    // #[test]
    // fn parse_multiple_into_filter() {
    //     assert_eq!(
    //         BasicEventFilter::from_str("@attr1:A @attr2:B").unwrap(),
    //         BasicEventFilter::And(vec![
    //             BasicEventFilter::Attribute("attr1".into(), "A".into()),
    //             BasicEventFilter::Attribute("attr2".into(), "B".into()),
    //         ])
    //     );
    //     assert_eq!(
    //         BasicEventFilter::from_str("#level:ERROR @attr2:B").unwrap(),
    //         BasicEventFilter::And(vec![
    //             BasicEventFilter::Level(4),
    //             BasicEventFilter::Attribute("attr2".into(), "B".into()),
    //         ])
    //     );
    //     assert_eq!(
    //         BasicEventFilter::from_str("#level:INFO+ @attr2:B").unwrap(),
    //         BasicEventFilter::And(vec![
    //             BasicEventFilter::Or(vec![
    //                 BasicEventFilter::Level(2),
    //                 BasicEventFilter::Level(3),
    //                 BasicEventFilter::Level(4),
    //             ]),
    //             BasicEventFilter::Attribute("attr2".into(), "B".into()),
    //         ])
    //     );
    // }

    // #[test]
    // fn parse_duration_into_filter() {
    //     assert_eq!(
    //         BasicSpanFilter::from_str("#duration:>1000000").unwrap(),
    //         BasicSpanFilter::Duration(DurationFilter::Gt(1000000.try_into().unwrap()))
    //     );
    //     assert_eq!(
    //         BasicSpanFilter::from_str("#duration:<1000000").unwrap(),
    //         BasicSpanFilter::Duration(DurationFilter::Lt(1000000.try_into().unwrap()))
    //     );
    // }
}
