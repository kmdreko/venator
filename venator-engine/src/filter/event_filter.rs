use std::collections::HashMap;
use std::ops::Add;

use regex::Regex;
use wildcard::WildcardBuilder;

use crate::context::{EventContext, SpanContext};
use crate::index::EventIndexes;
use crate::models::{
    FullSpanId, Level, SimpleLevel, SourceKind, SpanKey, Timestamp, TraceRoot, ValueOperator,
};
use crate::storage::Storage;
use crate::util::{
    merge, BoundSearch, CompoundIndexIterator, IndexIterator, SetIntersectionIterator,
    SetUnionIterator,
};
use crate::EventKey;

use super::input::{FilterPredicate, FilterPredicateSingle, FilterPropertyKind, ValuePredicate};
use super::value::{ValueFilter, ValueStringComparison};
use super::{validate_value_predicate, FallibleFilterPredicate, FileFilter, InputError, Order};

pub(crate) enum IndexedEventFilter<'i> {
    Single(&'i [Timestamp], Option<NonIndexedEventFilter>),
    Not(&'i [Timestamp], Box<IndexedEventFilter<'i>>),
    And(Vec<IndexedEventFilter<'i>>),
    Or(Vec<IndexedEventFilter<'i>>, bool),
}

impl<'a> IndexedEventFilter<'a> {
    pub fn build<S: Storage>(
        filter: Option<BasicEventFilter>,
        event_indexes: &'a EventIndexes,
        storage: &S,
    ) -> IndexedEventFilter<'a> {
        let Some(filter) = filter else {
            return IndexedEventFilter::Single(&event_indexes.all, None);
        };

        match filter {
            BasicEventFilter::All => IndexedEventFilter::Single(&event_indexes.all, None),
            BasicEventFilter::Timestamp(op, value) => match op {
                ValueOperator::Gt => {
                    let idx = event_indexes.all.upper_bound(&value);
                    IndexedEventFilter::Single(&event_indexes.all[idx..], None)
                }
                ValueOperator::Gte => {
                    let idx = event_indexes.all.lower_bound(&value);
                    IndexedEventFilter::Single(&event_indexes.all[idx..], None)
                }
                ValueOperator::Eq => {
                    let start = event_indexes.all.lower_bound(&value);
                    let end = event_indexes.all.upper_bound(&value);
                    IndexedEventFilter::Single(&event_indexes.all[start..end], None)
                }
                ValueOperator::Lte => {
                    let idx = event_indexes.all.upper_bound(&value);
                    IndexedEventFilter::Single(&event_indexes.all[..idx], None)
                }
                ValueOperator::Lt => {
                    let idx = event_indexes.all.lower_bound(&value);
                    IndexedEventFilter::Single(&event_indexes.all[..idx], None)
                }
            },
            BasicEventFilter::Level(level) => {
                IndexedEventFilter::Single(&event_indexes.levels[level as usize], None)
            }
            BasicEventFilter::Kind(kind) => IndexedEventFilter::Single(
                &event_indexes.all,
                Some(NonIndexedEventFilter::Kind(kind)),
            ),
            BasicEventFilter::Namespace(filter) => match filter {
                ValueStringComparison::None => IndexedEventFilter::Single(&[], None),
                ValueStringComparison::Compare(ValueOperator::Eq, value) => {
                    let namespace_index = event_indexes
                        .namespaces
                        .get(&value)
                        .map(Vec::as_slice)
                        .unwrap_or_default();

                    IndexedEventFilter::Single(namespace_index, None)
                }
                ValueStringComparison::Compare(_, _) => IndexedEventFilter::Single(
                    &event_indexes.all,
                    Some(NonIndexedEventFilter::Namespace(filter)),
                ),
                ValueStringComparison::Wildcard(_) => IndexedEventFilter::Single(
                    &event_indexes.all,
                    Some(NonIndexedEventFilter::Namespace(filter)),
                ),
                ValueStringComparison::Regex(_) => IndexedEventFilter::Single(
                    &event_indexes.all,
                    Some(NonIndexedEventFilter::Namespace(filter)),
                ),
                ValueStringComparison::All => IndexedEventFilter::Single(
                    &event_indexes.all,
                    Some(NonIndexedEventFilter::Namespace(filter)),
                ),
            },
            BasicEventFilter::Function(filter) => match filter {
                ValueStringComparison::None => IndexedEventFilter::Single(&[], None),
                ValueStringComparison::Compare(ValueOperator::Eq, value) => {
                    let namespace_index = event_indexes
                        .namespaces
                        .get(&value)
                        .map(Vec::as_slice)
                        .unwrap_or_default();

                    IndexedEventFilter::Single(namespace_index, None)
                }
                ValueStringComparison::Compare(_, _) => IndexedEventFilter::Single(
                    &event_indexes.all,
                    Some(NonIndexedEventFilter::Function(filter)),
                ),
                ValueStringComparison::Wildcard(_) => IndexedEventFilter::Single(
                    &event_indexes.all,
                    Some(NonIndexedEventFilter::Function(filter)),
                ),
                ValueStringComparison::Regex(_) => IndexedEventFilter::Single(
                    &event_indexes.all,
                    Some(NonIndexedEventFilter::Function(filter)),
                ),
                ValueStringComparison::All => IndexedEventFilter::Single(
                    &event_indexes.all,
                    Some(NonIndexedEventFilter::Function(filter)),
                ),
            },
            BasicEventFilter::File(filter) => match &filter.name {
                ValueStringComparison::None => IndexedEventFilter::Single(&[], None),
                ValueStringComparison::Compare(ValueOperator::Eq, value) => {
                    let filename_index = event_indexes
                        .filenames
                        .get(value)
                        .map(Vec::as_slice)
                        .unwrap_or_default();

                    IndexedEventFilter::Single(
                        filename_index,
                        filter.line.map(|_| NonIndexedEventFilter::File(filter)),
                    )
                }
                ValueStringComparison::Compare(_, _) => IndexedEventFilter::Single(
                    &event_indexes.all,
                    Some(NonIndexedEventFilter::File(filter)),
                ),
                ValueStringComparison::Wildcard(_) => IndexedEventFilter::Single(
                    &event_indexes.all,
                    Some(NonIndexedEventFilter::File(filter)),
                ),
                ValueStringComparison::Regex(_) => IndexedEventFilter::Single(
                    &event_indexes.all,
                    Some(NonIndexedEventFilter::File(filter)),
                ),
                ValueStringComparison::All => IndexedEventFilter::Single(
                    &event_indexes.all,
                    Some(NonIndexedEventFilter::File(filter)),
                ),
            },
            BasicEventFilter::Root => IndexedEventFilter::Single(&event_indexes.roots, None),
            BasicEventFilter::Trace(trace) => {
                let index = event_indexes
                    .traces
                    .get(&trace)
                    .map(Vec::as_slice)
                    .unwrap_or_default();

                IndexedEventFilter::Single(index, None)
            }
            BasicEventFilter::Parent(parent_key) => {
                let parent = SpanContext::new(parent_key, storage);

                let index = event_indexes
                    .traces
                    .get(&parent.trace_root())
                    .map(Vec::as_slice)
                    .unwrap_or_default();

                IndexedEventFilter::Single(index, Some(NonIndexedEventFilter::Parent(parent_key)))
            }
            BasicEventFilter::Content(value_filter) => {
                let filters = event_indexes
                    .contents
                    .make_indexed_filter(value_filter)
                    .into_iter()
                    .map(|(i, f)| {
                        IndexedEventFilter::Single(
                            i,
                            f.map(|f| NonIndexedEventFilter::Content(Box::new(f))),
                        )
                    })
                    .collect();

                IndexedEventFilter::Or(filters, true) // make_indexed_filter results are always distinct
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

                    IndexedEventFilter::Or(filters, true) // make_indexed_filter results are always distinct
                } else {
                    // we are creating indexes for all attributes, so if one
                    // doesn't exist, then there are no entities with that attribute
                    IndexedEventFilter::Single(&[], None)
                }
            }
            BasicEventFilter::Not(filter) => IndexedEventFilter::Not(
                &event_indexes.all,
                Box::new(IndexedEventFilter::build(
                    Some(*filter),
                    event_indexes,
                    storage,
                )),
            ),
            BasicEventFilter::And(filters) => IndexedEventFilter::And(
                filters
                    .into_iter()
                    .map(|f| IndexedEventFilter::build(Some(f), event_indexes, storage))
                    .collect(),
            ),
            BasicEventFilter::Or(filters) => IndexedEventFilter::Or(
                filters
                    .into_iter()
                    .map(|f| IndexedEventFilter::build(Some(f), event_indexes, storage))
                    .collect(),
                false,
            ),
        }
    }

    pub fn matches<S: Storage>(&self, event: &EventContext<'_, S>) -> bool {
        match self {
            IndexedEventFilter::Single(index, filter) => {
                let idx = index.lower_bound(&event.key());

                if index.get(idx).is_none_or(|e| *e != event.key()) {
                    return false;
                }

                if let Some(filter) = filter {
                    filter.matches(event)
                } else {
                    true
                }
            }
            IndexedEventFilter::Not(_, filter) => !filter.matches(event),
            IndexedEventFilter::And(indexed_filters) => {
                indexed_filters.iter().all(|f| f.matches(event))
            }
            IndexedEventFilter::Or(indexed_filters, _) => {
                indexed_filters.iter().any(|f| f.matches(event))
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
            IndexedEventFilter::Or(filters, _) => {
                // since OR filters can be completely disjoint, we can possibly
                // yield the sum of all filters
                filters.iter().map(Self::estimate_count).sum()
            }
        }
    }

    #[allow(unused)]
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
            IndexedEventFilter::Or(filters, _) => match filters.len() {
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

    pub fn with_timeframe(mut self, start: Timestamp, end: Timestamp) -> Self {
        self.trim_to_timeframe(start, end);
        self
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
            IndexedEventFilter::Or(filters, _) => filters
                .iter_mut()
                .for_each(|f| f.trim_to_timeframe(start, end)),
        }
    }

    pub fn with_pagination(mut self, previous: Option<Timestamp>, order: Order) -> Self {
        self.paginated(previous, order);
        self
    }

    pub fn paginated(&mut self, previous: Option<Timestamp>, order: Order) {
        let Some(previous) = previous else { return };

        match self {
            IndexedEventFilter::Single(index, _) | IndexedEventFilter::Not(index, _) => match order
            {
                Order::Asc => {
                    let idx = index.upper_bound(&previous);
                    *index = &index[idx..];
                }
                Order::Desc => {
                    let idx = index.lower_bound(&previous);
                    *index = &index[..idx];
                }
            },
            IndexedEventFilter::And(filters) | IndexedEventFilter::Or(filters, _) => filters
                .iter_mut()
                .for_each(|f| f.paginated(Some(previous), order)),
        }
    }

    pub fn with_optimization(mut self) -> Self {
        self.optimize();
        self
    }

    pub fn optimize(&mut self) {
        match self {
            IndexedEventFilter::Single(_, _) => { /* nothing to do */ }
            IndexedEventFilter::Not(_, _) => { /* nothing to do */ }
            IndexedEventFilter::And(filters) => filters.sort_by_key(Self::estimate_count),
            IndexedEventFilter::Or(filters, _) => filters.sort_by_key(Self::estimate_count),
        }
    }

    pub fn into_iterator<S: Storage>(self, storage: &'a S) -> CompoundIndexIterator<'a, EventKey> {
        match self {
            IndexedEventFilter::Single(index, Some(filter)) => {
                CompoundIndexIterator::Single(IndexIterator::new(
                    index,
                    Some(Box::new(move |key| {
                        filter.matches(&EventContext::new(*key, storage))
                    })),
                ))
            }
            IndexedEventFilter::Single(index, None) => {
                CompoundIndexIterator::Single(IndexIterator::new(index, None))
            }
            IndexedEventFilter::Not(index, filter) => {
                CompoundIndexIterator::Single(IndexIterator::new(
                    index,
                    Some(Box::new(move |key| {
                        !filter.matches(&EventContext::new(*key, storage))
                    })),
                ))
            }
            IndexedEventFilter::And(filters) => {
                CompoundIndexIterator::And(SetIntersectionIterator::new(
                    filters.into_iter().map(|f| Self::into_iterator(f, storage)),
                ))
            }
            IndexedEventFilter::Or(filters, distinct) => {
                CompoundIndexIterator::Or(SetUnionIterator::new(
                    filters.into_iter().map(|f| Self::into_iterator(f, storage)),
                    distinct,
                ))
            }
        }
    }
}

pub(crate) enum BasicEventFilter {
    All,
    Timestamp(ValueOperator, Timestamp),
    Level(SimpleLevel),
    Kind(SourceKind),
    Namespace(ValueStringComparison),
    Function(ValueStringComparison),
    File(FileFilter),
    Root,
    Trace(TraceRoot),
    Parent(SpanKey),
    Content(ValueFilter),
    Attribute(String, ValueFilter),
    Not(Box<BasicEventFilter>),
    And(Vec<BasicEventFilter>),
    Or(Vec<BasicEventFilter>),
}

impl BasicEventFilter {
    pub fn with_simplification(mut self) -> Self {
        self.simplify();
        self
    }

    pub fn simplify(&mut self) {
        match self {
            BasicEventFilter::All => {}
            BasicEventFilter::Timestamp(_, _) => {}
            BasicEventFilter::Level(_) => {}
            BasicEventFilter::Kind(_) => {}
            BasicEventFilter::Namespace(_) => {}
            BasicEventFilter::Function(_) => {}
            BasicEventFilter::File(_) => {}
            BasicEventFilter::Root => {}
            BasicEventFilter::Trace(_) => {}
            BasicEventFilter::Parent(_) => {}
            BasicEventFilter::Content(_) => {}
            BasicEventFilter::Attribute(_, _) => {}
            BasicEventFilter::Not(_) => {}
            BasicEventFilter::And(filters) => {
                for filter in &mut *filters {
                    filter.simplify()
                }

                // any "all" matches can be filtered out
                filters.retain(|filter| !matches!(filter, BasicEventFilter::All));

                match filters.len() {
                    0 => {
                        *self = BasicEventFilter::All;
                    }
                    1 => {
                        let mut filters = std::mem::take(filters);
                        let filter = filters.pop().unwrap();
                        *self = filter;
                    }
                    _ => {}
                }
            }
            BasicEventFilter::Or(filters) => {
                for filter in &mut *filters {
                    filter.simplify()
                }

                // any "all" matches can be hoisted up
                if filters
                    .iter()
                    .any(|filter| matches!(filter, BasicEventFilter::All))
                {
                    *self = BasicEventFilter::All;
                    return;
                }

                match filters.len() {
                    0 => {
                        // TODO: this may need to be some non-matching filter
                        // instead of an all-matching one
                        *self = BasicEventFilter::All;
                    }
                    1 => {
                        let mut filters = std::mem::take(filters);
                        let filter = filters.pop().unwrap();
                        *self = filter;
                    }
                    _ => {}
                }
            }
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
                "level" | "parent" | "namespace" | "target" | "function" | "file" | "trace"
                | "content" => Inherent,
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
            (Inherent, "namespace" | "target") => validate_value_predicate(
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
            (Inherent, "function") => validate_value_predicate(
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
            (Inherent, "content") => {
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

        Ok(FallibleFilterPredicate::Single(FilterPredicateSingle {
            property_kind: Some(property_kind),
            ..predicate
        }))
    }

    pub fn from_top_predicates(
        predicates: Vec<FilterPredicate>,
        span_key_map: &HashMap<FullSpanId, SpanKey>,
    ) -> Result<BasicEventFilter, InputError> {
        // top-level predicates are AND'd together
        Ok(BasicEventFilter::And(
            predicates
                .into_iter()
                .map(|p| BasicEventFilter::from_predicate(p, &span_key_map).unwrap())
                .collect(),
        ))
    }

    pub fn from_predicate(
        predicate: FilterPredicate,
        span_key_map: &HashMap<FullSpanId, SpanKey>,
    ) -> Result<BasicEventFilter, InputError> {
        use FilterPropertyKind::*;
        use ValueOperator::*;

        let predicate = match predicate {
            FilterPredicate::Single(single) => single,
            FilterPredicate::And(predicates) => {
                return predicates
                    .into_iter()
                    .map(|p| Self::from_predicate(p, span_key_map))
                    .collect::<Result<_, _>>()
                    .map(BasicEventFilter::And)
            }
            FilterPredicate::Or(predicates) => {
                return predicates
                    .into_iter()
                    .map(|p| Self::from_predicate(p, span_key_map))
                    .collect::<Result<_, _>>()
                    .map(BasicEventFilter::Or)
            }
        };

        let property_kind = predicate
            .property_kind
            .unwrap_or(match predicate.property.as_str() {
                "level" | "parent" | "namespace" | "target" | "file" | "trace" | "content" => {
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
                    "TRACE" => SimpleLevel::Trace,
                    "DEBUG" => SimpleLevel::Debug,
                    "INFO" => SimpleLevel::Info,
                    "WARN" => SimpleLevel::Warn,
                    "ERROR" => SimpleLevel::Error,
                    "FATAL" => SimpleLevel::Fatal,
                    _ => return Err(InputError::InvalidLevelValue),
                };

                let above = match op {
                    Eq => false,
                    Gte => true,
                    _ => return Err(InputError::InvalidLevelOperator),
                };

                if above {
                    if level == SimpleLevel::Trace {
                        BasicEventFilter::All
                    } else {
                        BasicEventFilter::Or(
                            level.iter_gte().map(BasicEventFilter::Level).collect(),
                        )
                    }
                } else {
                    BasicEventFilter::Level(level)
                }
            }
            (Inherent, "parent") => filterify_event_filter(
                predicate.value,
                |op, value| {
                    if op != ValueOperator::Eq {
                        return Err(InputError::InvalidParentOperator);
                    }

                    if value == "none" {
                        Ok(BasicEventFilter::Root)
                    } else {
                        let parent_id: FullSpanId =
                            value.parse().map_err(|_| InputError::InvalidParentValue)?;

                        let parent_key = span_key_map
                            .get(&parent_id)
                            .copied()
                            .unwrap_or(SpanKey::MIN);

                        Ok(BasicEventFilter::Parent(parent_key))
                    }
                },
                |_| Err(InputError::InvalidParentValue),
                |_| Err(InputError::InvalidParentValue),
            )?,
            (Inherent, "namespace") => filterify_event_filter(
                predicate.value,
                |op, value| {
                    let filter = ValueStringComparison::Compare(op, value);
                    Ok(BasicEventFilter::And(vec![
                        BasicEventFilter::Namespace(filter),
                        BasicEventFilter::Kind(SourceKind::Opentelemetry),
                    ]))
                },
                |wildcard| {
                    let wildcard = WildcardBuilder::from_owned(wildcard.into_bytes())
                        .without_one_metasymbol()
                        .build()
                        .map_err(|_| InputError::InvalidWildcardValue)?;

                    let filter = ValueStringComparison::Wildcard(wildcard);
                    Ok(BasicEventFilter::And(vec![
                        BasicEventFilter::Namespace(filter),
                        BasicEventFilter::Kind(SourceKind::Opentelemetry),
                    ]))
                },
                |regex| {
                    let regex = Regex::new(&regex).map_err(|_| InputError::InvalidWildcardValue)?;

                    let filter = ValueStringComparison::Regex(regex);
                    Ok(BasicEventFilter::And(vec![
                        BasicEventFilter::Namespace(filter),
                        BasicEventFilter::Kind(SourceKind::Opentelemetry),
                    ]))
                },
            )?,
            (Inherent, "target") => filterify_event_filter(
                predicate.value,
                |op, value| {
                    let filter = ValueStringComparison::Compare(op, value);
                    Ok(BasicEventFilter::And(vec![
                        BasicEventFilter::Namespace(filter),
                        BasicEventFilter::Kind(SourceKind::Tracing),
                    ]))
                },
                |wildcard| {
                    let wildcard = WildcardBuilder::from_owned(wildcard.into_bytes())
                        .without_one_metasymbol()
                        .build()
                        .map_err(|_| InputError::InvalidWildcardValue)?;

                    let filter = ValueStringComparison::Wildcard(wildcard);
                    Ok(BasicEventFilter::And(vec![
                        BasicEventFilter::Namespace(filter),
                        BasicEventFilter::Kind(SourceKind::Tracing),
                    ]))
                },
                |regex| {
                    let regex = Regex::new(&regex).map_err(|_| InputError::InvalidWildcardValue)?;

                    let filter = ValueStringComparison::Regex(regex);
                    Ok(BasicEventFilter::And(vec![
                        BasicEventFilter::Namespace(filter),
                        BasicEventFilter::Kind(SourceKind::Tracing),
                    ]))
                },
            )?,
            (Inherent, "function") => filterify_event_filter(
                predicate.value,
                |op, value| {
                    let filter = ValueStringComparison::Compare(op, value);
                    Ok(BasicEventFilter::Function(filter))
                },
                |wildcard| {
                    let wildcard = WildcardBuilder::from_owned(wildcard.into_bytes())
                        .without_one_metasymbol()
                        .build()
                        .map_err(|_| InputError::InvalidWildcardValue)?;

                    let filter = ValueStringComparison::Wildcard(wildcard);
                    Ok(BasicEventFilter::Function(filter))
                },
                |regex| {
                    let regex = Regex::new(&regex).map_err(|_| InputError::InvalidWildcardValue)?;

                    let filter = ValueStringComparison::Regex(regex);
                    Ok(BasicEventFilter::Function(filter))
                },
            )?,
            (Inherent, "file") => filterify_event_filter(
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

                    Ok(BasicEventFilter::File(filter))
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

                    Ok(BasicEventFilter::File(filter))
                },
                |regex| {
                    let regex = Regex::new(&regex).map_err(|_| InputError::InvalidWildcardValue)?;

                    Ok(BasicEventFilter::File(FileFilter {
                        name: ValueStringComparison::Regex(regex),
                        line: None,
                    }))
                },
            )?,
            (Inherent, "trace") => filterify_event_filter(
                predicate.value,
                |op, value| {
                    if op != ValueOperator::Eq {
                        return Err(InputError::InvalidTraceOperator);
                    }

                    let trace: TraceRoot =
                        value.parse().map_err(|_| InputError::InvalidTraceValue)?;

                    Ok(BasicEventFilter::Trace(trace))
                },
                |_| Err(InputError::InvalidTraceValue),
                |_| Err(InputError::InvalidTraceValue),
            )?,
            (Inherent, "content") => filterify_event_filter(
                predicate.value,
                |op, value| {
                    let value_filter = ValueFilter::from_input(op, &value);
                    Ok(BasicEventFilter::Content(value_filter))
                },
                |wildcard| {
                    let value_filter = ValueFilter::from_wildcard(wildcard)?;
                    Ok(BasicEventFilter::Content(value_filter))
                },
                |regex| {
                    let value_filter = ValueFilter::from_regex(regex)?;
                    Ok(BasicEventFilter::Content(value_filter))
                },
            )?,
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

    pub fn matches<S: Storage>(&self, context: &EventContext<'_, S>) -> bool {
        let event = context.event();
        match self {
            BasicEventFilter::All => true,
            BasicEventFilter::Timestamp(op, timestamp) => op.compare(&event.timestamp, timestamp),
            BasicEventFilter::Level(level) => event.level.into_simple_level() == *level,
            BasicEventFilter::Kind(kind) => kind == &event.kind,
            BasicEventFilter::Namespace(filter) => filter.matches_opt(event.namespace.as_deref()),
            BasicEventFilter::Function(filter) => filter.matches_opt(event.function.as_deref()),
            BasicEventFilter::File(filter) => {
                filter.matches(event.file_name.as_deref(), event.file_line)
            }
            BasicEventFilter::Root => event.parent_key.is_none(),
            BasicEventFilter::Trace(trace) => context.trace_root() == Some(*trace),
            BasicEventFilter::Parent(parent_key) => event.parent_key == Some(*parent_key),
            BasicEventFilter::Content(value_filter) => value_filter.matches(&event.content),
            BasicEventFilter::Attribute(attribute, value_filter) => context
                .attribute(attribute)
                .map(|v| value_filter.matches(v))
                .unwrap_or(false),
            BasicEventFilter::Not(inner_filter) => !inner_filter.matches(context),
            BasicEventFilter::And(filters) => filters.iter().all(|f| f.matches(context)),
            BasicEventFilter::Or(filters) => filters.iter().any(|f| f.matches(context)),
        }
    }

    pub fn into_indexed<'a, S: Storage>(
        self,
        event_indexes: &'a EventIndexes,
        storage: &S,
    ) -> IndexedEventFilter<'a> {
        IndexedEventFilter::build(Some(self), event_indexes, storage)
    }
}

pub(crate) enum NonIndexedEventFilter {
    Parent(SpanKey),
    Kind(SourceKind),
    Namespace(ValueStringComparison),
    Function(ValueStringComparison),
    File(FileFilter),
    Content(Box<ValueFilter>),
    Attribute(String, Box<ValueFilter>),
}

impl NonIndexedEventFilter {
    pub(crate) fn matches<S: Storage>(&self, context: &EventContext<'_, S>) -> bool {
        let event = context.event();
        match self {
            NonIndexedEventFilter::Parent(parent_key) => event.parent_key == Some(*parent_key),
            NonIndexedEventFilter::Kind(kind) => kind == &event.kind,
            NonIndexedEventFilter::Namespace(filter) => {
                filter.matches_opt(event.namespace.as_deref())
            }
            NonIndexedEventFilter::Function(filter) => {
                filter.matches_opt(event.function.as_deref())
            }
            NonIndexedEventFilter::File(filter) => {
                filter.matches(event.file_name.as_deref(), event.file_line)
            }
            NonIndexedEventFilter::Content(value_filter) => value_filter.matches(&event.content),
            NonIndexedEventFilter::Attribute(attribute, value_filter) => context
                .attribute(attribute)
                .map(|v| value_filter.matches(v))
                .unwrap_or(false),
        }
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

pub fn validate_event_filter(
    predicate: FilterPredicate,
) -> Result<FallibleFilterPredicate, InputError> {
    BasicEventFilter::validate(predicate)
}
