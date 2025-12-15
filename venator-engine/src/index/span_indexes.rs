use std::collections::{BTreeMap, HashMap};
use std::ops::Range;

use serde::{Deserialize, Serialize};

use crate::context::SpanContext;
use crate::models::{FullSpanId, Timestamp, TraceRoot, Value};
use crate::util::{BoundSearch, IndexExt};
use crate::{InstanceId, ResourceKey, SpanKey, Storage};

use super::{LevelIndex, ValueIndex};

#[derive(Serialize, Deserialize)]
pub(crate) struct SpanIndexes {
    pub all: Vec<Timestamp>,
    pub ids: HashMap<FullSpanId, SpanKey>,
    pub levels: LevelIndex,
    pub durations: SpanDurationIndex,
    pub instances: BTreeMap<InstanceId, Vec<Timestamp>>,
    pub resources: BTreeMap<ResourceKey, Vec<Timestamp>>,
    pub names: BTreeMap<String, Vec<Timestamp>>,
    pub functions: BTreeMap<String, Vec<Timestamp>>,
    pub namespaces: BTreeMap<String, Vec<Timestamp>>,
    pub filenames: BTreeMap<String, Vec<Timestamp>>,
    pub roots: Vec<Timestamp>,
    pub traces: HashMap<TraceRoot, Vec<Timestamp>>,
    pub attributes: BTreeMap<String, ValueIndex>,

    // spans whose `parent_id` has not been seen yet
    pub orphanage: HashMap<FullSpanId, Vec<Timestamp>>,
}

impl SpanIndexes {
    pub fn new() -> SpanIndexes {
        SpanIndexes {
            all: vec![],
            levels: LevelIndex::new(),
            durations: SpanDurationIndex::new(),
            instances: BTreeMap::new(),
            resources: BTreeMap::new(),
            names: BTreeMap::new(),
            functions: BTreeMap::new(),
            namespaces: BTreeMap::new(),
            filenames: BTreeMap::new(),
            roots: Vec::new(),
            traces: HashMap::new(),
            attributes: BTreeMap::new(),
            ids: HashMap::new(),
            orphanage: HashMap::new(),
        }
    }

    pub fn update_with_new_span<S: Storage>(
        &mut self,
        context: &SpanContext<'_, S>,
    ) -> Vec<SpanKey> {
        let span = context.span();
        let span_key = span.created_at;

        let idx = self.all.upper_bound_via_expansion(&span_key);
        self.all.insert(idx, span_key);

        self.ids.insert(span.id, span_key);

        self.levels
            .add_entry(span.level.into_simple_level(), span_key);

        let (duration_index, closed_at_index) = match span.duration() {
            None => (&mut self.durations.open, None),
            Some(0..=3999) => (
                &mut self.durations.closed_4_ms,
                Some(&mut self.durations.closed_4_ms_at),
            ),
            Some(4000..=15999) => (
                &mut self.durations.closed_16_ms,
                Some(&mut self.durations.closed_16_ms_at),
            ),
            Some(16000..=63999) => (
                &mut self.durations.closed_64_ms,
                Some(&mut self.durations.closed_64_ms_at),
            ),
            Some(64000..=255999) => (
                &mut self.durations.closed_256_ms,
                Some(&mut self.durations.closed_256_ms_at),
            ),
            Some(256000..=999999) => (
                &mut self.durations.closed_1_s,
                Some(&mut self.durations.closed_1_s_at),
            ),
            Some(1000000..=3999999) => (
                &mut self.durations.closed_4_s,
                Some(&mut self.durations.closed_4_s_at),
            ),
            Some(4000000..=15999999) => (
                &mut self.durations.closed_16_s,
                Some(&mut self.durations.closed_16_s_at),
            ),
            Some(16000000..=63999999) => (
                &mut self.durations.closed_64_s,
                Some(&mut self.durations.closed_64_s_at),
            ),
            Some(64000000..) => (
                &mut self.durations.closed_long,
                Some(&mut self.durations.closed_long_at),
            ),
        };
        let idx = duration_index.upper_bound_via_expansion(&span_key);
        duration_index.insert(idx, span_key);
        if let Some(closed_at_index) = closed_at_index {
            closed_at_index.insert(idx, span.closed_at.unwrap());
        }

        if let FullSpanId::Tracing(instance_id, _) = span.id {
            let instance_index = self.instances.entry(instance_id).or_default();
            let idx = instance_index.upper_bound_via_expansion(&span_key);
            instance_index.insert(idx, span_key);
        }

        // TODO: do I need a per-resource index?
        let resource_index = self.resources.entry(span.resource_key).or_default();
        let idx = resource_index.upper_bound_via_expansion(&span_key);
        resource_index.insert(idx, span_key);

        let name_index = self.names.entry(span.name.clone()).or_default();
        let idx = name_index.upper_bound_via_expansion(&span_key);
        name_index.insert(idx, span_key);

        if let Some(function) = span.function.clone() {
            let function_index = self.functions.entry(function).or_default();
            let idx = function_index.upper_bound_via_expansion(&span_key);
            function_index.insert(idx, span_key);
        }

        if let Some(namespace) = span.namespace.clone() {
            let namespace_index = self.namespaces.entry(namespace).or_default();
            let idx = namespace_index.upper_bound_via_expansion(&span_key);
            namespace_index.insert(idx, span_key);
        }

        if let Some(filename) = &span.file_name {
            let filename_index = self.filenames.entry(filename.clone()).or_default();
            let idx = filename_index.upper_bound_via_expansion(&span_key);
            filename_index.insert(idx, span_key);
        }

        let trace_index = self.traces.entry(context.trace_root()).or_default();
        let idx = trace_index.upper_bound_via_expansion(&span_key);
        trace_index.insert(idx, span_key);

        if span.parent_id.is_none() {
            let idx = self.roots.upper_bound_via_expansion(&span_key);
            self.roots.insert(idx, span_key);
        }

        if let (Some(parent_id), None) = (span.parent_id, span.parent_key) {
            let orphan_index = self.orphanage.entry(parent_id).or_default();
            let idx = orphan_index.upper_bound_via_expansion(&span_key);
            orphan_index.insert(idx, span_key);
        }

        for (attribute, value) in context.attributes() {
            let index = self
                .attributes
                .entry(attribute.to_owned())
                .or_insert_with(ValueIndex::new);

            index.add_entry(span_key, value);
        }

        self.orphanage.remove(&span.id).unwrap_or_default()
    }

    pub fn update_with_new_field_on_parent<S: Storage>(
        &mut self,
        context: &SpanContext<'_, S>,
        parent_key: Timestamp,
        parent_attributes: &BTreeMap<String, Value>,
    ) {
        for (attribute, new_value) in parent_attributes {
            let attribute_index = self
                .attributes
                .entry(attribute.to_owned())
                .or_insert_with(ValueIndex::new);

            if let Some((old_value, key)) = context.attribute_with_key(attribute) {
                if key <= parent_key && new_value != old_value {
                    attribute_index.remove_entry(context.key(), old_value);
                    attribute_index.add_entry(context.key(), new_value);
                }
            } else {
                // there was no old value, just insert
                attribute_index.add_entry(context.key(), new_value);
            }
        }
    }

    pub fn update_with_closed(&mut self, span_key: Timestamp, closed_at: Timestamp) -> bool {
        let idx = self.durations.open.lower_bound(&span_key);
        if self.durations.open.get(idx) == Some(&span_key) {
            self.durations.open.remove(idx);
        } else {
            return false;
        }

        let duration = closed_at.get().saturating_sub(span_key.get());
        let (index, closed_at_index) = match duration {
            0..=3999 => (
                &mut self.durations.closed_4_ms,
                Some(&mut self.durations.closed_4_ms_at),
            ),
            4000..=15999 => (
                &mut self.durations.closed_16_ms,
                Some(&mut self.durations.closed_16_ms_at),
            ),
            16000..=63999 => (
                &mut self.durations.closed_64_ms,
                Some(&mut self.durations.closed_64_ms_at),
            ),
            64000..=255999 => (
                &mut self.durations.closed_256_ms,
                Some(&mut self.durations.closed_256_ms_at),
            ),
            256000..=999999 => (
                &mut self.durations.closed_1_s,
                Some(&mut self.durations.closed_1_s_at),
            ),
            1000000..=3999999 => (
                &mut self.durations.closed_4_s,
                Some(&mut self.durations.closed_4_s_at),
            ),
            4000000..=15999999 => (
                &mut self.durations.closed_16_s,
                Some(&mut self.durations.closed_16_s_at),
            ),
            16000000..=63999999 => (
                &mut self.durations.closed_64_s,
                Some(&mut self.durations.closed_64_s_at),
            ),
            64000000.. => (&mut self.durations.closed_long, None),
        };

        let idx = index.upper_bound_via_expansion(&span_key);
        index.insert(idx, span_key);
        if let Some(closed_at_index) = closed_at_index {
            closed_at_index.insert(idx, closed_at);
        }
        true
    }

    pub fn remove_spans(&mut self, spans: &[SpanKey]) {
        self.all.remove_list_sorted(spans);

        self.ids.retain(|_, key| !spans.contains(key));

        self.levels.remove_entries(spans);

        self.durations.remove_spans(spans);

        for resource_index in self.resources.values_mut() {
            resource_index.remove_list_sorted(spans);
        }

        for name_index in self.functions.values_mut() {
            name_index.remove_list_sorted(spans);
        }

        for namespace_index in self.namespaces.values_mut() {
            namespace_index.remove_list_sorted(spans);
        }

        for function_index in self.functions.values_mut() {
            function_index.remove_list_sorted(spans);
        }

        for filename_index in self.filenames.values_mut() {
            filename_index.remove_list_sorted(spans);
        }

        self.roots.remove_list_sorted(spans);

        for attribute_index in self.attributes.values_mut() {
            attribute_index.remove_entries(spans);
        }
    }
}

#[rustfmt::skip]
#[derive(Serialize, Deserialize)]
pub struct SpanDurationIndex {
    closed_4_ms: Vec<Timestamp>,      // span ids with durations shorter than 4ms
    closed_4_ms_at: Vec<Timestamp>,
    closed_16_ms: Vec<Timestamp>,     // span ids with durations between [4ms and 16ms)
    closed_16_ms_at: Vec<Timestamp>,
    closed_64_ms: Vec<Timestamp>,     // span ids with durations between [16ms and 64ms)
    closed_64_ms_at: Vec<Timestamp>,
    closed_256_ms: Vec<Timestamp>,    // span ids with durations between [64ms and 256ms)
    closed_256_ms_at: Vec<Timestamp>,
    closed_1_s: Vec<Timestamp>,       // span ids with durations between [256ms and 1s)
    closed_1_s_at: Vec<Timestamp>,
    closed_4_s: Vec<Timestamp>,       // span ids with durations between [1s and 4s)
    closed_4_s_at: Vec<Timestamp>,
    closed_16_s: Vec<Timestamp>,      // span ids with durations between [4s and 16s)
    closed_16_s_at: Vec<Timestamp>,
    closed_64_s: Vec<Timestamp>,      // span ids with durations between [16s and 64s)
    closed_64_s_at: Vec<Timestamp>,
    closed_long: Vec<Timestamp>,      // span ids with durations 64s and longer
    closed_long_at: Vec<Timestamp>,
    pub open: Vec<Timestamp>,         // span ids that haven't finished yet
}

impl SpanDurationIndex {
    pub fn new() -> SpanDurationIndex {
        SpanDurationIndex {
            closed_4_ms: vec![],
            closed_4_ms_at: vec![],
            closed_16_ms: vec![],
            closed_16_ms_at: vec![],
            closed_64_ms: vec![],
            closed_64_ms_at: vec![],
            closed_256_ms: vec![],
            closed_256_ms_at: vec![],
            closed_1_s: vec![],
            closed_1_s_at: vec![],
            closed_4_s: vec![],
            closed_4_s_at: vec![],
            closed_16_s: vec![],
            closed_16_s_at: vec![],
            closed_64_s: vec![],
            closed_64_s_at: vec![],
            closed_long: vec![],
            closed_long_at: vec![],
            open: vec![],
        }
    }

    pub fn to_stratified_indexes(
        &self,
    ) -> Vec<(&'_ [Timestamp], Option<(&'_ [Timestamp], Range<u64>)>)> {
        vec![
            (&self.closed_4_ms, Some((&self.closed_4_ms_at, 0..4000))),
            (
                &self.closed_16_ms,
                Some((&self.closed_16_ms_at, 4000..16000)),
            ),
            (
                &self.closed_64_ms,
                Some((&self.closed_64_ms_at, 16000..64000)),
            ),
            (
                &self.closed_256_ms,
                Some((&self.closed_256_ms_at, 64000..256000)),
            ),
            (
                &self.closed_1_s,
                Some((&self.closed_1_s_at, 256000..1000000)),
            ),
            (
                &self.closed_4_s,
                Some((&self.closed_4_s_at, 1000000..4000000)),
            ),
            (
                &self.closed_16_s,
                Some((&self.closed_16_s_at, 4000000..16000000)),
            ),
            (
                &self.closed_64_s,
                Some((&self.closed_64_s_at, 16000000..64000000)),
            ),
            (
                &self.closed_long,
                Some((&self.closed_long_at, 64000000..u64::MAX)),
            ),
            (&self.open, None),
        ]
    }

    pub fn remove_spans(&mut self, spans: &[SpanKey]) {
        self.closed_4_ms
            .remove_list_sorted_with_tagalong(spans, &mut self.closed_4_ms_at);
        self.closed_16_ms
            .remove_list_sorted_with_tagalong(spans, &mut self.closed_16_ms_at);
        self.closed_64_ms
            .remove_list_sorted_with_tagalong(spans, &mut self.closed_64_ms_at);
        self.closed_256_ms
            .remove_list_sorted_with_tagalong(spans, &mut self.closed_256_ms_at);
        self.closed_1_s
            .remove_list_sorted_with_tagalong(spans, &mut self.closed_1_s_at);
        self.closed_4_s
            .remove_list_sorted_with_tagalong(spans, &mut self.closed_4_s_at);
        self.closed_16_s
            .remove_list_sorted_with_tagalong(spans, &mut self.closed_16_s_at);
        self.closed_64_s
            .remove_list_sorted_with_tagalong(spans, &mut self.closed_64_s_at);
        self.closed_long
            .remove_list_sorted_with_tagalong(spans, &mut self.closed_long_at);
        self.open.remove_list_sorted(spans);
    }
}
