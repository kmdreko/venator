use std::collections::{BTreeMap, HashMap};

use serde::{Deserialize, Serialize};

use crate::context::{EventContext, SpanContext};
use crate::models::{EventKey, FullSpanId, Timestamp, TraceRoot, Value};
use crate::util::{BoundSearch, IndexExt};
use crate::{ResourceKey, SpanKey, Storage};

use super::{LevelIndex, ValueIndex};

#[derive(Serialize, Deserialize)]
pub(crate) struct EventIndexes {
    pub all: Vec<Timestamp>,
    pub levels: LevelIndex,
    pub resources: BTreeMap<ResourceKey, Vec<Timestamp>>,
    pub namespaces: BTreeMap<String, Vec<Timestamp>>,
    pub functions: BTreeMap<String, Vec<Timestamp>>,
    pub filenames: BTreeMap<String, Vec<Timestamp>>,
    pub roots: Vec<Timestamp>,
    pub traces: HashMap<TraceRoot, Vec<Timestamp>>,
    pub contents: ValueIndex,
    pub attributes: BTreeMap<String, ValueIndex>,

    // events whose `parent_id` has not been seen yet
    pub orphanage: HashMap<FullSpanId, Vec<Timestamp>>,
}

impl EventIndexes {
    pub fn new() -> EventIndexes {
        EventIndexes {
            all: vec![],
            levels: LevelIndex::new(),
            resources: BTreeMap::new(),
            namespaces: BTreeMap::new(),
            functions: BTreeMap::new(),
            filenames: BTreeMap::new(),
            roots: Vec::new(),
            traces: HashMap::new(),
            contents: ValueIndex::new(),
            attributes: BTreeMap::new(),

            orphanage: HashMap::new(),
        }
    }

    pub fn update_with_new_event<S: Storage>(&mut self, context: &EventContext<'_, S>) {
        let event = context.event();
        let event_key = event.timestamp;

        let idx = self.all.upper_bound_via_expansion(&event_key);
        self.all.insert(idx, event_key);

        self.levels
            .add_entry(event.level.into_simple_level(), event_key);

        // TODO: do I need a per-resource index?
        let resource_index = self.resources.entry(event.resource_key).or_default();
        let idx = resource_index.upper_bound_via_expansion(&event_key);
        resource_index.insert(idx, event_key);

        if let Some(namespace) = event.namespace.clone() {
            let namespace_index = self.namespaces.entry(namespace).or_default();
            let idx = namespace_index.upper_bound_via_expansion(&event_key);
            namespace_index.insert(idx, event_key);
        }

        if let Some(function) = event.function.clone() {
            let function_index = self.functions.entry(function).or_default();
            let idx = function_index.upper_bound_via_expansion(&event_key);
            function_index.insert(idx, event_key);
        }

        if let Some(filename) = &event.file_name {
            let filename_index = self.filenames.entry(filename.clone()).or_default();
            let idx = filename_index.upper_bound_via_expansion(&event_key);
            filename_index.insert(idx, event_key);
        }

        if let Some(trace) = context.trace_root() {
            let trace_index = self.traces.entry(trace).or_default();
            let idx = trace_index.upper_bound_via_expansion(&event_key);
            trace_index.insert(idx, event_key);
        }

        if event.parent_id.is_none() {
            let idx = self.roots.upper_bound_via_expansion(&event_key);
            self.roots.insert(idx, event_key);
        }

        if let (Some(parent_id), None) = (event.parent_id, event.parent_key) {
            let orphan_index = self.orphanage.entry(parent_id).or_default();
            let idx = orphan_index.upper_bound_via_expansion(&event_key);
            orphan_index.insert(idx, event_key);
        }

        self.contents.add_entry(event_key, &event.content);

        for (attribute, value) in context.attributes() {
            let index = self
                .attributes
                .entry(attribute.to_owned())
                .or_insert_with(ValueIndex::new);

            index.add_entry(event_key, value);
        }
    }

    pub fn update_with_new_span<S: Storage>(
        &mut self,
        context: &SpanContext<'_, S>,
    ) -> Vec<EventKey> {
        self.orphanage
            .remove(&context.span().id)
            .unwrap_or_default()
    }

    pub fn update_with_new_field_on_parent<S: Storage>(
        &mut self,
        context: &EventContext<'_, S>,
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

    pub fn remove_events(&mut self, events: &[EventKey]) {
        self.all.remove_list_sorted(events);

        self.levels.remove_entries(events);

        for resource_index in self.resources.values_mut() {
            resource_index.remove_list_sorted(events);
        }

        for namespace_index in self.namespaces.values_mut() {
            namespace_index.remove_list_sorted(events);
        }

        for function_index in self.functions.values_mut() {
            function_index.remove_list_sorted(events);
        }

        for filename_index in self.filenames.values_mut() {
            filename_index.remove_list_sorted(events);
        }
        self.roots.remove_list_sorted(events);

        self.contents.remove_entries(events);

        for attribute_index in self.attributes.values_mut() {
            attribute_index.remove_entries(events);
        }
    }

    pub fn remove_spans(&mut self, _spans: &[SpanKey]) {}
}
