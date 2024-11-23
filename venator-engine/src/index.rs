use std::collections::{BTreeMap, HashMap};
use std::ops::Range;

use crate::filter::BoundSearch;
use crate::models::{EventKey, Timestamp, Value};
use crate::{ConnectionKey, EventContext, SpanContext, SpanKey, Storage};

mod attribute;
mod util;

pub(crate) use attribute::AttributeIndex;
pub(crate) use util::IndexExt;

pub struct EventIndexes {
    pub all: Vec<Timestamp>,
    pub levels: [Vec<Timestamp>; 5],
    pub connections: BTreeMap<ConnectionKey, Vec<Timestamp>>,
    pub targets: BTreeMap<String, Vec<Timestamp>>,
    pub filenames: BTreeMap<String, Vec<Timestamp>>,
    pub descendents: HashMap<Timestamp, Vec<Timestamp>>,
    pub roots: Vec<Timestamp>,
    pub attributes: BTreeMap<String, AttributeIndex>,
}

impl EventIndexes {
    pub fn new() -> EventIndexes {
        EventIndexes {
            all: vec![],
            levels: [Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new()],
            connections: BTreeMap::new(),
            targets: BTreeMap::new(),
            filenames: BTreeMap::new(),
            descendents: HashMap::new(),
            roots: Vec::new(),
            attributes: BTreeMap::new(),
        }
    }

    pub fn update_with_new_event<S: Storage>(&mut self, context: &EventContext<'_, S>) {
        let event = context.event();
        let event_key = event.timestamp;

        let idx = self.all.upper_bound_via_expansion(&event_key);
        self.all.insert(idx, event_key);

        let level_index = &mut self.levels[event.level as usize];
        let idx = level_index.upper_bound_via_expansion(&event_key);
        level_index.insert(idx, event_key);

        let connection_index = self.connections.entry(event.connection_key).or_default();
        let idx = connection_index.upper_bound_via_expansion(&event_key);
        connection_index.insert(idx, event_key);

        let target_index = self.targets.entry(event.target.clone()).or_default();
        let idx = target_index.upper_bound_via_expansion(&event_key);
        target_index.insert(idx, event_key);

        if let Some(filename) = &event.file_name {
            let filename_index = self.filenames.entry(filename.clone()).or_default();
            let idx = filename_index.upper_bound_via_expansion(&event_key);
            filename_index.insert(idx, event_key);
        }

        for parent in context.parents() {
            let descendent_index = self.descendents.entry(parent.key()).or_default();
            let idx = descendent_index.upper_bound_via_expansion(&event_key);
            descendent_index.insert(idx, event_key);
        }

        if event.span_key.is_none() {
            let idx = self.roots.upper_bound_via_expansion(&event_key);
            self.roots.insert(idx, event_key);
        }

        for (attribute, value) in context.attributes() {
            let index = self
                .attributes
                .entry(attribute.to_owned())
                .or_insert_with(AttributeIndex::new);

            index.add_entry(event_key, value);
        }
    }

    pub fn update_with_new_field_on_parent<S: Storage>(
        &mut self,
        context: &EventContext<'_, S>,
        parent_key: Timestamp,
        parent_fields: &BTreeMap<String, Value>,
    ) {
        for (attribute, new_value) in parent_fields {
            let attribute_index = self
                .attributes
                .entry(attribute.to_owned())
                .or_insert_with(AttributeIndex::new);

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

        for level_index in &mut self.levels {
            level_index.remove_list_sorted(events);
        }

        for connection_index in self.connections.values_mut() {
            connection_index.remove_list_sorted(events);
        }

        for target_index in self.targets.values_mut() {
            target_index.remove_list_sorted(events);
        }

        for filename_index in self.filenames.values_mut() {
            filename_index.remove_list_sorted(events);
        }

        for descendent_index in self.descendents.values_mut() {
            descendent_index.remove_list_sorted(events);
        }

        self.roots.remove_list_sorted(events);

        for attribute_index in self.attributes.values_mut() {
            attribute_index.remove_entries(events);
        }
    }

    pub fn remove_spans(&mut self, spans: &[SpanKey]) {
        for span_key in spans {
            self.descendents.remove(span_key);
        }
    }

    pub fn remove_connections(&mut self, connections: &[ConnectionKey]) {
        for connection_key in connections {
            self.connections.remove(connection_key);
        }
    }
}

pub struct SpanIndexes {
    pub all: Vec<Timestamp>,
    pub levels: [Vec<Timestamp>; 5],
    pub durations: SpanDurationIndex,
    pub connections: BTreeMap<ConnectionKey, Vec<Timestamp>>,
    pub names: BTreeMap<String, Vec<Timestamp>>,
    pub targets: BTreeMap<String, Vec<Timestamp>>,
    pub filenames: BTreeMap<String, Vec<Timestamp>>,
    pub descendents: HashMap<Timestamp, Vec<Timestamp>>,
    pub roots: Vec<Timestamp>,
    pub attributes: BTreeMap<String, AttributeIndex>,
}

impl SpanIndexes {
    pub fn new() -> SpanIndexes {
        SpanIndexes {
            all: vec![],
            levels: [Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new()],
            durations: SpanDurationIndex::new(),
            connections: BTreeMap::new(),
            names: BTreeMap::new(),
            targets: BTreeMap::new(),
            filenames: BTreeMap::new(),
            descendents: HashMap::new(),
            roots: Vec::new(),
            attributes: BTreeMap::new(),
        }
    }

    pub fn update_with_new_span<S: Storage>(&mut self, context: &SpanContext<'_, S>) {
        let span = context.span();
        let span_key = span.created_at;

        let idx = self.all.upper_bound_via_expansion(&span_key);
        self.all.insert(idx, span_key);

        let level_index = &mut self.levels[span.level as usize];
        let idx = level_index.upper_bound_via_expansion(&span_key);
        level_index.insert(idx, span_key);

        let duration_index = match span.duration() {
            None => &mut self.durations.open,
            Some(0..=3999) => &mut self.durations.closed_4_ms,
            Some(4000..=15999) => &mut self.durations.closed_16_ms,
            Some(16000..=63999) => &mut self.durations.closed_64_ms,
            Some(64000..=255999) => &mut self.durations.closed_256_ms,
            Some(256000..=999999) => &mut self.durations.closed_1_s,
            Some(1000000..=3999999) => &mut self.durations.closed_4_s,
            Some(4000000..=15999999) => &mut self.durations.closed_16_s,
            Some(16000000..=63999999) => &mut self.durations.closed_64_s,
            Some(64000000..) => &mut self.durations.closed_long,
        };
        let idx = duration_index.upper_bound_via_expansion(&span_key);
        duration_index.insert(idx, span_key);

        let connection_index = self.connections.entry(span.connection_key).or_default();
        let idx = connection_index.upper_bound_via_expansion(&span_key);
        connection_index.insert(idx, span_key);

        let name_index = self.names.entry(span.name.clone()).or_default();
        let idx = name_index.upper_bound_via_expansion(&span_key);
        name_index.insert(idx, span_key);

        let target_index = self.targets.entry(span.target.clone()).or_default();
        let idx = target_index.upper_bound_via_expansion(&span_key);
        target_index.insert(idx, span_key);

        if let Some(filename) = &span.file_name {
            let filename_index = self.filenames.entry(filename.clone()).or_default();
            let idx = filename_index.upper_bound_via_expansion(&span_key);
            filename_index.insert(idx, span_key);
        }

        self.descendents.insert(span_key, vec![span_key]);
        for parent in context.parents() {
            let descendent_index = self.descendents.entry(parent.key()).or_default();
            let idx = descendent_index.upper_bound_via_expansion(&span_key);
            descendent_index.insert(idx, span_key);
        }

        if span.parent_key.is_none() {
            let idx = self.roots.upper_bound_via_expansion(&span_key);
            self.roots.insert(idx, span_key);
        }

        for (attribute, value) in context.attributes() {
            let index = self
                .attributes
                .entry(attribute.to_owned())
                .or_insert_with(AttributeIndex::new);

            index.add_entry(span_key, value);
        }
    }

    pub fn update_with_new_field_on_parent<S: Storage>(
        &mut self,
        context: &SpanContext<'_, S>,
        parent_key: Timestamp,
        parent_fields: &BTreeMap<String, Value>,
    ) {
        for (attribute, new_value) in parent_fields {
            let attribute_index = self
                .attributes
                .entry(attribute.to_owned())
                .or_insert_with(AttributeIndex::new);

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

    pub fn update_with_closed(&mut self, span_key: Timestamp, closed_at: Timestamp) {
        let idx = self.durations.open.lower_bound(&span_key);
        if self.durations.open[idx] == span_key {
            self.durations.open.remove(idx);
        }

        let duration = closed_at.get().saturating_sub(span_key.get());
        let index = match duration {
            0..=3999 => &mut self.durations.closed_4_ms,
            4000..=15999 => &mut self.durations.closed_16_ms,
            16000..=63999 => &mut self.durations.closed_64_ms,
            64000..=255999 => &mut self.durations.closed_256_ms,
            256000..=999999 => &mut self.durations.closed_1_s,
            1000000..=3999999 => &mut self.durations.closed_4_s,
            4000000..=15999999 => &mut self.durations.closed_16_s,
            16000000..=63999999 => &mut self.durations.closed_64_s,
            64000000.. => &mut self.durations.closed_long,
        };

        let idx = index.upper_bound_via_expansion(&span_key);
        index.insert(idx, span_key);
    }

    pub fn remove_spans(&mut self, spans: &[SpanKey]) {
        self.all.remove_list_sorted(spans);

        for level_index in &mut self.levels {
            level_index.remove_list_sorted(spans);
        }

        self.durations.remove_spans(spans);

        for connection_index in self.connections.values_mut() {
            connection_index.remove_list_sorted(spans);
        }

        for name_index in self.names.values_mut() {
            name_index.remove_list_sorted(spans);
        }

        for target_index in self.targets.values_mut() {
            target_index.remove_list_sorted(spans);
        }

        for filename_index in self.filenames.values_mut() {
            filename_index.remove_list_sorted(spans);
        }

        for descendent_index in self.descendents.values_mut() {
            descendent_index.remove_list_sorted(spans);
        }

        self.roots.remove_list_sorted(spans);

        for attribute_index in self.attributes.values_mut() {
            attribute_index.remove_entries(spans);
        }
    }

    pub fn remove_connections(&mut self, connections: &[ConnectionKey]) {
        for connection_key in connections {
            self.connections.remove(connection_key);
        }
    }
}

pub struct SpanDurationIndex {
    closed_4_ms: Vec<Timestamp>,   // span ids with durations shorter than 4ms
    closed_16_ms: Vec<Timestamp>,  // span ids with durations between [4ms and 16ms)
    closed_64_ms: Vec<Timestamp>,  // span ids with durations between [16ms and 64ms)
    closed_256_ms: Vec<Timestamp>, // span ids with durations between [64ms and 256ms)
    closed_1_s: Vec<Timestamp>,    // span ids with durations between [256ms and 1s)
    closed_4_s: Vec<Timestamp>,    // span ids with durations between [1s and 4s)
    closed_16_s: Vec<Timestamp>,   // span ids with durations between [4s and 16s)
    closed_64_s: Vec<Timestamp>,   // span ids with durations between [16s and 64s)
    closed_long: Vec<Timestamp>,   // span ids with durations 64s and longer
    pub open: Vec<Timestamp>,      // span ids that haven't finished yet
}

impl SpanDurationIndex {
    pub fn new() -> SpanDurationIndex {
        SpanDurationIndex {
            closed_4_ms: vec![],
            closed_16_ms: vec![],
            closed_64_ms: vec![],
            closed_256_ms: vec![],
            closed_1_s: vec![],
            closed_4_s: vec![],
            closed_16_s: vec![],
            closed_64_s: vec![],
            closed_long: vec![],
            open: vec![],
        }
    }

    pub fn to_stratified_indexes(&self) -> Vec<(&'_ [Timestamp], Range<u64>)> {
        vec![
            (&self.closed_4_ms, 0..4000),
            (&self.closed_16_ms, 4000..16000),
            (&self.closed_64_ms, 16000..64000),
            (&self.closed_256_ms, 64000..256000),
            (&self.closed_1_s, 256000..1000000),
            (&self.closed_4_s, 1000000..4000000),
            (&self.closed_16_s, 4000000..16000000),
            (&self.closed_64_s, 16000000..64000000),
            (&self.closed_long, 64000000..u64::MAX),
            (&self.open, 0..u64::MAX),
        ]
    }

    pub fn remove_spans(&mut self, spans: &[SpanKey]) {
        self.closed_4_ms.remove_list_sorted(spans);
        self.closed_16_ms.remove_list_sorted(spans);
        self.closed_64_ms.remove_list_sorted(spans);
        self.closed_256_ms.remove_list_sorted(spans);
        self.closed_1_s.remove_list_sorted(spans);
        self.closed_4_s.remove_list_sorted(spans);
        self.closed_16_s.remove_list_sorted(spans);
        self.closed_64_s.remove_list_sorted(spans);
        self.closed_long.remove_list_sorted(spans);
        self.open.remove_list_sorted(spans);
    }
}
