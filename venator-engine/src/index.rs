use std::collections::{BTreeMap, HashMap};
use std::ops::Range;

use ghost_cell::GhostToken;

use crate::filter::BoundSearch;
use crate::models::{Event, EventKey, Span, Timestamp, Value};
use crate::{Ancestors, InstanceKey, SpanKey};

mod attribute;
mod util;

pub(crate) use attribute::AttributeIndex;
pub(crate) use util::IndexExt;

pub struct EventIndexes {
    pub all: Vec<Timestamp>,
    pub levels: [Vec<Timestamp>; 5],
    pub instances: BTreeMap<InstanceKey, Vec<Timestamp>>,
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
            instances: BTreeMap::new(),
            targets: BTreeMap::new(),
            filenames: BTreeMap::new(),
            descendents: HashMap::new(),
            roots: Vec::new(),
            attributes: BTreeMap::new(),
        }
    }

    pub fn update_with_new_event<'b>(
        &mut self,
        token: &GhostToken<'b>,
        event: &Event,
        event_ancestors: &Ancestors<'b>,
    ) {
        let event_key = event.timestamp;

        let idx = self.all.upper_bound_via_expansion(&event_key);
        self.all.insert(idx, event_key);

        let level_index = &mut self.levels[event.level as usize];
        let idx = level_index.upper_bound_via_expansion(&event_key);
        level_index.insert(idx, event_key);

        let instance_index = self.instances.entry(event.instance_key).or_default();
        let idx = instance_index.upper_bound_via_expansion(&event_key);
        instance_index.insert(idx, event_key);

        let target_index = self.targets.entry(event.target.clone()).or_default();
        let idx = target_index.upper_bound_via_expansion(&event_key);
        target_index.insert(idx, event_key);

        if let Some(filename) = &event.file_name {
            let filename_index = self.filenames.entry(filename.clone()).or_default();
            let idx = filename_index.upper_bound_via_expansion(&event_key);
            filename_index.insert(idx, event_key);
        }

        for (parent_span_key, _) in &event_ancestors.0[0..event_ancestors.0.len() - 1] {
            let descendent_index = self.descendents.entry(*parent_span_key).or_default();
            let idx = descendent_index.upper_bound_via_expansion(&event_key);
            descendent_index.insert(idx, event_key);
        }

        if event.span_key.is_none() {
            let idx = self.roots.upper_bound_via_expansion(&event_key);
            self.roots.insert(idx, event_key);
        }

        for (attribute, attr_index) in &mut self.attributes {
            if let Some(value) = event_ancestors.get_value(attribute, token) {
                attr_index.add_entry(event_key, value);
            }
        }
    }

    pub fn update_with_new_field_on_parent<'b>(
        &mut self,
        token: &GhostToken<'b>,
        event_key: Timestamp,
        event_ancestors: &Ancestors<'b>,
        parent_key: Timestamp,
        parent_fields: &BTreeMap<String, Value>,
    ) {
        for (attribute, attribute_index) in &mut self.attributes {
            if let Some(new_value) = parent_fields.get(attribute) {
                if let Some((old_value, key)) = event_ancestors.get_value_and_key(attribute, token)
                {
                    if key <= parent_key && new_value != old_value {
                        attribute_index.remove_entry(event_key, old_value);
                        attribute_index.add_entry(event_key, new_value);
                    }
                } else {
                    // there was no old value, just insert
                    attribute_index.add_entry(event_key, new_value);
                }
            }
        }
    }

    pub fn remove_events(&mut self, events: &[EventKey]) {
        self.all.remove_list_sorted(events);

        for level_index in &mut self.levels {
            level_index.remove_list_sorted(events);
        }

        for instance_index in self.instances.values_mut() {
            instance_index.remove_list_sorted(events);
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

    pub fn remove_instances(&mut self, instances: &[InstanceKey]) {
        for instance_key in instances {
            self.instances.remove(instance_key);
        }
    }
}

pub struct SpanIndexes {
    pub all: Vec<Timestamp>,
    pub levels: [Vec<Timestamp>; 5],
    pub durations: SpanDurationIndex,
    pub instances: BTreeMap<InstanceKey, Vec<Timestamp>>,
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
            instances: BTreeMap::new(),
            names: BTreeMap::new(),
            targets: BTreeMap::new(),
            filenames: BTreeMap::new(),
            descendents: HashMap::new(),
            roots: Vec::new(),
            attributes: BTreeMap::new(),
        }
    }

    pub fn update_with_new_span<'b>(
        &mut self,
        token: &GhostToken<'b>,
        span: &Span,
        span_ancestors: &Ancestors<'b>,
    ) {
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

        let instance_index = self.instances.entry(span.instance_key).or_default();
        let idx = instance_index.upper_bound_via_expansion(&span_key);
        instance_index.insert(idx, span_key);

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
        for (parent_span_key, _) in &span_ancestors.0[0..span_ancestors.0.len() - 1] {
            let descendent_index = self.descendents.entry(*parent_span_key).or_default();
            let idx = descendent_index.upper_bound_via_expansion(&span_key);
            descendent_index.insert(idx, span_key);
        }

        if span.parent_key.is_none() {
            let idx = self.roots.upper_bound_via_expansion(&span_key);
            self.roots.insert(idx, span_key);
        }

        for (attribute, attr_index) in &mut self.attributes {
            if let Some(value) = span_ancestors.get_value(attribute, token) {
                attr_index.add_entry(span_key, value);
            }
        }
    }

    pub fn update_with_new_field_on_parent<'b>(
        &mut self,
        token: &GhostToken<'b>,
        span_key: Timestamp,
        span_ancestors: &Ancestors<'b>,
        parent_key: Timestamp,
        parent_fields: &BTreeMap<String, Value>,
    ) {
        for (attribute, attribute_index) in &mut self.attributes {
            if let Some(new_value) = parent_fields.get(attribute) {
                if let Some((old_value, key)) = span_ancestors.get_value_and_key(attribute, token) {
                    if key <= parent_key && new_value != old_value {
                        attribute_index.remove_entry(span_key, old_value);
                        attribute_index.add_entry(span_key, new_value);
                    }
                } else {
                    // there was no old value, just insert
                    attribute_index.add_entry(span_key, new_value);
                }
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

        for instance_index in self.instances.values_mut() {
            instance_index.remove_list_sorted(spans);
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

    pub fn remove_instances(&mut self, instances: &[InstanceKey]) {
        for instance_key in instances {
            self.instances.remove(instance_key);
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
