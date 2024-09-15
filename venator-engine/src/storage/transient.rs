use std::collections::BTreeMap;

use super::{Boo, Storage};
use crate::{Event, Instance, Span, SpanEvent, SpanKey, Timestamp};

/// This storage implementation just holds elements in memory.
pub struct TransientStorage {
    instances: BTreeMap<Timestamp, Instance>,
    spans: BTreeMap<Timestamp, Span>,
    span_events: BTreeMap<Timestamp, SpanEvent>,
    events: BTreeMap<Timestamp, Event>,
}

impl TransientStorage {
    #[allow(clippy::new_without_default)]
    pub fn new() -> TransientStorage {
        TransientStorage {
            instances: BTreeMap::new(),
            spans: BTreeMap::new(),
            span_events: BTreeMap::new(),
            events: BTreeMap::new(),
        }
    }
}

impl Storage for TransientStorage {
    fn get_instance(&self, at: Timestamp) -> Option<Boo<'_, Instance>> {
        self.instances.get(&at).map(Boo::Borrowed)
    }

    fn get_span(&self, at: Timestamp) -> Option<Boo<'_, Span>> {
        self.spans.get(&at).map(Boo::Borrowed)
    }

    fn get_span_event(&self, at: Timestamp) -> Option<Boo<'_, SpanEvent>> {
        self.span_events.get(&at).map(Boo::Borrowed)
    }

    fn get_event(&self, at: Timestamp) -> Option<Boo<'_, Event>> {
        self.events.get(&at).map(Boo::Borrowed)
    }

    fn get_all_instances(&self) -> Box<dyn Iterator<Item = Boo<'_, Instance>> + '_> {
        Box::new(self.instances.values().map(Boo::Borrowed))
    }

    fn get_all_spans(&self) -> Box<dyn Iterator<Item = Boo<'_, Span>> + '_> {
        Box::new(self.spans.values().map(Boo::Borrowed))
    }

    fn get_all_span_events(&self) -> Box<dyn Iterator<Item = Boo<'_, SpanEvent>> + '_> {
        Box::new(self.span_events.values().map(Boo::Borrowed))
    }

    fn get_all_events(&self) -> Box<dyn Iterator<Item = Boo<'_, Event>> + '_> {
        Box::new(self.events.values().map(Boo::Borrowed))
    }

    fn insert_instance(&mut self, instance: Instance) {
        let at = instance.key();
        self.instances.insert(at, instance);
    }

    fn insert_span(&mut self, span: Span) {
        let at = span.created_at;
        self.spans.insert(at, span);
    }

    fn insert_span_event(&mut self, span_event: SpanEvent) {
        let at = span_event.timestamp;
        self.span_events.insert(at, span_event);
    }

    fn insert_event(&mut self, event: Event) {
        let at = event.timestamp;
        self.events.insert(at, event);
    }

    fn update_instance_disconnected(&mut self, at: Timestamp, disconnected_at: Timestamp) {
        if let Some(instance) = self.instances.get_mut(&at) {
            instance.disconnected_at = Some(disconnected_at);
        }
    }

    fn update_span_closed(&mut self, at: Timestamp, closed_at: Timestamp) {
        if let Some(span) = self.spans.get_mut(&at) {
            span.closed_at = Some(closed_at);
        }
    }

    fn update_span_fields(&mut self, at: Timestamp, fields: BTreeMap<String, String>) {
        if let Some(span) = self.spans.get_mut(&at) {
            span.fields.extend(fields);
        }
    }

    fn update_span_follows(&mut self, at: Timestamp, follows: SpanKey) {
        if let Some(span) = self.spans.get_mut(&at) {
            span.follows.push(follows);
        }
    }
}
