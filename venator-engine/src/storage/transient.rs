use std::collections::BTreeMap;
use std::sync::Arc;

use super::Storage;
use crate::index::{EventIndexes, SpanEventIndexes, SpanIndexes};
use crate::models::{EventKey, Value};
use crate::{Event, FullSpanId, Resource, Span, SpanEvent, SpanKey, Timestamp};

/// This storage implementation just holds elements in memory.
pub struct TransientStorage {
    resources: BTreeMap<Timestamp, Arc<Resource>>,
    spans: BTreeMap<Timestamp, Arc<Span>>,
    span_events: BTreeMap<Timestamp, Arc<SpanEvent>>,
    events: BTreeMap<Timestamp, Arc<Event>>,
}

impl TransientStorage {
    #[allow(clippy::new_without_default)]
    pub fn new() -> TransientStorage {
        TransientStorage {
            resources: BTreeMap::new(),
            spans: BTreeMap::new(),
            span_events: BTreeMap::new(),
            events: BTreeMap::new(),
        }
    }
}

impl Storage for TransientStorage {
    fn get_resource(&self, at: Timestamp) -> Option<Arc<Resource>> {
        self.resources.get(&at).cloned()
    }

    fn get_span(&self, at: Timestamp) -> Option<Arc<Span>> {
        self.spans.get(&at).cloned()
    }

    fn get_span_event(&self, at: Timestamp) -> Option<Arc<SpanEvent>> {
        self.span_events.get(&at).cloned()
    }

    fn get_event(&self, at: Timestamp) -> Option<Arc<Event>> {
        self.events.get(&at).cloned()
    }

    fn get_indexes(&self) -> Option<(SpanIndexes, SpanEventIndexes, EventIndexes)> {
        None
    }

    fn get_all_resources(&self) -> Box<dyn Iterator<Item = Arc<Resource>> + '_> {
        Box::new(self.resources.values().cloned())
    }

    fn get_all_spans(&self) -> Box<dyn Iterator<Item = Arc<Span>> + '_> {
        Box::new(self.spans.values().cloned())
    }

    fn get_all_span_events(&self) -> Box<dyn Iterator<Item = Arc<SpanEvent>> + '_> {
        Box::new(self.span_events.values().cloned())
    }

    fn get_all_events(&self) -> Box<dyn Iterator<Item = Arc<Event>> + '_> {
        Box::new(self.events.values().cloned())
    }

    fn insert_resource(&mut self, resource: Resource) {
        let at = resource.key();
        self.resources.insert(at, Arc::new(resource));
    }

    fn insert_span(&mut self, span: Span) {
        let at = span.created_at;
        self.spans.insert(at, Arc::new(span));
    }

    fn insert_span_event(&mut self, span_event: SpanEvent) {
        let at = span_event.timestamp;
        self.span_events.insert(at, Arc::new(span_event));
    }

    fn insert_event(&mut self, event: Event) {
        let at = event.timestamp;
        self.events.insert(at, Arc::new(event));
    }

    fn update_span_closed(&mut self, at: Timestamp, closed_at: Timestamp, busy: Option<u64>) {
        if let Some(span) = self.spans.get(&at) {
            let mut span = (**span).clone();
            span.closed_at = Some(closed_at);
            span.busy = busy;
            self.spans.insert(at, Arc::new(span));
        }
    }

    fn update_span_fields(&mut self, at: Timestamp, fields: BTreeMap<String, Value>) {
        if let Some(span) = self.spans.get(&at) {
            let mut span = (**span).clone();
            span.fields.extend(fields);
            self.spans.insert(at, Arc::new(span));
        }
    }

    fn update_span_link(
        &mut self,
        at: Timestamp,
        link: FullSpanId,
        fields: BTreeMap<String, Value>,
    ) {
        if let Some(span) = self.spans.get(&at) {
            let mut span = (**span).clone();
            span.links.push((link, fields));
            self.spans.insert(at, Arc::new(span));
        }
    }

    fn update_span_parents(&mut self, parent_key: SpanKey, spans: &[SpanKey]) {
        for span in spans {
            if let Some(span) = self.spans.get_mut(span) {
                let mut span = (**span).clone();
                span.parent_key = Some(parent_key);
                self.spans.insert(span.key(), Arc::new(span));
            }
        }
    }

    fn update_event_parents(&mut self, parent_key: SpanKey, events: &[EventKey]) {
        for event in events {
            if let Some(event) = self.events.get_mut(event) {
                let mut event = (**event).clone();
                event.parent_key = Some(parent_key);
                self.events.insert(event.key(), Arc::new(event));
            }
        }
    }

    fn update_indexes(
        &mut self,
        _span_indexes: &SpanIndexes,
        _span_event_indexes: &SpanEventIndexes,
        _event_indexes: &EventIndexes,
    ) {
        // do nothing
    }

    fn drop_resources(&mut self, resources: &[Timestamp]) {
        for at in resources {
            self.resources.remove(at);
        }
    }

    fn drop_spans(&mut self, spans: &[Timestamp]) {
        for at in spans {
            self.spans.remove(at);
        }
    }

    fn drop_span_events(&mut self, span_events: &[Timestamp]) {
        for at in span_events {
            self.span_events.remove(at);
        }
    }

    fn drop_events(&mut self, events: &[Timestamp]) {
        for at in events {
            self.events.remove(at);
        }
    }
}
