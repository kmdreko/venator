use std::collections::BTreeMap;
use std::sync::Arc;

use super::Storage;
use crate::models::Value;
use crate::{Connection, Event, Span, SpanEvent, SpanKey, Timestamp};

/// This storage implementation just holds elements in memory.
pub struct TransientStorage {
    connections: BTreeMap<Timestamp, Arc<Connection>>,
    spans: BTreeMap<Timestamp, Arc<Span>>,
    span_events: BTreeMap<Timestamp, Arc<SpanEvent>>,
    events: BTreeMap<Timestamp, Arc<Event>>,
}

impl TransientStorage {
    #[allow(clippy::new_without_default)]
    pub fn new() -> TransientStorage {
        TransientStorage {
            connections: BTreeMap::new(),
            spans: BTreeMap::new(),
            span_events: BTreeMap::new(),
            events: BTreeMap::new(),
        }
    }
}

impl Storage for TransientStorage {
    fn get_connection(&self, at: Timestamp) -> Option<Arc<Connection>> {
        self.connections.get(&at).cloned()
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

    fn get_all_connections(&self) -> Box<dyn Iterator<Item = Arc<Connection>> + '_> {
        Box::new(self.connections.values().cloned())
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

    fn insert_connection(&mut self, connection: Connection) {
        let at = connection.key();
        self.connections.insert(at, Arc::new(connection));
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

    fn update_connection_disconnected(&mut self, at: Timestamp, disconnected_at: Timestamp) {
        if let Some(connection) = self.connections.get(&at) {
            let mut connection = (**connection).clone();
            connection.disconnected_at = Some(disconnected_at);
            self.connections.insert(at, Arc::new(connection));
        }
    }

    fn update_span_closed(&mut self, at: Timestamp, closed_at: Timestamp) {
        if let Some(span) = self.spans.get(&at) {
            let mut span = (**span).clone();
            span.closed_at = Some(closed_at);
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

    fn update_span_follows(&mut self, at: Timestamp, follows: SpanKey) {
        if let Some(span) = self.spans.get(&at) {
            let mut span = (**span).clone();
            span.follows.push(follows);
            self.spans.insert(at, Arc::new(span));
        }
    }

    fn drop_connections(&mut self, connections: &[Timestamp]) {
        for at in connections {
            self.connections.remove(at);
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
