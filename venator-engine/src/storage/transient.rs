use std::collections::{BTreeMap, BTreeSet};

use super::{Boo, Storage};
use crate::models::Value;
use crate::{Connection, Event, Span, SpanEvent, SpanKey, Timestamp};

/// This storage implementation just holds elements in memory.
pub struct TransientStorage {
    connections: BTreeMap<Timestamp, Connection>,
    spans: BTreeMap<Timestamp, Span>,
    span_events: BTreeMap<Timestamp, SpanEvent>,
    events: BTreeMap<Timestamp, Event>,
    indexes: BTreeSet<String>,
}

impl TransientStorage {
    #[allow(clippy::new_without_default)]
    pub fn new() -> TransientStorage {
        TransientStorage {
            connections: BTreeMap::new(),
            spans: BTreeMap::new(),
            span_events: BTreeMap::new(),
            events: BTreeMap::new(),
            indexes: BTreeSet::new(),
        }
    }
}

impl Storage for TransientStorage {
    fn get_connection(&self, at: Timestamp) -> Option<Boo<'_, Connection>> {
        self.connections.get(&at).map(Boo::Borrowed)
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

    fn get_all_connections(&self) -> Box<dyn Iterator<Item = Boo<'_, Connection>> + '_> {
        Box::new(self.connections.values().map(Boo::Borrowed))
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

    fn get_all_indexes(&self) -> Box<dyn Iterator<Item = Boo<'_, String>> + '_> {
        Box::new(self.indexes.iter().map(Boo::Borrowed))
    }

    fn insert_connection(&mut self, connection: Connection) {
        let at = connection.key();
        self.connections.insert(at, connection);
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

    fn insert_index(&mut self, name: String) {
        self.indexes.insert(name);
    }

    fn update_connection_disconnected(&mut self, at: Timestamp, disconnected_at: Timestamp) {
        if let Some(connection) = self.connections.get_mut(&at) {
            connection.disconnected_at = Some(disconnected_at);
        }
    }

    fn update_span_closed(&mut self, at: Timestamp, closed_at: Timestamp) {
        if let Some(span) = self.spans.get_mut(&at) {
            span.closed_at = Some(closed_at);
        }
    }

    fn update_span_fields(&mut self, at: Timestamp, fields: BTreeMap<String, Value>) {
        if let Some(span) = self.spans.get_mut(&at) {
            span.fields.extend(fields);
        }
    }

    fn update_span_follows(&mut self, at: Timestamp, follows: SpanKey) {
        if let Some(span) = self.spans.get_mut(&at) {
            span.follows.push(follows);
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

    fn drop_index(&mut self, name: &str) {
        self.indexes.remove(name);
    }
}
