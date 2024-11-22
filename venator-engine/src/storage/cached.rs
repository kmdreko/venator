use std::cell::RefCell;
use std::collections::BTreeMap;
use std::num::NonZeroUsize;
use std::sync::Arc;

use lru::LruCache;

use crate::{Connection, Event, Span, SpanEvent, SpanKey, Timestamp, Value};

use super::Storage;

/// This storage wraps another storage implementation to keep some in memory.
pub struct CachedStorage<S> {
    connections: RefCell<LruCache<Timestamp, Arc<Connection>>>,
    spans: RefCell<LruCache<Timestamp, Arc<Span>>>,
    // span_events: RefCell<LruCache<Timestamp, Arc<SpanEvent>>>,
    events: RefCell<LruCache<Timestamp, Arc<Event>>>,
    inner: S,
}

impl<S> CachedStorage<S> {
    pub fn new(capacity: usize, storage: S) -> CachedStorage<S> {
        let capacity = NonZeroUsize::new(capacity).unwrap();

        CachedStorage {
            connections: RefCell::new(LruCache::new(capacity)),
            spans: RefCell::new(LruCache::new(capacity)),
            events: RefCell::new(LruCache::new(capacity)),
            inner: storage,
        }
    }
}

impl<S> Storage for CachedStorage<S>
where
    S: Storage,
{
    fn get_connection(&self, at: Timestamp) -> Option<Arc<Connection>> {
        if let Some(connection) = self.connections.borrow_mut().get(&at) {
            return Some(connection.clone());
        }

        if let Some(connection) = self.inner.get_connection(at) {
            self.connections.borrow_mut().put(at, connection.clone());
            return Some(connection);
        }

        None
    }

    fn get_span(&self, at: Timestamp) -> Option<Arc<Span>> {
        if let Some(span) = self.spans.borrow_mut().get(&at) {
            return Some(span.clone());
        }

        if let Some(span) = self.inner.get_span(at) {
            self.spans.borrow_mut().put(at, span.clone());
            return Some(span);
        }

        None
    }

    fn get_span_event(&self, at: Timestamp) -> Option<Arc<SpanEvent>> {
        self.inner.get_span_event(at)
    }

    fn get_event(&self, at: Timestamp) -> Option<Arc<Event>> {
        if let Some(event) = self.events.borrow_mut().get(&at) {
            return Some(event.clone());
        }

        if let Some(event) = self.inner.get_event(at) {
            self.events.borrow_mut().put(at, event.clone());
            return Some(event);
        }

        None
    }

    fn get_all_connections(&self) -> Box<dyn Iterator<Item = Arc<Connection>> + '_> {
        self.inner.get_all_connections()
    }

    fn get_all_spans(&self) -> Box<dyn Iterator<Item = Arc<Span>> + '_> {
        self.inner.get_all_spans()
    }

    fn get_all_span_events(&self) -> Box<dyn Iterator<Item = Arc<SpanEvent>> + '_> {
        self.inner.get_all_span_events()
    }

    fn get_all_events(&self) -> Box<dyn Iterator<Item = Arc<Event>> + '_> {
        self.inner.get_all_events()
    }

    fn insert_connection(&mut self, connection: Connection) {
        self.inner.insert_connection(connection)
    }

    fn insert_span(&mut self, span: Span) {
        self.inner.insert_span(span)
    }

    fn insert_span_event(&mut self, span_event: SpanEvent) {
        self.inner.insert_span_event(span_event)
    }

    fn insert_event(&mut self, event: Event) {
        self.inner.insert_event(event)
    }

    fn update_connection_disconnected(&mut self, at: Timestamp, disconnected: Timestamp) {
        self.connections.borrow_mut().pop(&at);
        self.inner.update_connection_disconnected(at, disconnected);
    }

    fn update_span_closed(&mut self, at: Timestamp, closed: Timestamp) {
        self.spans.borrow_mut().pop(&at);
        self.inner.update_span_closed(at, closed);
    }

    fn update_span_fields(&mut self, at: Timestamp, fields: BTreeMap<String, Value>) {
        self.spans.borrow_mut().pop(&at);
        self.inner.update_span_fields(at, fields);
    }

    fn update_span_follows(&mut self, at: Timestamp, follows: SpanKey) {
        self.spans.borrow_mut().pop(&at);
        self.inner.update_span_follows(at, follows);
    }

    fn drop_connections(&mut self, connections: &[Timestamp]) {
        for c in connections {
            self.connections.borrow_mut().pop(c);
        }

        self.inner.drop_connections(connections);
    }

    fn drop_spans(&mut self, spans: &[Timestamp]) {
        for s in spans {
            self.spans.borrow_mut().pop(s);
        }

        self.inner.drop_spans(spans);
    }

    fn drop_span_events(&mut self, span_events: &[Timestamp]) {
        self.inner.drop_span_events(span_events);
    }

    fn drop_events(&mut self, events: &[Timestamp]) {
        for s in events {
            self.events.borrow_mut().pop(s);
        }

        self.inner.drop_events(events);
    }
}
