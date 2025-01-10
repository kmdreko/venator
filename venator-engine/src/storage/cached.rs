use std::cell::RefCell;
use std::collections::BTreeMap;
use std::num::NonZeroUsize;
use std::sync::Arc;

use lru::LruCache;

use crate::{Event, EventKey, FullSpanId, Resource, Span, SpanEvent, SpanKey, Timestamp, Value};

use super::{IndexStorage, Storage};

/// This storage wraps another storage implementation to keep some in memory.
pub struct CachedStorage<S> {
    resources: RefCell<LruCache<Timestamp, Arc<Resource>>>,
    spans: RefCell<LruCache<Timestamp, Arc<Span>>>,
    // span_events: RefCell<LruCache<Timestamp, Arc<SpanEvent>>>,
    events: RefCell<LruCache<Timestamp, Arc<Event>>>,
    inner: S,
}

impl<S> CachedStorage<S> {
    pub fn new(capacity: usize, storage: S) -> CachedStorage<S> {
        let capacity = NonZeroUsize::new(capacity).unwrap();

        CachedStorage {
            resources: RefCell::new(LruCache::new(capacity)),
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
    fn get_resource(&self, at: Timestamp) -> Option<Arc<Resource>> {
        if let Some(resource) = self.resources.borrow_mut().get(&at) {
            return Some(resource.clone());
        }

        if let Some(resource) = self.inner.get_resource(at) {
            self.resources.borrow_mut().put(at, resource.clone());
            return Some(resource);
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

    fn get_all_resources(&self) -> Box<dyn Iterator<Item = Arc<Resource>> + '_> {
        self.inner.get_all_resources()
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

    fn insert_resource(&mut self, resource: Resource) {
        self.inner.insert_resource(resource)
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

    fn update_span_closed(&mut self, at: Timestamp, closed: Timestamp, busy: Option<u64>) {
        self.spans.borrow_mut().pop(&at);
        self.inner.update_span_closed(at, closed, busy);
    }

    fn update_span_attributes(&mut self, at: Timestamp, attributes: BTreeMap<String, Value>) {
        self.spans.borrow_mut().pop(&at);
        self.inner.update_span_attributes(at, attributes);
    }

    fn update_span_link(
        &mut self,
        at: Timestamp,
        link: FullSpanId,
        attributes: BTreeMap<String, Value>,
    ) {
        self.spans.borrow_mut().pop(&at);
        self.inner.update_span_link(at, link, attributes);
    }

    fn update_span_parents(&mut self, parent_key: SpanKey, spans: &[SpanKey]) {
        let mut cached_spans = self.spans.borrow_mut();
        for span in spans {
            cached_spans.pop(span);
        }
        drop(cached_spans);
        self.inner.update_span_parents(parent_key, spans);
    }

    fn update_event_parents(&mut self, parent_key: SpanKey, events: &[EventKey]) {
        let mut cached_events = self.events.borrow_mut();
        for event in events {
            cached_events.pop(event);
        }
        drop(cached_events);
        self.inner.update_event_parents(parent_key, events);
    }

    fn drop_resources(&mut self, resources: &[Timestamp]) {
        for c in resources {
            self.resources.borrow_mut().pop(c);
        }

        self.inner.drop_resources(resources);
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

    #[allow(private_interfaces)]
    fn as_index_storage(&self) -> Option<&dyn IndexStorage> {
        self.inner.as_index_storage()
    }

    #[allow(private_interfaces)]
    fn as_index_storage_mut(&mut self) -> Option<&mut dyn IndexStorage> {
        self.inner.as_index_storage_mut()
    }
}
