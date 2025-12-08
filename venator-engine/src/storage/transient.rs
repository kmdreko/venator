use std::collections::BTreeMap;
use std::sync::Arc;

use super::{Storage, StorageError};
use crate::models::{EventKey, Value};
use crate::{Event, FullSpanId, Resource, Span, SpanEvent, SpanKey, Timestamp};

/// This storage just holds all entities in memory.
pub struct TransientStorage {
    resources: BTreeMap<Timestamp, Arc<Resource>>,
    spans: BTreeMap<Timestamp, Arc<Span>>,
    span_events: BTreeMap<Timestamp, Arc<SpanEvent>>,
    events: BTreeMap<Timestamp, Arc<Event>>,
}

impl TransientStorage {
    pub fn new() -> TransientStorage {
        TransientStorage {
            resources: BTreeMap::new(),
            spans: BTreeMap::new(),
            span_events: BTreeMap::new(),
            events: BTreeMap::new(),
        }
    }
}

impl Default for TransientStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl Storage for TransientStorage {
    fn get_resource(&self, at: Timestamp) -> Result<Arc<Resource>, StorageError> {
        self.resources
            .get(&at)
            .cloned()
            .ok_or(StorageError::NotFound)
    }

    fn get_span(&self, at: Timestamp) -> Result<Arc<Span>, StorageError> {
        self.spans.get(&at).cloned().ok_or(StorageError::NotFound)
    }

    fn get_span_event(&self, at: Timestamp) -> Result<Arc<SpanEvent>, StorageError> {
        self.span_events
            .get(&at)
            .cloned()
            .ok_or(StorageError::NotFound)
    }

    fn get_event(&self, at: Timestamp) -> Result<Arc<Event>, StorageError> {
        self.events.get(&at).cloned().ok_or(StorageError::NotFound)
    }

    fn get_all_resources(
        &self,
    ) -> Result<Box<dyn Iterator<Item = Result<Arc<Resource>, StorageError>> + '_>, StorageError>
    {
        Ok(Box::new(self.resources.values().cloned().map(Ok)))
    }

    fn get_all_spans(
        &self,
    ) -> Result<Box<dyn Iterator<Item = Result<Arc<Span>, StorageError>> + '_>, StorageError> {
        Ok(Box::new(self.spans.values().cloned().map(Ok)))
    }

    fn get_all_span_events(
        &self,
    ) -> Result<Box<dyn Iterator<Item = Result<Arc<SpanEvent>, StorageError>> + '_>, StorageError>
    {
        Ok(Box::new(self.span_events.values().cloned().map(Ok)))
    }

    fn get_all_events(
        &self,
    ) -> Result<Box<dyn Iterator<Item = Result<Arc<Event>, StorageError>> + '_>, StorageError> {
        Ok(Box::new(self.events.values().cloned().map(Ok)))
    }

    fn insert_resource(&mut self, resource: Resource) -> Result<(), StorageError> {
        let at = resource.key();
        self.resources.insert(at, Arc::new(resource));
        Ok(())
    }

    fn insert_span(&mut self, span: Span) -> Result<(), StorageError> {
        let at = span.created_at;
        self.spans.insert(at, Arc::new(span));
        Ok(())
    }

    fn insert_span_event(&mut self, span_event: SpanEvent) -> Result<(), StorageError> {
        let at = span_event.timestamp;
        self.span_events.insert(at, Arc::new(span_event));
        Ok(())
    }

    fn insert_event(&mut self, event: Event) -> Result<(), StorageError> {
        let at = event.timestamp;
        self.events.insert(at, Arc::new(event));
        Ok(())
    }

    fn update_span_closed(
        &mut self,
        at: Timestamp,
        closed_at: Timestamp,
        busy: Option<u64>,
    ) -> Result<(), StorageError> {
        if let Some(span) = self.spans.get(&at) {
            let mut span = (**span).clone();
            span.closed_at = Some(closed_at);
            span.busy = busy;
            self.spans.insert(at, Arc::new(span));
        }

        Ok(())
    }

    fn update_span_attributes(
        &mut self,
        at: Timestamp,
        attributes: BTreeMap<String, Value>,
    ) -> Result<(), StorageError> {
        if let Some(span) = self.spans.get(&at) {
            let mut span = (**span).clone();
            span.attributes.extend(attributes);
            self.spans.insert(at, Arc::new(span));
        }

        Ok(())
    }

    fn update_span_link(
        &mut self,
        at: Timestamp,
        link: FullSpanId,
        attributes: BTreeMap<String, Value>,
    ) -> Result<(), StorageError> {
        if let Some(span) = self.spans.get(&at) {
            let mut span = (**span).clone();
            span.links.push((link, attributes));
            self.spans.insert(at, Arc::new(span));
        }

        Ok(())
    }

    fn update_span_parents(
        &mut self,
        parent_key: SpanKey,
        spans: &[SpanKey],
    ) -> Result<(), StorageError> {
        for span in spans {
            if let Some(span) = self.spans.get_mut(span) {
                let mut span = (**span).clone();
                span.parent_key = Some(parent_key);
                self.spans.insert(span.key(), Arc::new(span));
            }
        }

        Ok(())
    }

    fn update_event_parents(
        &mut self,
        parent_key: SpanKey,
        events: &[EventKey],
    ) -> Result<(), StorageError> {
        for event in events {
            if let Some(event) = self.events.get_mut(event) {
                let mut event = (**event).clone();
                event.parent_key = Some(parent_key);
                self.events.insert(event.key(), Arc::new(event));
            }
        }

        Ok(())
    }

    fn drop_resources(&mut self, resources: &[Timestamp]) -> Result<(), StorageError> {
        for at in resources {
            self.resources.remove(at);
        }

        Ok(())
    }

    fn drop_spans(&mut self, spans: &[Timestamp]) -> Result<(), StorageError> {
        for at in spans {
            self.spans.remove(at);
        }

        Ok(())
    }

    fn drop_span_events(&mut self, span_events: &[Timestamp]) -> Result<(), StorageError> {
        for at in span_events {
            self.span_events.remove(at);
        }

        Ok(())
    }

    fn drop_events(&mut self, events: &[Timestamp]) -> Result<(), StorageError> {
        for at in events {
            self.events.remove(at);
        }

        Ok(())
    }

    fn sync(&mut self) -> Result<(), StorageError> {
        Ok(())
    }
}
