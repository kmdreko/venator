use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::sync::Arc;

use crate::storage::{IndexStorage, Storage};
use crate::{Event, EventKey, Resource, Span, SpanEvent, SpanKey, Timestamp, Value};

use super::{StorageError, StorageIter};

pub enum BatchAction<T> {
    Create(T),
    Update(T),
    Delete(Timestamp),
}

impl<T> BatchAction<T> {
    fn order(&self) -> i32 {
        match self {
            BatchAction::Delete(_) => 0,
            BatchAction::Update(_) => 1,
            BatchAction::Create(_) => 2,
        }
    }
}

pub struct BatchedStorage<S> {
    inner: S,
    resources: BTreeMap<Timestamp, BatchAction<Arc<Resource>>>,
    spans: BTreeMap<Timestamp, BatchAction<Arc<Span>>>,
    span_events: BTreeMap<Timestamp, BatchAction<Arc<SpanEvent>>>,
    events: BTreeMap<Timestamp, BatchAction<Arc<Event>>>,
}

impl<S> BatchedStorage<S> {
    pub fn new(storage: S) -> BatchedStorage<S> {
        BatchedStorage {
            inner: storage,
            resources: BTreeMap::new(),
            spans: BTreeMap::new(),
            span_events: BTreeMap::new(),
            events: BTreeMap::new(),
        }
    }
}

impl<S> Storage for BatchedStorage<S>
where
    S: Storage + Batch,
{
    fn get_resource(&self, at: Timestamp) -> Result<Arc<Resource>, StorageError> {
        if let Some(action) = self.resources.get(&at) {
            match action {
                BatchAction::Create(resource) => return Ok(resource.clone()),
                BatchAction::Update(resource) => return Ok(resource.clone()),
                BatchAction::Delete(_) => return Err(StorageError::NotFound),
            }
        }

        self.inner.get_resource(at)
    }

    fn get_span(&self, at: Timestamp) -> Result<Arc<Span>, StorageError> {
        if let Some(action) = self.spans.get(&at) {
            match action {
                BatchAction::Create(span) => return Ok(span.clone()),
                BatchAction::Update(span) => return Ok(span.clone()),
                BatchAction::Delete(_) => return Err(StorageError::NotFound),
            }
        }

        self.inner.get_span(at)
    }

    fn get_span_event(&self, at: Timestamp) -> Result<Arc<SpanEvent>, StorageError> {
        if let Some(action) = self.span_events.get(&at) {
            match action {
                BatchAction::Create(span_event) => return Ok(span_event.clone()),
                BatchAction::Update(span_event) => return Ok(span_event.clone()),
                BatchAction::Delete(_) => return Err(StorageError::NotFound),
            }
        }

        self.inner.get_span_event(at)
    }

    fn get_event(&self, at: Timestamp) -> Result<Arc<Event>, StorageError> {
        if let Some(action) = self.events.get(&at) {
            match action {
                BatchAction::Create(event) => return Ok(event.clone()),
                BatchAction::Update(event) => return Ok(event.clone()),
                BatchAction::Delete(_) => return Err(StorageError::NotFound),
            }
        }

        self.inner.get_event(at)
    }

    fn get_all_resources(&self) -> Result<StorageIter<'_, Resource>, StorageError> {
        assert!(self.resources.is_empty());
        self.inner.get_all_resources()
    }

    fn get_all_spans(&self) -> Result<StorageIter<'_, Span>, StorageError> {
        assert!(self.spans.is_empty());
        self.inner.get_all_spans()
    }

    fn get_all_span_events(&self) -> Result<StorageIter<'_, SpanEvent>, StorageError> {
        assert!(self.span_events.is_empty());
        self.inner.get_all_span_events()
    }

    fn get_all_events(&self) -> Result<StorageIter<'_, Event>, StorageError> {
        assert!(self.events.is_empty());
        self.inner.get_all_events()
    }

    fn insert_resource(&mut self, resource: Resource) -> Result<(), StorageError> {
        self.resources
            .insert(resource.key(), BatchAction::Create(Arc::new(resource)));

        Ok(())
    }

    fn insert_span(&mut self, span: Span) -> Result<(), StorageError> {
        self.spans
            .insert(span.key(), BatchAction::Create(Arc::new(span)));

        Ok(())
    }

    fn insert_span_event(&mut self, span_event: SpanEvent) -> Result<(), StorageError> {
        self.span_events
            .insert(span_event.key(), BatchAction::Create(Arc::new(span_event)));

        Ok(())
    }

    fn insert_event(&mut self, event: Event) -> Result<(), StorageError> {
        self.events
            .insert(event.key(), BatchAction::Create(Arc::new(event)));

        Ok(())
    }

    fn update_span_closed(
        &mut self,
        at: Timestamp,
        closed: Timestamp,
        busy: Option<u64>,
    ) -> Result<(), StorageError> {
        let mut span_arc = self.get_span(at)?;
        let span = Arc::make_mut(&mut span_arc);

        span.closed_at = Some(closed);
        span.busy = busy;

        match self.spans.entry(at) {
            Entry::Vacant(entry) => {
                entry.insert(BatchAction::Update(span_arc));
            }
            Entry::Occupied(mut entry) => {
                match entry.get() {
                    BatchAction::Create(_) => entry.insert(BatchAction::Create(span_arc)),
                    BatchAction::Update(_) => entry.insert(BatchAction::Update(span_arc)),
                    BatchAction::Delete(_) => unreachable!(),
                };
            }
        }

        Ok(())
    }

    fn update_span_attributes(
        &mut self,
        at: Timestamp,
        attributes: BTreeMap<String, Value>,
    ) -> Result<(), StorageError> {
        let mut span_arc = self.get_span(at)?;
        let span = Arc::make_mut(&mut span_arc);

        span.attributes.extend(attributes);

        match self.spans.entry(at) {
            Entry::Vacant(entry) => {
                entry.insert(BatchAction::Update(span_arc));
            }
            Entry::Occupied(mut entry) => {
                match entry.get() {
                    BatchAction::Create(_) => entry.insert(BatchAction::Create(span_arc)),
                    BatchAction::Update(_) => entry.insert(BatchAction::Update(span_arc)),
                    BatchAction::Delete(_) => unreachable!(),
                };
            }
        }

        Ok(())
    }

    fn update_span_link(
        &mut self,
        at: Timestamp,
        link: crate::FullSpanId,
        attributes: BTreeMap<String, Value>,
    ) -> Result<(), StorageError> {
        let mut span_arc = self.get_span(at)?;
        let span = Arc::make_mut(&mut span_arc);

        span.links.push((link, attributes));

        match self.spans.entry(at) {
            Entry::Vacant(entry) => {
                entry.insert(BatchAction::Update(span_arc));
            }
            Entry::Occupied(mut entry) => {
                match entry.get() {
                    BatchAction::Create(_) => entry.insert(BatchAction::Create(span_arc)),
                    BatchAction::Update(_) => entry.insert(BatchAction::Update(span_arc)),
                    BatchAction::Delete(_) => unreachable!(),
                };
            }
        }

        Ok(())
    }

    fn update_span_parents(
        &mut self,
        parent_key: SpanKey,
        spans: &[SpanKey],
    ) -> Result<(), StorageError> {
        for key in spans {
            let mut span_arc = self.get_span(*key)?;
            let span = Arc::make_mut(&mut span_arc);

            span.parent_key = Some(parent_key);

            match self.spans.entry(*key) {
                Entry::Vacant(entry) => {
                    entry.insert(BatchAction::Update(span_arc));
                }
                Entry::Occupied(mut entry) => {
                    match entry.get() {
                        BatchAction::Create(_) => entry.insert(BatchAction::Create(span_arc)),
                        BatchAction::Update(_) => entry.insert(BatchAction::Update(span_arc)),
                        BatchAction::Delete(_) => unreachable!(),
                    };
                }
            }
        }

        Ok(())
    }

    fn update_event_parents(
        &mut self,
        parent_key: SpanKey,
        events: &[EventKey],
    ) -> Result<(), StorageError> {
        for key in events {
            let mut event_arc = self.get_event(*key)?;
            let event = Arc::make_mut(&mut event_arc);

            event.parent_key = Some(parent_key);

            match self.events.entry(*key) {
                Entry::Vacant(entry) => {
                    entry.insert(BatchAction::Update(event_arc));
                }
                Entry::Occupied(mut entry) => {
                    match entry.get() {
                        BatchAction::Create(_) => entry.insert(BatchAction::Create(event_arc)),
                        BatchAction::Update(_) => entry.insert(BatchAction::Update(event_arc)),
                        BatchAction::Delete(_) => unreachable!(),
                    };
                }
            }
        }

        Ok(())
    }

    fn drop_resources(&mut self, resources: &[Timestamp]) -> Result<(), StorageError> {
        for key in resources {
            self.resources.insert(*key, BatchAction::Delete(*key));
        }

        Ok(())
    }

    fn drop_spans(&mut self, spans: &[Timestamp]) -> Result<(), StorageError> {
        for key in spans {
            self.spans.insert(*key, BatchAction::Delete(*key));
        }

        Ok(())
    }

    fn drop_span_events(&mut self, span_events: &[Timestamp]) -> Result<(), StorageError> {
        for key in span_events {
            self.span_events.insert(*key, BatchAction::Delete(*key));
        }

        Ok(())
    }

    fn drop_events(&mut self, events: &[Timestamp]) -> Result<(), StorageError> {
        for key in events {
            self.events.insert(*key, BatchAction::Delete(*key));
        }

        Ok(())
    }

    fn sync(&mut self) -> Result<(), StorageError> {
        if self.resources.is_empty()
            && self.spans.is_empty()
            && self.span_events.is_empty()
            && self.events.is_empty()
        {
            return Ok(());
        }

        let mut resources = self.resources.values().collect::<Vec<_>>();
        let mut spans = self.spans.values().collect::<Vec<_>>();
        let mut span_events = self.span_events.values().collect::<Vec<_>>();
        let mut events = self.events.values().collect::<Vec<_>>();

        resources.sort_by_key(|action| action.order());
        spans.sort_by_key(|action| action.order());
        span_events.sort_by_key(|action| action.order());
        events.sort_by_key(|action| action.order());

        self.inner.batch(
            &mut resources.into_iter(),
            &mut spans.into_iter(),
            &mut span_events.into_iter(),
            &mut events.into_iter(),
        )?;

        self.inner.sync()?;

        self.resources.clear();
        self.spans.clear();
        self.span_events.clear();
        self.events.clear();

        Ok(())
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

pub trait Batch {
    fn batch(
        &mut self,
        resources: &mut dyn Iterator<Item = &BatchAction<Arc<Resource>>>,
        spans: &mut dyn Iterator<Item = &BatchAction<Arc<Span>>>,
        span_events: &mut dyn Iterator<Item = &BatchAction<Arc<SpanEvent>>>,
        events: &mut dyn Iterator<Item = &BatchAction<Arc<Event>>>,
    ) -> Result<(), StorageError>;
}
