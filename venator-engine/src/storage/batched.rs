use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::num::NonZeroU64;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::Sender;
use std::sync::Arc;
use std::thread::JoinHandle;

use crate::storage::{IndexStorage, Storage};
use crate::{Event, EventKey, Resource, Span, SpanEvent, SpanKey, Timestamp, Value};

use super::{StorageError, StorageIter, StorageSyncStatus};

type Epoch = NonZeroU64;

fn set_epoch(epoch: &AtomicU64, new: Epoch) {
    epoch.store(new.get(), Ordering::Release);
}

fn get_epoch(epoch: &AtomicU64) -> Option<Epoch> {
    let last = epoch.swap(0, Ordering::AcqRel);

    Epoch::new(last)
}

#[derive(Debug, Clone)]
pub enum BatchAction<T> {
    Create(T),
    Update(T),
    Delete(Timestamp),
}

struct WriteChanges {
    resources: Vec<BatchAction<Arc<Resource>>>,
    spans: Vec<BatchAction<Arc<Span>>>,
    span_events: Vec<BatchAction<Arc<SpanEvent>>>,
    events: Vec<BatchAction<Arc<Event>>>,
}

enum WriteThreadCommand {
    Sync(Epoch, WriteChanges),
}

pub struct BatchedStorage<S> {
    inner: S,
    resources: BTreeMap<Timestamp, (Epoch, BatchAction<Arc<Resource>>)>,
    spans: BTreeMap<Timestamp, (Epoch, BatchAction<Arc<Span>>)>,
    span_events: BTreeMap<Timestamp, (Epoch, BatchAction<Arc<SpanEvent>>)>,
    events: BTreeMap<Timestamp, (Epoch, BatchAction<Arc<Event>>)>,

    write_epoch_next: Epoch,
    write_epoch_last_confirmed: Epoch,
    write_epoch_last: Arc<AtomicU64>,
    write_thread: JoinHandle<()>,
    write_sender: Sender<WriteThreadCommand>,
}

impl<S: Clone + Batch + Send + 'static> BatchedStorage<S> {
    pub fn new(storage: S) -> BatchedStorage<S> {
        let (write_sender, write_receiver) = std::sync::mpsc::channel();

        let write_epoch_last = Arc::new(AtomicU64::new(0));

        let mut thread_storage = storage.clone();
        let thread_write_epoch_last = write_epoch_last.clone();

        let write_thread = std::thread::Builder::new()
            .name("engine-write".into())
            .spawn(move || {
                while let Ok(command) = write_receiver.recv() {
                    match command {
                        WriteThreadCommand::Sync(epoch, changes) => {
                            thread_storage
                                .batch(
                                    &mut changes.resources.iter(),
                                    &mut changes.spans.iter(),
                                    &mut changes.span_events.iter(),
                                    &mut changes.events.iter(),
                                )
                                .unwrap();

                            set_epoch(&thread_write_epoch_last, epoch);
                        }
                    }
                }
            })
            .unwrap();

        BatchedStorage {
            inner: storage,
            resources: BTreeMap::new(),
            spans: BTreeMap::new(),
            span_events: BTreeMap::new(),
            events: BTreeMap::new(),

            write_epoch_next: Epoch::MIN,
            write_epoch_last_confirmed: Epoch::MIN,
            write_epoch_last,
            write_thread,
            write_sender,
        }
    }
}

impl<S> Storage for BatchedStorage<S>
where
    S: Storage + Batch,
{
    fn get_resource(&self, at: Timestamp) -> Result<Arc<Resource>, StorageError> {
        if let Some((_, action)) = self.resources.get(&at) {
            match action {
                BatchAction::Create(resource) => return Ok(resource.clone()),
                BatchAction::Update(resource) => return Ok(resource.clone()),
                BatchAction::Delete(_) => return Err(StorageError::NotFound),
            }
        }

        self.inner.get_resource(at)
    }

    fn get_span(&self, at: Timestamp) -> Result<Arc<Span>, StorageError> {
        if let Some((_, action)) = self.spans.get(&at) {
            match action {
                BatchAction::Create(span) => return Ok(span.clone()),
                BatchAction::Update(span) => return Ok(span.clone()),
                BatchAction::Delete(_) => return Err(StorageError::NotFound),
            }
        }

        self.inner.get_span(at)
    }

    fn get_span_event(&self, at: Timestamp) -> Result<Arc<SpanEvent>, StorageError> {
        if let Some((_, action)) = self.span_events.get(&at) {
            match action {
                BatchAction::Create(span_event) => return Ok(span_event.clone()),
                BatchAction::Update(span_event) => return Ok(span_event.clone()),
                BatchAction::Delete(_) => return Err(StorageError::NotFound),
            }
        }

        self.inner.get_span_event(at)
    }

    fn get_event(&self, at: Timestamp) -> Result<Arc<Event>, StorageError> {
        if let Some((_, action)) = self.events.get(&at) {
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
        self.resources.insert(
            resource.key(),
            (
                self.write_epoch_next,
                BatchAction::Create(Arc::new(resource)),
            ),
        );

        Ok(())
    }

    fn insert_span(&mut self, span: Span) -> Result<(), StorageError> {
        self.spans.insert(
            span.key(),
            (self.write_epoch_next, BatchAction::Create(Arc::new(span))),
        );

        Ok(())
    }

    fn insert_span_event(&mut self, span_event: SpanEvent) -> Result<(), StorageError> {
        self.span_events.insert(
            span_event.key(),
            (
                self.write_epoch_next,
                BatchAction::Create(Arc::new(span_event)),
            ),
        );

        Ok(())
    }

    fn insert_event(&mut self, event: Event) -> Result<(), StorageError> {
        self.events.insert(
            event.key(),
            (self.write_epoch_next, BatchAction::Create(Arc::new(event))),
        );

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
                entry.insert((self.write_epoch_next, BatchAction::Update(span_arc)));
            }
            Entry::Occupied(mut entry) => {
                match entry.get() {
                    (epoch, BatchAction::Create(_)) if *epoch == self.write_epoch_next => {
                        entry.insert((self.write_epoch_next, BatchAction::Create(span_arc)))
                    }
                    (_, BatchAction::Create(_) | BatchAction::Update(_)) => {
                        entry.insert((self.write_epoch_next, BatchAction::Update(span_arc)))
                    }
                    (_, BatchAction::Delete(_)) => return Err(StorageError::NotFound),
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
                entry.insert((self.write_epoch_next, BatchAction::Update(span_arc)));
            }
            Entry::Occupied(mut entry) => {
                match entry.get() {
                    (epoch, BatchAction::Create(_)) if *epoch == self.write_epoch_next => {
                        entry.insert((self.write_epoch_next, BatchAction::Create(span_arc)))
                    }
                    (_, BatchAction::Create(_) | BatchAction::Update(_)) => {
                        entry.insert((self.write_epoch_next, BatchAction::Update(span_arc)))
                    }
                    (_, BatchAction::Delete(_)) => return Err(StorageError::NotFound),
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
                entry.insert((self.write_epoch_next, BatchAction::Update(span_arc)));
            }
            Entry::Occupied(mut entry) => {
                match entry.get() {
                    (epoch, BatchAction::Create(_)) if *epoch == self.write_epoch_next => {
                        entry.insert((self.write_epoch_next, BatchAction::Create(span_arc)))
                    }
                    (_, BatchAction::Create(_) | BatchAction::Update(_)) => {
                        entry.insert((self.write_epoch_next, BatchAction::Update(span_arc)))
                    }
                    (_, BatchAction::Delete(_)) => return Err(StorageError::NotFound),
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
                    entry.insert((self.write_epoch_next, BatchAction::Update(span_arc)));
                }
                Entry::Occupied(mut entry) => {
                    match entry.get() {
                        (epoch, BatchAction::Create(_)) if *epoch == self.write_epoch_next => {
                            entry.insert((self.write_epoch_next, BatchAction::Create(span_arc)))
                        }
                        (_, BatchAction::Create(_) | BatchAction::Update(_)) => {
                            entry.insert((self.write_epoch_next, BatchAction::Update(span_arc)))
                        }
                        (_, BatchAction::Delete(_)) => return Err(StorageError::NotFound),
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
                    entry.insert((self.write_epoch_next, BatchAction::Update(event_arc)));
                }
                Entry::Occupied(mut entry) => {
                    match entry.get() {
                        (epoch, BatchAction::Create(_)) if *epoch == self.write_epoch_next => {
                            entry.insert((self.write_epoch_next, BatchAction::Create(event_arc)))
                        }
                        (_, BatchAction::Create(_) | BatchAction::Update(_)) => {
                            entry.insert((self.write_epoch_next, BatchAction::Update(event_arc)))
                        }
                        (_, BatchAction::Delete(_)) => return Err(StorageError::NotFound),
                    };
                }
            }
        }

        Ok(())
    }

    fn drop_resources(&mut self, resources: &[Timestamp]) -> Result<(), StorageError> {
        for key in resources {
            match self.resources.entry(*key) {
                Entry::Vacant(entry) => {
                    entry.insert((self.write_epoch_next, BatchAction::Delete(*key)));
                }
                Entry::Occupied(mut entry) => match entry.get() {
                    (epoch, BatchAction::Create(_)) if *epoch == self.write_epoch_next => {
                        entry.remove();
                    }
                    _ => {
                        entry.insert((self.write_epoch_next, BatchAction::Delete(*key)));
                    }
                },
            }
        }

        Ok(())
    }

    fn drop_spans(&mut self, spans: &[Timestamp]) -> Result<(), StorageError> {
        for key in spans {
            match self.spans.entry(*key) {
                Entry::Vacant(entry) => {
                    entry.insert((self.write_epoch_next, BatchAction::Delete(*key)));
                }
                Entry::Occupied(mut entry) => match entry.get() {
                    (epoch, BatchAction::Create(_)) if *epoch == self.write_epoch_next => {
                        entry.remove();
                    }
                    _ => {
                        entry.insert((self.write_epoch_next, BatchAction::Delete(*key)));
                    }
                },
            }
        }

        Ok(())
    }

    fn drop_span_events(&mut self, span_events: &[Timestamp]) -> Result<(), StorageError> {
        for key in span_events {
            match self.span_events.entry(*key) {
                Entry::Vacant(entry) => {
                    entry.insert((self.write_epoch_next, BatchAction::Delete(*key)));
                }
                Entry::Occupied(mut entry) => match entry.get() {
                    (epoch, BatchAction::Create(_)) if *epoch == self.write_epoch_next => {
                        entry.remove();
                    }
                    _ => {
                        entry.insert((self.write_epoch_next, BatchAction::Delete(*key)));
                    }
                },
            }
        }

        Ok(())
    }

    fn drop_events(&mut self, events: &[Timestamp]) -> Result<(), StorageError> {
        for key in events {
            match self.events.entry(*key) {
                Entry::Vacant(entry) => {
                    entry.insert((self.write_epoch_next, BatchAction::Delete(*key)));
                }
                Entry::Occupied(mut entry) => match entry.get() {
                    (epoch, BatchAction::Create(_)) if *epoch == self.write_epoch_next => {
                        entry.remove();
                    }
                    _ => {
                        entry.insert((self.write_epoch_next, BatchAction::Delete(*key)));
                    }
                },
            }
        }

        Ok(())
    }

    fn sync(&mut self) -> Result<StorageSyncStatus, StorageError> {
        if let Some(last) = get_epoch(&self.write_epoch_last) {
            // get the last epoch written, and if there was one, remove all
            // objects that were written before that epoch

            self.resources.retain(|_, (epoch, _)| *epoch > last);
            self.spans.retain(|_, (epoch, _)| *epoch > last);
            self.span_events.retain(|_, (epoch, _)| *epoch > last);
            self.events.retain(|_, (epoch, _)| *epoch > last);

            self.write_epoch_last_confirmed = last;
        }

        if self.write_thread.is_finished() {
            return Err(StorageError::Internal("write thread crashed".to_owned()));
        }

        if self.resources.is_empty()
            && self.spans.is_empty()
            && self.span_events.is_empty()
            && self.events.is_empty()
        {
            if self.write_epoch_last_confirmed.saturating_add(1) < self.write_epoch_next {
                return Ok(StorageSyncStatus::Behind);
            } else {
                return Ok(StorageSyncStatus::Synced);
            }
        }

        let resources = self
            .resources
            .values()
            .filter(|(epoch, _)| *epoch == self.write_epoch_next)
            .map(|(_, action)| action)
            .cloned()
            .collect::<Vec<_>>();
        let spans = self
            .spans
            .values()
            .filter(|(epoch, _)| *epoch == self.write_epoch_next)
            .map(|(_, action)| action)
            .cloned()
            .collect::<Vec<_>>();
        let span_events = self
            .span_events
            .values()
            .filter(|(epoch, _)| *epoch == self.write_epoch_next)
            .map(|(_, action)| action)
            .cloned()
            .collect::<Vec<_>>();
        let events = self
            .events
            .values()
            .filter(|(epoch, _)| *epoch == self.write_epoch_next)
            .map(|(_, action)| action)
            .cloned()
            .collect::<Vec<_>>();

        self.write_sender
            .send(WriteThreadCommand::Sync(
                self.write_epoch_next,
                WriteChanges {
                    resources,
                    spans,
                    span_events,
                    events,
                },
            ))
            .map_err(|_| StorageError::Internal("failed to queue for writer thread".into()))?;

        self.write_epoch_next = self.write_epoch_next.checked_add(1).unwrap();

        if self.write_epoch_last_confirmed.saturating_add(2) < self.write_epoch_next {
            return Ok(StorageSyncStatus::Behind);
        } else {
            return Ok(StorageSyncStatus::Syncing);
        }
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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::storage::Storage;
    use crate::{FullSpanId, Level, SourceKind};

    use super::*;

    fn dummy_span() -> Span {
        Span {
            kind: SourceKind::Opentelemetry,
            resource_key: NonZeroU64::MIN,
            id: FullSpanId::Opentelemetry(0, 1),
            created_at: NonZeroU64::MIN,
            closed_at: Some(NonZeroU64::MIN.saturating_add(1)),
            busy: None,
            parent_id: None,
            parent_key: None,
            links: Vec::new(),
            name: "span".to_owned(),
            namespace: None,
            function: None,
            level: Level::Info,
            file_name: None,
            file_line: None,
            file_column: None,
            instrumentation_attributes: BTreeMap::new(),
            attributes: BTreeMap::new(),
        }
    }

    #[derive(Copy, Clone)]
    struct DummyStorage;

    impl Batch for DummyStorage {
        fn batch(
            &mut self,
            _resources: &mut dyn Iterator<Item = &BatchAction<Arc<Resource>>>,
            _spans: &mut dyn Iterator<Item = &BatchAction<Arc<Span>>>,
            _span_events: &mut dyn Iterator<Item = &BatchAction<Arc<SpanEvent>>>,
            _events: &mut dyn Iterator<Item = &BatchAction<Arc<Event>>>,
        ) -> Result<(), StorageError> {
            Ok(())
        }
    }

    impl Storage for DummyStorage {
        fn get_resource(&self, _at: Timestamp) -> Result<Arc<Resource>, StorageError> {
            unimplemented!()
        }

        fn get_span(&self, _at: Timestamp) -> Result<Arc<Span>, StorageError> {
            unimplemented!()
        }

        fn get_span_event(&self, _at: Timestamp) -> Result<Arc<SpanEvent>, StorageError> {
            unimplemented!()
        }

        fn get_event(&self, _at: Timestamp) -> Result<Arc<Event>, StorageError> {
            unimplemented!()
        }

        fn get_all_resources(&self) -> Result<StorageIter<'_, Resource>, StorageError> {
            unimplemented!()
        }

        fn get_all_spans(&self) -> Result<StorageIter<'_, Span>, StorageError> {
            unimplemented!()
        }

        fn get_all_span_events(&self) -> Result<StorageIter<'_, SpanEvent>, StorageError> {
            unimplemented!()
        }

        fn get_all_events(&self) -> Result<StorageIter<'_, Event>, StorageError> {
            unimplemented!()
        }

        fn insert_resource(&mut self, _resource: Resource) -> Result<(), StorageError> {
            unimplemented!()
        }

        fn insert_span(&mut self, _span: Span) -> Result<(), StorageError> {
            unimplemented!()
        }

        fn insert_span_event(&mut self, _span_event: SpanEvent) -> Result<(), StorageError> {
            unimplemented!()
        }

        fn insert_event(&mut self, _event: Event) -> Result<(), StorageError> {
            unimplemented!()
        }

        fn update_span_closed(
            &mut self,
            _at: Timestamp,
            _closed: Timestamp,
            _busy: Option<u64>,
        ) -> Result<(), StorageError> {
            unimplemented!()
        }

        fn update_span_attributes(
            &mut self,
            _at: Timestamp,
            _attributes: BTreeMap<String, Value>,
        ) -> Result<(), StorageError> {
            unimplemented!()
        }

        fn update_span_link(
            &mut self,
            _at: Timestamp,
            _link: FullSpanId,
            _attributes: BTreeMap<String, Value>,
        ) -> Result<(), StorageError> {
            unimplemented!()
        }

        fn update_span_parents(
            &mut self,
            _parent_key: SpanKey,
            _spans: &[SpanKey],
        ) -> Result<(), StorageError> {
            unimplemented!()
        }

        fn update_event_parents(
            &mut self,
            _parent_key: SpanKey,
            _events: &[EventKey],
        ) -> Result<(), StorageError> {
            unimplemented!()
        }

        fn drop_resources(&mut self, _resources: &[Timestamp]) -> Result<(), StorageError> {
            unimplemented!()
        }

        fn drop_spans(&mut self, _spans: &[Timestamp]) -> Result<(), StorageError> {
            unimplemented!()
        }

        fn drop_span_events(&mut self, _span_events: &[Timestamp]) -> Result<(), StorageError> {
            unimplemented!()
        }

        fn drop_events(&mut self, _events: &[Timestamp]) -> Result<(), StorageError> {
            unimplemented!()
        }

        fn sync(&mut self) -> Result<StorageSyncStatus, StorageError> {
            Ok(StorageSyncStatus::Synced)
        }
    }

    #[test]
    fn batched_sync_is_synced_with_no_writes() {
        let mut storage = BatchedStorage::new(DummyStorage);

        let result = storage.sync();
        assert!(matches!(result, Ok(StorageSyncStatus::Synced)));
    }

    #[test]
    fn batched_sync_is_synced_after_waiting_for_write() {
        let mut storage = BatchedStorage::new(DummyStorage);

        storage.insert_span(dummy_span()).unwrap();
        let result = storage.sync();
        assert!(matches!(result, Ok(StorageSyncStatus::Syncing)));

        std::thread::sleep(Duration::from_millis(100));

        let result = storage.sync();
        assert!(matches!(result, Ok(StorageSyncStatus::Synced)));
    }

    #[test]
    fn batched_sync_is_behind_after_two_fast_writes() {
        let mut storage = BatchedStorage::new(DummyStorage);

        storage.insert_span(dummy_span()).unwrap();
        let result = storage.sync();
        assert!(matches!(result, Ok(StorageSyncStatus::Syncing)));

        storage.insert_span(dummy_span()).unwrap();
        let result = storage.sync();
        assert!(matches!(result, Ok(StorageSyncStatus::Behind)));
    }
}
