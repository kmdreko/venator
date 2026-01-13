use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use redb::{
    CommitError, Database, Key, ReadableDatabase, TableDefinition, TableError, TransactionError,
    Value as RedbValue,
};
use rkyv::rancor::Error as RkyvError;
use rkyv::{Archive, Deserialize, Serialize};
use tracing::instrument;

use crate::index::{EventIndexes, SpanEventIndexes, SpanIndexes};
use crate::models::{EventKey, Value};
use crate::storage::batched::{Batch, BatchAction};
use crate::{Event, FullSpanId, Resource, Span, SpanEvent, SpanKey, Timestamp};

use super::{IndexStorage, Storage, StorageError, StorageIter, StorageSyncStatus};

mod db_model;

use db_model::DbModel;

#[derive(Debug, Copy, Clone, PartialEq, Eq, Default, Archive, Serialize, Deserialize)]
enum IndexState {
    #[default]
    Stale,
    Fresh,
}

#[derive(Debug, Default, Archive, Serialize, Deserialize)]
struct Meta {
    indexes: IndexState,
}

type MetaDbModel<'a> = DbModel<'a, Meta>;
type ResourceDbModel<'a> = DbModel<'a, Resource>;
type SpanDbModel<'a> = DbModel<'a, Span>;
type SpanEventDbModel<'a> = DbModel<'a, SpanEvent>;
type EventDbModel<'a> = DbModel<'a, Event>;

#[derive(Debug, Copy, Clone)]
struct TimestampKey(Timestamp);

impl RedbValue for TimestampKey {
    type SelfType<'a> = TimestampKey;

    type AsBytes<'a> = [u8; 8];

    fn fixed_width() -> Option<usize> {
        Some(8)
    }

    fn from_bytes<'a>(data: &'a [u8]) -> TimestampKey
    where
        Self: 'a,
    {
        let timestamp = <[u8; 8]>::try_from(data).unwrap();
        let timestamp = u64::from_be_bytes(timestamp);
        let timestamp = Timestamp::try_from(timestamp).unwrap();
        TimestampKey(timestamp)
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a TimestampKey) -> [u8; 8]
    where
        Self: 'b,
    {
        value.0.get().to_be_bytes()
    }
}

impl Key for TimestampKey {
    fn compare(data1: &[u8], data2: &[u8]) -> Ordering {
        Ord::cmp(data1, data2)
    }
}

#[allow(unused)]
#[derive(Debug)]
enum FileStorageError {
    Table(TableError),
    Storage(redb::StorageError),
    Transaction(TransactionError),
    Commit(CommitError),
    Deserialize(RkyvError),
    NotFound,
}

impl From<FileStorageError> for StorageError {
    fn from(value: FileStorageError) -> Self {
        StorageError::Internal(format!("{value:?}"))
    }
}

/// This storage holds all entities in an SQLite database at the provided path.
#[derive(Clone)]
pub struct FileStorage {
    database: Arc<Database>,
    index_state: IndexState,
}

impl FileStorage {
    pub fn new(path: &Path) -> FileStorage {
        let database = Database::create(path).unwrap();

        let tx = database.begin_write().unwrap();

        tx.open_table::<u32, MetaDbModel>(TableDefinition::new("meta"))
            .unwrap();
        tx.open_table::<u32, Vec<u8>>(TableDefinition::new("indexes"))
            .unwrap();
        tx.open_table::<TimestampKey, ResourceDbModel>(TableDefinition::new("resources"))
            .unwrap();
        tx.open_table::<TimestampKey, SpanDbModel>(TableDefinition::new("spans"))
            .unwrap();
        tx.open_table::<TimestampKey, SpanEventDbModel>(TableDefinition::new("span_events"))
            .unwrap();
        tx.open_table::<TimestampKey, EventDbModel>(TableDefinition::new("events"))
            .unwrap();

        tx.commit().unwrap();

        FileStorage {
            database: Arc::new(database),
            index_state: IndexState::Fresh,
        }
    }

    fn invalidate_indexes(&mut self) -> Result<(), FileStorageError> {
        if self.index_state == IndexState::Fresh {
            let mut maybe_meta = self
                .database
                .begin_read()
                .map_err(FileStorageError::Transaction)?
                .open_table::<u32, MetaDbModel>(TableDefinition::new("meta"))
                .map_err(FileStorageError::Table)?
                .get(1)
                .map_err(FileStorageError::Storage)?;

            let mut meta = maybe_meta
                .as_mut()
                .map(|guard| guard.value().to_unarchived())
                .transpose()
                .map_err(FileStorageError::Deserialize)?
                .unwrap_or_default();

            meta.indexes = IndexState::Stale;

            let tx = self
                .database
                .begin_write()
                .map_err(FileStorageError::Transaction)?;

            tx.open_table::<u32, MetaDbModel>(TableDefinition::new("meta"))
                .map_err(FileStorageError::Table)?
                .insert(1, MetaDbModel::from_unarchived(&meta))
                .map_err(FileStorageError::Storage)?;

            tx.commit().map_err(FileStorageError::Commit)?;

            self.index_state = IndexState::Stale;
        }

        Ok(())
    }
}

impl Storage for FileStorage {
    #[instrument(level = tracing::Level::TRACE, skip_all)]
    fn get_resource(&self, at: Timestamp) -> Result<Arc<Resource>, StorageError> {
        let resource = self
            .database
            .begin_read()
            .map_err(FileStorageError::Transaction)?
            .open_table::<TimestampKey, ResourceDbModel>(TableDefinition::new("resources"))
            .map_err(FileStorageError::Table)?
            .get(TimestampKey(at))
            .map_err(FileStorageError::Storage)?
            .ok_or(FileStorageError::NotFound)?
            .value()
            .to_unarchived()
            .map_err(FileStorageError::Deserialize)?;

        Ok(Arc::new(resource))
    }

    #[instrument(level = tracing::Level::TRACE, skip_all)]
    fn get_span(&self, at: Timestamp) -> Result<Arc<Span>, StorageError> {
        let span = self
            .database
            .begin_read()
            .map_err(FileStorageError::Transaction)?
            .open_table::<TimestampKey, SpanDbModel>(TableDefinition::new("spans"))
            .map_err(FileStorageError::Table)?
            .get(TimestampKey(at))
            .map_err(FileStorageError::Storage)?
            .ok_or(FileStorageError::NotFound)?
            .value()
            .to_unarchived()
            .map_err(FileStorageError::Deserialize)?;

        Ok(Arc::new(span))
    }

    #[instrument(level = tracing::Level::TRACE, skip_all)]
    fn get_span_event(&self, at: Timestamp) -> Result<Arc<SpanEvent>, StorageError> {
        let span_event = self
            .database
            .begin_read()
            .map_err(FileStorageError::Transaction)?
            .open_table::<TimestampKey, SpanEventDbModel>(TableDefinition::new("span_events"))
            .map_err(FileStorageError::Table)?
            .get(TimestampKey(at))
            .map_err(FileStorageError::Storage)?
            .ok_or(FileStorageError::NotFound)?
            .value()
            .to_unarchived()
            .map_err(FileStorageError::Deserialize)?;

        Ok(Arc::new(span_event))
    }

    #[instrument(level = tracing::Level::TRACE, skip_all)]
    fn get_event(&self, at: Timestamp) -> Result<Arc<Event>, StorageError> {
        let event = self
            .database
            .begin_read()
            .map_err(FileStorageError::Transaction)?
            .open_table::<TimestampKey, EventDbModel>(TableDefinition::new("events"))
            .map_err(FileStorageError::Table)?
            .get(TimestampKey(at))
            .map_err(FileStorageError::Storage)?
            .ok_or(FileStorageError::NotFound)?
            .value()
            .to_unarchived()
            .map_err(FileStorageError::Deserialize)?;

        Ok(Arc::new(event))
    }

    #[instrument(level = tracing::Level::TRACE, skip_all)]
    fn get_all_resources(&self) -> Result<StorageIter<'_, Resource>, StorageError> {
        let resources = self
            .database
            .begin_read()
            .map_err(FileStorageError::Transaction)?
            .open_table::<TimestampKey, ResourceDbModel>(TableDefinition::new("resources"))
            .map_err(FileStorageError::Table)?
            .range::<TimestampKey>(..)
            .map_err(FileStorageError::Storage)?
            .map(|result| {
                result
                    .map_err(FileStorageError::Storage)
                    .and_then(|(_key, resource)| {
                        resource
                            .value()
                            .to_unarchived()
                            .map_err(FileStorageError::Deserialize)
                    })
                    .map(Arc::new)
                    .map_err(StorageError::from)
            })
            .collect::<Vec<_>>();

        Ok(Box::new(resources.into_iter()))
    }

    #[instrument(level = tracing::Level::TRACE, skip_all)]
    fn get_all_spans(&self) -> Result<StorageIter<'_, Span>, StorageError> {
        let spans = self
            .database
            .begin_read()
            .map_err(FileStorageError::Transaction)?
            .open_table::<TimestampKey, SpanDbModel>(TableDefinition::new("spans"))
            .map_err(FileStorageError::Table)?
            .range::<TimestampKey>(..)
            .map_err(FileStorageError::Storage)?
            .map(|result| {
                result
                    .map_err(FileStorageError::Storage)
                    .and_then(|(_key, span)| {
                        span.value()
                            .to_unarchived()
                            .map_err(FileStorageError::Deserialize)
                    })
                    .map(Arc::new)
                    .map_err(StorageError::from)
            })
            .collect::<Vec<_>>();

        Ok(Box::new(spans.into_iter()))
    }

    #[instrument(level = tracing::Level::TRACE, skip_all)]
    fn get_all_span_events(&self) -> Result<StorageIter<'_, SpanEvent>, StorageError> {
        let span_events = self
            .database
            .begin_read()
            .map_err(FileStorageError::Transaction)?
            .open_table::<TimestampKey, SpanEventDbModel>(TableDefinition::new("span_events"))
            .map_err(FileStorageError::Table)?
            .range::<TimestampKey>(..)
            .map_err(FileStorageError::Storage)?
            .map(|result| {
                result
                    .map_err(FileStorageError::Storage)
                    .and_then(|(_key, span_event)| {
                        span_event
                            .value()
                            .to_unarchived()
                            .map_err(FileStorageError::Deserialize)
                    })
                    .map(Arc::new)
                    .map_err(StorageError::from)
            })
            .collect::<Vec<_>>();

        Ok(Box::new(span_events.into_iter()))
    }

    #[instrument(level = tracing::Level::TRACE, skip_all)]
    fn get_all_events(&self) -> Result<StorageIter<'_, Event>, StorageError> {
        let events = self
            .database
            .begin_read()
            .map_err(FileStorageError::Transaction)?
            .open_table::<TimestampKey, EventDbModel>(TableDefinition::new("events"))
            .map_err(FileStorageError::Table)?
            .range::<TimestampKey>(..)
            .map_err(FileStorageError::Storage)?
            .map(|result| {
                result
                    .map_err(FileStorageError::Storage)
                    .and_then(|(_key, event)| {
                        event
                            .value()
                            .to_unarchived()
                            .map_err(FileStorageError::Deserialize)
                    })
                    .map(Arc::new)
                    .map_err(StorageError::from)
            })
            .collect::<Vec<_>>();

        Ok(Box::new(events.into_iter()))
    }

    #[instrument(level = tracing::Level::TRACE, skip_all)]
    fn insert_resource(&mut self, resource: Resource) -> Result<(), StorageError> {
        self.invalidate_indexes()?;

        let tx = self
            .database
            .begin_write()
            .map_err(FileStorageError::Transaction)?;

        tx.open_table::<TimestampKey, ResourceDbModel>(TableDefinition::new("resources"))
            .map_err(FileStorageError::Table)?
            .insert(
                TimestampKey(resource.key()),
                ResourceDbModel::from_unarchived(&resource),
            )
            .map_err(FileStorageError::Storage)?;

        tx.commit().map_err(FileStorageError::Commit)?;

        Ok(())
    }

    #[instrument(level = tracing::Level::TRACE, skip_all)]
    fn insert_span(&mut self, span: Span) -> Result<(), StorageError> {
        self.invalidate_indexes()?;

        let tx = self
            .database
            .begin_write()
            .map_err(FileStorageError::Transaction)?;

        tx.open_table::<TimestampKey, SpanDbModel>(TableDefinition::new("spans"))
            .map_err(FileStorageError::Table)?
            .insert(
                TimestampKey(span.key()),
                SpanDbModel::from_unarchived(&span),
            )
            .map_err(FileStorageError::Storage)?;

        tx.commit().map_err(FileStorageError::Commit)?;

        Ok(())
    }

    #[instrument(level = tracing::Level::TRACE, skip_all)]
    fn insert_span_event(&mut self, span_event: SpanEvent) -> Result<(), StorageError> {
        self.invalidate_indexes()?;

        let tx = self
            .database
            .begin_write()
            .map_err(FileStorageError::Transaction)?;

        tx.open_table::<TimestampKey, SpanEventDbModel>(TableDefinition::new("span_events"))
            .map_err(FileStorageError::Table)?
            .insert(
                TimestampKey(span_event.timestamp),
                SpanEventDbModel::from_unarchived(&span_event),
            )
            .map_err(FileStorageError::Storage)?;

        tx.commit().map_err(FileStorageError::Commit)?;

        Ok(())
    }

    #[instrument(level = tracing::Level::TRACE, skip_all)]
    fn insert_event(&mut self, event: Event) -> Result<(), StorageError> {
        self.invalidate_indexes()?;

        let tx = self
            .database
            .begin_write()
            .map_err(FileStorageError::Transaction)?;

        // tx.set_durability(Durability::None).unwrap();

        tx.open_table::<TimestampKey, EventDbModel>(TableDefinition::new("events"))
            .map_err(FileStorageError::Table)?
            .insert(
                TimestampKey(event.key()),
                EventDbModel::from_unarchived(&event),
            )
            .map_err(FileStorageError::Storage)?;

        tx.commit().map_err(FileStorageError::Commit)?;

        Ok(())
    }

    #[instrument(level = tracing::Level::TRACE, skip_all)]
    fn update_span_closed(
        &mut self,
        at: Timestamp,
        closed: Timestamp,
        busy: Option<u64>,
    ) -> Result<(), StorageError> {
        self.invalidate_indexes()?;

        let mut span = Arc::unwrap_or_clone(self.get_span(at)?);

        span.closed_at = Some(closed);
        span.busy = busy;

        self.insert_span(span)?;

        Ok(())
    }

    #[instrument(level = tracing::Level::TRACE, skip_all)]
    fn update_span_attributes(
        &mut self,
        at: Timestamp,
        attributes: BTreeMap<String, Value>,
    ) -> Result<(), StorageError> {
        self.invalidate_indexes()?;

        let mut span = Arc::unwrap_or_clone(self.get_span(at)?);

        span.attributes.extend(attributes);

        self.insert_span(span)?;

        Ok(())
    }

    #[instrument(level = tracing::Level::TRACE, skip_all)]
    fn update_span_link(
        &mut self,
        at: Timestamp,
        link: FullSpanId,
        attributes: BTreeMap<String, Value>,
    ) -> Result<(), StorageError> {
        self.invalidate_indexes()?;

        let mut span = Arc::unwrap_or_clone(self.get_span(at)?);

        span.links.push((link, attributes));

        self.insert_span(span)?;

        Ok(())
    }

    #[instrument(level = tracing::Level::TRACE, skip_all)]
    fn update_span_parents(
        &mut self,
        parent_key: SpanKey,
        spans: &[SpanKey],
    ) -> Result<(), StorageError> {
        self.invalidate_indexes()?;

        for span_key in spans {
            let mut span = Arc::unwrap_or_clone(self.get_span(*span_key)?);

            span.parent_key = Some(parent_key);

            self.insert_span(span)?;
        }

        Ok(())
    }

    #[instrument(level = tracing::Level::TRACE, skip_all)]
    fn update_event_parents(
        &mut self,
        parent_key: SpanKey,
        events: &[EventKey],
    ) -> Result<(), StorageError> {
        self.invalidate_indexes()?;

        for event_key in events {
            let mut event = Arc::unwrap_or_clone(self.get_event(*event_key)?);

            event.parent_key = Some(parent_key);

            self.insert_event(event)?;
        }

        Ok(())
    }

    #[instrument(level = tracing::Level::TRACE, skip_all)]
    fn drop_resources(&mut self, resources: &[Timestamp]) -> Result<(), StorageError> {
        let tx = self
            .database
            .begin_write()
            .map_err(FileStorageError::Transaction)?;

        let mut table = tx
            .open_table::<TimestampKey, ResourceDbModel>(TableDefinition::new("resources"))
            .map_err(FileStorageError::Table)?;

        for resource_key in resources {
            table
                .remove(TimestampKey(*resource_key))
                .map_err(FileStorageError::Storage)?;
        }

        drop(table);
        tx.commit().map_err(FileStorageError::Commit)?;

        Ok(())
    }

    #[instrument(level = tracing::Level::TRACE, skip_all)]
    fn drop_spans(&mut self, spans: &[Timestamp]) -> Result<(), StorageError> {
        let tx = self
            .database
            .begin_write()
            .map_err(FileStorageError::Transaction)?;

        let mut table = tx
            .open_table::<TimestampKey, SpanDbModel>(TableDefinition::new("spans"))
            .map_err(FileStorageError::Table)?;

        for span_key in spans {
            table
                .remove(TimestampKey(*span_key))
                .map_err(FileStorageError::Storage)?;
        }

        drop(table);
        tx.commit().map_err(FileStorageError::Commit)?;

        Ok(())
    }

    #[instrument(level = tracing::Level::TRACE, skip_all)]
    fn drop_span_events(&mut self, span_events: &[Timestamp]) -> Result<(), StorageError> {
        let tx = self
            .database
            .begin_write()
            .map_err(FileStorageError::Transaction)?;

        let mut table = tx
            .open_table::<TimestampKey, SpanEventDbModel>(TableDefinition::new("span_events"))
            .map_err(FileStorageError::Table)?;

        for span_event_key in span_events {
            table
                .remove(TimestampKey(*span_event_key))
                .map_err(FileStorageError::Storage)?;
        }

        drop(table);
        tx.commit().map_err(FileStorageError::Commit)?;

        Ok(())
    }

    #[instrument(level = tracing::Level::TRACE, skip_all)]
    fn drop_events(&mut self, events: &[Timestamp]) -> Result<(), StorageError> {
        let tx = self
            .database
            .begin_write()
            .map_err(FileStorageError::Transaction)?;

        let mut table = tx
            .open_table::<TimestampKey, EventDbModel>(TableDefinition::new("events"))
            .map_err(FileStorageError::Table)?;

        for event_key in events {
            table
                .remove(TimestampKey(*event_key))
                .map_err(FileStorageError::Storage)?;
        }

        drop(table);
        tx.commit().map_err(FileStorageError::Commit)?;

        Ok(())
    }

    fn sync(&mut self) -> Result<StorageSyncStatus, StorageError> {
        // we sync on each commit, nothing to do
        Ok(StorageSyncStatus::Synced)
    }

    #[allow(private_interfaces)]
    fn as_index_storage(&self) -> Option<&dyn IndexStorage> {
        Some(self)
    }

    #[allow(private_interfaces)]
    fn as_index_storage_mut(&mut self) -> Option<&mut dyn IndexStorage> {
        Some(self)
    }
}

impl Batch for FileStorage {
    fn batch(
        &mut self,
        resources: &mut dyn Iterator<Item = &BatchAction<Arc<Resource>>>,
        spans: &mut dyn Iterator<Item = &BatchAction<Arc<Span>>>,
        span_events: &mut dyn Iterator<Item = &BatchAction<Arc<SpanEvent>>>,
        events: &mut dyn Iterator<Item = &BatchAction<Arc<Event>>>,
    ) -> Result<(), StorageError> {
        let tx = self
            .database
            .begin_write()
            .map_err(FileStorageError::Transaction)?;

        let mut resources_table = tx
            .open_table::<TimestampKey, ResourceDbModel>(TableDefinition::new("resources"))
            .map_err(FileStorageError::Table)?;
        let mut spans_table = tx
            .open_table::<TimestampKey, SpanDbModel>(TableDefinition::new("spans"))
            .map_err(FileStorageError::Table)?;
        let mut span_events_table = tx
            .open_table::<TimestampKey, SpanEventDbModel>(TableDefinition::new("span_events"))
            .map_err(FileStorageError::Table)?;
        let mut events_table = tx
            .open_table::<TimestampKey, EventDbModel>(TableDefinition::new("events"))
            .map_err(FileStorageError::Table)?;

        for resource in resources {
            match resource {
                BatchAction::Create(resource) | BatchAction::Update(resource) => {
                    resources_table
                        .insert(
                            TimestampKey(resource.key()),
                            ResourceDbModel::from_unarchived(&resource),
                        )
                        .map_err(FileStorageError::Storage)?;
                }
                BatchAction::Delete(resource_key) => {
                    resources_table
                        .remove(TimestampKey(*resource_key))
                        .map_err(FileStorageError::Storage)?;
                }
            }
        }

        for span in spans {
            match span {
                BatchAction::Create(span) | BatchAction::Update(span) => {
                    spans_table
                        .insert(
                            TimestampKey(span.key()),
                            SpanDbModel::from_unarchived(&span),
                        )
                        .map_err(FileStorageError::Storage)?;
                }
                BatchAction::Delete(span_key) => {
                    spans_table
                        .remove(TimestampKey(*span_key))
                        .map_err(FileStorageError::Storage)?;
                }
            }
        }

        for span_event in span_events {
            match span_event {
                BatchAction::Create(span_event) | BatchAction::Update(span_event) => {
                    span_events_table
                        .insert(
                            TimestampKey(span_event.key()),
                            SpanEventDbModel::from_unarchived(&span_event),
                        )
                        .map_err(FileStorageError::Storage)?;
                }
                BatchAction::Delete(span_event_key) => {
                    span_events_table
                        .remove(TimestampKey(*span_event_key))
                        .map_err(FileStorageError::Storage)?;
                }
            }
        }

        for event in events {
            match event {
                BatchAction::Create(event) | BatchAction::Update(event) => {
                    events_table
                        .insert(
                            TimestampKey(event.key()),
                            EventDbModel::from_unarchived(&event),
                        )
                        .map_err(FileStorageError::Storage)?;
                }
                BatchAction::Delete(event_key) => {
                    events_table
                        .remove(TimestampKey(*event_key))
                        .map_err(FileStorageError::Storage)?;
                }
            }
        }

        drop(resources_table);
        drop(spans_table);
        drop(span_events_table);
        drop(events_table);
        tx.commit().map_err(FileStorageError::Commit)?;

        Ok(())
    }
}

impl IndexStorage for FileStorage {
    #[instrument(level = tracing::Level::TRACE, skip_all)]
    fn get_indexes(
        &self,
    ) -> Result<Option<(SpanIndexes, SpanEventIndexes, EventIndexes)>, StorageError> {
        use bincode::{DefaultOptions, Options};

        if self.index_state == IndexState::Stale {
            return Ok(None);
        }

        let index_table = self
            .database
            .begin_read()
            .map_err(FileStorageError::Transaction)?
            .open_table::<u32, Vec<u8>>(TableDefinition::new("indexes"))
            .map_err(FileStorageError::Table)?;

        let span_index_data = index_table
            .get(1)
            .map_err(FileStorageError::Storage)?
            .ok_or(FileStorageError::NotFound)?;

        let span_event_index_data = index_table
            .get(2)
            .map_err(FileStorageError::Storage)?
            .ok_or(FileStorageError::NotFound)?;

        let event_index_data = index_table
            .get(3)
            .map_err(FileStorageError::Storage)?
            .ok_or(FileStorageError::NotFound)?;

        let bincode_options = DefaultOptions::new().with_fixint_encoding();

        let span_indexes = bincode_options
            .deserialize(&span_index_data.value())
            .unwrap();
        let span_event_indexes = bincode_options
            .deserialize(&span_event_index_data.value())
            .unwrap();
        let event_indexes = bincode_options
            .deserialize(&event_index_data.value())
            .unwrap();

        Ok(Some((span_indexes, span_event_indexes, event_indexes)))
    }

    #[instrument(level = tracing::Level::TRACE, skip_all)]
    fn update_indexes(
        &mut self,
        span_indexes: &SpanIndexes,
        span_event_indexes: &SpanEventIndexes,
        event_indexes: &EventIndexes,
    ) -> Result<(), StorageError> {
        use bincode::{DefaultOptions, Options};

        let bincode_options = DefaultOptions::new().with_fixint_encoding();

        let span_index_data = bincode_options.serialize(span_indexes).unwrap();
        let span_event_index_data = bincode_options.serialize(span_event_indexes).unwrap();
        let event_index_data = bincode_options.serialize(event_indexes).unwrap();

        let tx = self
            .database
            .begin_write()
            .map_err(FileStorageError::Transaction)?;

        let mut index_table = tx
            .open_table::<u32, Vec<u8>>(TableDefinition::new("indexes"))
            .map_err(FileStorageError::Table)?;

        index_table
            .insert(1, span_index_data)
            .map_err(FileStorageError::Storage)?;

        index_table
            .insert(2, span_event_index_data)
            .map_err(FileStorageError::Storage)?;

        index_table
            .insert(3, event_index_data)
            .map_err(FileStorageError::Storage)?;

        // also update metadata
        tx.open_table::<u32, MetaDbModel>(TableDefinition::new("meta"))
            .map_err(FileStorageError::Table)?
            .insert(
                1,
                MetaDbModel::from_unarchived(&Meta {
                    indexes: IndexState::Fresh,
                }),
            )
            .map_err(FileStorageError::Storage)?;

        drop(index_table);
        tx.commit().map_err(FileStorageError::Commit)?;

        Ok(())
    }
}
