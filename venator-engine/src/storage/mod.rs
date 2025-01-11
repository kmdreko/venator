//! Traits and implementations for storing data in the engine.
//!
//! There are two primary storage implementations: `TransientStorage` that holds
//! all data in memory and `FileStorage` which persists the data in an SQLite
//! database file. Though when using file storage, it is best to wrap it in a
//! `CachedStorage` layer since the engine does not cache data itself and new
//! lookups are often temporally related.
//!
//! Custom implemenations can be created and used with the engine with the only
//! caveat that index persistence is only supported by `FileStorage` - any other
//! implementation must rebuild indexes based on `get_all_*` calls on startup.

use std::collections::BTreeMap;
use std::sync::Arc;

mod cached;
#[cfg(feature = "persist")]
mod file;
mod transient;

use crate::index::{EventIndexes, SpanEventIndexes, SpanIndexes};
use crate::models::{Event, EventKey, Resource, Span, SpanEvent, SpanKey, Timestamp, Value};
use crate::FullSpanId;

pub use cached::CachedStorage;
#[cfg(feature = "persist")]
pub use file::FileStorage;
pub use transient::TransientStorage;

#[derive(Debug)]
pub enum StorageError {
    NotFound,
    Internal(String),
}

pub type StorageIter<'a, T> = Box<dyn Iterator<Item = Result<Arc<T>, StorageError>> + 'a>;

/// This serves as the backing storage of resources, spans, events, and span
/// events.
///
/// An implementation must provide fast lookups for each respective entity based
/// on its "timestamp" (`timestamp` for events and span events, `created_at` for
/// resources and spans).
pub trait Storage {
    fn get_resource(&self, at: Timestamp) -> Result<Arc<Resource>, StorageError>;
    fn get_span(&self, at: Timestamp) -> Result<Arc<Span>, StorageError>;
    fn get_span_event(&self, at: Timestamp) -> Result<Arc<SpanEvent>, StorageError>;
    fn get_event(&self, at: Timestamp) -> Result<Arc<Event>, StorageError>;

    fn get_all_resources(&self) -> Result<StorageIter<'_, Resource>, StorageError>;
    fn get_all_spans(&self) -> Result<StorageIter<'_, Span>, StorageError>;
    fn get_all_span_events(&self) -> Result<StorageIter<'_, SpanEvent>, StorageError>;
    fn get_all_events(&self) -> Result<StorageIter<'_, Event>, StorageError>;

    fn insert_resource(&mut self, resource: Resource) -> Result<(), StorageError>;
    fn insert_span(&mut self, span: Span) -> Result<(), StorageError>;
    fn insert_span_event(&mut self, span_event: SpanEvent) -> Result<(), StorageError>;
    fn insert_event(&mut self, event: Event) -> Result<(), StorageError>;

    fn update_span_closed(
        &mut self,
        at: Timestamp,
        closed: Timestamp,
        busy: Option<u64>,
    ) -> Result<(), StorageError>;
    fn update_span_attributes(
        &mut self,
        at: Timestamp,
        attributes: BTreeMap<String, Value>,
    ) -> Result<(), StorageError>;
    fn update_span_link(
        &mut self,
        at: Timestamp,
        link: FullSpanId,
        attributes: BTreeMap<String, Value>,
    ) -> Result<(), StorageError>;
    fn update_span_parents(
        &mut self,
        parent_key: SpanKey,
        spans: &[SpanKey],
    ) -> Result<(), StorageError>;
    fn update_event_parents(
        &mut self,
        parent_key: SpanKey,
        events: &[EventKey],
    ) -> Result<(), StorageError>;

    fn drop_resources(&mut self, resources: &[Timestamp]) -> Result<(), StorageError>;
    fn drop_spans(&mut self, spans: &[Timestamp]) -> Result<(), StorageError>;
    fn drop_span_events(&mut self, span_events: &[Timestamp]) -> Result<(), StorageError>;
    fn drop_events(&mut self, events: &[Timestamp]) -> Result<(), StorageError>;

    #[doc(hidden)]
    #[allow(private_interfaces)]
    fn as_index_storage(&self) -> Option<&dyn IndexStorage> {
        None
    }

    #[doc(hidden)]
    #[allow(private_interfaces)]
    fn as_index_storage_mut(&mut self) -> Option<&mut dyn IndexStorage> {
        None
    }
}

pub(crate) trait IndexStorage {
    fn get_indexes(&self) -> Option<(SpanIndexes, SpanEventIndexes, EventIndexes)>;
    fn update_indexes(
        &mut self,
        span_indexes: &SpanIndexes,
        span_event_indexes: &SpanEventIndexes,
        event_indexes: &EventIndexes,
    );
}
