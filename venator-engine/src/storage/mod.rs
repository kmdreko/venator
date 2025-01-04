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

/// This reflects the backing storage of spans, events, and span events. This
/// (currently) does not store indexes; they are re-created on startup.
///
/// This interface enforces that elements are directly accessible by their
/// `timestamp` (`created_at` for spans) and that those timestamps are unique.
///
/// The *get all* methods are used to load on startup, and backfill new indexes.
pub trait Storage {
    fn get_resource(&self, at: Timestamp) -> Option<Arc<Resource>>;
    fn get_span(&self, at: Timestamp) -> Option<Arc<Span>>;
    fn get_span_event(&self, at: Timestamp) -> Option<Arc<SpanEvent>>;
    fn get_event(&self, at: Timestamp) -> Option<Arc<Event>>;
    fn get_indexes(&self) -> Option<(SpanIndexes, SpanEventIndexes, EventIndexes)>;

    fn get_all_resources(&self) -> Box<dyn Iterator<Item = Arc<Resource>> + '_>;
    fn get_all_spans(&self) -> Box<dyn Iterator<Item = Arc<Span>> + '_>;
    fn get_all_span_events(&self) -> Box<dyn Iterator<Item = Arc<SpanEvent>> + '_>;
    fn get_all_events(&self) -> Box<dyn Iterator<Item = Arc<Event>> + '_>;

    fn insert_resource(&mut self, resource: Resource);
    fn insert_span(&mut self, span: Span);
    fn insert_span_event(&mut self, span_event: SpanEvent);
    fn insert_event(&mut self, event: Event);

    fn update_span_closed(&mut self, at: Timestamp, closed: Timestamp, busy: Option<u64>);
    fn update_span_fields(&mut self, at: Timestamp, fields: BTreeMap<String, Value>);
    fn update_span_link(
        &mut self,
        at: Timestamp,
        link: FullSpanId,
        fields: BTreeMap<String, Value>,
    );
    fn update_span_parents(&mut self, parent_key: SpanKey, spans: &[SpanKey]);
    fn update_event_parents(&mut self, parent_key: SpanKey, events: &[EventKey]);
    fn update_indexes(
        &mut self,
        span_indexes: &SpanIndexes,
        span_event_indexes: &SpanEventIndexes,
        event_indexes: &EventIndexes,
    );

    fn drop_resources(&mut self, resources: &[Timestamp]);
    fn drop_spans(&mut self, spans: &[Timestamp]);
    fn drop_span_events(&mut self, span_events: &[Timestamp]);
    fn drop_events(&mut self, events: &[Timestamp]);
}
