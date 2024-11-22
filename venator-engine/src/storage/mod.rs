use std::collections::BTreeMap;
use std::sync::Arc;

mod cached;
#[cfg(feature = "persist")]
mod file;
mod transient;

use crate::models::{Connection, Event, Span, SpanEvent, Timestamp, Value};
use crate::SpanKey;

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
    fn get_connection(&self, at: Timestamp) -> Option<Arc<Connection>>;
    fn get_span(&self, at: Timestamp) -> Option<Arc<Span>>;
    fn get_span_event(&self, at: Timestamp) -> Option<Arc<SpanEvent>>;
    fn get_event(&self, at: Timestamp) -> Option<Arc<Event>>;

    fn get_all_connections(&self) -> Box<dyn Iterator<Item = Arc<Connection>> + '_>;
    fn get_all_spans(&self) -> Box<dyn Iterator<Item = Arc<Span>> + '_>;
    fn get_all_span_events(&self) -> Box<dyn Iterator<Item = Arc<SpanEvent>> + '_>;
    fn get_all_events(&self) -> Box<dyn Iterator<Item = Arc<Event>> + '_>;

    fn insert_connection(&mut self, connection: Connection);
    fn insert_span(&mut self, span: Span);
    fn insert_span_event(&mut self, span_event: SpanEvent);
    fn insert_event(&mut self, event: Event);

    fn update_connection_disconnected(&mut self, at: Timestamp, disconnected: Timestamp);
    fn update_span_closed(&mut self, at: Timestamp, closed: Timestamp);
    fn update_span_fields(&mut self, at: Timestamp, fields: BTreeMap<String, Value>);
    fn update_span_follows(&mut self, at: Timestamp, follows: SpanKey);

    fn drop_connections(&mut self, connections: &[Timestamp]);
    fn drop_spans(&mut self, spans: &[Timestamp]);
    fn drop_span_events(&mut self, span_events: &[Timestamp]);
    fn drop_events(&mut self, events: &[Timestamp]);
}
