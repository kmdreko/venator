use std::collections::BTreeMap;
use std::ops::Deref;

#[cfg(feature = "persist")]
mod file;
mod transient;

use crate::models::{Connection, Event, Span, SpanEvent, Timestamp, Value};
use crate::SpanKey;

#[cfg(feature = "persist")]
pub use file::FileStorage;
pub use transient::TransientStorage;

/// Stands for "borrowed or owned", a `Cow` without the `Clone` requirement.
pub enum Boo<'a, T> {
    Borrowed(&'a T),
    Owned(T),
}

impl<T> Boo<'_, T>
where
    T: Clone,
{
    pub fn into_owned(self) -> T {
        match self {
            Boo::Borrowed(borrow) => borrow.clone(),
            Boo::Owned(own) => own,
        }
    }
}

impl<T> Deref for Boo<'_, T> {
    type Target = T;

    fn deref(&self) -> &T {
        match self {
            Boo::Borrowed(borrow) => borrow,
            Boo::Owned(own) => own,
        }
    }
}

/// This reflects the backing storage of spans, events, and span events. This
/// (currently) does not store indexes; they are re-created on startup.
///
/// This interface enforces that elements are directly accessible by their
/// `timestamp` (`created_at` for spans) and that those timestamps are unique.
///
/// The *get all* methods are used to load on startup, and backfill new indexes.
pub trait Storage {
    fn get_connection(&self, at: Timestamp) -> Option<Boo<'_, Connection>>;
    fn get_span(&self, at: Timestamp) -> Option<Boo<'_, Span>>;
    fn get_span_event(&self, at: Timestamp) -> Option<Boo<'_, SpanEvent>>;
    fn get_event(&self, at: Timestamp) -> Option<Boo<'_, Event>>;

    fn get_all_connections(&self) -> Box<dyn Iterator<Item = Boo<'_, Connection>> + '_>;
    fn get_all_spans(&self) -> Box<dyn Iterator<Item = Boo<'_, Span>> + '_>;
    fn get_all_span_events(&self) -> Box<dyn Iterator<Item = Boo<'_, SpanEvent>> + '_>;
    fn get_all_events(&self) -> Box<dyn Iterator<Item = Boo<'_, Event>> + '_>;
    fn get_all_indexes(&self) -> Box<dyn Iterator<Item = Boo<'_, String>> + '_>;

    fn insert_connection(&mut self, connection: Connection);
    fn insert_span(&mut self, span: Span);
    fn insert_span_event(&mut self, span_event: SpanEvent);
    fn insert_event(&mut self, event: Event);
    fn insert_index(&mut self, name: String);

    fn update_connection_disconnected(&mut self, at: Timestamp, disconnected: Timestamp);
    fn update_span_closed(&mut self, at: Timestamp, closed: Timestamp);
    fn update_span_fields(&mut self, at: Timestamp, fields: BTreeMap<String, Value>);
    fn update_span_follows(&mut self, at: Timestamp, follows: SpanKey);

    fn drop_connections(&mut self, connections: &[Timestamp]);
    fn drop_spans(&mut self, spans: &[Timestamp]);
    fn drop_span_events(&mut self, span_events: &[Timestamp]);
    fn drop_events(&mut self, events: &[Timestamp]);
    fn drop_index(&mut self, name: &str);
}
