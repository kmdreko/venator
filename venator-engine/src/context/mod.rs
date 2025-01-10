//! A "context" provides an entity methods to get its related content (e.g. the
//! `EventContext` can access the event's parent spans and root resource). It is
//! necessary to deduce an event/span's full attribute set among other things.

use std::cell::OnceCell;
use std::sync::Arc;

mod event_context;
mod span_context;

pub(crate) use event_context::EventContext;
pub(crate) use span_context::SpanContext;

enum RefOrDeferredArc<'a, T> {
    Ref(&'a T),
    Deferred(OnceCell<Arc<T>>),
}
