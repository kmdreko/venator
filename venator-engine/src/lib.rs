//! The "engine" crate represents the core functionality to injest, store,
//! index, and query the events and spans. It does not provide functionality
//! outside of its Rust API.

mod context;
pub mod engine;
pub mod filter;
mod index;
mod models;
pub mod storage;
mod subscription;

use storage::Storage;

pub use models::{
    Ancestor, Attribute, AttributeSource, ComposedEvent, ComposedSpan, CreateSpanEvent,
    DatasetStats, DeleteFilter, DeleteMetrics, EngineStatus, Event, EventKey, FullSpanId,
    InstanceId, Level, LevelConvertError, NewCloseSpanEvent, NewCreateSpanEvent, NewEnterSpanEvent,
    NewEvent, NewFollowsSpanEvent, NewResource, NewSpanEvent, NewSpanEventKind, NewUpdateSpanEvent,
    Resource, ResourceKey, SourceKind, Span, SpanEvent, SpanEventKey, SpanEventKind, SpanId,
    SpanKey, Timestamp, TraceId, TraceRoot, UpdateSpanEvent, Value, ValueOperator,
};
pub use subscription::{SubscriptionId, SubscriptionResponse};
