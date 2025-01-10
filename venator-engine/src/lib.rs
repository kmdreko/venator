//! The "engine" crate represents the core functionality to injest, store,
//! index, and query the events and spans. It does not provide functionality
//! outside of its Rust API.

mod context;
pub mod engine;
mod filter;
mod index;
mod models;
pub mod storage;
mod subscription;

use storage::Storage;

pub use filter::input::{
    FilterPredicate, FilterPredicateSingle, FilterPropertyKind, ValuePredicate,
};
pub use filter::{
    BasicEventFilter, BasicSpanFilter, FallibleFilterPredicate, InputError, Order, Query,
};
pub use models::{
    AncestorView, AttributeSourceView, AttributeTypeView, AttributeView, CreateSpanEvent,
    DeleteFilter, DeleteMetrics, EngineStatusView, Event, EventKey, EventView, FullSpanId,
    InstanceId, Level, LevelConvertError, NewCloseSpanEvent, NewCreateSpanEvent, NewEnterSpanEvent,
    NewEvent, NewFollowsSpanEvent, NewResource, NewSpanEvent, NewSpanEventKind, NewUpdateSpanEvent,
    Resource, ResourceKey, SourceKind, Span, SpanEvent, SpanEventKey, SpanEventKind, SpanId,
    SpanKey, SpanView, StatsView, Timestamp, TraceId, TraceRoot, UpdateSpanEvent, Value,
    ValueOperator,
};
pub use subscription::{SubscriptionId, SubscriptionResponse};
