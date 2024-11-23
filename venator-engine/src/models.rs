use std::collections::BTreeMap;
use std::fmt::{Display, Error as FmtError, Formatter};
use std::num::NonZeroU64;

use serde::{Deserialize, Serialize};

pub type Timestamp = NonZeroU64;

/// This is the internal type used to identify connections. The value is the
/// unique timestamp from when the connection was created.
pub type ConnectionKey = NonZeroU64;

/// This is the external type used to identity an connection. This is generated
/// client-side and should be random to make it unique.
pub type ConnectionId = u64;

/// This is the internal type used to identify spans. The value is the unique
/// timestamp from when the span was created.
pub type SpanKey = NonZeroU64;

/// This is the internal type used to identify span eventss. The value is the
/// semi-unique timestamp from when the span event was created. "Semi-unique"
/// because the "create" event shares a timestamp with the span it creates.
pub type SpanEventKey = NonZeroU64;

/// This is the internal type used to identify events. The value is the unique
/// timestamp from when the event was created.
pub type EventKey = NonZeroU64;

/// This is the external type used to identity a span. This is generated client-
/// side and is unique but only within that connection.
pub type SpanId = NonZeroU64;

pub type ConnectionIdView = String;
pub type FullSpanIdView = String;

pub type FullSpanId = (ConnectionId, SpanId);

pub type SubscriptionId = usize;

pub fn parse_full_span_id(s: &str) -> Option<FullSpanId> {
    let (connection_id, span_id) = s.split_once('-')?;
    let connection_id: ConnectionId = connection_id.parse().ok()?;
    let span_id: SpanId = span_id.parse().ok()?;

    Some((connection_id, span_id))
}

#[derive(
    Debug, Copy, Clone, PartialEq, Eq, serde_repr::Serialize_repr, serde_repr::Deserialize_repr,
)]
#[repr(i32)]
pub enum Level {
    Trace = 0,
    Debug = 1,
    Info = 2,
    Warn = 3,
    Error = 4,
}

impl TryFrom<i32> for Level {
    type Error = ();

    fn try_from(value: i32) -> Result<Self, ()> {
        match value {
            0 => Ok(Level::Trace),
            1 => Ok(Level::Debug),
            2 => Ok(Level::Info),
            3 => Ok(Level::Warn),
            4 => Ok(Level::Error),
            _ => Err(()),
        }
    }
}

pub struct NewConnection {
    pub id: ConnectionId,
    pub fields: BTreeMap<String, Value>,
}

#[derive(Clone)]
pub struct Connection {
    pub id: ConnectionId,
    pub connected_at: Timestamp,
    pub disconnected_at: Option<Timestamp>,
    pub fields: BTreeMap<String, Value>,
}

impl Connection {
    pub fn key(&self) -> ConnectionKey {
        self.connected_at
    }

    // gets the duration of the span in microseconds if disconnected
    pub fn duration(&self) -> Option<u64> {
        self.disconnected_at.map(|disconnected_at| {
            disconnected_at
                .get()
                .saturating_sub(self.connected_at.get())
        })
    }
}

#[derive(Serialize)]
pub struct ConnectionView {
    pub id: ConnectionIdView,
    pub connected_at: Timestamp,
    pub disconnected_at: Option<Timestamp>,
    pub attributes: Vec<AttributeView>,
}

pub struct NewSpanEvent {
    pub connection_key: ConnectionKey,
    pub timestamp: Timestamp,
    pub span_id: SpanId,
    pub kind: NewSpanEventKind,
}

pub enum NewSpanEventKind {
    Create(NewCreateSpanEvent),
    Update(NewUpdateSpanEvent),
    Follows(NewFollowsSpanEvent),
    Enter,
    Exit,
    Close,
}

#[derive(Clone)]
pub struct SpanEvent {
    pub connection_key: ConnectionKey,
    pub timestamp: Timestamp,
    pub span_key: SpanKey,
    pub kind: SpanEventKind,
}

#[derive(Clone)]
pub enum SpanEventKind {
    Create(CreateSpanEvent),
    Update(UpdateSpanEvent),
    Follows(FollowsSpanEvent),
    Enter,
    Exit,
    Close,
}

pub struct NewCreateSpanEvent {
    pub parent_id: Option<SpanId>,
    pub target: String,
    pub name: String,
    pub level: i32,
    pub file_name: Option<String>,
    pub file_line: Option<u32>,
    pub fields: BTreeMap<String, Value>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct CreateSpanEvent {
    pub parent_key: Option<SpanKey>,
    pub target: String,
    pub name: String,
    pub level: Level,
    pub file_name: Option<String>,
    pub file_line: Option<u32>,
    pub fields: BTreeMap<String, Value>,
}

pub struct NewUpdateSpanEvent {
    pub fields: BTreeMap<String, Value>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct UpdateSpanEvent {
    pub fields: BTreeMap<String, Value>,
}

pub struct NewFollowsSpanEvent {
    pub follows: SpanId,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct FollowsSpanEvent {
    pub follows: SpanKey,
}

pub struct NewEvent {
    pub connection_key: ConnectionKey,
    pub timestamp: Timestamp,
    pub span_id: Option<SpanId>,
    pub name: String,
    pub target: String,
    pub level: i32,
    pub file_name: Option<String>,
    pub file_line: Option<u32>,
    pub fields: BTreeMap<String, Value>,
}

#[derive(Clone, Serialize)]
pub struct Event {
    pub connection_key: ConnectionKey,
    pub timestamp: Timestamp,
    pub span_key: Option<SpanKey>,
    pub name: String,
    pub target: String,
    pub level: Level,
    pub file_name: Option<String>,
    pub file_line: Option<u32>,
    pub fields: BTreeMap<String, Value>,
}

impl Event {
    pub fn key(&self) -> EventKey {
        self.timestamp
    }
}

#[derive(Clone, Serialize)]
pub struct EventView {
    pub connection_id: ConnectionIdView,
    pub ancestors: Vec<AncestorView>, // in root-first order
    pub timestamp: Timestamp,
    pub target: String,
    pub name: String,
    pub level: i32,
    pub file: Option<String>,
    pub attributes: Vec<AttributeView>,
}

#[derive(Debug, Clone, Serialize)]
pub struct Span {
    pub connection_key: ConnectionKey,
    pub id: SpanId,
    pub created_at: Timestamp,
    pub closed_at: Option<Timestamp>,
    pub parent_key: Option<SpanKey>,
    pub follows: Vec<SpanKey>,
    pub target: String,
    pub name: String,
    pub level: Level,
    pub file_name: Option<String>,
    pub file_line: Option<u32>,
    pub fields: BTreeMap<String, Value>,
}

impl Span {
    pub fn key(&self) -> SpanKey {
        self.created_at
    }
}

#[derive(Serialize)]
pub struct SpanView {
    pub id: FullSpanIdView,
    pub ancestors: Vec<AncestorView>, // in root-first order
    pub created_at: Timestamp,
    pub closed_at: Option<Timestamp>,
    pub target: String,
    pub name: String,
    pub level: i32,
    pub file: Option<String>,
    pub attributes: Vec<AttributeView>,
}

#[derive(Clone, Serialize)]
pub struct AncestorView {
    pub id: FullSpanIdView,
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub enum Value {
    F64(f64),
    I64(i64),
    U64(u64),
    I128(i128),
    U128(u128),
    Bool(bool),
    Str(String),
}

impl Value {
    pub fn to_type_view(&self) -> AttributeTypeView {
        match self {
            Value::F64(_) => AttributeTypeView::F64,
            Value::I64(_) => AttributeTypeView::I64,
            Value::U64(_) => AttributeTypeView::U64,
            Value::I128(_) => AttributeTypeView::I128,
            Value::U128(_) => AttributeTypeView::U128,
            Value::Bool(_) => AttributeTypeView::Bool,
            Value::Str(_) => AttributeTypeView::String,
        }
    }
}

impl Display for Value {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), FmtError> {
        match self {
            Value::F64(value) => write!(f, "{value}"),
            Value::I64(value) => write!(f, "{value}"),
            Value::U64(value) => write!(f, "{value}"),
            Value::I128(value) => write!(f, "{value}"),
            Value::U128(value) => write!(f, "{value}"),
            Value::Bool(value) => write!(f, "{value}"),
            Value::Str(value) => write!(f, "{value}"),
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ValueOperator {
    Gt,
    Gte,
    Eq,
    Lt,
    Lte,
}

impl ValueOperator {
    pub fn compare<T: PartialOrd>(&self, lhs: T, rhs: T) -> bool {
        match self {
            ValueOperator::Gt => lhs > rhs,
            ValueOperator::Gte => lhs >= rhs,
            ValueOperator::Eq => lhs == rhs,
            ValueOperator::Lt => lhs < rhs,
            ValueOperator::Lte => lhs <= rhs,
        }
    }
}

#[derive(Clone, Serialize)]
pub struct AttributeView {
    pub name: String,
    pub value: String,
    #[serde(rename = "type")]
    pub typ: AttributeTypeView,
    #[serde(flatten)]
    pub source: AttributeSourceView,
}

#[derive(Copy, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AttributeTypeView {
    F64,
    I64,
    U64,
    I128,
    U128,
    Bool,
    String,
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "snake_case", tag = "source")]
pub enum AttributeSourceView {
    Connection { connection_id: ConnectionIdView },
    Span { span_id: FullSpanIdView },
    Inherent,
}

impl Span {
    // gets the duration of the span in microseconds if closed
    pub fn duration(&self) -> Option<u64> {
        self.closed_at
            .map(|closed_at| closed_at.get().saturating_sub(self.created_at.get()))
    }
}

#[derive(Serialize)]
pub struct StatsView {
    pub start: Option<Timestamp>,
    pub end: Option<Timestamp>,
    pub total_spans: usize,
    pub total_events: usize,
}
