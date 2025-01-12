use std::borrow::Cow;
use std::collections::BTreeMap;
use std::fmt::{Display, Error as FmtError, Formatter};
use std::num::NonZeroU64;
use std::str::FromStr;

use serde::de::Error;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

pub type Timestamp = NonZeroU64;

/// This is the internal type used to identify resources. The value is the
/// unique timestamp from when the resource was created.
pub type ResourceKey = NonZeroU64;

/// This is the external type used to identity a tracing instance. This is
/// generated client-side and should be random to make it unique.
pub type InstanceId = u128;

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
/// side and is either unique within that instance (for tracing data) or unique
/// within that trace (for opentelemetry data).
pub type SpanId = u64;

pub type TraceId = u128;

#[derive(Debug)]
pub struct FullSpanIdParseError;

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum FullSpanId {
    Tracing(InstanceId, SpanId),
    Opentelemetry(TraceId, SpanId),
}

impl FromStr for FullSpanId {
    type Err = FullSpanIdParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut iter = s.split('-');
        let kind = iter.next().ok_or(FullSpanIdParseError)?;
        let first = iter.next().ok_or(FullSpanIdParseError)?;
        let second = iter.next().ok_or(FullSpanIdParseError)?;

        match kind {
            "tracing" => {
                let instance = u128::from_str_radix(first, 16).map_err(|_| FullSpanIdParseError)?;
                let span = u64::from_str_radix(second, 16).map_err(|_| FullSpanIdParseError)?;
                Ok(FullSpanId::Tracing(instance, span))
            }
            "otel" => {
                let trace = u128::from_str_radix(first, 16).map_err(|_| FullSpanIdParseError)?;
                let span = u64::from_str_radix(second, 16).map_err(|_| FullSpanIdParseError)?;
                Ok(FullSpanId::Opentelemetry(trace, span))
            }
            _ => Err(FullSpanIdParseError),
        }
    }
}

impl Display for FullSpanId {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), FmtError> {
        match self {
            FullSpanId::Tracing(instance, span) => write!(f, "tracing-{instance:032x}-{span:016x}"),
            FullSpanId::Opentelemetry(trace, span) => write!(f, "otel-{trace:032x}-{span:016x}"),
        }
    }
}

impl Serialize for FullSpanId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'d> Deserialize<'d> for FullSpanId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'d>,
    {
        let s = <Cow<'d, str>>::deserialize(deserializer)?;
        FullSpanId::from_str(&s).map_err(|_| D::Error::custom("invalid full span id"))
    }
}

#[derive(Debug)]
pub struct TraceRootParseError;

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TraceRoot {
    Tracing(InstanceId, SpanId),
    Opentelemetry(TraceId),
}

impl FromStr for TraceRoot {
    type Err = TraceRootParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (kind, rest) = s.split_once('-').ok_or(TraceRootParseError)?;

        match kind {
            "tracing" => {
                let (instance, span) = rest.split_once('-').ok_or(TraceRootParseError)?;
                let instance =
                    u128::from_str_radix(instance, 16).map_err(|_| TraceRootParseError)?;
                let span = u64::from_str_radix(span, 16).map_err(|_| TraceRootParseError)?;
                Ok(TraceRoot::Tracing(instance, span))
            }
            "otel" => {
                let trace = u128::from_str_radix(rest, 16).map_err(|_| TraceRootParseError)?;
                Ok(TraceRoot::Opentelemetry(trace))
            }
            _ => Err(TraceRootParseError),
        }
    }
}

#[derive(Debug)]
pub struct SourceKindConvertError;

#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[repr(i32)]
pub enum SourceKind {
    Tracing,
    Opentelemetry,
}

impl Display for SourceKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), FmtError> {
        match self {
            SourceKind::Tracing => write!(f, "tracing"),
            SourceKind::Opentelemetry => write!(f, "opentelemetry"),
        }
    }
}

impl TryFrom<i32> for SourceKind {
    type Error = SourceKindConvertError;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(SourceKind::Tracing),
            1 => Ok(SourceKind::Opentelemetry),
            _ => Err(SourceKindConvertError),
        }
    }
}

#[derive(Debug)]
pub struct LevelConvertError;

#[derive(
    Debug, Copy, Clone, PartialEq, Eq, serde_repr::Serialize_repr, serde_repr::Deserialize_repr,
)]
#[repr(i32)]
pub enum Level {
    Trace = 1,
    Trace2 = 2,
    Trace3 = 3,
    Trace4 = 4,
    Debug = 5,
    Debug2 = 6,
    Debug3 = 7,
    Debug4 = 8,
    Info = 9,
    Info2 = 10,
    Info3 = 11,
    Info4 = 12,
    Warn = 13,
    Warn2 = 14,
    Warn3 = 15,
    Warn4 = 16,
    Error = 17,
    Error2 = 18,
    Error3 = 19,
    Error4 = 20,
    Fatal = 21,
    Fatal2 = 22,
    Fatal3 = 23,
    Fatal4 = 24,
}

impl Level {
    pub(crate) fn to_db(self) -> i32 {
        self as i32
    }

    pub(crate) fn from_db(value: i32) -> Result<Level, LevelConvertError> {
        Self::from_otel_severity(value)
    }

    pub fn from_tracing_level(level: i32) -> Result<Level, LevelConvertError> {
        match level {
            0 => Ok(Level::Trace),
            1 => Ok(Level::Debug),
            2 => Ok(Level::Info),
            3 => Ok(Level::Warn),
            4 => Ok(Level::Error),
            _ => Err(LevelConvertError),
        }
    }

    pub fn from_otel_severity(severity: i32) -> Result<Level, LevelConvertError> {
        match severity {
            1 => Ok(Level::Trace),
            2 => Ok(Level::Trace2),
            3 => Ok(Level::Trace3),
            4 => Ok(Level::Trace4),
            5 => Ok(Level::Debug),
            6 => Ok(Level::Debug2),
            7 => Ok(Level::Debug3),
            8 => Ok(Level::Debug4),
            9 => Ok(Level::Info),
            10 => Ok(Level::Info2),
            11 => Ok(Level::Info3),
            12 => Ok(Level::Info4),
            13 => Ok(Level::Warn),
            14 => Ok(Level::Warn2),
            15 => Ok(Level::Warn3),
            16 => Ok(Level::Warn4),
            17 => Ok(Level::Error),
            18 => Ok(Level::Error2),
            19 => Ok(Level::Error3),
            20 => Ok(Level::Error4),
            21 => Ok(Level::Fatal),
            22 => Ok(Level::Fatal2),
            23 => Ok(Level::Fatal3),
            24 => Ok(Level::Fatal4),
            _ => Err(LevelConvertError),
        }
    }

    pub fn into_simple_level(self) -> SimpleLevel {
        match self {
            Level::Trace => SimpleLevel::Trace,
            Level::Trace2 => SimpleLevel::Trace,
            Level::Trace3 => SimpleLevel::Trace,
            Level::Trace4 => SimpleLevel::Trace,
            Level::Debug => SimpleLevel::Debug,
            Level::Debug2 => SimpleLevel::Debug,
            Level::Debug3 => SimpleLevel::Debug,
            Level::Debug4 => SimpleLevel::Debug,
            Level::Info => SimpleLevel::Info,
            Level::Info2 => SimpleLevel::Info,
            Level::Info3 => SimpleLevel::Info,
            Level::Info4 => SimpleLevel::Info,
            Level::Warn => SimpleLevel::Warn,
            Level::Warn2 => SimpleLevel::Warn,
            Level::Warn3 => SimpleLevel::Warn,
            Level::Warn4 => SimpleLevel::Warn,
            Level::Error => SimpleLevel::Error,
            Level::Error2 => SimpleLevel::Error,
            Level::Error3 => SimpleLevel::Error,
            Level::Error4 => SimpleLevel::Error,
            Level::Fatal => SimpleLevel::Fatal,
            Level::Fatal2 => SimpleLevel::Fatal,
            Level::Fatal3 => SimpleLevel::Fatal,
            Level::Fatal4 => SimpleLevel::Fatal,
        }
    }
}

#[derive(
    Debug, Copy, Clone, PartialEq, Eq, serde_repr::Serialize_repr, serde_repr::Deserialize_repr,
)]
#[repr(usize)]
pub enum SimpleLevel {
    Trace = 0,
    Debug = 1,
    Info = 2,
    Warn = 3,
    Error = 4,
    Fatal = 5,
}

impl SimpleLevel {
    pub fn iter_gte(self) -> impl Iterator<Item = SimpleLevel> {
        use SimpleLevel::*;

        [Trace, Debug, Info, Warn, Error, Fatal]
            .into_iter()
            .skip(self as usize)
    }
}

#[derive(Debug)]
pub struct NewResource {
    pub attributes: BTreeMap<String, Value>,
}

#[derive(Clone, PartialEq)]
pub struct Resource {
    pub created_at: Timestamp,
    pub attributes: BTreeMap<String, Value>,
}

impl Resource {
    pub fn key(&self) -> ResourceKey {
        self.created_at
    }
}

#[derive(Debug)]
pub struct NewSpanEvent {
    pub timestamp: Timestamp,
    pub span_id: FullSpanId,
    pub kind: NewSpanEventKind,
}

#[derive(Debug)]
pub enum NewSpanEventKind {
    Create(NewCreateSpanEvent),
    Update(NewUpdateSpanEvent),
    Follows(NewFollowsSpanEvent),
    Enter(NewEnterSpanEvent),
    Exit,
    Close(NewCloseSpanEvent),
}

#[derive(Clone)]
pub struct SpanEvent {
    pub timestamp: Timestamp,
    pub span_key: SpanKey,
    pub kind: SpanEventKind,
}

#[derive(Clone)]
pub enum SpanEventKind {
    Create(CreateSpanEvent),
    Update(UpdateSpanEvent),
    Follows(FollowsSpanEvent),
    Enter(EnterSpanEvent),
    Exit,
    Close(CloseSpanEvent),
}

#[derive(Debug)]
pub struct NewCreateSpanEvent {
    pub kind: SourceKind,
    pub resource_key: ResourceKey,
    pub parent_id: Option<FullSpanId>,
    pub name: String,
    pub namespace: Option<String>,
    pub function: Option<String>,
    pub level: Level,
    pub file_name: Option<String>,
    pub file_line: Option<u32>,
    pub file_column: Option<u32>,
    pub instrumentation_attributes: BTreeMap<String, Value>,
    pub attributes: BTreeMap<String, Value>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct CreateSpanEvent {
    pub kind: SourceKind,
    pub resource_key: ResourceKey,
    pub parent_key: Option<SpanKey>,
    pub name: String,
    pub namespace: Option<String>,
    pub function: Option<String>,
    pub level: Level,
    pub file_name: Option<String>,
    pub file_line: Option<u32>,
    pub file_column: Option<u32>,
    pub instrumentation_attributes: BTreeMap<String, Value>,
    pub attributes: BTreeMap<String, Value>,
}

#[derive(Debug)]
pub struct NewUpdateSpanEvent {
    pub attributes: BTreeMap<String, Value>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct UpdateSpanEvent {
    pub attributes: BTreeMap<String, Value>,
}

#[derive(Debug)]
pub struct NewFollowsSpanEvent {
    pub follows: SpanId,
}

#[derive(Debug)]
pub struct NewEnterSpanEvent {
    pub thread_id: u64,
}

#[derive(Debug)]
pub struct NewCloseSpanEvent {
    pub busy: Option<u64>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct FollowsSpanEvent {
    pub follows: SpanKey,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct EnterSpanEvent {
    pub thread_id: u64,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct CloseSpanEvent {
    pub busy: Option<u64>,
}

pub struct NewEvent {
    pub kind: SourceKind,
    pub resource_key: ResourceKey,
    pub timestamp: Timestamp,
    pub span_id: Option<FullSpanId>,
    pub content: Value,
    pub namespace: Option<String>,
    pub function: Option<String>,
    pub level: Level,
    pub file_name: Option<String>,
    pub file_line: Option<u32>,
    pub file_column: Option<u32>,
    pub attributes: BTreeMap<String, Value>,
}

#[derive(Clone)]
pub struct Event {
    pub kind: SourceKind,
    pub resource_key: ResourceKey,
    pub timestamp: Timestamp,
    pub parent_id: Option<FullSpanId>,
    pub parent_key: Option<SpanKey>,
    pub content: Value,
    pub namespace: Option<String>,
    pub function: Option<String>,
    pub level: Level,
    pub file_name: Option<String>,
    pub file_line: Option<u32>,
    pub file_column: Option<u32>,
    pub attributes: BTreeMap<String, Value>,
}

impl Event {
    pub fn key(&self) -> EventKey {
        self.timestamp
    }
}

#[derive(Clone)]
pub struct ComposedEvent {
    pub kind: SourceKind,
    pub ancestors: Vec<Ancestor>, // in root-first order
    pub timestamp: Timestamp,
    pub content: Value,
    pub namespace: Option<String>,
    pub function: Option<String>,
    pub level: SimpleLevel,
    pub file: Option<String>,
    pub attributes: Vec<Attribute>,
}

#[derive(Debug, Clone)]
pub struct Span {
    pub kind: SourceKind,
    pub resource_key: ResourceKey,
    pub id: FullSpanId,
    pub created_at: Timestamp,
    pub closed_at: Option<Timestamp>,
    pub busy: Option<u64>,
    pub parent_id: Option<FullSpanId>,
    pub parent_key: Option<SpanKey>,
    pub links: Vec<(FullSpanId, BTreeMap<String, Value>)>,
    pub name: String,
    pub namespace: Option<String>,
    pub function: Option<String>,
    pub level: Level,
    pub file_name: Option<String>,
    pub file_line: Option<u32>,
    pub file_column: Option<u32>,
    pub instrumentation_attributes: BTreeMap<String, Value>,
    pub attributes: BTreeMap<String, Value>,
}

impl Span {
    pub fn key(&self) -> SpanKey {
        self.created_at
    }

    // gets the duration of the span in microseconds if closed
    pub fn duration(&self) -> Option<u64> {
        self.closed_at
            .map(|closed_at| closed_at.get().saturating_sub(self.created_at.get()))
    }
}

#[derive(Clone)]
pub struct ComposedSpan {
    pub kind: SourceKind,
    pub id: FullSpanId,
    pub ancestors: Vec<Ancestor>, // in root-first order
    pub created_at: Timestamp,
    pub closed_at: Option<Timestamp>,
    pub busy: Option<u64>,
    pub name: String,
    pub namespace: Option<String>,
    pub function: Option<String>,
    pub level: SimpleLevel,
    pub file: Option<String>,
    pub links: Vec<(FullSpanId, BTreeMap<String, Value>)>,
    pub attributes: Vec<Attribute>,
}

#[derive(Clone, Serialize)]
pub struct Ancestor {
    pub id: FullSpanId,
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub enum Value {
    Null,
    F64(f64),
    I64(i64),
    U64(u64),
    I128(i128),
    U128(u128),
    Bool(bool),
    Str(String),
    Bytes(Vec<u8>),
    Array(Vec<Value>),
    Object(BTreeMap<String, Value>),
}

impl Display for Value {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), FmtError> {
        match self {
            Value::Null => write!(f, "NULL"),
            Value::F64(value) => write!(f, "{value}"),
            Value::I64(value) => write!(f, "{value}"),
            Value::U64(value) => write!(f, "{value}"),
            Value::I128(value) => write!(f, "{value}"),
            Value::U128(value) => write!(f, "{value}"),
            Value::Bool(value) => write!(f, "{value}"),
            Value::Str(value) => write!(f, "{value}"),
            Value::Bytes(_) => Ok(()),
            Value::Array(_) => Ok(()),
            Value::Object(_) => Ok(()),
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

#[derive(Clone)]
pub struct Attribute {
    pub name: String,
    pub value: Value,
    pub source: AttributeSource,
}

#[derive(Clone)]
pub enum AttributeSource {
    Resource,
    Span { span_id: FullSpanId },
    Inherent,
}

pub struct DatasetStats {
    pub start: Option<Timestamp>,
    pub end: Option<Timestamp>,
    pub total_spans: usize,
    pub total_events: usize,
}

pub struct DeleteFilter {
    pub start: Timestamp,
    pub end: Timestamp,
    pub inside: bool,
    pub dry_run: bool,
}

pub struct DeleteMetrics {
    pub spans: usize,
    pub span_events: usize,
    pub events: usize,
}

pub struct EngineStatus {
    pub load: f64,
}
