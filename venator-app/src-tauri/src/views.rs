use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use venator_engine::filter::{
    FallibleFilterPredicate, FilterPredicateSingle, FilterPropertyKind, InputError, ValuePredicate,
};
use venator_engine::{
    Ancestor, Attribute, AttributeSource, ComposedEvent, ComposedSpan, DatasetStats, DeleteMetrics,
    FullSpanId, Timestamp, Value,
};

pub type FullSpanIdView = String;
pub type SourceKindView = String;
pub type SimpleLevelView = i32;

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct InputView {
    #[serde(flatten)]
    pub result: FilterPredicateResultView,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "input", rename_all = "camelCase")]
pub(crate) enum FilterPredicateResultView {
    Valid(FilterPredicateView),
    Invalid { text: String, error: String },
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(
    tag = "predicate_kind",
    rename_all = "camelCase",
    content = "predicate"
)]
pub(crate) enum FilterPredicateView {
    Single(FilterPredicateSingleView),
    And(Vec<InputView>),
    Or(Vec<InputView>),
}

impl From<Result<FallibleFilterPredicate, (InputError, String)>> for InputView {
    fn from(result: Result<FallibleFilterPredicate, (InputError, String)>) -> Self {
        match result {
            Ok(FallibleFilterPredicate::Single(single)) => InputView {
                result: FilterPredicateResultView::Valid(FilterPredicateView::Single(
                    FilterPredicateSingleView::from(single),
                )),
            },
            Ok(FallibleFilterPredicate::And(predicates)) => InputView {
                result: FilterPredicateResultView::Valid(FilterPredicateView::And(
                    predicates.into_iter().map(InputView::from).collect(),
                )),
            },
            Ok(FallibleFilterPredicate::Or(predicates)) => InputView {
                result: FilterPredicateResultView::Valid(FilterPredicateView::Or(
                    predicates.into_iter().map(InputView::from).collect(),
                )),
            },
            Err((err, text)) => InputView {
                result: FilterPredicateResultView::Invalid {
                    text,
                    error: err.to_string(),
                },
            },
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct FilterPredicateSingleView {
    pub text: String,
    pub property_kind: Option<FilterPropertyKind>,
    pub property: String,
    #[serde(flatten)]
    pub value: ValuePredicate,
}

impl From<FilterPredicateSingle> for FilterPredicateSingleView {
    fn from(inner: FilterPredicateSingle) -> FilterPredicateSingleView {
        FilterPredicateSingleView {
            text: inner.to_string(),
            property_kind: inner.property_kind,
            property: inner.property,
            value: inner.value,
        }
    }
}

#[derive(Clone, Serialize)]
pub struct EventView {
    pub kind: SourceKindView,
    pub ancestors: Vec<AncestorView>, // in root-first order
    pub timestamp: Timestamp,
    pub content: String,
    pub namespace: Option<String>,
    pub function: Option<String>,
    pub level: SimpleLevelView,
    pub file: Option<String>,
    pub attributes: Vec<AttributeView>,
}

impl From<ComposedEvent> for EventView {
    fn from(event: ComposedEvent) -> Self {
        EventView {
            kind: event.kind.to_string(),
            ancestors: event
                .ancestors
                .into_iter()
                .map(AncestorView::from)
                .collect(),
            timestamp: event.timestamp,
            content: event.content.to_string(),
            namespace: event.namespace,
            function: event.function,
            level: event.level as i32,
            file: event.file,
            attributes: event
                .attributes
                .into_iter()
                .map(AttributeView::from)
                .collect(),
        }
    }
}

#[derive(Clone, Serialize)]
pub struct SpanView {
    pub kind: SourceKindView,
    pub id: FullSpanIdView,
    pub ancestors: Vec<AncestorView>, // in root-first order
    pub created_at: Timestamp,
    pub closed_at: Option<Timestamp>,
    pub busy: Option<u64>,
    pub name: String,
    pub namespace: Option<String>,
    pub function: Option<String>,
    pub level: SimpleLevelView,
    pub file: Option<String>,
    pub links: Vec<LinkView>,
    pub attributes: Vec<AttributeView>,
}

impl From<ComposedSpan> for SpanView {
    fn from(span: ComposedSpan) -> Self {
        SpanView {
            kind: span.kind.to_string(),
            id: span.id.to_string(),
            ancestors: span.ancestors.into_iter().map(AncestorView::from).collect(),
            created_at: span.created_at,
            closed_at: span.closed_at,
            busy: span.busy,
            name: span.name,
            namespace: span.namespace,
            function: span.function,
            level: span.level as i32,
            file: span.file,
            links: span.links.into_iter().map(LinkView::from).collect(),
            attributes: span
                .attributes
                .into_iter()
                .map(AttributeView::from)
                .collect(),
        }
    }
}

#[derive(Clone, Serialize)]
pub struct AncestorView {
    pub id: FullSpanIdView,
    pub name: String,
}

impl From<Ancestor> for AncestorView {
    fn from(ancestor: Ancestor) -> Self {
        AncestorView {
            id: ancestor.id.to_string(),
            name: ancestor.name,
        }
    }
}

#[derive(Clone, Serialize)]
pub struct AttributeView {
    pub name: String,
    pub value: String,
    #[serde(rename = "type")]
    pub value_type: ValueTypeView,
    #[serde(flatten)]
    pub source: AttributeSourceView,
}

impl From<Attribute> for AttributeView {
    fn from(attribute: Attribute) -> Self {
        AttributeView {
            name: attribute.name,
            value: attribute.value.to_string(),
            value_type: ValueTypeView::from_value(&attribute.value),
            source: attribute.source.into(),
        }
    }
}

#[derive(Clone, Serialize)]
pub struct LinkView {
    span_id: FullSpanIdView,
    attributes: Vec<LinkAttributeView>,
}

impl From<(FullSpanId, BTreeMap<String, Value>)> for LinkView {
    fn from((span_id, attributes): (FullSpanId, BTreeMap<String, Value>)) -> Self {
        LinkView {
            span_id: span_id.to_string(),
            attributes: attributes
                .into_iter()
                .map(LinkAttributeView::from)
                .collect(),
        }
    }
}

#[derive(Clone, Serialize)]
pub struct LinkAttributeView {
    pub name: String,
    pub value: String,
    #[serde(rename = "type")]
    pub value_type: ValueTypeView,
}

impl From<(String, Value)> for LinkAttributeView {
    fn from((name, value): (String, Value)) -> Self {
        LinkAttributeView {
            name,
            value: value.to_string(),
            value_type: ValueTypeView::from_value(&value),
        }
    }
}

#[derive(Copy, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ValueTypeView {
    Null,
    F64,
    I64,
    U64,
    I128,
    U128,
    Bool,
    String,
    Bytes,
    Array,
    Object,
}

impl ValueTypeView {
    fn from_value(value: &Value) -> ValueTypeView {
        match value {
            Value::Null => ValueTypeView::Null,
            Value::F64(_) => ValueTypeView::F64,
            Value::I64(_) => ValueTypeView::I64,
            Value::U64(_) => ValueTypeView::U64,
            Value::I128(_) => ValueTypeView::I128,
            Value::U128(_) => ValueTypeView::U128,
            Value::Bool(_) => ValueTypeView::Bool,
            Value::Str(_) => ValueTypeView::String,
            Value::Bytes(_) => ValueTypeView::Bytes,
            Value::Array(_) => ValueTypeView::Array,
            Value::Object(_) => ValueTypeView::Object,
        }
    }
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "snake_case", tag = "source")]
pub enum AttributeSourceView {
    Resource,
    Span { span_id: String },
    Inherent,
}

impl From<AttributeSource> for AttributeSourceView {
    fn from(source: AttributeSource) -> Self {
        match source {
            AttributeSource::Resource => AttributeSourceView::Resource,
            AttributeSource::Span { span_id } => AttributeSourceView::Span {
                span_id: span_id.to_string(),
            },
            AttributeSource::Inherent => AttributeSourceView::Inherent,
        }
    }
}

#[derive(Serialize)]
pub(crate) struct DatasetStatsView {
    pub start: Option<Timestamp>,
    pub end: Option<Timestamp>,
    pub total_spans: usize,
    pub total_events: usize,
}

impl From<DatasetStats> for DatasetStatsView {
    fn from(stats: DatasetStats) -> Self {
        DatasetStatsView {
            start: stats.start,
            end: stats.end,
            total_spans: stats.total_spans,
            total_events: stats.total_events,
        }
    }
}

#[derive(Serialize)]
pub(crate) struct StatusView {
    pub ingress_message: String,
    pub ingress_error: Option<String>,
    pub ingress_connections: usize,
    pub ingress_bytes_per_second: f64,
    pub dataset_name: String,
    pub engine_load: f64,
}

#[derive(Serialize)]
pub(crate) struct DeleteMetricsView {
    pub spans: usize,
    pub span_events: usize,
    pub events: usize,
}

impl From<DeleteMetrics> for DeleteMetricsView {
    fn from(metrics: DeleteMetrics) -> Self {
        DeleteMetricsView {
            spans: metrics.spans,
            span_events: metrics.span_events,
            events: metrics.events,
        }
    }
}

#[derive(Clone, Serialize)]
#[serde(tag = "kind", content = "entity", rename_all = "snake_case")]
pub(crate) enum SubscriptionResponseView<T> {
    Add(T),
    Remove(Timestamp),
}

#[derive(Default, Serialize, Deserialize)]
pub(crate) struct Session {
    tabs: Vec<SessionTab>,
}

#[derive(Serialize, Deserialize)]
pub(crate) struct SessionTab {
    kind: String,
    start: Timestamp,
    end: Timestamp,
    filter: String,
    columns: Vec<String>,
}
