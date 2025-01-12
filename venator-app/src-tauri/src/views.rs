use serde::{Deserialize, Serialize};
use venator_engine::filter::{
    FallibleFilterPredicate, FilterPredicateSingle, FilterPropertyKind, InputError, ValuePredicate,
};
use venator_engine::{DeleteMetrics, Timestamp};

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
