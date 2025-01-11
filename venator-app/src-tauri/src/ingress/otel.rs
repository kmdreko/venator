use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::extract::{FromRequest, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use opentelemetry::proto::collector::logs::v1::logs_service_server::{
    LogsService, LogsServiceServer,
};
use opentelemetry::proto::collector::logs::v1::{
    ExportLogsServiceRequest, ExportLogsServiceResponse,
};
use opentelemetry::proto::collector::metrics::v1::metrics_service_server::{
    MetricsService, MetricsServiceServer,
};
use opentelemetry::proto::collector::metrics::v1::{
    ExportMetricsServiceRequest, ExportMetricsServiceResponse,
};
use opentelemetry::proto::collector::trace::v1::trace_service_server::{
    TraceService, TraceServiceServer,
};
use opentelemetry::proto::collector::trace::v1::{
    ExportTraceServiceRequest, ExportTraceServiceResponse,
};
use opentelemetry::proto::common::v1::{AnyValue, KeyValue};
use prost::bytes::{Bytes, BytesMut};
use tonic::{async_trait, Request, Response, Status};

use venator_engine::engine::AsyncEngine;
use venator_engine::{
    FullSpanId, Level, NewCloseSpanEvent, NewCreateSpanEvent, NewEvent, NewResource, NewSpanEvent,
    NewSpanEventKind, SourceKind, SpanId, Timestamp, TraceId, Value,
};

use super::IngressState;

mod opentelemetry {
    pub mod proto {
        pub mod collector {
            pub mod logs {
                #[path = "../../../../../otel/opentelemetry.proto.collector.logs.v1.rs"]
                pub mod v1;
            }

            pub mod metrics {
                #[path = "../../../../../otel/opentelemetry.proto.collector.metrics.v1.rs"]
                pub mod v1;
            }

            pub mod trace {
                #[path = "../../../../../otel/opentelemetry.proto.collector.trace.v1.rs"]
                pub mod v1;
            }
        }

        pub mod common {
            #[path = "../../../../otel/opentelemetry.proto.common.v1.rs"]
            pub mod v1;
        }

        pub mod logs {
            #[path = "../../../../otel/opentelemetry.proto.logs.v1.rs"]
            pub mod v1;
        }

        pub mod metrics {
            #[path = "../../../../otel/opentelemetry.proto.metrics.v1.rs"]
            pub mod v1;
        }

        pub mod resource {
            #[path = "../../../../otel/opentelemetry.proto.resource.v1.rs"]
            pub mod v1;
        }

        pub mod trace {
            #[path = "../../../../otel/opentelemetry.proto.trace.v1.rs"]
            pub mod v1;
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct Protobuf<T>(pub T);

#[async_trait]
impl<T, S> FromRequest<S> for Protobuf<T>
where
    T: prost::Message + Default,
    S: Send + Sync,
{
    type Rejection = (StatusCode, String);

    async fn from_request(req: axum::extract::Request, state: &S) -> Result<Self, Self::Rejection> {
        let mut bytes = Bytes::from_request(req, state)
            .await
            .map_err(|err| (StatusCode::UNPROCESSABLE_ENTITY, err.to_string()))?;

        match T::decode(&mut bytes) {
            Ok(value) => Ok(Protobuf(value)),
            Err(err) => Err((StatusCode::UNPROCESSABLE_ENTITY, err.to_string())),
        }
    }
}

impl<T> IntoResponse for Protobuf<T>
where
    T: prost::Message + Default,
{
    fn into_response(self) -> axum::response::Response {
        let mut buf = BytesMut::with_capacity(128);
        match &self.0.encode(&mut buf) {
            Ok(()) => buf.into_response(),
            Err(err) => (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()).into_response(),
        }
    }
}

pub(super) struct LogsCollector {
    engine: AsyncEngine,
}

#[async_trait]
impl LogsService for LogsCollector {
    async fn export(
        &self,
        request: Request<ExportLogsServiceRequest>,
    ) -> Result<Response<ExportLogsServiceResponse>, Status> {
        let request = request.get_ref();
        let response = process_logs_request(&self.engine, request).await;
        Ok(Response::new(response))
    }
}

pub(super) struct MetricsCollector {
    engine: AsyncEngine,
}

#[async_trait]
impl MetricsService for MetricsCollector {
    async fn export(
        &self,
        request: Request<ExportMetricsServiceRequest>,
    ) -> Result<Response<ExportMetricsServiceResponse>, Status> {
        let request = request.get_ref();
        let response = process_metrics_request(&self.engine, request).await;
        Ok(Response::new(response))
    }
}

pub(super) struct TraceCollector {
    engine: AsyncEngine,
}

#[async_trait]
impl TraceService for TraceCollector {
    async fn export(
        &self,
        request: Request<ExportTraceServiceRequest>,
    ) -> Result<Response<ExportTraceServiceResponse>, Status> {
        let request = request.get_ref();
        let response = process_trace_request(&self.engine, request).await;
        Ok(Response::new(response))
    }
}

pub fn logs_service(engine: AsyncEngine) -> LogsServiceServer<LogsCollector> {
    LogsServiceServer::new(LogsCollector { engine })
}

pub fn metrics_service(engine: AsyncEngine) -> MetricsServiceServer<MetricsCollector> {
    MetricsServiceServer::new(MetricsCollector { engine })
}

pub fn trace_service(engine: AsyncEngine) -> TraceServiceServer<TraceCollector> {
    TraceServiceServer::new(TraceCollector { engine })
}

pub(super) async fn post_otel_logs_handler(
    State(state): State<Arc<IngressState>>,
    Protobuf(request): Protobuf<ExportLogsServiceRequest>,
) -> Protobuf<ExportLogsServiceResponse> {
    let response = process_logs_request(&state.engine, &request).await;
    Protobuf(response)
}

pub(super) async fn post_otel_metrics_handler(
    State(state): State<Arc<IngressState>>,
    Protobuf(request): Protobuf<ExportMetricsServiceRequest>,
) -> Protobuf<ExportMetricsServiceResponse> {
    let response = process_metrics_request(&state.engine, &request).await;
    Protobuf(response)
}

pub(super) async fn post_otel_trace_handler(
    State(state): State<Arc<IngressState>>,
    Protobuf(request): Protobuf<ExportTraceServiceRequest>,
) -> Protobuf<ExportTraceServiceResponse> {
    let response = process_trace_request(&state.engine, &request).await;
    Protobuf(response)
}

async fn process_logs_request(
    engine: &AsyncEngine,
    request: &ExportLogsServiceRequest,
) -> ExportLogsServiceResponse {
    for resource_log in &request.resource_logs {
        let resource_attributes = if let Some(resource) = &resource_log.resource {
            conv_value_map(&resource.attributes)
        } else {
            // resource info is unknown
            BTreeMap::new()
        };

        let resource = NewResource {
            attributes: resource_attributes,
        };
        let resource_key = match engine.insert_resource(resource).await {
            Ok(key) => key,
            Err(err) => {
                tracing::warn!(?err, "could not insert resource");
                continue;
            }
        };

        for scope_log in &resource_log.scope_logs {
            // I'm not going to worry about instrumentation scope

            for log_record in &scope_log.log_records {
                let timestamp = if log_record.time_unix_nano != 0 {
                    log_record.time_unix_nano / 1000
                } else if log_record.observed_time_unix_nano != 0 {
                    log_record.observed_time_unix_nano / 1000
                } else {
                    SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .expect("now should not be before the UNIX epoch")
                        .as_micros() as u64
                };

                let trace_id = parse_trace_id(&log_record.trace_id);
                let span_id = parse_span_id(&log_record.span_id);

                let Ok(level) = Level::from_otel_severity(log_record.severity_number) else {
                    tracing::warn!("failed to interpret level from log record");
                    continue;
                };

                let mut attributes = conv_value_map(&log_record.attributes);

                let namespace = extract_namespace(&mut attributes);
                let function = extract_function(&mut attributes);
                let file_name = extract_file_name(&mut attributes);
                let file_line = extract_file_line(&mut attributes);
                let file_column = extract_file_column(&mut attributes);

                let event = NewEvent {
                    kind: SourceKind::Opentelemetry,
                    resource_key,
                    timestamp: Timestamp::new(timestamp)
                        .expect("now should not be at the UNIX epoch"),
                    span_id: trace_id.and_then(|trace_id| {
                        span_id.map(|span_id| FullSpanId::Opentelemetry(trace_id, span_id))
                    }),
                    content: log_record
                        .body
                        .as_ref()
                        .map(conv_value)
                        .unwrap_or(Value::Null),
                    namespace,
                    function,
                    level,
                    file_name,
                    file_line,
                    file_column,
                    attributes,
                };

                // we have no need for the result, and the insert is
                // executed regardless if we poll
                #[allow(clippy::let_underscore_future)]
                let _ = engine.insert_event(event);
            }
        }
    }

    ExportLogsServiceResponse {
        partial_success: None,
    }
}

async fn process_metrics_request(
    _engine: &AsyncEngine,
    _request: &ExportMetricsServiceRequest,
) -> ExportMetricsServiceResponse {
    ExportMetricsServiceResponse {
        partial_success: None,
    }
}

async fn process_trace_request(
    engine: &AsyncEngine,
    request: &ExportTraceServiceRequest,
) -> ExportTraceServiceResponse {
    for resource_span in &request.resource_spans {
        let resource_attributes = if let Some(resource) = &resource_span.resource {
            conv_value_map(&resource.attributes)
        } else {
            // resource info is unknown
            BTreeMap::new()
        };

        let resource = NewResource {
            attributes: resource_attributes,
        };
        let resource_key = match engine.insert_resource(resource).await {
            Ok(key) => key,
            Err(err) => {
                tracing::warn!(?err, "could not insert resource");
                continue;
            }
        };

        for scope_span in &resource_span.scope_spans {
            // I'm not going to worry about instrumentation scope

            let mut instrumentation_attributes = scope_span
                .scope
                .as_ref()
                .map(|scope| conv_value_map(&scope.attributes))
                .unwrap_or_default();

            let scope_level = extract_level(&mut instrumentation_attributes);
            let scope_namespace = extract_namespace(&mut instrumentation_attributes);
            let scope_function = extract_function(&mut instrumentation_attributes);
            let scope_file_name = extract_file_name(&mut instrumentation_attributes);
            let scope_file_line = extract_file_line(&mut instrumentation_attributes);
            let scope_file_column = extract_file_column(&mut instrumentation_attributes);

            for span in &scope_span.spans {
                let created_timestamp = span.start_time_unix_nano / 1000;
                let closed_timestamp = span.end_time_unix_nano / 1000;

                let Some(trace_id) = parse_trace_id(&span.trace_id) else {
                    tracing::warn!("failed to parse trace id from span");
                    continue;
                };

                let Some(span_id) = parse_span_id(&span.span_id) else {
                    tracing::warn!("failed to parse span id from span");
                    continue;
                };

                let parent_span_id = parse_span_id(&span.parent_span_id);
                let mut attributes = conv_value_map(&span.attributes);

                let level = extract_level(&mut attributes).or(scope_level);
                let Ok(level) = Level::from_otel_severity(level.unwrap_or(9)) else {
                    tracing::warn!("failed to interpret level from span");
                    continue;
                };

                // spans don't have levels so just set to @level if it has
                // it or just fallback to INFO (todo: there is a "status"
                // which could be "error")
                let busy = extract_busy(&mut attributes);
                let namespace =
                    extract_namespace(&mut attributes).or_else(|| scope_namespace.clone());
                let function = extract_function(&mut attributes).or_else(|| scope_function.clone());
                let file_name =
                    extract_file_name(&mut attributes).or_else(|| scope_file_name.clone());
                let file_line = extract_file_line(&mut attributes).or(scope_file_line);
                let file_column = extract_file_column(&mut attributes).or(scope_file_column);

                let create_span_event = NewSpanEvent {
                    timestamp: Timestamp::new(created_timestamp)
                        .expect("now should not be at the UNIX epoch"),
                    span_id: FullSpanId::Opentelemetry(trace_id, span_id),
                    kind: NewSpanEventKind::Create(NewCreateSpanEvent {
                        kind: SourceKind::Opentelemetry,
                        resource_key,
                        parent_id: parent_span_id
                            .map(|parent_id| FullSpanId::Opentelemetry(trace_id, parent_id)),
                        name: span.name.clone(),
                        namespace,
                        function,
                        level,
                        file_name,
                        file_line,
                        file_column,
                        instrumentation_attributes: instrumentation_attributes.clone(),
                        attributes,
                    }),
                };

                // we have no need for the result, and the insert is
                // executed regardless if we poll
                #[allow(clippy::let_underscore_future)]
                let _ = engine.insert_span_event(create_span_event);

                let close_span_event = NewSpanEvent {
                    timestamp: Timestamp::new(closed_timestamp)
                        .expect("now should not be at the UNIX epoch"),
                    span_id: FullSpanId::Opentelemetry(trace_id, span_id),
                    kind: NewSpanEventKind::Close(NewCloseSpanEvent { busy }),
                };

                // we have no need for the result, and the insert is
                // executed regardless if we poll
                #[allow(clippy::let_underscore_future)]
                let _ = engine.insert_span_event(close_span_event);

                for event in &span.events {
                    let timestamp = event.time_unix_nano / 1000;

                    let mut attributes = conv_value_map(&event.attributes);

                    // spans events don't have levels so just set to @level
                    // if it has it or just fallback to INFO (todo: there is
                    // a "status" which could be "error")
                    let level = extract_level(&mut attributes).or(scope_level);
                    let Ok(level) = Level::from_otel_severity(level.unwrap_or(9)) else {
                        tracing::warn!("failed to interpret level from log record");
                        continue;
                    };

                    let namespace =
                        extract_namespace(&mut attributes).or_else(|| scope_namespace.clone());
                    let function =
                        extract_function(&mut attributes).or_else(|| scope_function.clone());
                    let file_name =
                        extract_file_name(&mut attributes).or_else(|| scope_file_name.clone());
                    let file_line = extract_file_line(&mut attributes).or(scope_file_line);
                    let file_column = extract_file_column(&mut attributes).or(scope_file_column);

                    let event = NewEvent {
                        kind: SourceKind::Opentelemetry,
                        resource_key,
                        timestamp: Timestamp::new(timestamp)
                            .expect("now should not be at the UNIX epoch"),
                        span_id: Some(FullSpanId::Opentelemetry(trace_id, span_id)),
                        content: Value::Str(event.name.to_owned()),
                        namespace,
                        function,
                        level,
                        file_name,
                        file_line,
                        file_column,
                        attributes,
                    };

                    // we have no need for the result, and the insert is
                    // executed regardless if we poll
                    #[allow(clippy::let_underscore_future)]
                    let _ = engine.insert_event(event);
                }
            }
        }
    }

    ExportTraceServiceResponse {
        partial_success: None,
    }
}

fn conv_value_map(key_values: &[KeyValue]) -> BTreeMap<String, Value> {
    key_values
        .iter()
        .map(|key_value| {
            let key = key_value.key.to_owned();
            let value = match &key_value.value {
                Some(any_value) => conv_value(any_value),
                None => Value::Null,
            };

            (key, value)
        })
        .collect()
}

fn conv_value(any_value: &AnyValue) -> Value {
    use opentelemetry::proto::common::v1::any_value::Value as AnyValue;

    let any_value = any_value.value.as_ref();

    match any_value {
        None => Value::Null,
        Some(AnyValue::BoolValue(v)) => Value::Bool(*v),
        Some(AnyValue::IntValue(v)) => Value::I64(*v),
        Some(AnyValue::DoubleValue(v)) => Value::F64(*v),
        Some(AnyValue::StringValue(v)) => Value::Str(v.to_owned()),
        Some(AnyValue::BytesValue(v)) => Value::Bytes(v.to_owned()),
        Some(AnyValue::ArrayValue(v)) => Value::Array(v.values.iter().map(conv_value).collect()),
        Some(AnyValue::KvlistValue(v)) => Value::Object(conv_value_map(&v.values)),
    }
}

fn parse_trace_id(bytes: &[u8]) -> Option<TraceId> {
    <&[u8; 16]>::try_from(bytes)
        .map(|bytes| u128::from_be_bytes(*bytes))
        .ok()
}

fn parse_span_id(bytes: &[u8]) -> Option<SpanId> {
    <&[u8; 8]>::try_from(bytes)
        .map(|bytes| u64::from_be_bytes(*bytes))
        .ok()
}

fn extract_namespace(attributes: &mut BTreeMap<String, Value>) -> Option<String> {
    let val = attributes.remove_entry("code.namespace");

    match val {
        Some((_, Value::Str(path))) => Some(path),
        Some((key, val)) => {
            attributes.insert(key, val);
            None
        }
        None => None,
    }
}

fn extract_function(attributes: &mut BTreeMap<String, Value>) -> Option<String> {
    let val = attributes.remove_entry("code.function");

    match val {
        Some((_, Value::Str(path))) => Some(path),
        Some((key, val)) => {
            attributes.insert(key, val);
            None
        }
        None => None,
    }
}

fn extract_file_name(attributes: &mut BTreeMap<String, Value>) -> Option<String> {
    let val = attributes.remove_entry("code.filepath");

    match val {
        Some((_, Value::Str(path))) => Some(path),
        Some((key, val)) => {
            attributes.insert(key, val);
            None
        }
        None => None,
    }
}

fn extract_file_line(attributes: &mut BTreeMap<String, Value>) -> Option<u32> {
    let val = attributes.remove_entry("code.lineno");

    match val {
        Some((_, Value::I64(line))) => Some(line as u32),
        Some((key, val)) => {
            attributes.insert(key, val);
            None
        }
        None => None,
    }
}

fn extract_file_column(attributes: &mut BTreeMap<String, Value>) -> Option<u32> {
    let val = attributes.remove_entry("code.column");

    match val {
        Some((_, Value::I64(column))) => Some(column as u32),
        Some((key, val)) => {
            attributes.insert(key, val);
            None
        }
        None => None,
    }
}

fn extract_busy(attributes: &mut BTreeMap<String, Value>) -> Option<u64> {
    let val = attributes.remove_entry("busy_ns");
    let _ = attributes.remove_entry("idle_ns");

    match val {
        Some((_, Value::I64(busy))) => Some(busy as u64 / 1000),
        Some((key, val)) => {
            attributes.insert(key, val);
            None
        }
        None => None,
    }
}

fn extract_level(attributes: &mut BTreeMap<String, Value>) -> Option<i32> {
    let val = attributes.remove_entry("level");

    match val {
        Some((key, Value::Str(level))) => match level.trim().to_lowercase().as_str() {
            "trace" => Some(1),
            "debug" => Some(5),
            "info" => Some(9),
            "warn" => Some(13),
            "error" => Some(17),
            "fatal" => Some(21),
            _ => {
                attributes.insert(key, Value::Str(level));
                None
            }
        },
        Some((key, val)) => {
            attributes.insert(key, val);
            None
        }
        None => None,
    }
}
