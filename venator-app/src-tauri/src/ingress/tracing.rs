use std::collections::BTreeMap;
use std::io::Error as IoError;
use std::num::NonZeroU64;
use std::sync::Arc;

use axum::body::Body;
use axum::extract::{FromRequestParts, State};
use axum::http::request::Parts;
use axum::http::StatusCode;
use bincode::{DefaultOptions, Options};
use futures::TryStreamExt;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio_util::io::StreamReader;

use venator_engine::engine::AsyncEngine;
use venator_engine::{
    FullSpanId, Level, NewCloseSpanEvent, NewCreateSpanEvent, NewEnterSpanEvent, NewEvent,
    NewFollowsSpanEvent, NewResource, NewSpanEvent, NewSpanEventKind, NewUpdateSpanEvent,
    SourceKind,
};

use super::IngressState;

pub(super) struct InstanceId(u128);

impl<S> FromRequestParts<S> for InstanceId
where
    S: Send + Sync,
{
    type Rejection = StatusCode;

    async fn from_request_parts(part: &mut Parts, _state: &S) -> Result<InstanceId, StatusCode> {
        part.headers
            .get("Instance-Id")
            .and_then(|value| value.to_str().ok())
            .and_then(|value| u128::from_str_radix(value, 16).ok())
            .map(InstanceId)
            .ok_or(StatusCode::BAD_REQUEST)
    }
}

pub(super) async fn post_tracing_handler(
    State(state): State<Arc<IngressState>>,
    InstanceId(id): InstanceId,
    body: Body,
) -> Result<(), StatusCode> {
    let stream = StreamReader::new(body.into_data_stream().map_err(IoError::other));

    {
        let mut instances = state
            .tracing_instances
            .lock()
            .expect("should never be poisoned");

        if instances.contains(&id) {
            return Err(StatusCode::CONFLICT);
        } else {
            instances.insert(id);
        }
    }

    handle_tracing_stream(stream, state.engine.clone(), id).await;

    {
        let mut instances = state
            .tracing_instances
            .lock()
            .expect("should never be poisoned");

        instances.remove(&id);
    }

    // we await sending the disconnect, but we don't need to await the response
    #[allow(clippy::let_underscore_future)]
    let _ = state.engine.disconnect_tracing_instance(id).await;

    Ok(())
}

async fn handle_tracing_stream<S: AsyncRead + Unpin>(
    mut stream: S,
    engine: AsyncEngine,
    instance_id: u128,
) {
    let deserializer = DefaultOptions::new()
        .with_varint_encoding()
        .with_big_endian()
        .with_limit(u16::MAX as u64);

    let mut buffer = vec![];

    let mut length_bytes = [0u8; 2];
    if let Err(err) = stream.read_exact(&mut length_bytes).await {
        tracing::warn!("failed to read handshake length: {err:?}");
        return;
    }

    let length = u16::from_be_bytes(length_bytes);

    buffer.resize(length as usize, 0u8);
    if let Err(err) = stream.read_exact(&mut buffer).await {
        tracing::warn!("failed to read handshake: {err:?}");
        return;
    }

    let handshake: Handshake = match deserializer.deserialize_from(buffer.as_slice()) {
        Ok(handshake) => handshake,
        Err(err) => {
            tracing::warn!("failed to parse handshake: {err:?}");
            return;
        }
    };

    let resource = NewResource {
        attributes: conv_value_map(handshake.attributes),
    };

    let resource_key = match engine.insert_resource(resource).await.await {
        Ok(Ok(key)) => key,
        Ok(Err(err)) => {
            tracing::warn!("failed to insert connection: {err:?}");
            return;
        }
        Err(err) => {
            tracing::warn!("failed to insert connection: {err:?}");
            return;
        }
    };

    loop {
        let mut length_bytes = [0u8; 2];
        if let Err(_err) = stream.read_exact(&mut length_bytes).await {
            // assume any error here is a normal disconnect
            break;
        }

        let length = u16::from_be_bytes(length_bytes);

        buffer.resize(length as usize, 0u8);
        if let Err(err) = stream.read_exact(&mut buffer).await {
            tracing::warn!("failed to read message: {err:?}");
            break;
        }

        let msg: Message = match deserializer.deserialize_from(buffer.as_slice()) {
            Ok(message) => message,
            Err(err) => {
                tracing::warn!("failed to parse message: {err:?}");
                break;
            }
        };

        match msg.data {
            MessageData::Create(create_data) => {
                let Some(span_id) = msg.span_id else {
                    tracing::warn!("create span message must have a span id");
                    continue;
                };

                let Ok(level) = Level::from_tracing_level(create_data.level) else {
                    tracing::warn!("failed to interpret level from span");
                    continue;
                };

                let span_event = NewSpanEvent {
                    timestamp: msg.timestamp,
                    span_id: FullSpanId::Tracing(instance_id, span_id),
                    kind: NewSpanEventKind::Create(NewCreateSpanEvent {
                        kind: SourceKind::Tracing,
                        resource_key,
                        parent_id: create_data
                            .parent_id
                            .map(|parent_id| FullSpanId::Tracing(instance_id, parent_id)),
                        name: create_data.name,
                        namespace: Some(create_data.target),
                        function: None,
                        level,
                        file_name: create_data.file_name,
                        file_line: create_data.file_line,
                        file_column: None,
                        instrumentation_attributes: BTreeMap::new(),
                        attributes: conv_value_map(create_data.attributes),
                    }),
                };

                // we await sending the event, but we don't need to await the
                // response
                #[allow(clippy::let_underscore_future)]
                let _ = engine.insert_span_event(span_event).await;
            }
            MessageData::Update(update_data) => {
                let Some(span_id) = msg.span_id else {
                    tracing::warn!("update span message must have a span id");
                    continue;
                };

                let span_event = NewSpanEvent {
                    timestamp: msg.timestamp,
                    span_id: FullSpanId::Tracing(instance_id, span_id),
                    kind: NewSpanEventKind::Update(NewUpdateSpanEvent {
                        attributes: conv_value_map(update_data.attributes),
                    }),
                };

                // we await sending the event, but we don't need to await the
                // response
                #[allow(clippy::let_underscore_future)]
                let _ = engine.insert_span_event(span_event).await;
            }
            MessageData::Follows(follows_data) => {
                let Some(span_id) = msg.span_id else {
                    tracing::warn!("follows message must have a span id");
                    continue;
                };

                let span_event = NewSpanEvent {
                    timestamp: msg.timestamp,
                    span_id: FullSpanId::Tracing(instance_id, span_id),
                    kind: NewSpanEventKind::Follows(NewFollowsSpanEvent {
                        follows: follows_data.follows,
                    }),
                };

                // we await sending the event, but we don't need to await the
                // response
                #[allow(clippy::let_underscore_future)]
                let _ = engine.insert_span_event(span_event).await;
            }
            MessageData::Enter(enter_data) => {
                let Some(span_id) = msg.span_id else {
                    tracing::warn!("enter span message must have a span id");
                    continue;
                };

                let span_event = NewSpanEvent {
                    timestamp: msg.timestamp,
                    span_id: FullSpanId::Tracing(instance_id, span_id),
                    kind: NewSpanEventKind::Enter(NewEnterSpanEvent {
                        thread_id: enter_data.thread_id,
                    }),
                };

                // we await sending the event, but we don't need to await the
                // response
                #[allow(clippy::let_underscore_future)]
                let _ = engine.insert_span_event(span_event).await;
            }
            MessageData::Exit => {
                let Some(span_id) = msg.span_id else {
                    tracing::warn!("exit span message must have a span id");
                    continue;
                };

                let span_event = NewSpanEvent {
                    timestamp: msg.timestamp,
                    span_id: FullSpanId::Tracing(instance_id, span_id),
                    kind: NewSpanEventKind::Exit,
                };

                // we await sending the event, but we don't need to await the
                // response
                #[allow(clippy::let_underscore_future)]
                let _ = engine.insert_span_event(span_event).await;
            }
            MessageData::Close => {
                let Some(span_id) = msg.span_id else {
                    tracing::warn!("close span message must have a span id");
                    continue;
                };

                let span_event = NewSpanEvent {
                    timestamp: msg.timestamp,
                    span_id: FullSpanId::Tracing(instance_id, span_id),
                    kind: NewSpanEventKind::Close(NewCloseSpanEvent { busy: None }),
                };

                // we await sending the event, but we don't need to await the
                // response
                #[allow(clippy::let_underscore_future)]
                let _ = engine.insert_span_event(span_event).await;
            }
            MessageData::Event(event) => {
                let Ok(level) = Level::from_tracing_level(event.level) else {
                    tracing::warn!("failed to interpret level from span");
                    continue;
                };

                let mut attributes = conv_value_map(event.attributes);

                let content = extract_content(&mut attributes)
                    .unwrap_or(venator_engine::Value::Str(event.name));

                let event = NewEvent {
                    kind: SourceKind::Tracing,
                    resource_key,
                    timestamp: msg.timestamp,
                    span_id: msg
                        .span_id
                        .map(|span_id| FullSpanId::Tracing(instance_id, span_id)),
                    content,
                    namespace: Some(event.target),
                    function: None,
                    level,
                    file_name: event.file_name,
                    file_line: event.file_line,
                    file_column: None,
                    attributes,
                };

                // we await sending the event, but we don't need to await the
                // response
                #[allow(clippy::let_underscore_future)]
                let _ = engine.insert_event(event).await;
            }
        };
    }
}

#[derive(Deserialize)]
pub struct Handshake {
    pub attributes: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Message {
    timestamp: NonZeroU64,
    span_id: Option<u64>,
    data: MessageData,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum MessageData {
    Create(CreateData),
    Update(UpdateData),
    Follows(FollowsData),
    Enter(EnterData),
    Exit,
    Close,
    Event(EventData),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CreateData {
    parent_id: Option<u64>,
    target: String,
    name: String,
    level: i32,
    file_name: Option<String>,
    file_line: Option<u32>,
    attributes: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct UpdateData {
    attributes: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FollowsData {
    follows: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct EnterData {
    thread_id: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct EventData {
    target: String,
    name: String,
    level: i32,
    file_name: Option<String>,
    file_line: Option<u32>,
    attributes: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub enum Value {
    F64(f64),
    I64(i64),
    U64(u64),
    I128(i128),
    U128(u128),
    Bool(bool),
    Str(String),
    Format(String),
}

fn conv_value_map(vmap: BTreeMap<String, Value>) -> BTreeMap<String, venator_engine::Value> {
    vmap.into_iter().map(|(k, v)| (k, conv_value(v))).collect()
}

fn conv_value(v: Value) -> venator_engine::Value {
    match v {
        Value::F64(v) => venator_engine::Value::F64(v),
        Value::I64(v) => venator_engine::Value::I64(v),
        Value::U64(v) => venator_engine::Value::U64(v),
        Value::I128(v) => venator_engine::Value::I128(v),
        Value::U128(v) => venator_engine::Value::U128(v),
        Value::Bool(v) => venator_engine::Value::Bool(v),
        Value::Str(v) => venator_engine::Value::Str(v),
        Value::Format(v) => venator_engine::Value::Str(v),
    }
}

fn extract_content(
    attributes: &mut BTreeMap<String, venator_engine::Value>,
) -> Option<venator_engine::Value> {
    attributes.remove("message")
}
