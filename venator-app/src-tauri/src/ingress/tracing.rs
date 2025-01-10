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

#[axum::async_trait]
impl<S> FromRequestParts<S> for InstanceId {
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
        let mut instances = state.tracing_instances.lock().unwrap();
        if instances.contains(&id) {
            return Err(StatusCode::CONFLICT);
        } else {
            instances.insert(id);
        }
    }

    handle_tracing_stream(stream, state.engine.clone(), id).await;

    {
        let mut instances = state.tracing_instances.lock().unwrap();
        instances.remove(&id);
    }

    // we have no need for the result, and the disconnect is executed
    // regardless if we poll
    #[allow(clippy::let_underscore_future)]
    let _ = state.engine.disconnect_tracing_instance(id);

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
        eprintln!("failed to read handshake length: {err:?}");
        return;
    }

    let length = u16::from_be_bytes(length_bytes);

    buffer.resize(length as usize, 0u8);
    if let Err(err) = stream.read_exact(&mut buffer).await {
        eprintln!("failed to read handshake: {err:?}");
        return;
    }

    let handshake: Handshake = match deserializer.deserialize_from(buffer.as_slice()) {
        Ok(handshake) => handshake,
        Err(err) => {
            eprintln!("failed to parse handshake: {err:?}");
            return;
        }
    };

    let resource = NewResource {
        attributes: conv_value_map(handshake.attributes),
    };

    let resource_key = match engine.insert_resource(resource).await {
        Ok(key) => key,
        Err(err) => {
            eprintln!("failed to insert connection: {err:?}");
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
            eprintln!("failed to read message: {err:?}");
            break;
        }

        // stats
        //     .bytes_since_last_check
        //     .fetch_add(length as usize + 2, Ordering::Relaxed);

        let msg: Message = match deserializer.deserialize_from(buffer.as_slice()) {
            Ok(message) => message,
            Err(err) => {
                eprintln!("failed to parse message: {err:?}");
                break;
            }
        };

        match msg.data {
            MessageData::Create(create_data) => {
                // we have no need for the result, and the insert is
                // executed regardless if we poll
                #[allow(clippy::let_underscore_future)]
                let _ = engine.insert_span_event(NewSpanEvent {
                    timestamp: msg.timestamp,
                    span_id: FullSpanId::Tracing(instance_id, msg.span_id.unwrap()),
                    kind: NewSpanEventKind::Create(NewCreateSpanEvent {
                        kind: SourceKind::Tracing,
                        resource_key,
                        parent_id: create_data
                            .parent_id
                            .map(|parent_id| FullSpanId::Tracing(instance_id, parent_id)),
                        name: create_data.name,
                        namespace: Some(create_data.target),
                        function: None,
                        level: Level::from_tracing_level(create_data.level).unwrap(),
                        file_name: create_data.file_name,
                        file_line: create_data.file_line,
                        file_column: None,
                        instrumentation_attributes: BTreeMap::new(),
                        attributes: conv_value_map(create_data.attributes),
                    }),
                });
            }
            MessageData::Update(update_data) => {
                // we have no need for the result, and the insert is
                // executed regardless if we poll
                #[allow(clippy::let_underscore_future)]
                let _ = engine.insert_span_event(NewSpanEvent {
                    timestamp: msg.timestamp,
                    span_id: FullSpanId::Tracing(instance_id, msg.span_id.unwrap()),
                    kind: NewSpanEventKind::Update(NewUpdateSpanEvent {
                        attributes: conv_value_map(update_data.attributes),
                    }),
                });
            }
            MessageData::Follows(follows_data) => {
                // we have no need for the result, and the insert is
                // executed regardless if we poll
                #[allow(clippy::let_underscore_future)]
                let _ = engine.insert_span_event(NewSpanEvent {
                    timestamp: msg.timestamp,
                    span_id: FullSpanId::Tracing(instance_id, msg.span_id.unwrap()),
                    kind: NewSpanEventKind::Follows(NewFollowsSpanEvent {
                        follows: follows_data.follows,
                    }),
                });
            }
            MessageData::Enter(enter_data) => {
                // we have no need for the result, and the insert is
                // executed regardless if we poll
                #[allow(clippy::let_underscore_future)]
                let _ = engine.insert_span_event(NewSpanEvent {
                    timestamp: msg.timestamp,
                    span_id: FullSpanId::Tracing(instance_id, msg.span_id.unwrap()),
                    kind: NewSpanEventKind::Enter(NewEnterSpanEvent {
                        thread_id: enter_data.thread_id,
                    }),
                });
            }
            MessageData::Exit => {
                // we have no need for the result, and the insert is
                // executed regardless if we poll
                #[allow(clippy::let_underscore_future)]
                let _ = engine.insert_span_event(NewSpanEvent {
                    timestamp: msg.timestamp,
                    span_id: FullSpanId::Tracing(instance_id, msg.span_id.unwrap()),
                    kind: NewSpanEventKind::Exit,
                });
            }
            MessageData::Close => {
                // we have no need for the result, and the insert is
                // executed regardless if we poll
                #[allow(clippy::let_underscore_future)]
                let _ = engine.insert_span_event(NewSpanEvent {
                    timestamp: msg.timestamp,
                    span_id: FullSpanId::Tracing(instance_id, msg.span_id.unwrap()),
                    kind: NewSpanEventKind::Close(NewCloseSpanEvent { busy: None }),
                });
            }
            MessageData::Event(event) => {
                let mut attributes = conv_value_map(event.attributes);

                let content = extract_content(&mut attributes)
                    .unwrap_or(venator_engine::Value::Str(event.name));

                // we have no need for the result, and the insert is
                // executed regardless if we poll
                #[allow(clippy::let_underscore_future)]
                let _ = engine.insert_event(NewEvent {
                    kind: SourceKind::Tracing,
                    resource_key,
                    timestamp: msg.timestamp,
                    span_id: msg
                        .span_id
                        .map(|span_id| FullSpanId::Tracing(instance_id, span_id)),
                    content,
                    namespace: Some(event.target),
                    function: None,
                    level: Level::from_tracing_level(event.level).unwrap(),
                    file_name: event.file_name,
                    file_line: event.file_line,
                    file_column: None,
                    attributes,
                });
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

// Only used to adjust how the JSON is formatted
#[derive(Debug, Clone, Serialize, Deserialize)]
struct MessageView {
    timestamp: NonZeroU64,
    span_id: Option<u64>,
    data: MessageDataView,
}

impl From<Message> for MessageView {
    fn from(value: Message) -> Self {
        MessageView {
            timestamp: value.timestamp,
            span_id: value.span_id,
            data: match value.data {
                MessageData::Create(create) => MessageDataView::Create(create),
                MessageData::Update(update) => MessageDataView::Update(update),
                MessageData::Follows(follows) => MessageDataView::Follows(follows),
                MessageData::Enter(enter) => MessageDataView::Enter(enter),
                MessageData::Exit => MessageDataView::Exit,
                MessageData::Close => MessageDataView::Close,
                MessageData::Event(event) => MessageDataView::Event(event),
            },
        }
    }
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

// Only used to adjust how the JSON is formatted
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
enum MessageDataView {
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
