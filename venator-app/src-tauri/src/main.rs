// Prevents additional console window on Windows in release, DO NOT REMOVE!!
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use std::collections::BTreeMap;
use std::hash::{BuildHasher, RandomState};
use std::io::ErrorKind;
use std::num::NonZeroU64;

use bincode::{DefaultOptions, Options};
use serde::{Deserialize, Serialize};
use tauri::menu::{MenuBuilder, MenuItem};
use tauri::{AppHandle, Emitter, State};
use tokio::io::{AsyncReadExt, BufReader};
use tokio::net::TcpListener;
use venator_engine::{
    BasicEventFilter, BasicInstanceFilter, BasicSpanFilter, Engine, EventView, FileStorage,
    FilterPredicate, FilterPropertyKind, InstanceView, NewCreateSpanEvent, NewEvent,
    NewFollowsSpanEvent, NewInstance, NewSpanEvent, NewSpanEventKind, NewUpdateSpanEvent, Order,
    Query, SpanView, StatsView, SubscriptionId, Timestamp, ValuePredicate,
};

#[tauri::command]
async fn get_instances(
    engine: State<'_, Engine>,
    filter: Vec<FilterPredicate>,
    order: Order,
    previous: Option<Timestamp>,
    start: Option<Timestamp>,
    end: Option<Timestamp>,
) -> Result<Vec<InstanceView>, ()> {
    let events = engine
        .query_instance(Query {
            filter,
            order,
            limit: 50,
            start: start.unwrap_or(Timestamp::MIN),
            end: end.unwrap_or(Timestamp::MAX),
            previous,
        })
        .await;

    Ok(events)
}

#[tauri::command]
async fn parse_instance_filter(
    _engine: State<'_, Engine>,
    filter: &str,
) -> Result<Vec<FilterPredicateView>, String> {
    FilterPredicate::parse(filter)
        .map_err(|e| format!("{e:?}"))?
        .into_iter()
        .map(|p| BasicInstanceFilter::validate(p).map(FilterPredicateView::from_inner))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("{e:?}"))
}

#[tauri::command]
async fn get_events(
    engine: State<'_, Engine>,
    filter: Vec<FilterPredicate>,
    order: Order,
    previous: Option<Timestamp>,
    start: Option<Timestamp>,
    end: Option<Timestamp>,
) -> Result<Vec<EventView>, ()> {
    let events = engine
        .query_event(Query {
            filter,
            order,
            limit: 50,
            start: start.unwrap_or(Timestamp::MIN),
            end: end.unwrap_or(Timestamp::MAX),
            previous,
        })
        .await;

    Ok(events)
}

#[tauri::command]
async fn get_event_count(
    engine: State<'_, Engine>,
    filter: Vec<FilterPredicate>,
    start: Timestamp,
    end: Timestamp,
) -> Result<usize, ()> {
    let events = engine
        .query_event_count(Query {
            filter,
            order: Order::Asc, // this doesn't matter
            limit: 20,         // this doesn't matter
            start,
            end,
            previous: None,
        })
        .await;

    Ok(events)
}

#[tauri::command]
async fn parse_event_filter(
    _engine: State<'_, Engine>,
    filter: &str,
) -> Result<Vec<FilterPredicateView>, String> {
    FilterPredicate::parse(filter)
        .map_err(|e| format!("{e:?}"))?
        .into_iter()
        .map(|p| BasicEventFilter::validate(p).map(FilterPredicateView::from_inner))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("{e:?}"))
}

#[tauri::command]
async fn get_spans(
    engine: State<'_, Engine>,
    filter: Vec<FilterPredicate>,
    order: Order,
    previous: Option<Timestamp>,
    start: Option<Timestamp>,
    end: Option<Timestamp>,
) -> Result<Vec<SpanView>, ()> {
    let spans = engine
        .query_span(Query {
            filter,
            order,
            limit: 50,
            start: start.unwrap_or(Timestamp::MIN),
            end: end.unwrap_or(Timestamp::MAX),
            previous,
        })
        .await;

    Ok(spans)
}

#[tauri::command]
async fn parse_span_filter(
    _engine: State<'_, Engine>,
    filter: &str,
) -> Result<Vec<FilterPredicateView>, String> {
    FilterPredicate::parse(filter)
        .map_err(|e| format!("{e:?}"))?
        .into_iter()
        .map(|p| BasicSpanFilter::validate(p).map(FilterPredicateView::from_inner))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("{e:?}"))
}

#[tauri::command]
async fn get_stats(engine: State<'_, Engine>) -> Result<StatsView, ()> {
    Ok(engine.query_stats().await)
}

#[tauri::command]
async fn subscribe_to_events(
    app: AppHandle,
    engine: State<'_, Engine>,
    filter: Vec<FilterPredicate>,
) -> Result<SubscriptionId, String> {
    let (id, mut receiver) = engine.subscribe_to_events(filter).await;

    tokio::spawn(async move {
        while let Some(event) = receiver.recv().await {
            let _ = app.emit("live", LiveEventPayload { id, data: event });
        }
    });

    Ok(id)
}

#[tauri::command]
async fn unsubscribe_from_events(
    engine: State<'_, Engine>,
    id: SubscriptionId,
) -> Result<(), String> {
    engine.unsubscribe_from_events(id).await;

    Ok(())
}

fn main() {
    let engine = Engine::new(FileStorage::new("local.db"));

    let engine_for_ingress = engine.clone();
    std::thread::spawn(|| ingress_task(engine_for_ingress));

    tauri::Builder::default()
        .setup(|app| {
            let handle = app.handle();
            let menu = MenuBuilder::new(handle)
                .item(&MenuItem::new(handle, "File", true, None::<&str>)?)
                .item(&MenuItem::new(handle, "Edit", true, None::<&str>)?)
                .item(&MenuItem::new(handle, "View", true, None::<&str>)?)
                .item(&MenuItem::new(handle, "Tools", true, None::<&str>)?)
                .item(&MenuItem::new(handle, "Help", true, None::<&str>)?)
                .build()?;
            app.set_menu(menu)?;
            Ok(())
        })
        .manage(engine)
        .invoke_handler(tauri::generate_handler![
            get_instances,
            parse_instance_filter,
            get_events,
            get_event_count,
            parse_event_filter,
            get_spans,
            parse_span_filter,
            get_stats,
            subscribe_to_events,
            unsubscribe_from_events,
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}

#[tokio::main(worker_threads = 2)]
async fn ingress_task(engine: Engine) {
    let listener = TcpListener::bind("0.0.0.0:8362").await.unwrap();

    loop {
        let (stream, _) = listener.accept().await.unwrap();
        let mut stream = BufReader::new(stream);
        let engine = engine.clone();
        let deserializer = DefaultOptions::new()
            .with_varint_encoding()
            .with_big_endian()
            .with_limit(u16::MAX as u64);

        tokio::spawn(async move {
            let mut buffer = vec![];

            let mut length_bytes = [0u8; 2];
            if let Err(err) = stream.read_exact(&mut length_bytes).await {
                println!("failed to read handshake length: {err:?}");
                return;
            }

            let length = u16::from_be_bytes(length_bytes);

            buffer.resize(length as usize, 0u8);
            if let Err(err) = stream.read_exact(&mut buffer).await {
                println!("failed to read handshake: {err:?}");
                return;
            }

            let handshake: Handshake = match deserializer.deserialize_from(buffer.as_slice()) {
                Ok(handshake) => handshake,
                Err(err) => {
                    println!("failed to parse handshake: {err:?}");
                    return;
                }
            };

            let instance_id = RandomState::new().hash_one(0u64);
            let instance = NewInstance {
                id: instance_id,
                fields: handshake
                    .fields
                    .into_iter()
                    .map(|(k, v)| (k, venator_engine::Value::Str(v)))
                    .collect(),
            };

            let instance_key = match engine.insert_instance(instance).await {
                Ok(key) => key,
                Err(err) => {
                    println!("failed to insert instance: {err:?}");
                    return;
                }
            };

            loop {
                let mut length_bytes = [0u8; 2];
                if let Err(err) = stream.read_exact(&mut length_bytes).await {
                    if err.kind() != ErrorKind::UnexpectedEof {
                        println!("failed to read message length: {err:?}");
                    }
                    break;
                }

                let length = u16::from_be_bytes(length_bytes);

                buffer.resize(length as usize, 0u8);
                if let Err(err) = stream.read_exact(&mut buffer).await {
                    println!("failed to read message: {err:?}");
                    break;
                }

                let msg: Message = match deserializer.deserialize_from(buffer.as_slice()) {
                    Ok(message) => message,
                    Err(err) => {
                        println!("failed to parse message: {err:?}");
                        break;
                    }
                };

                match msg.data {
                    MessageData::Create(create_data) => {
                        // we have no need for the result, and the insert is
                        // executed regardless if we poll
                        #[allow(clippy::let_underscore_future)]
                        let _ = engine.insert_span_event(NewSpanEvent {
                            instance_key,
                            timestamp: msg.timestamp,
                            span_id: msg.span_id.unwrap(),
                            kind: NewSpanEventKind::Create(NewCreateSpanEvent {
                                parent_id: create_data.parent_id,
                                target: create_data.target,
                                name: create_data.name,
                                level: create_data.level,
                                file_name: create_data.file_name,
                                file_line: create_data.file_line,
                                fields: conv_value_map(create_data.fields),
                            }),
                        });
                    }
                    MessageData::Update(update_data) => {
                        // we have no need for the result, and the insert is
                        // executed regardless if we poll
                        #[allow(clippy::let_underscore_future)]
                        let _ = engine.insert_span_event(NewSpanEvent {
                            instance_key,
                            timestamp: msg.timestamp,
                            span_id: msg.span_id.unwrap(),
                            kind: NewSpanEventKind::Update(NewUpdateSpanEvent {
                                fields: conv_value_map(update_data.fields),
                            }),
                        });
                    }
                    MessageData::Follows(follows_data) => {
                        // we have no need for the result, and the insert is
                        // executed regardless if we poll
                        #[allow(clippy::let_underscore_future)]
                        let _ = engine.insert_span_event(NewSpanEvent {
                            instance_key,
                            timestamp: msg.timestamp,
                            span_id: msg.span_id.unwrap(),
                            kind: NewSpanEventKind::Follows(NewFollowsSpanEvent {
                                follows: follows_data.follows,
                            }),
                        });
                    }
                    MessageData::Enter => {
                        // we have no need for the result, and the insert is
                        // executed regardless if we poll
                        #[allow(clippy::let_underscore_future)]
                        let _ = engine.insert_span_event(NewSpanEvent {
                            instance_key,
                            timestamp: msg.timestamp,
                            span_id: msg.span_id.unwrap(),
                            kind: NewSpanEventKind::Enter,
                        });
                    }
                    MessageData::Exit => {
                        // we have no need for the result, and the insert is
                        // executed regardless if we poll
                        #[allow(clippy::let_underscore_future)]
                        let _ = engine.insert_span_event(NewSpanEvent {
                            instance_key,
                            timestamp: msg.timestamp,
                            span_id: msg.span_id.unwrap(),
                            kind: NewSpanEventKind::Exit,
                        });
                    }
                    MessageData::Close => {
                        // we have no need for the result, and the insert is
                        // executed regardless if we poll
                        #[allow(clippy::let_underscore_future)]
                        let _ = engine.insert_span_event(NewSpanEvent {
                            instance_key,
                            timestamp: msg.timestamp,
                            span_id: msg.span_id.unwrap(),
                            kind: NewSpanEventKind::Close,
                        });
                    }
                    MessageData::Event(event) => {
                        // we have no need for the result, and the insert is
                        // executed regardless if we poll
                        #[allow(clippy::let_underscore_future)]
                        let _ = engine.insert_event(NewEvent {
                            instance_key,
                            timestamp: msg.timestamp,
                            span_id: msg.span_id,
                            target: event.target,
                            name: event.name,
                            level: event.level,
                            file_name: event.file_name,
                            file_line: event.file_line,
                            fields: conv_value_map(event.fields),
                        });
                    }
                };
            }

            // we have no need for the result, and the disconnect is executed
            // regardless if we poll
            #[allow(clippy::let_underscore_future)]
            let _ = engine.disconnect_instance(instance_id);
        });
    }
}

#[derive(Clone, Serialize)]
pub struct LiveEventPayload<T> {
    id: SubscriptionId,
    data: T,
}

#[derive(Deserialize)]
pub struct Handshake {
    pub fields: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Message {
    timestamp: NonZeroU64,
    span_id: Option<NonZeroU64>,
    data: MessageData,
}

// Only used to adjust how the JSON is formatted
#[derive(Debug, Clone, Serialize, Deserialize)]
struct MessageView {
    timestamp: NonZeroU64,
    span_id: Option<NonZeroU64>,
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
                MessageData::Enter => MessageDataView::Enter,
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
    Enter,
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
    Enter,
    Exit,
    Close,
    Event(EventData),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CreateData {
    parent_id: Option<NonZeroU64>,
    target: String,
    name: String,
    level: i32,
    file_name: Option<String>,
    file_line: Option<u32>,
    fields: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct UpdateData {
    fields: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FollowsData {
    follows: NonZeroU64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct EventData {
    target: String,
    name: String,
    level: i32,
    file_name: Option<String>,
    file_line: Option<u32>,
    fields: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FilterPredicateView {
    text: String,
    property_kind: Option<FilterPropertyKind>,
    property: String,
    #[serde(flatten)]
    value: ValuePredicate,
}

impl FilterPredicateView {
    fn from_inner(inner: FilterPredicate) -> FilterPredicateView {
        FilterPredicateView {
            text: inner.to_string(),
            property_kind: inner.property_kind,
            property: inner.property,
            value: inner.value,
        }
    }
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
    vmap.into_iter()
        .map(|(k, v)| match v {
            Value::F64(v) => (k, venator_engine::Value::F64(v)),
            Value::I64(v) => (k, venator_engine::Value::I64(v)),
            Value::U64(v) => (k, venator_engine::Value::U64(v)),
            Value::I128(v) => (k, venator_engine::Value::I128(v)),
            Value::U128(v) => (k, venator_engine::Value::U128(v)),
            Value::Bool(v) => (k, venator_engine::Value::Bool(v)),
            Value::Str(v) => (k, venator_engine::Value::Str(v)),
            Value::Format(v) => (k, venator_engine::Value::Str(v)),
        })
        .collect()
}
