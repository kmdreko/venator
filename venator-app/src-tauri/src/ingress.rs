use std::collections::BTreeMap;
use std::hash::{BuildHasher, RandomState};
use std::io::Error as IoError;
use std::io::ErrorKind;
use std::num::NonZeroU64;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Instant;

use bincode::{DefaultOptions, Options};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, BufReader};
use tokio::net::TcpListener;

use venator_engine::{
    Engine, NewConnection, NewCreateSpanEvent, NewEnterSpanEvent, NewEvent, NewFollowsSpanEvent,
    NewSpanEvent, NewSpanEventKind, NewUpdateSpanEvent,
};

enum IngressState {
    Listening(Option<JoinHandle<IoError>>),
    ListeningFailure(IoError),
}

impl IngressState {
    fn check_state(&mut self) {
        let err = match self {
            IngressState::Listening(h) if h.as_ref().is_some_and(|h| h.is_finished()) => {
                h.take().unwrap().join().unwrap()
            }
            _ => return,
        };

        *self = IngressState::ListeningFailure(err);
    }

    fn check_error(&self) -> Option<&IoError> {
        match self {
            IngressState::Listening(_) => None,
            IngressState::ListeningFailure(error) => Some(error),
        }
    }
}

struct IngressStats {
    last_check: Mutex<Instant>,
    bytes_since_last_check: AtomicUsize,
    connected_connections: AtomicUsize,
}

pub struct Ingress {
    bind: String,
    state: IngressState,
    stats: Arc<IngressStats>,
}

impl Ingress {
    pub fn start(bind: String, engine: Engine) -> Ingress {
        let stats = Arc::new(IngressStats {
            last_check: Mutex::new(Instant::now()),
            bytes_since_last_check: AtomicUsize::new(0),
            connected_connections: AtomicUsize::new(0),
        });

        let b = bind.clone();
        let s = stats.clone();
        let thread = std::thread::spawn(|| ingress_task(b, engine, s));

        Ingress {
            bind,
            state: IngressState::Listening(Some(thread)),
            stats,
        }
    }

    pub fn status(&mut self) -> (String, Option<String>) {
        self.state.check_state();
        match self.state.check_error() {
            Some(err) => {
                let msg = format!("not listening on {}", self.bind);
                let err = format!("{err}");

                (msg, Some(err))
            }
            None => {
                let msg = format!("listening on {}", self.bind);

                (msg, None)
            }
        }
    }

    // returns:
    // - number of connections
    // - bytes per second
    pub fn stats(&self) -> (usize, f64) {
        let now = Instant::now();
        let last = std::mem::replace(&mut *self.stats.last_check.lock().unwrap(), now);
        let elapsed = (now - last).as_secs_f64();

        let bytes = self.stats.bytes_since_last_check.load(Ordering::Relaxed);
        let connected = self.stats.connected_connections.load(Ordering::Relaxed);

        self.stats
            .bytes_since_last_check
            .store(0, Ordering::Relaxed);

        (connected, bytes as f64 / elapsed)
    }
}

#[tokio::main(worker_threads = 2)]
async fn ingress_task(bind: String, engine: Engine, stats: Arc<IngressStats>) -> IoError {
    let listener = match TcpListener::bind(&bind).await {
        Ok(listener) => listener,
        Err(err) => return err,
    };

    loop {
        let (stream, _) = match listener.accept().await {
            Ok(res) => res,
            Err(err) => return err,
        };

        stats.connected_connections.fetch_add(1, Ordering::Relaxed);

        let mut stream = BufReader::new(stream);
        let engine = engine.clone();
        let stats = stats.clone();
        let deserializer = DefaultOptions::new()
            .with_varint_encoding()
            .with_big_endian()
            .with_limit(u16::MAX as u64);

        tokio::spawn(async move {
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

            stats
                .bytes_since_last_check
                .fetch_add(length as usize + 2, Ordering::Relaxed);

            let connection_id = RandomState::new().hash_one(0u64);
            let connection = NewConnection {
                id: connection_id,
                fields: conv_value_map(handshake.fields),
            };

            let connection_key = match engine.insert_connection(connection).await {
                Ok(key) => key,
                Err(err) => {
                    eprintln!("failed to insert connection: {err:?}");
                    return;
                }
            };

            loop {
                let mut length_bytes = [0u8; 2];
                if let Err(err) = stream.read_exact(&mut length_bytes).await {
                    if err.kind() != ErrorKind::UnexpectedEof {
                        eprintln!("failed to read message length: {err:?}");
                    }
                    break;
                }

                let length = u16::from_be_bytes(length_bytes);

                buffer.resize(length as usize, 0u8);
                if let Err(err) = stream.read_exact(&mut buffer).await {
                    eprintln!("failed to read message: {err:?}");
                    break;
                }

                stats
                    .bytes_since_last_check
                    .fetch_add(length as usize + 2, Ordering::Relaxed);

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
                            connection_key,
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
                            connection_key,
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
                            connection_key,
                            timestamp: msg.timestamp,
                            span_id: msg.span_id.unwrap(),
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
                            connection_key,
                            timestamp: msg.timestamp,
                            span_id: msg.span_id.unwrap(),
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
                            connection_key,
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
                            connection_key,
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
                            connection_key,
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
            let _ = engine.disconnect_connection(connection_id);

            stats.connected_connections.fetch_sub(1, Ordering::Relaxed);
        });
    }
}

#[derive(Deserialize)]
pub struct Handshake {
    pub fields: BTreeMap<String, Value>,
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
    fields: BTreeMap<String, Value>,
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
