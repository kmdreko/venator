use std::collections::BTreeMap;
use std::io::Error as IoError;
use std::num::NonZeroU64;
use std::time::{SystemTime, UNIX_EPOCH};

use bincode::{DefaultOptions, Error as BincodeError, Options};
use serde::Serialize;
use tracing::span::{Attributes, Record};
use tracing::{Event, Level, Subscriber};
use tracing_subscriber::layer::Context;
use tracing_subscriber::registry::LookupSpan;

use crate::attributes::OwnedValue;
use crate::ids::VenatorId;

fn now() -> NonZeroU64 {
    let microseconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("now should not be before the UNIX epoch")
        .as_micros();

    let microseconds = u64::try_from(microseconds)
        .expect("microseconds shouldn't exceed a u64 until the year 586,912 AD");

    NonZeroU64::new(microseconds).expect("now should not be at the UNIX epoch")
}

pub(crate) fn encode_message<T: Serialize>(
    buffer: &mut Vec<u8>,
    payload: &T,
) -> Result<(), BincodeError> {
    // this uses a two-byte length prefix followed by the bincode-ed payload

    buffer.resize(2, 0);

    DefaultOptions::new()
        .with_varint_encoding()
        .with_big_endian()
        .with_limit(u16::MAX as u64)
        .serialize_into(&mut *buffer, payload)?;

    let payload_size = buffer.len() - 2;
    let payload_size_bytes = (payload_size as u16).to_be_bytes();

    buffer[0..2].copy_from_slice(&payload_size_bytes);

    Ok(())
}

pub(crate) fn encode_chunk(buffer: &mut Vec<u8>, payload: &[u8]) -> Result<(), IoError> {
    use std::io::Write;

    buffer.clear();
    write!(buffer, "{:x}\r\n", payload.len())?;
    buffer.extend_from_slice(payload);
    write!(buffer, "\r\n")?;

    Ok(())
}

#[derive(Serialize)]
pub struct Handshake {
    pub attributes: BTreeMap<String, OwnedValue>,
}

#[derive(Serialize)]
pub struct Message<'a, 'callsite> {
    timestamp: NonZeroU64,
    span_id: Option<NonZeroU64>,
    data: MessageData<'a, 'callsite>,
}

#[derive(Serialize)]
enum MessageData<'a, 'callsite> {
    Create(CreateData<'a, 'callsite>),
    Update(UpdateData<'a, 'callsite>),
    Follows(FollowsData),
    Enter(EnterData),
    Exit,
    Close,
    Event(EventData<'a, 'callsite>),
}

impl Message<'_, '_> {
    pub(crate) fn from_new_span<'a, 'callsite, S: Subscriber + for<'lookup> LookupSpan<'lookup>>(
        attrs: &'a Attributes<'callsite>,
        id: &VenatorId,
        ctx: &Context<'_, S>,
    ) -> Message<'a, 'callsite> {
        let timestamp = now();
        let metadata = attrs.metadata();
        let parent_id = ctx.current_span().id().cloned();

        let parent_id = parent_id
            .and_then(|id| ctx.span(&id))
            .and_then(|span| span.extensions().get::<VenatorId>().copied());

        Message {
            timestamp,
            span_id: Some(id.0),
            data: MessageData::Create(CreateData {
                parent_id: parent_id.map(|id| id.0),
                target: metadata.target(),
                name: metadata.name(),
                level: level_to_number(*metadata.level()),
                file_name: metadata.file(),
                file_line: metadata.line(),
                attributes: attrs,
            }),
        }
    }

    pub(crate) fn from_record<'a, 'callsite>(
        id: &VenatorId,
        values: &'a Record<'callsite>,
    ) -> Message<'a, 'callsite> {
        let timestamp = now();

        Message {
            timestamp,
            span_id: Some(id.0),
            data: MessageData::Update(UpdateData { attributes: values }),
        }
    }

    pub(crate) fn from_follows(
        id: &VenatorId,
        follows_id: &VenatorId,
    ) -> Message<'static, 'static> {
        let timestamp = now();

        Message {
            timestamp,
            span_id: Some(id.0),
            data: MessageData::Follows(FollowsData {
                follows: follows_id.0,
            }),
        }
    }

    pub(crate) fn from_enter(id: &VenatorId) -> Message<'static, 'static> {
        let timestamp = now();

        Message {
            timestamp,
            span_id: Some(id.0),
            data: MessageData::Enter(EnterData {
                thread_id: thread_id::get() as u64,
            }),
        }
    }

    pub(crate) fn from_exit(id: &VenatorId) -> Message<'static, 'static> {
        let timestamp = now();

        Message {
            timestamp,
            span_id: Some(id.0),
            data: MessageData::Exit,
        }
    }

    pub(crate) fn from_close(id: &VenatorId) -> Message<'static, 'static> {
        let timestamp = now();

        Message {
            timestamp,
            span_id: Some(id.0),
            data: MessageData::Close,
        }
    }

    pub(crate) fn from_event<'a, 'callsite, S: Subscriber + for<'lookup> LookupSpan<'lookup>>(
        event: &'a Event<'callsite>,
        ctx: &Context<'_, S>,
    ) -> Message<'a, 'callsite> {
        let timestamp = now();
        let metadata = event.metadata();

        let parent_id = ctx
            .event_span(event)
            .and_then(|span| span.extensions().get::<VenatorId>().copied());

        Message {
            timestamp,
            span_id: parent_id.map(|id| id.0),
            data: MessageData::Event(EventData {
                name: metadata.name(),
                target: metadata.target(),
                level: level_to_number(*metadata.level()),
                file_name: metadata.file(),
                file_line: metadata.line(),
                attributes: event,
            }),
        }
    }
}

#[derive(Serialize)]
struct CreateData<'a, 'callsite> {
    parent_id: Option<NonZeroU64>,
    target: &'static str,
    name: &'static str,
    level: i32,
    file_name: Option<&'static str>,
    file_line: Option<u32>,
    #[serde(serialize_with = "crate::attributes::from_attributes")]
    attributes: &'a Attributes<'callsite>,
}

#[derive(Serialize)]
struct UpdateData<'a, 'callsite> {
    #[serde(serialize_with = "crate::attributes::from_record")]
    attributes: &'a Record<'callsite>,
}

#[derive(Serialize)]
struct FollowsData {
    follows: NonZeroU64,
}

#[derive(Serialize)]
struct EnterData {
    thread_id: u64,
}

#[derive(Serialize)]
struct EventData<'a, 'callsite> {
    target: &'static str,
    name: &'static str,
    level: i32,
    file_name: Option<&'static str>,
    file_line: Option<u32>,
    #[serde(serialize_with = "crate::attributes::from_event")]
    attributes: &'a Event<'callsite>,
}

fn level_to_number(level: Level) -> i32 {
    match level {
        Level::TRACE => 0,
        Level::DEBUG => 1,
        Level::INFO => 2,
        Level::WARN => 3,
        Level::ERROR => 4,
    }
}
