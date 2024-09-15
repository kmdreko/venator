use std::collections::BTreeMap;
use std::fmt::Debug;
use std::num::NonZeroU64;
use std::time::{SystemTime, UNIX_EPOCH};

use bincode::{DefaultOptions, Error as BincodeError, Options};
use serde::Serialize;
use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Record};
use tracing::{Event, Level, Subscriber};
use tracing_subscriber::layer::Context;
use tracing_subscriber::registry::LookupSpan;

use crate::ids::VenatorId;

fn now() -> NonZeroU64 {
    // this only errors if "now" is at or before the UNIX epoch, if so you are a
    // liar and deserve to crash

    let microseconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros();

    // microseconds won't exceed a u64 until the year 586,912 AD
    NonZeroU64::new(microseconds as u64).unwrap()
}

pub(crate) fn encode<T: Serialize>(buffer: &mut Vec<u8>, payload: &T) -> Result<(), BincodeError> {
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

#[derive(Serialize)]
pub struct Handshake {
    pub fields: BTreeMap<String, String>,
}

#[derive(Serialize)]
pub struct Message {
    timestamp: NonZeroU64,
    span_id: Option<NonZeroU64>,
    data: MessageData,
}

#[derive(Serialize)]
enum MessageData {
    Create(CreateData),
    Update(UpdateData),
    Follows(FollowsData),
    Enter,
    Exit,
    Close,
    Event(EventData),
}

impl Message {
    pub(crate) fn from_new_span<S: Subscriber + for<'lookup> LookupSpan<'lookup>>(
        attrs: &Attributes<'_>,
        id: &VenatorId,
        ctx: &Context<'_, S>,
    ) -> Message {
        let timestamp = now();
        let metadata = attrs.metadata();
        let parent_id = ctx.current_span().id().cloned();

        let parent_id = parent_id
            .and_then(|id| ctx.span(&id))
            .and_then(|span| span.extensions().get::<VenatorId>().copied());

        let mut fields = Fields::new();
        attrs.record(&mut fields);

        Message {
            timestamp,
            span_id: Some(id.0),
            data: MessageData::Create(CreateData {
                parent_id: parent_id.map(|id| id.0),
                target: metadata.target(),
                name: metadata.name(),
                level: match *metadata.level() {
                    Level::TRACE => 0,
                    Level::DEBUG => 1,
                    Level::INFO => 2,
                    Level::WARN => 3,
                    Level::ERROR => 4,
                },
                file_name: metadata.file(),
                file_line: metadata.line(),
                fields,
            }),
        }
    }

    pub(crate) fn from_record(id: &VenatorId, values: &Record<'_>) -> Message {
        let timestamp = now();

        let mut fields = Fields::new();
        values.record(&mut fields);

        Message {
            timestamp,
            span_id: Some(id.0),
            data: MessageData::Update(UpdateData { fields }),
        }
    }

    pub(crate) fn from_follows(id: &VenatorId, follows_id: &VenatorId) -> Message {
        let timestamp = now();

        Message {
            timestamp,
            span_id: Some(id.0),
            data: MessageData::Follows(FollowsData {
                follows: follows_id.0,
            }),
        }
    }

    pub(crate) fn from_enter(id: &VenatorId) -> Message {
        let timestamp = now();

        Message {
            timestamp,
            span_id: Some(id.0),
            data: MessageData::Enter,
        }
    }

    pub(crate) fn from_exit(id: &VenatorId) -> Message {
        let timestamp = now();

        Message {
            timestamp,
            span_id: Some(id.0),
            data: MessageData::Exit,
        }
    }

    pub(crate) fn from_close(id: &VenatorId) -> Message {
        let timestamp = now();

        Message {
            timestamp,
            span_id: Some(id.0),
            data: MessageData::Close,
        }
    }

    pub(crate) fn from_event<S: Subscriber + for<'lookup> LookupSpan<'lookup>>(
        event: &Event<'_>,
        ctx: &Context<'_, S>,
    ) -> Message {
        let timestamp = now();
        let metadata = event.metadata();

        let parent_id = ctx
            .event_span(event)
            .and_then(|span| span.extensions().get::<VenatorId>().copied());

        let mut fields = Fields::new();
        event.record(&mut fields);

        Message {
            timestamp,
            span_id: parent_id.map(|id| id.0),
            data: MessageData::Event(EventData {
                name: metadata.name(),
                target: metadata.target(),
                level: level_to_number(*metadata.level()),
                file_name: metadata.file(),
                file_line: metadata.line(),
                fields,
            }),
        }
    }
}

#[derive(Serialize)]
struct CreateData {
    parent_id: Option<NonZeroU64>,
    target: &'static str,
    name: &'static str,
    level: i32,
    file_name: Option<&'static str>,
    file_line: Option<u32>,
    fields: Fields,
}

#[derive(Serialize)]
struct UpdateData {
    fields: Fields,
}

#[derive(Serialize)]
struct FollowsData {
    follows: NonZeroU64,
}

#[derive(Serialize)]
struct EventData {
    target: &'static str,
    name: &'static str,
    level: i32,
    file_name: Option<&'static str>,
    file_line: Option<u32>,
    fields: Fields,
}

#[derive(Serialize)]
struct Fields {
    inner: BTreeMap<&'static str, String>,
}

impl Fields {
    fn new() -> Fields {
        Fields {
            inner: BTreeMap::new(),
        }
    }
}

impl Visit for Fields {
    fn record_str(&mut self, field: &Field, value: &str) {
        self.inner.insert(field.name(), value.to_owned());
    }

    fn record_debug(&mut self, field: &Field, value: &dyn Debug) {
        self.inner.insert(field.name(), format!("{value:?}"));
    }
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
