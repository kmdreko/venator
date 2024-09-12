use std::collections::BTreeMap;
use std::fmt::Debug;

use chrono::{DateTime, Utc};
use serde::Serialize;
use tracing_core::field::{Field, Visit};
use tracing_core::span::{Attributes, Id, Record};
use tracing_core::{Event, Level, Subscriber};
use tracing_subscriber::layer::Context;

#[derive(Serialize)]
pub struct Handshake {
    pub fields: BTreeMap<String, String>,
}

#[derive(Serialize)]
pub struct Message {
    timestamp: DateTime<Utc>,
    span_id: Option<u64>,
    data: MessageData,
}

#[derive(Serialize)]
enum MessageData {
    Create(CreateData),
    Update(UpdateData),
    Enter,
    Exit,
    Close,
    Event(EventData),
}

impl Message {
    pub fn from_new_span<S: Subscriber>(
        attrs: &Attributes<'_>,
        id: &Id,
        ctx: &Context<'_, S>,
    ) -> Message {
        let timestamp = Utc::now();
        let metadata = attrs.metadata();
        let parent_id = ctx.current_span().id().cloned();

        let mut fields = Fields::new();
        attrs.record(&mut fields);

        Message {
            timestamp,
            span_id: Some(id.into_u64()),
            data: MessageData::Create(CreateData {
                parent_id: parent_id.map(|id| id.into_u64()),
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

    pub fn from_record(id: &Id, values: &Record<'_>) -> Message {
        let timestamp = Utc::now();

        let mut fields = Fields::new();
        values.record(&mut fields);

        Message {
            timestamp,
            span_id: Some(id.into_u64()),
            data: MessageData::Update(UpdateData { fields }),
        }
    }

    pub fn from_enter(id: &Id) -> Message {
        let timestamp = Utc::now();

        Message {
            timestamp,
            span_id: Some(id.into_u64()),
            data: MessageData::Enter,
        }
    }

    pub fn from_exit(id: &Id) -> Message {
        let timestamp = Utc::now();

        Message {
            timestamp,
            span_id: Some(id.into_u64()),
            data: MessageData::Exit,
        }
    }

    pub fn from_close(id: &Id) -> Message {
        let timestamp = Utc::now();

        Message {
            timestamp,
            span_id: Some(id.into_u64()),
            data: MessageData::Close,
        }
    }

    pub fn from_event<S: Subscriber>(event: &Event<'_>, ctx: &Context<'_, S>) -> Message {
        let timestamp = Utc::now();
        let metadata = event.metadata();

        // NOTE: This is how `Context::event_span` looks up the event's span but
        // we don't use that because we don't require `S: LookupSpan`.
        let parent_id = if event.is_root() {
            None
        } else if event.is_contextual() {
            ctx.current_span().id().cloned()
        } else {
            event.parent().cloned()
        };

        let mut fields = Fields::new();
        event.record(&mut fields);

        Message {
            timestamp,
            span_id: parent_id.map(|id| id.into_u64()),
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
    parent_id: Option<u64>,
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
