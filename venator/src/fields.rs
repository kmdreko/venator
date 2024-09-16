use std::fmt::Arguments;

use serde::ser::SerializeMap;
use serde::{Serialize, Serializer};
use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Record};
use tracing::Event;

#[derive(Debug)]
struct SerdeMapVisitor<S: SerializeMap> {
    serializer: S,
    state: Result<(), S::Error>,
}

impl<S> SerdeMapVisitor<S>
where
    S: SerializeMap,
{
    fn new(serializer: S) -> Self {
        Self {
            serializer,
            state: Ok(()),
        }
    }

    fn finish(self) -> Result<S::Ok, S::Error> {
        self.state?;
        self.serializer.end()
    }
}

impl<S> Visit for SerdeMapVisitor<S>
where
    S: SerializeMap,
{
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if self.state.is_ok() {
            self.state = self
                .serializer
                .serialize_entry(field.name(), &Value::Format(format_args!("{:?}", value)))
        }
    }

    fn record_f64(&mut self, field: &Field, value: f64) {
        if self.state.is_ok() {
            self.state = self
                .serializer
                .serialize_entry(field.name(), &Value::F64(value))
        }
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        if self.state.is_ok() {
            self.state = self
                .serializer
                .serialize_entry(field.name(), &Value::I64(value))
        }
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        if self.state.is_ok() {
            self.state = self
                .serializer
                .serialize_entry(field.name(), &Value::U64(value))
        }
    }

    fn record_i128(&mut self, field: &Field, value: i128) {
        if self.state.is_ok() {
            self.state = self
                .serializer
                .serialize_entry(field.name(), &Value::I128(value))
        }
    }

    fn record_u128(&mut self, field: &Field, value: u128) {
        if self.state.is_ok() {
            self.state = self
                .serializer
                .serialize_entry(field.name(), &Value::U128(value))
        }
    }

    fn record_bool(&mut self, field: &Field, value: bool) {
        if self.state.is_ok() {
            self.state = self
                .serializer
                .serialize_entry(field.name(), &Value::Bool(value))
        }
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        if self.state.is_ok() {
            self.state = self
                .serializer
                .serialize_entry(field.name(), &Value::Str(value))
        }
    }
}

pub(crate) fn record_as_fields<S>(r: &Record<'_>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    // we don't call `.len()` because it lies
    let mut counter = RecordCountVisitor::default();
    r.record(&mut counter);

    let serializer = serializer.serialize_map(Some(counter.count))?;
    let mut visitor = SerdeMapVisitor::new(serializer);
    r.record(&mut visitor);
    visitor.finish()
}

pub(crate) fn attributes_as_fields<S>(a: &Attributes<'_>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    // we don't call `values().len()` because it lies
    let mut counter = RecordCountVisitor::default();
    a.record(&mut counter);

    let serializer = serializer.serialize_map(Some(counter.count))?;
    let mut visitor = SerdeMapVisitor::new(serializer);
    a.record(&mut visitor);
    visitor.finish()
}

pub(crate) fn event_as_fields<S>(e: &Event<'_>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    // we don't call `.len()` because it doesn't exist
    let mut counter = RecordCountVisitor::default();
    e.record(&mut counter);

    let serializer = serializer.serialize_map(Some(counter.count))?;
    let mut visitor = SerdeMapVisitor::new(serializer);
    e.record(&mut visitor);
    visitor.finish()
}

#[derive(Serialize)]
enum Value<'a> {
    F64(f64),
    I64(i64),
    U64(u64),
    I128(i128),
    U128(u128),
    Bool(bool),
    Str(&'a str),
    Format(Arguments<'a>),
}

#[derive(Default)]
struct RecordCountVisitor {
    count: usize,
}

impl Visit for RecordCountVisitor {
    fn record_debug(&mut self, _field: &Field, _value: &dyn std::fmt::Debug) {
        self.count += 1;
    }
}
