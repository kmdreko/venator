use std::fmt::Arguments;

use serde::ser::SerializeMap;
use serde::{Serialize, Serializer};
use tracing::field::{AsField, Field, Visit};
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

pub(crate) fn from_record<S>(r: &Record<'_>, serializer: S) -> Result<S::Ok, S::Error>
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

pub(crate) fn from_attributes<S>(a: &Attributes<'_>, serializer: S) -> Result<S::Ok, S::Error>
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

pub(crate) fn from_event<S>(e: &Event<'_>, serializer: S) -> Result<S::Ok, S::Error>
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

#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) enum OwnedValue {
    F64(f64),
    I64(i64),
    U64(u64),
    I128(i128),
    U128(u128),
    Bool(bool),
    Str(String),
}

impl OwnedValue {
    pub(crate) fn from_tracing<V: tracing::Value>(v: V) -> Option<OwnedValue> {
        // This is a bit weird, but its probably the best way to create metadata
        // for the field (required for recording) without any side-effects.

        use tracing::callsite::Callsite;
        use tracing::field::FieldSet;
        use tracing::metadata::{Kind, Level, Metadata};
        use tracing::subscriber::Interest;
        use tracing_core::identify_callsite;

        struct NoopCallsite(&'static Metadata<'static>);
        impl Callsite for NoopCallsite {
            fn set_interest(&self, _: Interest) {}
            fn metadata(&self) -> &Metadata<'_> {
                self.0
            }
        }

        static CALLSITE: NoopCallsite = NoopCallsite(&META);
        static META: Metadata<'static> = Metadata::new(
            "dummy",
            "dummy",
            Level::TRACE,
            None,
            None,
            None,
            FieldSet::new(&["dummy"], identify_callsite!(&CALLSITE)),
            Kind::SPAN,
        );

        let mut visitor = OwnedVisitor { value: None };
        v.record(&"dummy".as_field(&META).unwrap(), &mut visitor);
        visitor.value
    }
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

#[derive(Default)]
struct OwnedVisitor {
    value: Option<OwnedValue>,
}

impl Visit for OwnedVisitor {
    fn record_debug(&mut self, _field: &Field, value: &dyn std::fmt::Debug) {
        self.value = Some(OwnedValue::Str(format!("{value:?}")));
    }

    fn record_f64(&mut self, _field: &Field, value: f64) {
        self.value = Some(OwnedValue::F64(value));
    }

    fn record_i64(&mut self, _field: &Field, value: i64) {
        self.value = Some(OwnedValue::I64(value));
    }

    fn record_u64(&mut self, _field: &Field, value: u64) {
        self.value = Some(OwnedValue::U64(value));
    }

    fn record_i128(&mut self, _field: &Field, value: i128) {
        self.value = Some(OwnedValue::I128(value));
    }

    fn record_u128(&mut self, _field: &Field, value: u128) {
        self.value = Some(OwnedValue::U128(value));
    }

    fn record_bool(&mut self, _field: &Field, value: bool) {
        self.value = Some(OwnedValue::Bool(value));
    }

    fn record_str(&mut self, _field: &Field, value: &str) {
        self.value = Some(OwnedValue::Str(value.to_owned()));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn owned_value() {
        assert_eq!(OwnedValue::from_tracing(true), Some(OwnedValue::Bool(true)));
    }
}
