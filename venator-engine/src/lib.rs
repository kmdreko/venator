//! The "engine" crate represents the core functionality to injest, store,
//! index, and query the events and spans. It does not provide functionality
//! outside of its Rust API.

pub mod engine;
mod filter;
mod index;
mod models;
pub mod storage;
mod subscription;

use std::cell::OnceCell;
use std::collections::BTreeMap;
use std::sync::Arc;

use models::{AttributeTypeView, EventKey, TraceRoot};

use storage::Storage;

pub use filter::input::{
    FilterPredicate, FilterPredicateSingle, FilterPropertyKind, ValuePredicate,
};
pub use filter::{
    BasicEventFilter, BasicSpanFilter, FallibleFilterPredicate, InputError, Order, Query,
};
pub use models::{
    AncestorView, AttributeSourceView, AttributeView, CreateSpanEvent, DeleteFilter, DeleteMetrics,
    EngineStatusView, Event, EventView, FullSpanId, InstanceId, Level, LevelConvertError,
    NewCloseSpanEvent, NewCreateSpanEvent, NewEnterSpanEvent, NewEvent, NewFollowsSpanEvent,
    NewResource, NewSpanEvent, NewSpanEventKind, NewUpdateSpanEvent, Resource, ResourceKey,
    SourceKind, Span, SpanEvent, SpanEventKey, SpanEventKind, SpanId, SpanKey, SpanView, StatsView,
    Timestamp, TraceId, UpdateSpanEvent, Value, ValueOperator,
};
pub use subscription::{SubscriptionId, SubscriptionResponse};

struct EventContext<'a, S> {
    event_key: EventKey,
    storage: &'a S,
    event: RefOrDeferredArc<'a, Event>,
    parents: OnceCell<Vec<Arc<Span>>>,
    resource: OnceCell<Arc<Resource>>,
}

impl<'a, S> EventContext<'a, S>
where
    S: Storage,
{
    fn new(event_key: EventKey, storage: &'a S) -> EventContext<'a, S> {
        EventContext {
            event_key,
            storage,
            event: RefOrDeferredArc::Deferred(OnceCell::new()),
            parents: OnceCell::new(),
            resource: OnceCell::new(),
        }
    }

    fn with_event(event: &'a Event, storage: &'a S) -> EventContext<'a, S> {
        EventContext {
            event_key: event.key(),
            storage,
            event: RefOrDeferredArc::Ref(event),
            parents: OnceCell::new(),
            resource: OnceCell::new(),
        }
    }

    fn key(&self) -> EventKey {
        self.event_key
    }

    fn trace_root(&self) -> Option<TraceRoot> {
        let parent_id = self.event().parent_id;

        match parent_id {
            Some(FullSpanId::Tracing(_, _)) => {
                let root_parent_id = self.parents().last().map(|p| p.id);
                if let Some(FullSpanId::Tracing(instance_id, span_id)) = root_parent_id {
                    Some(TraceRoot::Tracing(instance_id, span_id))
                } else {
                    panic!("tracing event's root span doesnt have tracing id or is missing");
                }
            }
            Some(FullSpanId::Opentelemetry(trace_id, _)) => {
                Some(TraceRoot::Opentelemetry(trace_id))
            }
            None => None,
        }
    }

    fn event(&self) -> &Event {
        match &self.event {
            RefOrDeferredArc::Ref(event) => event,
            RefOrDeferredArc::Deferred(deferred) => {
                deferred.get_or_init(|| self.storage.get_event(self.event_key).unwrap())
            }
        }
    }

    fn parents(&self) -> impl Iterator<Item = &Span> {
        let event = self.event();

        self.parents
            .get_or_init(|| {
                let mut parents = vec![];
                let mut parent_key_next = event.parent_key;

                while let Some(parent_key) = parent_key_next {
                    let parent = self.storage.get_span(parent_key).unwrap();

                    parent_key_next = parent.parent_key;
                    parents.push(parent);
                }

                parents
            })
            .iter()
            .map(|p| &**p)
    }

    fn resource(&self) -> &Resource {
        let event = self.event();

        self.resource
            .get_or_init(|| self.storage.get_resource(event.resource_key).unwrap())
            .as_ref()
    }

    fn attribute(&self, attr: &str) -> Option<&Value> {
        let event = self.event();
        if let Some(v) = event.fields.get(attr) {
            return Some(v);
        }

        let parents = self.parents();
        for parent in parents {
            if let Some(v) = parent.fields.get(attr) {
                return Some(v);
            }
        }

        let resource = self.resource();
        if let Some(v) = resource.fields.get(attr) {
            return Some(v);
        }

        None
    }

    fn attribute_with_key(&self, attr: &str) -> Option<(&Value, Timestamp)> {
        let event = self.event();
        if let Some(v) = event.fields.get(attr) {
            return Some((v, event.key()));
        }

        let parents = self.parents();
        for parent in parents {
            if let Some(v) = parent.fields.get(attr) {
                return Some((v, parent.key()));
            }
        }

        let resource = self.resource();
        if let Some(v) = resource.fields.get(attr) {
            return Some((v, resource.key()));
        }

        None
    }

    fn attributes(&self) -> impl Iterator<Item = (&str, &Value)> {
        let mut attributes = BTreeMap::new();

        let event = self.event();
        for (attr, value) in &event.fields {
            attributes.entry(&**attr).or_insert(value);
        }

        let parents = self.parents();
        for parent in parents {
            for (attr, value) in &parent.fields {
                attributes.entry(&**attr).or_insert(value);
            }
        }

        let resource = self.resource();
        for (attr, value) in &resource.fields {
            attributes.entry(&**attr).or_insert(value);
        }

        attributes.into_iter()
    }

    fn render(&self) -> EventView {
        let event = self.event();

        let mut attributes =
            BTreeMap::<String, (AttributeSourceView, String, AttributeTypeView)>::new();
        for (attribute, value) in &self.event().fields {
            attributes.insert(
                attribute.to_owned(),
                (
                    AttributeSourceView::Inherent,
                    value.to_string(),
                    value.to_type_view(),
                ),
            );
        }
        for parent in self.parents() {
            for (attribute, value) in &parent.fields {
                if !attributes.contains_key(attribute) {
                    attributes.insert(
                        attribute.to_owned(),
                        (
                            AttributeSourceView::Span {
                                span_id: parent.id.to_string(),
                            },
                            value.to_string(),
                            value.to_type_view(),
                        ),
                    );
                }
            }
        }
        for (attribute, value) in &self.resource().fields {
            if !attributes.contains_key(attribute) {
                attributes.insert(
                    attribute.to_owned(),
                    (
                        AttributeSourceView::Resource,
                        value.to_string(),
                        value.to_type_view(),
                    ),
                );
            }
        }

        EventView {
            kind: event.kind,
            ancestors: {
                let mut ancestors = self
                    .parents()
                    .map(|parent| AncestorView {
                        id: parent.id.to_string(),
                        name: parent.name.clone(),
                    })
                    .collect::<Vec<_>>();

                ancestors.reverse();
                ancestors
            },
            timestamp: event.timestamp,
            level: event.level.into_simple_level(),
            content: event.content.to_string(),
            namespace: event.namespace.clone(),
            function: event.function.clone(),
            file: match (&event.file_name, event.file_line) {
                (None, _) => None,
                (Some(name), None) => Some(name.clone()),
                (Some(name), Some(line)) => Some(format!("{name}:{line}")),
            },
            attributes: attributes
                .into_iter()
                .map(|(name, (kind, value, typ))| AttributeView {
                    name,
                    value,
                    typ,
                    source: kind,
                })
                .collect(),
        }
    }
}

struct SpanContext<'a, S> {
    span_key: SpanKey,
    storage: &'a S,
    span: RefOrDeferredArc<'a, Span>,
    parents: OnceCell<Vec<Arc<Span>>>,
    resource: OnceCell<Arc<Resource>>,
}

impl<'a, S> SpanContext<'a, S>
where
    S: Storage,
{
    fn new(span_key: SpanKey, storage: &'a S) -> SpanContext<'a, S> {
        SpanContext {
            span_key,
            storage,
            span: RefOrDeferredArc::Deferred(OnceCell::new()),
            parents: OnceCell::new(),
            resource: OnceCell::new(),
        }
    }

    fn with_span(span: &'a Span, storage: &'a S) -> SpanContext<'a, S> {
        SpanContext {
            span_key: span.key(),
            storage,
            span: RefOrDeferredArc::Ref(span),
            parents: OnceCell::new(),
            resource: OnceCell::new(),
        }
    }

    fn key(&self) -> SpanKey {
        self.span_key
    }

    fn trace_root(&self) -> TraceRoot {
        let id = self.span().id;

        match id {
            FullSpanId::Tracing(_, _) => {
                let root_parent_id = self.parents().last().map(|p| p.id).unwrap_or(id);
                if let FullSpanId::Tracing(instance_id, span_id) = root_parent_id {
                    TraceRoot::Tracing(instance_id, span_id)
                } else {
                    panic!("tracing span's root span doesnt have tracing id");
                }
            }
            FullSpanId::Opentelemetry(trace_id, _) => TraceRoot::Opentelemetry(trace_id),
        }
    }

    fn span(&self) -> &Span {
        match &self.span {
            RefOrDeferredArc::Ref(span) => span,
            RefOrDeferredArc::Deferred(deferred) => {
                deferred.get_or_init(|| self.storage.get_span(self.span_key).unwrap())
            }
        }
    }

    fn parents(&self) -> impl Iterator<Item = &Span> {
        let span = self.span();

        self.parents
            .get_or_init(|| {
                let mut parents = vec![];
                let mut parent_key_next = span.parent_key;

                while let Some(parent_key) = parent_key_next {
                    let parent = self.storage.get_span(parent_key).unwrap();

                    parent_key_next = parent.parent_key;
                    parents.push(parent);
                }

                parents
            })
            .iter()
            .map(|p| &**p)
    }

    fn resource(&self) -> &Resource {
        let span = self.span();

        self.resource
            .get_or_init(|| self.storage.get_resource(span.resource_key).unwrap())
            .as_ref()
    }

    fn attribute(&self, attr: &str) -> Option<&Value> {
        let span = self.span();
        if let Some(v) = span.fields.get(attr) {
            return Some(v);
        }

        if let Some(v) = span.instrumentation_fields.get(attr) {
            return Some(v);
        }

        let parents = self.parents();
        for parent in parents {
            if let Some(v) = parent.fields.get(attr) {
                return Some(v);
            }
        }

        let resource = self.resource();
        if let Some(v) = resource.fields.get(attr) {
            return Some(v);
        }

        None
    }

    fn attribute_with_key(&self, attr: &str) -> Option<(&Value, Timestamp)> {
        let span = self.span();
        if let Some(v) = span.fields.get(attr) {
            return Some((v, span.key()));
        }

        if let Some(v) = span.instrumentation_fields.get(attr) {
            return Some((v, span.key()));
        }

        let parents = self.parents();
        for parent in parents {
            if let Some(v) = parent.fields.get(attr) {
                return Some((v, parent.key()));
            }
        }

        let resource = self.resource();
        if let Some(v) = resource.fields.get(attr) {
            return Some((v, resource.key()));
        }

        None
    }

    fn attributes(&self) -> impl Iterator<Item = (&str, &Value)> {
        let mut attributes = BTreeMap::new();

        let span = self.span();
        for (attr, value) in &span.fields {
            attributes.entry(&**attr).or_insert(value);
        }

        for (attr, value) in &span.instrumentation_fields {
            attributes.entry(&**attr).or_insert(value);
        }

        let parents = self.parents();
        for parent in parents {
            for (attr, value) in &parent.fields {
                attributes.entry(&**attr).or_insert(value);
            }
        }

        let resource = self.resource();
        for (attr, value) in &resource.fields {
            attributes.entry(&**attr).or_insert(value);
        }

        attributes.into_iter()
    }

    fn render(&self) -> SpanView {
        let span = self.span();

        let mut attributes =
            BTreeMap::<String, (AttributeSourceView, String, AttributeTypeView)>::new();
        for (attribute, value) in &self.span().fields {
            attributes.insert(
                attribute.to_owned(),
                (
                    AttributeSourceView::Inherent,
                    value.to_string(),
                    value.to_type_view(),
                ),
            );
        }
        for parent in self.parents() {
            for (attribute, value) in &parent.fields {
                if !attributes.contains_key(attribute) {
                    attributes.insert(
                        attribute.to_owned(),
                        (
                            AttributeSourceView::Span {
                                span_id: parent.id.to_string(),
                            },
                            value.to_string(),
                            value.to_type_view(),
                        ),
                    );
                }
            }
        }
        for (attribute, value) in &self.resource().fields {
            if !attributes.contains_key(attribute) {
                attributes.insert(
                    attribute.to_owned(),
                    (
                        AttributeSourceView::Resource,
                        value.to_string(),
                        value.to_type_view(),
                    ),
                );
            }
        }

        SpanView {
            kind: span.kind,
            id: span.id.to_string(),
            ancestors: {
                let mut ancestors = self
                    .parents()
                    .map(|parent| AncestorView {
                        id: parent.id.to_string(),
                        name: parent.name.clone(),
                    })
                    .collect::<Vec<_>>();

                ancestors.reverse();
                ancestors
            },
            created_at: span.created_at,
            closed_at: span.closed_at,
            busy: span.busy,
            level: span.level.into_simple_level(),
            name: span.name.clone(),
            namespace: span.namespace.clone(),
            function: span.function.clone(),
            file: match (&span.file_name, span.file_line) {
                (None, _) => None,
                (Some(name), None) => Some(name.clone()),
                (Some(name), Some(line)) => Some(format!("{name}:{line}")),
            },
            links: span.links.clone(),
            attributes: attributes
                .into_iter()
                .map(|(name, (kind, value, typ))| AttributeView {
                    name,
                    value,
                    typ,
                    source: kind,
                })
                .collect(),
        }
    }
}

enum RefOrDeferredArc<'a, T> {
    Ref(&'a T),
    Deferred(OnceCell<Arc<T>>),
}
