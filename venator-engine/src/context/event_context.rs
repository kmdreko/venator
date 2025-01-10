use std::cell::OnceCell;
use std::collections::BTreeMap;
use std::sync::Arc;

use crate::storage::Storage;
use crate::{
    AncestorView, AttributeSourceView, AttributeTypeView, AttributeView, Event, EventKey,
    EventView, FullSpanId, Resource, Span, Timestamp, TraceRoot, Value,
};

use super::RefOrDeferredArc;

pub(crate) struct EventContext<'a, S> {
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
    pub(crate) fn new(event_key: EventKey, storage: &'a S) -> EventContext<'a, S> {
        EventContext {
            event_key,
            storage,
            event: RefOrDeferredArc::Deferred(OnceCell::new()),
            parents: OnceCell::new(),
            resource: OnceCell::new(),
        }
    }

    pub(crate) fn with_event(event: &'a Event, storage: &'a S) -> EventContext<'a, S> {
        EventContext {
            event_key: event.key(),
            storage,
            event: RefOrDeferredArc::Ref(event),
            parents: OnceCell::new(),
            resource: OnceCell::new(),
        }
    }

    pub(crate) fn key(&self) -> EventKey {
        self.event_key
    }

    pub(crate) fn trace_root(&self) -> Option<TraceRoot> {
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

    pub(crate) fn event(&self) -> &Event {
        match &self.event {
            RefOrDeferredArc::Ref(event) => event,
            RefOrDeferredArc::Deferred(deferred) => {
                deferred.get_or_init(|| self.storage.get_event(self.event_key).unwrap())
            }
        }
    }

    pub(crate) fn parents(&self) -> impl Iterator<Item = &Span> {
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

    pub(crate) fn resource(&self) -> &Resource {
        let event = self.event();

        self.resource
            .get_or_init(|| self.storage.get_resource(event.resource_key).unwrap())
            .as_ref()
    }

    pub(crate) fn attribute(&self, attr: &str) -> Option<&Value> {
        let event = self.event();
        if let Some(v) = event.attributes.get(attr) {
            return Some(v);
        }

        let parents = self.parents();
        for parent in parents {
            if let Some(v) = parent.attributes.get(attr) {
                return Some(v);
            }
        }

        let resource = self.resource();
        if let Some(v) = resource.attributes.get(attr) {
            return Some(v);
        }

        None
    }

    pub(crate) fn attribute_with_key(&self, attr: &str) -> Option<(&Value, Timestamp)> {
        let event = self.event();
        if let Some(v) = event.attributes.get(attr) {
            return Some((v, event.key()));
        }

        let parents = self.parents();
        for parent in parents {
            if let Some(v) = parent.attributes.get(attr) {
                return Some((v, parent.key()));
            }
        }

        let resource = self.resource();
        if let Some(v) = resource.attributes.get(attr) {
            return Some((v, resource.key()));
        }

        None
    }

    pub(crate) fn attributes(&self) -> impl Iterator<Item = (&str, &Value)> {
        let mut attributes = BTreeMap::new();

        let event = self.event();
        for (attr, value) in &event.attributes {
            attributes.entry(&**attr).or_insert(value);
        }

        let parents = self.parents();
        for parent in parents {
            for (attr, value) in &parent.attributes {
                attributes.entry(&**attr).or_insert(value);
            }
        }

        let resource = self.resource();
        for (attr, value) in &resource.attributes {
            attributes.entry(&**attr).or_insert(value);
        }

        attributes.into_iter()
    }

    pub(crate) fn render(&self) -> EventView {
        let event = self.event();

        let mut attributes =
            BTreeMap::<String, (AttributeSourceView, String, AttributeTypeView)>::new();
        for (attribute, value) in &self.event().attributes {
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
            for (attribute, value) in &parent.attributes {
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
        for (attribute, value) in &self.resource().attributes {
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
