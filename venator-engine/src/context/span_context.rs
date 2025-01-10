use std::cell::OnceCell;
use std::collections::BTreeMap;
use std::sync::Arc;

use crate::storage::Storage;
use crate::{
    AncestorView, AttributeSourceView, AttributeTypeView, AttributeView, FullSpanId, Resource,
    Span, SpanKey, SpanView, Timestamp, TraceRoot, Value,
};

use super::RefOrDeferredArc;

pub(crate) struct SpanContext<'a, S> {
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
    pub(crate) fn new(span_key: SpanKey, storage: &'a S) -> SpanContext<'a, S> {
        SpanContext {
            span_key,
            storage,
            span: RefOrDeferredArc::Deferred(OnceCell::new()),
            parents: OnceCell::new(),
            resource: OnceCell::new(),
        }
    }

    pub(crate) fn with_span(span: &'a Span, storage: &'a S) -> SpanContext<'a, S> {
        SpanContext {
            span_key: span.key(),
            storage,
            span: RefOrDeferredArc::Ref(span),
            parents: OnceCell::new(),
            resource: OnceCell::new(),
        }
    }

    pub(crate) fn key(&self) -> SpanKey {
        self.span_key
    }

    pub(crate) fn trace_root(&self) -> TraceRoot {
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

    pub(crate) fn span(&self) -> &Span {
        match &self.span {
            RefOrDeferredArc::Ref(span) => span,
            RefOrDeferredArc::Deferred(deferred) => {
                deferred.get_or_init(|| self.storage.get_span(self.span_key).unwrap())
            }
        }
    }

    pub(crate) fn parents(&self) -> impl Iterator<Item = &Span> {
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

    pub(crate) fn resource(&self) -> &Resource {
        let span = self.span();

        self.resource
            .get_or_init(|| self.storage.get_resource(span.resource_key).unwrap())
            .as_ref()
    }

    pub(crate) fn attribute(&self, attr: &str) -> Option<&Value> {
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

    pub(crate) fn attribute_with_key(&self, attr: &str) -> Option<(&Value, Timestamp)> {
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

    pub(crate) fn attributes(&self) -> impl Iterator<Item = (&str, &Value)> {
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

    pub(crate) fn render(&self) -> SpanView {
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