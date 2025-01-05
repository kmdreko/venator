//! The "engine" crate represents the core functionality to injest, store,
//! index, and query the events and spans. It does not provide functionality
//! outside of its Rust API.

mod filter;
mod index;
mod models;
mod storage;
mod subscription;

use std::cell::{Cell, OnceCell};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::future::Future;
use std::sync::Arc;
use std::time::Instant;

use models::{
    AttributeTypeView, CloseSpanEvent, EnterSpanEvent, EventKey, FollowsSpanEvent, TraceRoot,
};
use serde::Serialize;
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};
use tokio::sync::oneshot::{self, Sender as OneshotSender};

use filter::{
    IndexedEventFilter, IndexedEventFilterIterator, IndexedSpanFilter, IndexedSpanFilterIterator,
};
use index::{EventIndexes, SpanEventIndexes, SpanIndexes};
use subscription::EventSubscription;

pub use filter::input::{
    FilterPredicate, FilterPredicateSingle, FilterPropertyKind, ValuePredicate,
};
pub use filter::{
    BasicEventFilter, BasicSpanFilter, FallibleFilterPredicate, InputError, Order, Query,
};
pub use models::{
    AncestorView, AttributeSourceView, AttributeView, CreateSpanEvent, Event, EventView,
    FullSpanId, InstanceId, Level, LevelConvertError, NewCloseSpanEvent, NewCreateSpanEvent,
    NewEnterSpanEvent, NewEvent, NewFollowsSpanEvent, NewResource, NewSpanEvent, NewSpanEventKind,
    NewUpdateSpanEvent, Resource, ResourceKey, SourceKind, Span, SpanEvent, SpanEventKey,
    SpanEventKind, SpanId, SpanKey, SpanView, StatsView, Timestamp, TraceId, UpdateSpanEvent,
    Value, ValueOperator,
};
pub use storage::{CachedStorage, Storage, TransientStorage};
pub use subscription::{SubscriptionId, SubscriptionResponse};

#[cfg(feature = "persist")]
pub use storage::FileStorage;

#[derive(Debug, Copy, Clone, Serialize)]
pub enum EngineInsertError {
    DuplicateConnectionId,
    DuplicateSpanId,
    InvalidSpanIdKind,
    UnknownConnectionId,
    UnknownSpanId,
    UnknownParentSpanId,
    UnknownLevel,
    AlreadyDisconnected,
}

#[derive(Clone)]
pub struct Engine {
    insert_sender: UnboundedSender<EngineCommand>,
    query_sender: UnboundedSender<EngineCommand>,
}

impl Engine {
    pub fn new<S: Storage + Send + 'static>(storage: S) -> Engine {
        let (insert_sender, mut insert_receiver) = mpsc::unbounded_channel();
        let (query_sender, mut query_receiver) = mpsc::unbounded_channel();

        std::thread::spawn(move || {
            let mut engine = RawEngine::new(storage);

            let mut last_check = Instant::now();
            let mut computed_ms_since_last_check: u128 = 0;

            let mut recv = || {
                futures::executor::block_on(async {
                    tokio::select! {
                        biased;
                        msg = query_receiver.recv() => {
                            msg
                        }
                        msg = insert_receiver.recv() => {
                            msg
                        }
                    }
                })
            };

            while let Some(cmd) = recv() {
                let cmd_start = Instant::now();
                match cmd {
                    EngineCommand::QuerySpan(query, sender) => {
                        let spans = engine.query_span(query);
                        let _ = sender.send(spans);
                    }
                    EngineCommand::QuerySpanCount(query, sender) => {
                        let events = engine.query_span_count(query);
                        let _ = sender.send(events);
                    }
                    EngineCommand::QuerySpanEvent(query, sender) => {
                        let span_events = engine.query_span_event(query);
                        let _ = sender.send(span_events);
                    }
                    EngineCommand::QueryEvent(query, sender) => {
                        let events = engine.query_event(query);
                        let _ = sender.send(events);
                    }
                    EngineCommand::QueryEventCount(query, sender) => {
                        let events = engine.query_event_count(query);
                        let _ = sender.send(events);
                    }
                    EngineCommand::QueryStats(sender) => {
                        let stats = engine.query_stats();
                        let _ = sender.send(stats);
                    }
                    EngineCommand::InsertResource(resource, sender) => {
                        let res = engine.insert_resource(resource);
                        if let Err(err) = &res {
                            eprintln!("rejecting resource insert due to: {err:?}");
                        }
                        let _ = sender.send(res);
                    }
                    EngineCommand::DisconnectTracingInstance(instance_id, sender) => {
                        let res = engine.disconnect_tracing_instance(instance_id);
                        if let Err(err) = &res {
                            eprintln!("rejecting disconnect due to: {err:?}");
                        }
                        let _ = sender.send(res);
                    }
                    EngineCommand::InsertSpanEvent(span_event, sender) => {
                        let res = engine.insert_span_event(span_event);
                        if let Err(err) = &res {
                            eprintln!("rejecting span event insert due to: {err:?}");
                        }
                        let _ = sender.send(res);
                    }
                    EngineCommand::InsertEvent(event, sender) => {
                        let res = engine.insert_event(event);
                        if let Err(err) = &res {
                            eprintln!("rejecting event insert due to: {err:?}");
                        }
                        let _ = sender.send(res);
                    }
                    EngineCommand::Delete(filter, sender) => {
                        let metrics = engine.delete(filter);
                        let _ = sender.send(metrics);
                    }
                    EngineCommand::EventSubscribe(filter, sender) => {
                        let res = engine.subscribe_to_events(filter);
                        let _ = sender.send(res);
                    }
                    EngineCommand::EventUnsubscribe(id, sender) => {
                        engine.unsubscribe_from_events(id);
                        let _ = sender.send(());
                    }
                    EngineCommand::CopyDataset(to, sender) => {
                        engine.copy_dataset(to);
                        let _ = sender.send(());
                    }
                    EngineCommand::GetStatus(sender) => {
                        let elapsed_ms = last_check.elapsed().as_millis();
                        let computed_ms = computed_ms_since_last_check;

                        last_check = Instant::now();
                        computed_ms_since_last_check = 0;

                        let load = computed_ms as f64 / elapsed_ms as f64;

                        let _ = sender.send(EngineStatusView {
                            load: load.min(1.0) * 100.0,
                        });
                    }
                    EngineCommand::Save(sender) => {
                        engine.save();
                        let _ = sender.send(());
                    }
                }
                let cmd_elapsed = cmd_start.elapsed().as_millis();
                computed_ms_since_last_check += cmd_elapsed;
            }
        });

        Engine {
            insert_sender,
            query_sender,
        }
    }

    // The query is executed even if the returned future is not awaited
    pub fn query_span(&self, query: Query) -> impl Future<Output = Vec<SpanView>> {
        let (sender, receiver) = oneshot::channel();
        let _ = self
            .query_sender
            .send(EngineCommand::QuerySpan(query, sender));
        async move { receiver.await.unwrap() }
    }

    // The query is executed even if the returned future is not awaited
    pub fn query_span_count(&self, query: Query) -> impl Future<Output = usize> {
        let (sender, receiver) = oneshot::channel();
        let _ = self
            .query_sender
            .send(EngineCommand::QuerySpanCount(query, sender));
        async move { receiver.await.unwrap() }
    }

    // The query is executed even if the returned future is not awaited
    pub fn query_span_event(&self, query: Query) -> impl Future<Output = Vec<SpanEvent>> {
        let (sender, receiver) = oneshot::channel();
        let _ = self
            .query_sender
            .send(EngineCommand::QuerySpanEvent(query, sender));
        async move { receiver.await.unwrap() }
    }

    // The query is executed even if the returned future is not awaited
    pub fn query_event(&self, query: Query) -> impl Future<Output = Vec<EventView>> {
        let (sender, receiver) = oneshot::channel();
        let _ = self
            .query_sender
            .send(EngineCommand::QueryEvent(query, sender));
        async move { receiver.await.unwrap() }
    }

    // The query is executed even if the returned future is not awaited
    pub fn query_event_count(&self, query: Query) -> impl Future<Output = usize> {
        let (sender, receiver) = oneshot::channel();
        let _ = self
            .query_sender
            .send(EngineCommand::QueryEventCount(query, sender));
        async move { receiver.await.unwrap() }
    }

    // The query is executed even if the returned future is not awaited
    pub fn query_stats(&self) -> impl Future<Output = StatsView> {
        let (sender, receiver) = oneshot::channel();
        let _ = self.query_sender.send(EngineCommand::QueryStats(sender));
        async move { receiver.await.unwrap() }
    }

    pub fn insert_resource(
        &self,
        resource: NewResource,
    ) -> impl Future<Output = Result<ResourceKey, EngineInsertError>> {
        let (sender, receiver) = oneshot::channel();
        let _ = self
            .insert_sender
            .send(EngineCommand::InsertResource(resource, sender));
        async move { receiver.await.unwrap() }
    }

    pub fn disconnect_tracing_instance(
        &self,
        id: InstanceId,
    ) -> impl Future<Output = Result<(), EngineInsertError>> {
        let (sender, receiver) = oneshot::channel();
        let _ = self
            .insert_sender
            .send(EngineCommand::DisconnectTracingInstance(id, sender));
        async move { receiver.await.unwrap() }
    }

    pub fn insert_span_event(
        &self,
        span_event: NewSpanEvent,
    ) -> impl Future<Output = Result<SpanKey, EngineInsertError>> {
        let (sender, receiver) = oneshot::channel();
        let _ = self
            .insert_sender
            .send(EngineCommand::InsertSpanEvent(span_event, sender));
        async move { receiver.await.unwrap() }
    }

    pub fn insert_event(
        &self,
        event: NewEvent,
    ) -> impl Future<Output = Result<(), EngineInsertError>> {
        let (sender, receiver) = oneshot::channel();
        let _ = self
            .insert_sender
            .send(EngineCommand::InsertEvent(event, sender));
        async move { receiver.await.unwrap() }
    }

    pub fn delete(&self, filter: DeleteFilter) -> impl Future<Output = DeleteMetrics> {
        let (sender, receiver) = oneshot::channel();
        let _ = self
            .insert_sender
            .send(EngineCommand::Delete(filter, sender));
        async move { receiver.await.unwrap() }
    }

    pub fn subscribe_to_events(
        &self,
        filter: Vec<FilterPredicate>,
    ) -> impl Future<
        Output = (
            SubscriptionId,
            UnboundedReceiver<SubscriptionResponse<EventView>>,
        ),
    > {
        let (sender, receiver) = oneshot::channel();
        let _ = self
            .query_sender
            .send(EngineCommand::EventSubscribe(filter, sender));
        async move { receiver.await.unwrap() }
    }

    pub fn unsubscribe_from_events(&self, id: SubscriptionId) -> impl Future<Output = ()> {
        let (sender, receiver) = oneshot::channel();
        let _ = self
            .query_sender
            .send(EngineCommand::EventUnsubscribe(id, sender));
        async move { receiver.await.unwrap() }
    }

    pub fn copy_dataset(&self, to: Box<dyn Storage + Send>) -> impl Future<Output = ()> {
        let (sender, receiver) = oneshot::channel();
        let _ = self
            .query_sender
            .send(EngineCommand::CopyDataset(to, sender));
        async move { receiver.await.unwrap() }
    }

    pub fn get_status(&self) -> impl Future<Output = EngineStatusView> {
        let (sender, receiver) = oneshot::channel();
        let _ = self.query_sender.send(EngineCommand::GetStatus(sender));
        async move { receiver.await.unwrap() }
    }

    pub fn save(&self) -> impl Future<Output = ()> {
        let (sender, receiver) = oneshot::channel();
        let _ = self.insert_sender.send(EngineCommand::Save(sender));
        async move { receiver.await.unwrap() }
    }
}

enum EngineCommand {
    QuerySpan(Query, OneshotSender<Vec<SpanView>>),
    QuerySpanCount(Query, OneshotSender<usize>),
    QuerySpanEvent(Query, OneshotSender<Vec<SpanEvent>>),
    QueryEvent(Query, OneshotSender<Vec<EventView>>),
    QueryEventCount(Query, OneshotSender<usize>),
    QueryStats(OneshotSender<StatsView>),
    InsertResource(
        NewResource,
        OneshotSender<Result<ResourceKey, EngineInsertError>>,
    ),
    DisconnectTracingInstance(InstanceId, OneshotSender<Result<(), EngineInsertError>>),
    InsertSpanEvent(
        NewSpanEvent,
        OneshotSender<Result<SpanKey, EngineInsertError>>,
    ),
    InsertEvent(NewEvent, OneshotSender<Result<(), EngineInsertError>>),
    Delete(DeleteFilter, OneshotSender<DeleteMetrics>),

    EventSubscribe(
        Vec<FilterPredicate>,
        OneshotSender<(
            SubscriptionId,
            UnboundedReceiver<SubscriptionResponse<EventView>>,
        )>,
    ),
    EventUnsubscribe(SubscriptionId, OneshotSender<()>),

    CopyDataset(Box<dyn Storage + Send>, OneshotSender<()>),
    GetStatus(OneshotSender<EngineStatusView>),

    Save(OneshotSender<()>),
}

pub struct EngineStatusView {
    pub load: f64,
}

pub struct DeleteFilter {
    pub start: Timestamp,
    pub end: Timestamp,
    pub inside: bool,
    pub dry_run: bool,
}

pub struct DeleteMetrics {
    pub spans: usize,
    pub span_events: usize,
    pub events: usize,
}

struct RawEngine<S> {
    storage: S,
    keys: KeyCache,
    resources: HashMap<ResourceKey, Resource>,

    span_indexes: SpanIndexes,
    span_event_indexes: SpanEventIndexes,
    event_indexes: EventIndexes,

    next_subscriber_id: usize,
    event_subscribers: HashMap<usize, EventSubscription>,
}

impl<S: Storage> RawEngine<S> {
    fn new(storage: S) -> RawEngine<S> {
        let mut engine = RawEngine {
            storage,
            keys: KeyCache::new(),
            resources: HashMap::new(),
            span_indexes: SpanIndexes::new(),
            span_event_indexes: SpanEventIndexes::new(),
            event_indexes: EventIndexes::new(),

            next_subscriber_id: 0,
            event_subscribers: HashMap::new(),
        };

        let resources = engine.storage.get_all_resources().collect::<Vec<_>>();

        for resource in resources {
            engine.insert_resource_bookeeping(&resource);
        }

        if let Some(indexes) = engine.storage.get_indexes() {
            let (span_indexes, span_event_indexes, event_indexes) = indexes;

            engine.span_indexes = span_indexes;
            engine.span_event_indexes = span_event_indexes;
            engine.event_indexes = event_indexes;
        } else {
            let spans = engine.storage.get_all_spans().collect::<Vec<_>>();

            for span in spans {
                engine.insert_span_bookeeping(&span);
            }

            let span_events = engine.storage.get_all_span_events().collect::<Vec<_>>();

            for span_event in span_events {
                engine.insert_span_event_bookeeping(&span_event);
            }

            let events = engine.storage.get_all_events().collect::<Vec<_>>();

            for event in events {
                engine.insert_event_bookeeping(&event);
            }
        }

        if !engine.span_indexes.durations.open.is_empty() {
            let last_event = engine.event_indexes.all.last();
            let last_span_event = engine.span_event_indexes.all.last();
            let last_at = match (last_event, last_span_event) {
                (Some(event), Some(span_event)) => Ord::max(*event, *span_event),
                (None, Some(span_event)) => *span_event,
                (Some(event), None) => *event,
                (None, None) => panic!("not possible to have open span but no span events"),
            };

            let at = last_at.saturating_add(1);

            for span_key in engine.span_indexes.durations.open.clone() {
                engine.span_indexes.update_with_closed(span_key, at);
                engine.storage.update_span_closed(span_key, at, None);
            }
        }

        engine
    }

    pub fn query_event(&self, query: Query) -> Vec<EventView> {
        let limit = query.limit;
        IndexedEventFilterIterator::new(query, self)
            .take(limit)
            .map(|event_key| self.storage.get_event(event_key).unwrap())
            .map(|event| EventContext::with_event(&event, &self.storage).render())
            .collect()
    }

    pub fn query_event_count(&self, query: Query) -> usize {
        let event_iter = IndexedEventFilterIterator::new(query, self);

        match event_iter.size_hint() {
            (min, Some(max)) if min == max => min,
            _ => event_iter.count(),
        }
    }

    pub fn query_span(&self, query: Query) -> Vec<SpanView> {
        let limit = query.limit;
        IndexedSpanFilterIterator::new(query, self)
            .take(limit)
            .map(|span_key| self.storage.get_span(span_key).unwrap())
            .map(|span| self.render_span(&span))
            .collect()
    }

    pub fn query_span_count(&self, query: Query) -> usize {
        let span_iter = IndexedSpanFilterIterator::new(query, self);

        match span_iter.size_hint() {
            (min, Some(max)) if min == max => min,
            _ => span_iter.count(),
        }
    }

    fn render_span(&self, span: &Span) -> SpanView {
        let context = SpanContext::with_span(span, &self.storage);

        let mut attributes =
            BTreeMap::<String, (AttributeSourceView, String, AttributeTypeView)>::new();
        for (attribute, value) in &context.span().fields {
            attributes.insert(
                attribute.to_owned(),
                (
                    AttributeSourceView::Inherent,
                    value.to_string(),
                    value.to_type_view(),
                ),
            );
        }
        for parent in context.parents() {
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
        for (attribute, value) in &context.resource().fields {
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
                let mut ancestors = context
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

    pub fn query_span_event(&self, _query: Query) -> Vec<SpanEvent> {
        unimplemented!()
    }

    pub fn query_stats(&self) -> StatsView {
        let event_start = self.event_indexes.all.first().copied();
        let event_end = self.event_indexes.all.last().copied();
        let span_start = self.span_indexes.all.first().copied();
        let span_end = self.span_indexes.all.last().copied(); // TODO: not technically right, but maybe okay

        StatsView {
            start: filter::merge(event_start, span_start, Ord::min),
            end: filter::merge(event_end, span_end, Ord::max),
            total_events: self.event_indexes.all.len(),
            total_spans: self.span_indexes.all.len(),
        }
    }

    pub fn insert_resource(
        &mut self,
        resource: NewResource,
    ) -> Result<ResourceKey, EngineInsertError> {
        if let Some((key, _)) = self
            .resources
            .iter()
            .find(|(_, r)| r.fields == resource.fields)
        {
            return Ok(*key);
        }

        let now = now();
        let resource_key = self.keys.register(now, now);
        let resource = Resource {
            created_at: resource_key,
            fields: resource.fields,
        };

        self.insert_resource_bookeeping(&resource);
        self.storage.insert_resource(resource);

        Ok(resource_key)
    }

    fn insert_resource_bookeeping(&mut self, resource: &Resource) {
        self.resources.insert(resource.key(), resource.clone());
    }

    pub fn disconnect_tracing_instance(
        &mut self,
        instance_id: InstanceId,
    ) -> Result<(), EngineInsertError> {
        let now = now();
        let at = self.keys.register(now, now);

        let filter = IndexedSpanFilter::And(vec![
            IndexedSpanFilter::Single(&self.span_indexes.durations.open, None),
            IndexedSpanFilter::Single(
                self.span_indexes
                    .instances
                    .get(&instance_id)
                    .map(Vec::as_slice)
                    .unwrap_or_default(),
                None,
            ),
        ]);

        let open_spans = IndexedSpanFilterIterator::new_internal(filter, self).collect::<Vec<_>>();

        for span_key in open_spans {
            self.span_indexes.update_with_closed(span_key, at);
            self.storage.update_span_closed(span_key, at, None);
        }

        Ok(())
    }

    pub fn insert_span_event(
        &mut self,
        mut new_span_event: NewSpanEvent,
    ) -> Result<SpanEventKey, EngineInsertError> {
        let span_event_key = self.keys.register(now(), new_span_event.timestamp);
        new_span_event.timestamp = span_event_key;

        match new_span_event.kind {
            NewSpanEventKind::Create(new_create_event) => {
                if self.span_indexes.ids.contains_key(&new_span_event.span_id) {
                    return Err(EngineInsertError::DuplicateSpanId);
                }

                // parent may not yet exist, that is ok
                let parent_id = new_create_event.parent_id;
                let parent_key = parent_id.and_then(|id| self.span_indexes.ids.get(&id).copied());

                let span = Span {
                    kind: new_create_event.kind,
                    resource_key: new_create_event.resource_key,
                    id: new_span_event.span_id,
                    created_at: new_span_event.timestamp,
                    closed_at: None,
                    busy: None,
                    parent_id,
                    parent_key,
                    links: Vec::new(),
                    name: new_create_event.name.clone(),
                    namespace: new_create_event.namespace.clone(),
                    function: new_create_event.function.clone(),
                    level: new_create_event.level,
                    file_name: new_create_event.file_name.clone(),
                    file_line: new_create_event.file_line,
                    file_column: new_create_event.file_column,
                    instrumentation_fields: new_create_event.instrumentation_fields.clone(),
                    fields: new_create_event.fields.clone(),
                };

                let span_event = SpanEvent {
                    timestamp: new_span_event.timestamp,
                    span_key: span.created_at,
                    kind: SpanEventKind::Create(CreateSpanEvent {
                        kind: new_create_event.kind,
                        resource_key: new_create_event.resource_key,
                        parent_key,
                        name: new_create_event.name,
                        namespace: new_create_event.namespace,
                        function: new_create_event.function,
                        level: new_create_event.level,
                        file_name: new_create_event.file_name,
                        file_line: new_create_event.file_line,
                        file_column: new_create_event.file_column,
                        instrumentation_fields: new_create_event.instrumentation_fields,
                        fields: new_create_event.fields,
                    }),
                };

                let (child_spans, child_events) = self.insert_span_bookeeping(&span);
                self.storage.insert_span(span.clone());
                self.storage.update_span_parents(span.key(), &child_spans);
                self.storage.update_event_parents(span.key(), &child_events);

                self.insert_span_event_bookeeping(&span_event);
                self.storage.insert_span_event(span_event);

                if !self.event_subscribers.is_empty() {
                    let root = SpanContext::with_span(&span, &self.storage).trace_root();
                    let descendent_events = self
                        .event_indexes
                        .traces
                        .get(&root)
                        .map(Vec::as_slice)
                        .unwrap_or_default()
                        .iter()
                        .copied()
                        .filter(|key| {
                            EventContext::new(*key, &self.storage)
                                .parents()
                                .any(|p| p.key() == span.key())
                        });

                    // update subscribers for events that may have been updated by
                    // a new parent
                    for event_key in descendent_events {
                        let context = EventContext::new(event_key, &self.storage);
                        for subscriber in self.event_subscribers.values_mut() {
                            subscriber.on_event(&context);
                        }
                    }
                }
            }
            NewSpanEventKind::Update(new_update_event) => {
                let span_key = self
                    .span_indexes
                    .ids
                    .get(&new_span_event.span_id)
                    .copied()
                    .ok_or(EngineInsertError::UnknownSpanId)?;

                let span = SpanContext::new(span_key, &self.storage);
                let trace = span.trace_root();

                let update_event = UpdateSpanEvent {
                    fields: new_update_event.fields.clone(),
                };

                let descendent_spans = self
                    .span_indexes
                    .traces
                    .get(&trace)
                    .map(Vec::as_slice)
                    .unwrap_or_default()
                    .iter()
                    .cloned()
                    .filter(|key| {
                        SpanContext::new(*key, &self.storage)
                            .parents()
                            .any(|p| p.key() == span_key)
                    })
                    .collect::<Vec<_>>();

                for child_span_key in descendent_spans {
                    // check if nested span attribute changed
                    self.span_indexes.update_with_new_field_on_parent(
                        &SpanContext::new(child_span_key, &self.storage),
                        span_key,
                        &update_event.fields,
                    );
                }

                let descendent_events = self
                    .event_indexes
                    .traces
                    .get(&trace)
                    .map(Vec::as_slice)
                    .unwrap_or_default()
                    .iter()
                    .cloned()
                    .filter(|key| {
                        EventContext::new(*key, &self.storage)
                            .parents()
                            .any(|p| p.key() == span_key)
                    })
                    .collect::<Vec<_>>();

                for event_key in descendent_events {
                    // check if nested event attribute changed
                    self.event_indexes.update_with_new_field_on_parent(
                        &EventContext::new(event_key, &self.storage),
                        span_key,
                        &update_event.fields,
                    );
                }

                let span_event = SpanEvent {
                    timestamp: new_span_event.timestamp,
                    span_key,
                    kind: SpanEventKind::Update(update_event),
                };

                self.storage
                    .update_span_fields(span_key, new_update_event.fields);

                self.insert_span_event_bookeeping(&span_event);
                self.storage.insert_span_event(span_event);

                if !self.event_subscribers.is_empty() {
                    let descendent_events = self
                        .event_indexes
                        .traces
                        .get(&trace)
                        .map(Vec::as_slice)
                        .unwrap_or_default()
                        .iter()
                        .copied()
                        .filter(|key| {
                            EventContext::new(*key, &self.storage)
                                .parents()
                                .any(|p| p.key() == span_key)
                        });

                    // update subscribers for events that may have been updated by
                    // an updated parent
                    for event_key in descendent_events {
                        let context = EventContext::new(event_key, &self.storage);
                        for subscriber in self.event_subscribers.values_mut() {
                            subscriber.on_event(&context);
                        }
                    }
                }
            }
            NewSpanEventKind::Follows(new_follows_event) => {
                let span_key = self
                    .span_indexes
                    .ids
                    .get(&new_span_event.span_id)
                    .copied()
                    .ok_or(EngineInsertError::UnknownSpanId)?;

                let FullSpanId::Tracing(instance_id, _) = new_span_event.span_id else {
                    return Err(EngineInsertError::InvalidSpanIdKind);
                };

                let follows_span_id = FullSpanId::Tracing(instance_id, new_follows_event.follows);
                let follows_span_key = self
                    .span_indexes
                    .ids
                    .get(&follows_span_id)
                    .copied()
                    .ok_or(EngineInsertError::UnknownSpanId)?;

                // TODO: check against circular following
                // TODO: check against duplicates

                let span_event = SpanEvent {
                    timestamp: new_span_event.timestamp,
                    span_key,
                    kind: SpanEventKind::Follows(FollowsSpanEvent {
                        follows: follows_span_key,
                    }),
                };

                self.storage
                    .update_span_link(span_key, follows_span_id, BTreeMap::new());

                self.insert_span_event_bookeeping(&span_event);
                self.storage.insert_span_event(span_event);
            }
            NewSpanEventKind::Enter(new_enter_event) => {
                let span_key = self
                    .span_indexes
                    .ids
                    .get(&new_span_event.span_id)
                    .copied()
                    .ok_or(EngineInsertError::UnknownSpanId)?;

                let span_event = SpanEvent {
                    timestamp: new_span_event.timestamp,
                    span_key,
                    kind: SpanEventKind::Enter(EnterSpanEvent {
                        thread_id: new_enter_event.thread_id,
                    }),
                };

                self.insert_span_event_bookeeping(&span_event);
                self.storage.insert_span_event(span_event);
            }
            NewSpanEventKind::Exit => {
                let span_key = self
                    .span_indexes
                    .ids
                    .get(&new_span_event.span_id)
                    .copied()
                    .ok_or(EngineInsertError::UnknownSpanId)?;

                let span_event = SpanEvent {
                    timestamp: new_span_event.timestamp,
                    span_key,
                    kind: SpanEventKind::Exit,
                };

                self.insert_span_event_bookeeping(&span_event);
                self.storage.insert_span_event(span_event);
            }
            NewSpanEventKind::Close(new_close_event) => {
                let span_key = self
                    .span_indexes
                    .ids
                    .get(&new_span_event.span_id)
                    .copied()
                    .ok_or(EngineInsertError::UnknownSpanId)?;

                let busy = if let Some(busy) = new_close_event.busy {
                    Some(busy)
                } else {
                    let mut busy = 0;
                    let mut last_enter = None;
                    for span_event_key in &self.span_event_indexes.spans[&span_key] {
                        let span_event = self.storage.get_span_event(*span_event_key).unwrap();
                        match &span_event.kind {
                            SpanEventKind::Enter(_) => {
                                last_enter = Some(span_event.timestamp);
                            }
                            SpanEventKind::Exit => {
                                if let Some(enter) = last_enter {
                                    busy += span_event.timestamp.get() - enter.get();
                                }
                                last_enter = None;
                            }
                            _ => {}
                        }
                    }

                    if busy == 0 {
                        None
                    } else {
                        Some(busy)
                    }
                };

                let span_event = SpanEvent {
                    timestamp: new_span_event.timestamp,
                    span_key,
                    kind: SpanEventKind::Close(CloseSpanEvent { busy }),
                };

                self.span_indexes
                    .update_with_closed(span_key, new_span_event.timestamp);

                self.storage
                    .update_span_closed(span_key, new_span_event.timestamp, busy);

                self.insert_span_event_bookeeping(&span_event);
                self.storage.insert_span_event(span_event);
            }
        }

        Ok(span_event_key)
    }

    fn insert_span_bookeeping(&mut self, span: &Span) -> (Vec<SpanKey>, Vec<EventKey>) {
        let span_key = span.created_at;

        let spans_to_update_parent = self
            .span_indexes
            .update_with_new_span(&SpanContext::with_span(span, &self.storage));

        let trace = SpanContext::with_span(span, &self.storage).trace_root();

        let descendent_spans = self
            .span_indexes
            .traces
            .get(&trace)
            .map(Vec::as_slice)
            .unwrap_or_default()
            .iter()
            .cloned()
            .filter(|key| *key != span_key)
            .filter(|key| {
                SpanContext::new(*key, &self.storage)
                    .parents()
                    .any(|p| p.key() == span_key)
            })
            .collect::<Vec<_>>();

        for descendent in descendent_spans {
            self.span_indexes.update_with_new_field_on_parent(
                &SpanContext::new(descendent, &self.storage),
                span.key(),
                &span.fields,
            );
        }

        let events_to_update_parent = self
            .event_indexes
            .update_with_new_span(&SpanContext::with_span(span, &self.storage));

        let descendent_events = self
            .event_indexes
            .traces
            .get(&trace)
            .map(Vec::as_slice)
            .unwrap_or_default()
            .iter()
            .cloned()
            .filter(|key| {
                EventContext::new(*key, &self.storage)
                    .parents()
                    .any(|p| p.key() == span_key)
            })
            .collect::<Vec<_>>();

        for descendent in descendent_events {
            self.event_indexes.update_with_new_field_on_parent(
                &EventContext::new(descendent, &self.storage),
                span.key(),
                &span.fields,
            );
        }

        (spans_to_update_parent, events_to_update_parent)
    }

    fn insert_span_event_bookeeping(&mut self, span_event: &SpanEvent) {
        self.span_event_indexes
            .update_with_new_span_event(span_event);
    }

    pub fn insert_event(&mut self, mut new_event: NewEvent) -> Result<(), EngineInsertError> {
        let event_key = self.keys.register(now(), new_event.timestamp);
        new_event.timestamp = event_key;

        // parent may not yet exist, that is ok
        let parent_id = new_event.span_id;
        let parent_key = parent_id.and_then(|id| self.span_indexes.ids.get(&id).copied());

        let event = Event {
            kind: new_event.kind,
            resource_key: new_event.resource_key,
            timestamp: new_event.timestamp,
            parent_id,
            parent_key,
            content: new_event.content,
            namespace: new_event.namespace,
            function: new_event.function,
            level: new_event.level,
            file_name: new_event.file_name,
            file_line: new_event.file_line,
            file_column: new_event.file_column,
            fields: new_event.fields,
        };

        self.insert_event_bookeeping(&event);
        self.storage.insert_event(event.clone());

        let context = EventContext::with_event(&event, &self.storage);
        for subscriber in self.event_subscribers.values_mut() {
            subscriber.on_event(&context);
        }

        self.event_subscribers.retain(|_, s| s.connected());

        Ok(())
    }

    fn insert_event_bookeeping(&mut self, event: &Event) {
        self.event_indexes
            .update_with_new_event(&EventContext::with_event(event, &self.storage));
    }

    pub fn delete(&mut self, filter: DeleteFilter) -> DeleteMetrics {
        // TODO: clean up resources as well

        let root_spans =
            self.get_root_spans_in_range_filter(filter.start, filter.end, filter.inside);
        let root_events =
            self.get_root_events_in_range_filter(filter.start, filter.end, filter.inside);

        let spans_from_root_spans = root_spans
            .iter()
            .flat_map(|root| {
                self.span_indexes
                    .traces
                    .get(&SpanContext::new(*root, &self.storage).trace_root())
                    .map(Vec::as_slice)
                    .unwrap_or_default()
                    .iter()
                    .cloned()
            })
            .collect::<Vec<SpanKey>>();
        let events_from_root_spans = root_spans
            .iter()
            .flat_map(|root| {
                self.event_indexes
                    .traces
                    .get(&SpanContext::new(*root, &self.storage).trace_root())
                    .map(Vec::as_slice)
                    .unwrap_or_default()
                    .iter()
                    .cloned()
            })
            .collect::<Vec<EventKey>>();
        let span_events = spans_from_root_spans
            .iter()
            .flat_map(|span| {
                self.span_event_indexes
                    .spans
                    .get(span)
                    .map(Vec::as_slice)
                    .unwrap_or_default()
                    .iter()
                    .cloned()
            })
            .collect::<Vec<SpanEventKey>>();

        if filter.dry_run {
            return DeleteMetrics {
                spans: spans_from_root_spans.len(),
                span_events: span_events.len(),
                events: root_events.len() + events_from_root_spans.len(),
            };
        }

        let mut spans_to_delete = spans_from_root_spans;
        let mut span_events_to_delete = span_events;
        let mut events_to_delete = root_events;
        events_to_delete.extend(events_from_root_spans);

        spans_to_delete.sort();
        span_events_to_delete.sort();
        events_to_delete.sort();

        // drop smaller scoped entities from storage first to avoid integrity
        // issues if things go wrong

        self.storage.drop_events(&events_to_delete);
        self.storage.drop_span_events(&span_events_to_delete);
        self.storage.drop_spans(&spans_to_delete);

        // remove smaller scoped entities from indexes last for some efficiency

        self.remove_spans_bookeeping(&spans_to_delete);
        self.remove_span_events_bookeeping(&span_events_to_delete);
        self.remove_events_bookeeping(&events_to_delete);

        DeleteMetrics {
            spans: spans_to_delete.len(),
            span_events: span_events_to_delete.len(),
            events: events_to_delete.len(),
        }
    }

    pub fn get_root_spans_in_range_filter(
        &self,
        start: Timestamp,
        end: Timestamp,
        inside: bool,
    ) -> Vec<SpanKey> {
        let filter = if inside {
            BasicSpanFilter::And(vec![
                BasicSpanFilter::Created(ValueOperator::Lte, end),
                BasicSpanFilter::Closed(ValueOperator::Gte, start),
                BasicSpanFilter::Root,
            ])
        } else {
            BasicSpanFilter::And(vec![
                BasicSpanFilter::Or(vec![
                    BasicSpanFilter::Created(ValueOperator::Gt, end),
                    BasicSpanFilter::Closed(ValueOperator::Lt, start),
                ]),
                BasicSpanFilter::Root,
            ])
        };

        let indexed_filter =
            IndexedSpanFilter::build(Some(filter), &self.span_indexes, &self.storage);
        let iter = IndexedSpanFilterIterator::new_internal(indexed_filter, self);

        iter.collect()
    }

    pub fn get_root_events_in_range_filter(
        &self,
        start: Timestamp,
        end: Timestamp,
        inside: bool,
    ) -> Vec<SpanKey> {
        let filter = if inside {
            BasicEventFilter::And(vec![
                BasicEventFilter::Timestamp(ValueOperator::Lte, end),
                BasicEventFilter::Timestamp(ValueOperator::Gte, start),
                BasicEventFilter::Root,
            ])
        } else {
            BasicEventFilter::And(vec![
                BasicEventFilter::Or(vec![
                    BasicEventFilter::Timestamp(ValueOperator::Gt, end),
                    BasicEventFilter::Timestamp(ValueOperator::Lt, start),
                ]),
                BasicEventFilter::Root,
            ])
        };

        let indexed_filter =
            IndexedEventFilter::build(Some(filter), &self.event_indexes, &self.storage);
        let iter = IndexedEventFilterIterator::new_internal(indexed_filter, self);

        iter.collect()
    }

    fn remove_spans_bookeeping(&mut self, spans: &[SpanKey]) {
        self.span_indexes.remove_spans(spans);
        self.span_event_indexes.remove_spans(spans);
        self.event_indexes.remove_spans(spans);
    }

    fn remove_span_events_bookeeping(&mut self, span_events: &[SpanEventKey]) {
        self.span_event_indexes.remove_span_events(span_events);
    }

    fn remove_events_bookeeping(&mut self, events: &[EventKey]) {
        self.event_indexes.remove_events(events);
    }

    pub fn copy_dataset(&self, mut to: Box<dyn Storage + Send>) {
        let resources = self.storage.get_all_resources().collect::<Vec<_>>();

        for resource in resources {
            to.insert_resource((*resource).clone());
        }

        let spans = self.storage.get_all_spans().collect::<Vec<_>>();

        for span in spans {
            to.insert_span((*span).clone());
        }

        let span_events = self.storage.get_all_span_events().collect::<Vec<_>>();

        for span_event in span_events {
            to.insert_span_event((*span_event).clone());
        }

        let events = self.storage.get_all_events().collect::<Vec<_>>();

        for event in events {
            to.insert_event((*event).clone());
        }
    }

    pub fn subscribe_to_events(
        &mut self,
        filter: Vec<FilterPredicate>,
    ) -> (
        SubscriptionId,
        UnboundedReceiver<SubscriptionResponse<EventView>>,
    ) {
        let mut filter = BasicEventFilter::And(
            filter
                .into_iter()
                .map(|p| BasicEventFilter::from_predicate(p, &self.span_indexes.ids).unwrap())
                .collect(),
        );
        filter.simplify();

        let id = self.next_subscriber_id;
        self.next_subscriber_id += 1;

        let (sender, receiver) = mpsc::unbounded_channel();

        self.event_subscribers
            .insert(id, EventSubscription::new(filter, sender));

        (id, receiver)
    }

    pub fn unsubscribe_from_events(&mut self, id: SubscriptionId) {
        self.event_subscribers.remove(&id);
    }

    pub fn save(&mut self) {
        self.storage.update_indexes(
            &self.span_indexes,
            &self.span_event_indexes,
            &self.event_indexes,
        );
    }
}

struct KeyCache {
    keys: Cell<VecDeque<Timestamp>>,
}

impl KeyCache {
    fn new() -> KeyCache {
        KeyCache {
            keys: Cell::new(VecDeque::new()),
        }
    }

    fn register(&self, now: Timestamp, desired: Timestamp) -> Timestamp {
        let mut keys = self.keys.take();

        // only keep 10s of cached keys and limit to 1s in the future
        let min = saturating_sub(now, 10000000);
        let max = now.saturating_add(1000000);

        let mut desired = desired.max(min).min(max);

        let idx = keys.partition_point(|key| *key < min);
        keys.drain(..idx);

        let mut idx = keys.partition_point(|key| *key < desired);
        while idx < keys.len() {
            if keys[idx] != desired {
                break;
            } else {
                idx += 1;
                desired = desired.saturating_add(1);
            }
        }

        keys.insert(idx, desired);

        self.keys.set(keys);
        desired
    }
}

fn saturating_sub(a: Timestamp, b: u64) -> Timestamp {
    Timestamp::new(a.get().saturating_sub(b)).unwrap_or(Timestamp::MIN)
}

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
}

enum RefOrDeferredArc<'a, T> {
    Ref(&'a T),
    Deferred(OnceCell<Arc<T>>),
}

fn now() -> Timestamp {
    #[cfg(test)]
    return Timestamp::new(1000).unwrap();

    #[cfg(not(test))]
    {
        use std::time::{SystemTime, UNIX_EPOCH};
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64;
        Timestamp::new(timestamp).unwrap()
    }
}

#[cfg(test)]
mod tests {
    use filter::Order;
    use models::{Level, NewCloseSpanEvent, NewCreateSpanEvent, NewUpdateSpanEvent, SourceKind};

    use super::*;

    #[test]
    fn test_event_filters() {
        let mut engine = RawEngine::new(TransientStorage::new());

        let resource_key = engine
            .insert_resource(NewResource {
                fields: BTreeMap::new(),
            })
            .unwrap();

        let simple = |id: u64, level: i32, attribute1: &str, attribute2: &str| -> NewEvent {
            NewEvent {
                kind: SourceKind::Tracing,
                resource_key,
                timestamp: id.try_into().unwrap(),
                span_id: None,
                content: Value::Str("event".to_owned()),
                namespace: Some("crate::storage::tests".to_owned()),
                function: Some("test".to_owned()),
                level: Level::from_tracing_level(level).unwrap(),
                file_name: None,
                file_line: None,
                file_column: None,
                fields: BTreeMap::from_iter([
                    ("attribute1".to_owned(), Value::Str(attribute1.to_owned())),
                    ("attribute2".to_owned(), Value::Str(attribute2.to_owned())),
                ]),
            }
        };

        engine.insert_event(simple(1, 4, "test", "A")).unwrap(); // excluded by timestamp
        engine.insert_event(simple(2, 1, "test", "A")).unwrap(); // excluded by level
        engine.insert_event(simple(3, 2, "test", "A")).unwrap(); // excluded by level
        engine.insert_event(simple(4, 3, "test", "A")).unwrap();
        engine.insert_event(simple(5, 4, "test", "A")).unwrap();
        engine.insert_event(simple(6, 4, "blah", "A")).unwrap(); // excluded by "blah"
        engine.insert_event(simple(7, 4, "test", "B")).unwrap(); // excluded by "B"
        engine.insert_event(simple(8, 4, "test", "C")).unwrap(); // excluded by "C"
        engine.insert_event(simple(9, 4, "test", "A")).unwrap(); // excluded by timestamp

        let events = engine.query_event(Query {
            filter: FilterPredicate::parse(
                "#level: >=WARN @\"attribute1\": test @\"attribute2\": A",
            )
            .unwrap(),
            order: Order::Asc,
            limit: 3,
            start: Timestamp::new(2).unwrap(),
            end: Timestamp::new(8).unwrap(),
            previous: None,
        });

        assert_eq!(events.len(), 2);
        assert_eq!(events[0].timestamp, Timestamp::new(4).unwrap());
        assert_eq!(events[1].timestamp, Timestamp::new(5).unwrap());
    }

    #[test]
    fn test_span_filters() {
        let mut engine = RawEngine::new(TransientStorage::new());

        let resource_key = engine
            .insert_resource(NewResource {
                fields: BTreeMap::new(),
            })
            .unwrap();

        let simple_open =
            |open: u64, level: i32, attribute1: &str, attribute2: &str| -> NewSpanEvent {
                NewSpanEvent {
                    timestamp: Timestamp::new(open).unwrap(),
                    span_id: FullSpanId::Tracing(1.try_into().unwrap(), open),
                    kind: NewSpanEventKind::Create(NewCreateSpanEvent {
                        kind: SourceKind::Tracing,
                        resource_key,
                        parent_id: None,
                        name: "test".to_owned(),
                        namespace: Some("crate::storage::tests".to_owned()),
                        function: None,
                        level: Level::from_tracing_level(level).unwrap(),
                        file_name: None,
                        file_line: None,
                        file_column: None,
                        instrumentation_fields: BTreeMap::default(),
                        fields: BTreeMap::from_iter([
                            ("attribute1".to_owned(), Value::Str(attribute1.to_owned())),
                            ("attribute2".to_owned(), Value::Str(attribute2.to_owned())),
                        ]),
                    }),
                }
            };

        let simple_close = |open: u64, close: u64| -> NewSpanEvent {
            NewSpanEvent {
                timestamp: Timestamp::new(close).unwrap(),
                span_id: FullSpanId::Tracing(1.try_into().unwrap(), open),
                kind: NewSpanEventKind::Close(NewCloseSpanEvent { busy: None }),
            }
        };

        engine
            .insert_span_event(simple_open(1, 4, "test", "A"))
            .unwrap(); // excluded by timestamp
        engine.insert_span_event(simple_close(1, 2)).unwrap();
        engine
            .insert_span_event(simple_open(3, 1, "test", "A"))
            .unwrap(); // excluded by level
        engine.insert_span_event(simple_close(3, 6)).unwrap();
        engine
            .insert_span_event(simple_open(4, 2, "test", "A"))
            .unwrap(); // excluded by level
        engine.insert_span_event(simple_close(4, 7)).unwrap();
        engine
            .insert_span_event(simple_open(5, 3, "test", "A"))
            .unwrap();
        engine.insert_span_event(simple_close(5, 8)).unwrap();
        engine
            .insert_span_event(simple_open(9, 4, "test", "A"))
            .unwrap();
        engine
            .insert_span_event(simple_open(10, 4, "blah", "A"))
            .unwrap(); // excluded by "blah"
        engine
            .insert_span_event(simple_open(11, 4, "test", "B"))
            .unwrap(); // excluded by "B"
        engine
            .insert_span_event(simple_open(12, 4, "test", "C"))
            .unwrap(); // excluded by "C"
        engine
            .insert_span_event(simple_open(13, 4, "test", "A"))
            .unwrap(); // excluded by timestamp

        let spans = engine.query_span(Query {
            filter: FilterPredicate::parse(
                "#level: >=WARN @\"attribute1\": test @\"attribute2\": A",
            )
            .unwrap(),
            order: Order::Asc,
            limit: 5,
            start: Timestamp::new(2).unwrap(),
            end: Timestamp::new(10).unwrap(),
            previous: None,
        });

        assert_eq!(spans.len(), 2);
        assert_eq!(spans[0].created_at, Timestamp::new(5).unwrap());
        assert_eq!(spans[1].created_at, Timestamp::new(9).unwrap());
    }

    #[test]
    fn event_found_with_resource_attribute() {
        let mut engine = RawEngine::new(TransientStorage::new());

        let resource_key = engine
            .insert_resource(NewResource {
                fields: BTreeMap::from_iter([("attr1".to_owned(), Value::Str("A".to_owned()))]),
            })
            .unwrap();

        let now = now();
        engine
            .insert_event(NewEvent {
                kind: SourceKind::Tracing,
                resource_key,
                timestamp: now.saturating_add(1),
                span_id: None,
                content: Value::Str("event".to_owned()),
                namespace: Some("crate::storage::tests".to_owned()),
                function: Some("test".to_owned()),
                level: Level::Error,
                file_name: None,
                file_line: None,
                file_column: None,
                fields: BTreeMap::new(),
            })
            .unwrap();

        let events = engine.query_event(Query {
            filter: FilterPredicate::parse("@\"attr1\": A").unwrap(),
            order: Order::Asc,
            limit: 5,
            start: now,
            end: now.saturating_add(2),
            previous: None,
        });

        assert_eq!(events.len(), 1);

        let events = engine.query_event(Query {
            filter: FilterPredicate::parse("@\"attr1\": B").unwrap(),
            order: Order::Asc,
            limit: 5,
            start: now,
            end: now.saturating_add(2),
            previous: None,
        });

        assert_eq!(events.len(), 0);
    }

    #[test]
    fn event_found_with_inherent_attribute() {
        let mut engine = RawEngine::new(TransientStorage::new());

        let resource_key = engine
            .insert_resource(NewResource {
                fields: BTreeMap::from_iter([("attr1".to_owned(), Value::Str("A".to_owned()))]),
            })
            .unwrap();

        let now = now();
        engine
            .insert_event(NewEvent {
                kind: SourceKind::Tracing,
                resource_key,
                timestamp: now.saturating_add(1),
                span_id: None,
                content: Value::Str("event".to_owned()),
                namespace: Some("crate::storage::tests".to_owned()),
                function: Some("test".to_owned()),
                level: Level::Error,
                file_name: None,
                file_line: None,
                file_column: None,
                fields: BTreeMap::from_iter([("attr1".to_owned(), Value::Str("B".to_owned()))]),
            })
            .unwrap();

        let events = engine.query_event(Query {
            filter: FilterPredicate::parse("@\"attr1\": A").unwrap(),
            order: Order::Asc,
            limit: 5,
            start: now,
            end: now.saturating_add(2),
            previous: None,
        });

        assert_eq!(events.len(), 0);

        let events = engine.query_event(Query {
            filter: FilterPredicate::parse("@\"attr1\": B").unwrap(),
            order: Order::Asc,
            limit: 5,
            start: now,
            end: now.saturating_add(2),
            previous: None,
        });

        assert_eq!(events.len(), 1);
    }

    #[test]
    fn event_found_with_span_attribute() {
        let mut engine = RawEngine::new(TransientStorage::new());

        let resource_key = engine
            .insert_resource(NewResource {
                fields: BTreeMap::from_iter([("attr1".to_owned(), Value::Str("A".to_owned()))]),
            })
            .unwrap();

        engine
            .insert_span_event(NewSpanEvent {
                timestamp: now(),
                span_id: FullSpanId::Tracing(1.try_into().unwrap(), 1),
                kind: NewSpanEventKind::Create(NewCreateSpanEvent {
                    kind: SourceKind::Tracing,
                    resource_key,
                    parent_id: None,
                    name: "test".to_owned(),
                    namespace: Some("crate::storage::tests".to_owned()),
                    function: None,
                    level: Level::Error,
                    file_name: None,
                    file_line: None,
                    file_column: None,
                    instrumentation_fields: BTreeMap::default(),
                    fields: BTreeMap::from_iter([("attr1".to_owned(), Value::Str("C".to_owned()))]),
                }),
            })
            .unwrap();

        let now = now();
        engine
            .insert_event(NewEvent {
                kind: SourceKind::Tracing,
                resource_key,
                timestamp: now.saturating_add(1),
                span_id: Some(FullSpanId::Tracing(1.try_into().unwrap(), 1)),
                content: Value::Str("event".to_owned()),
                namespace: Some("crate::storage::tests".to_owned()),
                function: Some("test".to_owned()),
                level: Level::Error,
                file_name: None,
                file_line: None,
                file_column: None,
                fields: BTreeMap::new(),
            })
            .unwrap();

        let events = engine.query_event(Query {
            filter: FilterPredicate::parse("@\"attr1\": A").unwrap(),
            order: Order::Asc,
            limit: 5,
            start: now,
            end: now.saturating_add(2),
            previous: None,
        });

        assert_eq!(events.len(), 0);

        let events = engine.query_event(Query {
            filter: FilterPredicate::parse("@\"attr1\": C").unwrap(),
            order: Order::Asc,
            limit: 5,
            start: now,
            end: now.saturating_add(2),
            previous: None,
        });

        assert_eq!(events.len(), 1);
    }

    #[test]
    fn event_found_with_nonindexed_updated_span_attribute() {
        let mut engine = RawEngine::new(TransientStorage::new());

        let resource_key = engine
            .insert_resource(NewResource {
                fields: BTreeMap::from_iter([("attr1".to_owned(), Value::Str("A".to_owned()))]),
            })
            .unwrap();

        engine
            .insert_span_event(NewSpanEvent {
                timestamp: now(),
                span_id: FullSpanId::Tracing(1.try_into().unwrap(), 1),
                kind: NewSpanEventKind::Create(NewCreateSpanEvent {
                    kind: SourceKind::Tracing,
                    resource_key,
                    parent_id: None,
                    name: "test".to_owned(),
                    namespace: Some("crate::storage::tests".to_owned()),
                    function: None,
                    level: Level::Error,
                    file_name: None,
                    file_line: None,
                    file_column: None,
                    instrumentation_fields: BTreeMap::default(),
                    fields: BTreeMap::new(),
                }),
            })
            .unwrap();

        let now = now();
        engine
            .insert_event(NewEvent {
                kind: SourceKind::Tracing,
                resource_key,
                timestamp: now.saturating_add(1),
                span_id: Some(FullSpanId::Tracing(1.try_into().unwrap(), 1)),
                content: Value::Str("event".to_owned()),
                namespace: Some("crate::storage::tests".to_owned()),
                function: Some("test".to_owned()),
                level: Level::Error,
                file_name: None,
                file_line: None,
                file_column: None,
                fields: BTreeMap::new(),
            })
            .unwrap();

        engine
            .insert_span_event(NewSpanEvent {
                timestamp: super::now(),
                span_id: FullSpanId::Tracing(1.try_into().unwrap(), 1),
                kind: NewSpanEventKind::Update(NewUpdateSpanEvent {
                    fields: BTreeMap::from_iter([("attr1".to_owned(), Value::Str("C".to_owned()))]),
                }),
            })
            .unwrap();

        let events = engine.query_event(Query {
            filter: FilterPredicate::parse("@\"attr1\": A").unwrap(),
            order: Order::Asc,
            limit: 5,
            start: now,
            end: now.saturating_add(2),
            previous: None,
        });

        assert_eq!(events.len(), 0);

        let events = engine.query_event(Query {
            filter: FilterPredicate::parse("@\"attr1\": C").unwrap(),
            order: Order::Asc,
            limit: 5,
            start: now,
            end: now.saturating_add(2),
            previous: None,
        });

        assert_eq!(events.len(), 1);
    }

    #[test]
    fn event_found_with_indexed_updated_span_attribute() {
        let mut engine = RawEngine::new(TransientStorage::new());

        let resource_key = engine
            .insert_resource(NewResource {
                fields: BTreeMap::from_iter([("attr1".to_owned(), Value::Str("A".to_owned()))]),
            })
            .unwrap();

        engine
            .insert_span_event(NewSpanEvent {
                timestamp: now(),
                span_id: FullSpanId::Tracing(1.try_into().unwrap(), 1),
                kind: NewSpanEventKind::Create(NewCreateSpanEvent {
                    kind: SourceKind::Tracing,
                    resource_key,
                    parent_id: None,
                    name: "test".to_owned(),
                    namespace: Some("crate::storage::tests".to_owned()),
                    function: None,
                    level: Level::Error,
                    file_name: None,
                    file_line: None,
                    file_column: None,
                    instrumentation_fields: BTreeMap::default(),
                    fields: BTreeMap::new(),
                }),
            })
            .unwrap();

        let now = now();
        engine
            .insert_event(NewEvent {
                kind: SourceKind::Tracing,
                resource_key,
                timestamp: now.saturating_add(1),
                span_id: Some(FullSpanId::Tracing(1.try_into().unwrap(), 1)),
                content: Value::Str("event".to_owned()),
                namespace: Some("crate::storage::tests".to_owned()),
                function: Some("test".to_owned()),
                level: Level::Error,
                file_name: None,
                file_line: None,
                file_column: None,
                fields: BTreeMap::new(),
            })
            .unwrap();

        engine
            .insert_span_event(NewSpanEvent {
                timestamp: super::now(),
                span_id: FullSpanId::Tracing(1.try_into().unwrap(), 1),
                kind: NewSpanEventKind::Update(NewUpdateSpanEvent {
                    fields: BTreeMap::from_iter([("attr1".to_owned(), Value::Str("C".to_owned()))]),
                }),
            })
            .unwrap();

        let events = engine.query_event(Query {
            filter: FilterPredicate::parse("@\"attr1\": A").unwrap(),
            order: Order::Asc,
            limit: 5,
            start: now,
            end: now.saturating_add(2),
            previous: None,
        });

        assert_eq!(events.len(), 0);

        let events = engine.query_event(Query {
            filter: FilterPredicate::parse("@\"attr1\": C").unwrap(),
            order: Order::Asc,
            limit: 5,
            start: now,
            end: now.saturating_add(2),
            previous: None,
        });

        assert_eq!(events.len(), 1);
    }

    #[test]
    fn key_cache() {
        let cache = KeyCache::new();

        assert_eq!(
            cache.register(1000.try_into().unwrap(), 1.try_into().unwrap()),
            Timestamp::new(1).unwrap()
        );

        assert_eq!(
            cache.register(1000.try_into().unwrap(), 1.try_into().unwrap()),
            Timestamp::new(2).unwrap()
        );

        assert_eq!(
            cache.register(20000000.try_into().unwrap(), 1.try_into().unwrap()),
            Timestamp::new(10000000).unwrap()
        );
    }
}
