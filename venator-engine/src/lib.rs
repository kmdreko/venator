//! The "engine" crate represents the core functionality to injest, store,
//! index, and query the events and spans. It does not provide functionality
//! outside of its Rust API.

mod filter;
mod index;
mod models;
mod storage;

use std::cell::Cell;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::future::Future;
use std::rc::Rc;

use ghost_cell::{GhostCell, GhostToken};
use models::{AttributeTypeView, FollowsSpanEvent};
use serde::Serialize;
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};
use tokio::sync::oneshot::{self, Sender as OneshotSender};

use filter::{
    BoundSearch, IndexedEventFilterIterator, IndexedSpanFilter, IndexedSpanFilterIterator,
};
use index::{AttributeIndex, EventIndexes, SpanIndexes};

pub use filter::input::{FilterPredicate, FilterPropertyKind, ValuePredicate};
pub use filter::{BasicEventFilter, BasicInstanceFilter, BasicSpanFilter, Order, Query};
pub use models::{
    AncestorView, AttributeSourceView, AttributeView, CreateSpanEvent, Event, EventView, Instance,
    InstanceId, InstanceKey, InstanceView, NewCreateSpanEvent, NewEvent, NewFollowsSpanEvent,
    NewInstance, NewSpanEvent, NewSpanEventKind, NewUpdateSpanEvent, Span, SpanEvent, SpanEventKey,
    SpanEventKind, SpanId, SpanKey, SpanView, StatsView, SubscriptionId, Timestamp,
    UpdateSpanEvent, Value, ValueOperator,
};
pub use storage::{Boo, Storage, TransientStorage};

#[cfg(feature = "persist")]
pub use storage::FileStorage;

#[derive(Debug, Copy, Clone, Serialize)]
pub enum EngineInsertError {
    DuplicateInstanceId,
    DuplicateSpanId,
    UnknownInstanceId,
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
            GhostToken::new(|token| {
                let mut engine = RawEngine::new(storage, token);

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
                    match cmd {
                        EngineCommand::QueryInstance(query, sender) => {
                            let instances = engine.query_instance(query);
                            let _ = sender.send(instances);
                        }
                        EngineCommand::QueryInstanceCount(query, sender) => {
                            let events = engine.query_instance_count(query);
                            let _ = sender.send(events);
                        }
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
                        EngineCommand::InsertInstance(instance, sender) => {
                            let res = engine.insert_instance(instance);
                            if let Err(err) = &res {
                                eprintln!("rejecting instance insert due to: {err:?}");
                            }
                            let _ = sender.send(res);
                        }
                        EngineCommand::DisconnectInstance(instance_id, sender) => {
                            let res = engine.disconnect_instance(instance_id);
                            if let Err(err) = &res {
                                eprintln!("rejecting instance disconnect due to: {err:?}");
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
                        EngineCommand::AddAttributeIndex(name) => {
                            engine.add_attribute_index(name);
                        }
                        EngineCommand::EventSubscribe(filter, sender) => {
                            let res = engine.subscribe_to_events(filter);
                            let _ = sender.send(res);
                        }
                        EngineCommand::EventUnsubscribe(id, sender) => {
                            engine.unsubscribe_from_events(id);
                            let _ = sender.send(());
                        }
                    }
                }
            })
        });

        Engine {
            insert_sender,
            query_sender,
        }
    }

    // The query is executed even if the returned future is not awaited
    pub fn query_instance(&self, query: Query) -> impl Future<Output = Vec<InstanceView>> {
        let (sender, receiver) = oneshot::channel();
        let _ = self
            .query_sender
            .send(EngineCommand::QueryInstance(query, sender));
        async move { receiver.await.unwrap() }
    }

    // The query is executed even if the returned future is not awaited
    pub fn query_instance_count(&self, query: Query) -> impl Future<Output = usize> {
        let (sender, receiver) = oneshot::channel();
        let _ = self
            .query_sender
            .send(EngineCommand::QueryInstanceCount(query, sender));
        async move { receiver.await.unwrap() }
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

    pub fn insert_instance(
        &self,
        instance: NewInstance,
    ) -> impl Future<Output = Result<InstanceKey, EngineInsertError>> {
        let (sender, receiver) = oneshot::channel();
        let _ = self
            .insert_sender
            .send(EngineCommand::InsertInstance(instance, sender));
        async move { receiver.await.unwrap() }
    }

    pub fn disconnect_instance(
        &self,
        id: InstanceId,
    ) -> impl Future<Output = Result<(), EngineInsertError>> {
        let (sender, receiver) = oneshot::channel();
        let _ = self
            .insert_sender
            .send(EngineCommand::DisconnectInstance(id, sender));
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

    pub fn add_attribute_index(&self, name: String) {
        let _ = self
            .insert_sender
            .send(EngineCommand::AddAttributeIndex(name));
    }

    pub fn subscribe_to_events(
        &self,
        filter: Vec<FilterPredicate>,
    ) -> impl Future<Output = (SubscriptionId, UnboundedReceiver<EventView>)> {
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
}

enum EngineCommand {
    QueryInstance(Query, OneshotSender<Vec<InstanceView>>),
    QueryInstanceCount(Query, OneshotSender<usize>),
    QuerySpan(Query, OneshotSender<Vec<SpanView>>),
    QuerySpanCount(Query, OneshotSender<usize>),
    QuerySpanEvent(Query, OneshotSender<Vec<SpanEvent>>),
    QueryEvent(Query, OneshotSender<Vec<EventView>>),
    QueryEventCount(Query, OneshotSender<usize>),
    QueryStats(OneshotSender<StatsView>),
    InsertInstance(
        NewInstance,
        OneshotSender<Result<InstanceKey, EngineInsertError>>,
    ),
    DisconnectInstance(InstanceId, OneshotSender<Result<(), EngineInsertError>>),
    InsertSpanEvent(
        NewSpanEvent,
        OneshotSender<Result<SpanKey, EngineInsertError>>,
    ),
    InsertEvent(NewEvent, OneshotSender<Result<(), EngineInsertError>>),
    AddAttributeIndex(String),

    EventSubscribe(
        Vec<FilterPredicate>,
        OneshotSender<(SubscriptionId, UnboundedReceiver<EventView>)>,
    ),
    EventUnsubscribe(SubscriptionId, OneshotSender<()>),
}

struct RawEngine<'b, S> {
    token: GhostToken<'b>,
    storage: S,
    keys: KeyCache,
    instance_key_map: HashMap<InstanceId, InstanceKey>,
    #[allow(clippy::type_complexity)]
    instances: BTreeMap<InstanceKey, (Instance, Rc<GhostCell<'b, BTreeMap<String, Value>>>)>,
    span_key_map: HashMap<(InstanceKey, SpanId), SpanKey>,
    span_id_map: HashMap<SpanKey, SpanId>,
    span_indexes: SpanIndexes,
    span_ancestors: HashMap<Timestamp, Ancestors<'b>>,
    span_event_ids: Vec<Timestamp>,
    event_indexes: EventIndexes,
    event_ancestors: HashMap<Timestamp, Ancestors<'b>>,

    next_subscriber_id: usize,
    event_subscribers: HashMap<usize, (BasicEventFilter, UnboundedSender<EventView>)>,
}

impl<'b, S: Storage> RawEngine<'b, S> {
    fn new(storage: S, token: GhostToken<'b>) -> RawEngine<'b, S> {
        let mut engine = RawEngine {
            token,
            storage,
            keys: KeyCache::new(),
            instance_key_map: HashMap::new(),
            instances: BTreeMap::new(),
            span_key_map: HashMap::new(),
            span_id_map: HashMap::new(),
            span_indexes: SpanIndexes::new(),
            span_ancestors: HashMap::new(),
            span_event_ids: vec![],
            event_indexes: EventIndexes::new(),
            event_ancestors: HashMap::new(),

            next_subscriber_id: 0,
            event_subscribers: HashMap::new(),
        };

        let instances = engine
            .storage
            .get_all_instances()
            .map(Boo::into_owned)
            .collect::<Vec<_>>();

        let mut instances_not_disconnected = vec![];
        for instance in instances {
            if instance.disconnected_at.is_none() {
                instances_not_disconnected.push(instance.key());
            }

            engine.insert_instance_bookeeping(&instance);
        }

        let spans = engine
            .storage
            .get_all_spans()
            .map(Boo::into_owned)
            .collect::<Vec<_>>();

        let mut spans_not_closed = vec![];
        for span in spans {
            if span.closed_at.is_none() {
                spans_not_closed.push(span.key());
            }
            engine.insert_span_bookeeping(&span);
        }

        let span_events = engine
            .storage
            .get_all_span_events()
            .map(Boo::into_owned)
            .collect::<Vec<_>>();

        for span_event in span_events {
            engine.insert_span_event_bookeeping(&span_event);
        }

        let events = engine
            .storage
            .get_all_events()
            .map(Boo::into_owned)
            .collect::<Vec<_>>();

        for event in events {
            engine.insert_event_bookeeping(&event);
        }

        if !instances_not_disconnected.is_empty() || !spans_not_closed.is_empty() {
            let last_event = engine.event_indexes.all.last();
            let last_span_event = engine.span_event_ids.last();
            let last_at = match (last_event, last_span_event) {
                (Some(event), Some(span_event)) => Ord::max(*event, *span_event),
                (None, Some(span_event)) => *span_event,
                (Some(event), None) => *event,
                (None, None) => panic!("not possible to have open span but no span events"),
            };

            let at = last_at.saturating_add(1);

            for instance_key in instances_not_disconnected {
                engine
                    .storage
                    .update_instance_disconnected(instance_key, at);
            }

            for span_key in spans_not_closed {
                engine.span_indexes.update_with_closed(span_key, at);
                engine.storage.update_span_closed(span_key, at);
            }
        }

        engine
    }

    pub fn query_instance(&self, query: Query) -> Vec<InstanceView> {
        let limit = query.limit;

        let mut filter = BasicInstanceFilter::And(
            query
                .filter
                .into_iter()
                .map(|p| BasicInstanceFilter::from_predicate(p).unwrap())
                .collect(),
        );
        filter.simplify();

        let (start, end) = match query.order {
            Order::Asc => {
                let start = query
                    .previous
                    .map(|p| p.saturating_add(1))
                    .unwrap_or(Timestamp::MIN);
                let end = query.end;

                (start, end)
            }
            Order::Desc => {
                let start = Timestamp::MIN;
                let end = query
                    .previous
                    .map(|p| Timestamp::new(p.get() - 1).unwrap())
                    .unwrap_or(query.end);

                (start, end)
            }
        };

        let mut instances = self
            .instances
            .range(start..=end)
            .map(|(_key, (instance, _))| instance)
            .collect::<Vec<_>>();

        if query.order == Order::Desc {
            instances.reverse();
        }

        instances
            .into_iter()
            .filter(|instance| {
                instance.connected_at <= query.end
                    && instance
                        .disconnected_at
                        .map(|d| d >= query.start)
                        .unwrap_or(true)
            })
            .filter(|instance| filter.matches(&self.storage, instance.key()))
            .take(limit)
            .map(|instance| self.render_instance(instance))
            .collect()
    }

    pub fn query_instance_count(&self, query: Query) -> usize {
        // TODO: make this better
        self.query_instance(query).len()
    }

    fn render_instance(&self, instance: &Instance) -> InstanceView {
        let instance_id = instance.id;

        InstanceView {
            id: instance_id.to_string(),
            connected_at: instance.connected_at,
            disconnected_at: instance.disconnected_at,
            attributes: instance
                .fields
                .iter()
                .map(|(name, value)| AttributeView {
                    name: name.to_owned(),
                    value: value.to_string(),
                    typ: value.to_type_view(),
                    source: AttributeSourceView::Instance {
                        instance_id: instance_id.to_string(),
                    },
                })
                .collect(),
        }
    }

    pub fn query_event(&self, query: Query) -> Vec<EventView> {
        let limit = query.limit;
        IndexedEventFilterIterator::new(query, self)
            .take(limit)
            .map(|event_key| self.storage.get_event(event_key).unwrap())
            .map(|event| self.render_event(&event))
            .collect()
    }

    pub fn query_event_count(&self, query: Query) -> usize {
        let event_iter = IndexedEventFilterIterator::new(query, self);

        match event_iter.size_hint() {
            (min, Some(max)) if min == max => min,
            _ => event_iter.count(),
        }
    }

    fn render_event(&self, event: &Event) -> EventView {
        let (instance, _) = self.instances.get(&event.instance_key).unwrap();
        let instance_id = instance.id;

        let ancestors = self.event_ancestors.get(&event.timestamp).unwrap();

        let mut attributes =
            BTreeMap::<String, (AttributeSourceView, String, AttributeTypeView)>::new();
        for (attribute, value) in ancestors.0.last().unwrap().1.borrow(&self.token) {
            attributes.insert(
                attribute.to_owned(),
                (
                    AttributeSourceView::Inherent,
                    value.to_string(),
                    value.to_type_view(),
                ),
            );
        }
        for (parent_key, fields) in &ancestors.0[1..ancestors.0.len() - 1] {
            for (attribute, value) in fields.borrow(&self.token) {
                if !attributes.contains_key(attribute) {
                    let parent_id = *self.span_id_map.get(parent_key).unwrap();
                    attributes.insert(
                        attribute.to_owned(),
                        (
                            AttributeSourceView::Span {
                                span_id: format!("{instance_id}-{parent_id}"),
                            },
                            value.to_string(),
                            value.to_type_view(),
                        ),
                    );
                }
            }
        }
        for (attribute, value) in ancestors.0.first().unwrap().1.borrow(&self.token) {
            if !attributes.contains_key(attribute) {
                attributes.insert(
                    attribute.to_owned(),
                    (
                        AttributeSourceView::Instance {
                            instance_id: instance_id.to_string(),
                        },
                        value.to_string(),
                        value.to_type_view(),
                    ),
                );
            }
        }

        EventView {
            instance_id: instance_id.to_string(),
            ancestors: ancestors.0[1..ancestors.0.len() - 1]
                .iter()
                .map(|(parent_key, _)| {
                    let parent_span = self.storage.get_span(*parent_key).unwrap();
                    let parent_id = parent_span.id;

                    AncestorView {
                        id: format!("{instance_id}-{parent_id}"),
                        name: parent_span.name.clone(),
                    }
                })
                .collect(),
            timestamp: event.timestamp,
            level: event.level as i32,
            target: event.target.clone(),
            name: event.name.clone(),
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
        let (instance, _) = self.instances.get(&span.instance_key).unwrap();
        let instance_id = instance.id;

        let ancestors = self.span_ancestors.get(&span.created_at).unwrap();

        let mut attributes =
            BTreeMap::<String, (AttributeSourceView, String, AttributeTypeView)>::new();
        for (attribute, value) in ancestors.0.last().unwrap().1.borrow(&self.token) {
            attributes.insert(
                attribute.to_owned(),
                (
                    AttributeSourceView::Inherent,
                    value.to_string(),
                    value.to_type_view(),
                ),
            );
        }
        for (parent_key, fields) in &ancestors.0[1..ancestors.0.len() - 1] {
            for (attribute, value) in fields.borrow(&self.token) {
                if !attributes.contains_key(attribute) {
                    let parent_id = *self.span_id_map.get(parent_key).unwrap();
                    attributes.insert(
                        attribute.to_owned(),
                        (
                            AttributeSourceView::Span {
                                span_id: format!("{instance_id}-{parent_id}"),
                            },
                            value.to_string(),
                            value.to_type_view(),
                        ),
                    );
                }
            }
        }
        for (attribute, value) in ancestors.0.first().unwrap().1.borrow(&self.token) {
            if !attributes.contains_key(attribute) {
                attributes.insert(
                    attribute.to_owned(),
                    (
                        AttributeSourceView::Instance {
                            instance_id: instance_id.to_string(),
                        },
                        value.to_string(),
                        value.to_type_view(),
                    ),
                );
            }
        }

        SpanView {
            id: format!("{instance_id}-{}", span.id),
            ancestors: ancestors.0[1..ancestors.0.len() - 1]
                .iter()
                .map(|(parent_key, _)| {
                    let parent_span = self.storage.get_span(*parent_key).unwrap();
                    let parent_id = parent_span.id;

                    AncestorView {
                        id: format!("{instance_id}-{parent_id}"),
                        name: parent_span.name.clone(),
                    }
                })
                .collect(),
            created_at: span.created_at,
            closed_at: span.closed_at,
            level: span.level as i32,
            target: span.target.clone(),
            name: span.name.clone(),
            file: match (&span.file_name, span.file_line) {
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

    pub fn query_span_event(&self, _query: Query) -> Vec<SpanEvent> {
        unimplemented!()
    }

    pub fn query_stats(&self) -> StatsView {
        StatsView {
            start: self.event_indexes.all.first().copied(),
            end: self.event_indexes.all.last().copied(),
            total_events: self.event_indexes.all.len(),
            total_spans: self.span_indexes.all.len(),
            indexed_attributes: self.event_indexes.attributes.keys().cloned().collect(),
        }
    }

    pub fn insert_instance(
        &mut self,
        instance: NewInstance,
    ) -> Result<InstanceKey, EngineInsertError> {
        if self.instance_key_map.contains_key(&instance.id) {
            return Err(EngineInsertError::DuplicateInstanceId);
        }

        let now = now();
        let instance_key = self.keys.register(now, now);
        let instance = Instance {
            id: instance.id,
            connected_at: instance_key,
            disconnected_at: None,
            fields: instance.fields,
        };

        self.insert_instance_bookeeping(&instance);
        self.storage.insert_instance(instance);

        Ok(instance_key)
    }

    fn insert_instance_bookeeping(&mut self, instance: &Instance) {
        let current_fields = Rc::new(GhostCell::new(instance.fields.clone()));

        self.instance_key_map.insert(instance.id, instance.key());
        self.instances
            .insert(instance.key(), (instance.clone(), current_fields));
    }

    pub fn disconnect_instance(
        &mut self,
        instance_id: InstanceId,
    ) -> Result<(), EngineInsertError> {
        let now = now();
        let at = self.keys.register(now, now);

        let instance_key = *self
            .instance_key_map
            .get(&instance_id)
            .ok_or(EngineInsertError::UnknownInstanceId)?;

        let instance = self.storage.get_instance(instance_key).unwrap();

        if instance.disconnected_at.is_some() {
            return Err(EngineInsertError::AlreadyDisconnected);
        }

        let filter = IndexedSpanFilter::And(vec![
            IndexedSpanFilter::Single(&self.span_indexes.durations.open, None),
            IndexedSpanFilter::Single(
                self.span_indexes
                    .instances
                    .get(&instance_key)
                    .map(Vec::as_slice)
                    .unwrap_or_default(),
                None,
            ),
        ]);

        let open_spans = IndexedSpanFilterIterator::new_internal(filter, self).collect::<Vec<_>>();

        for span_key in open_spans {
            self.span_indexes.update_with_closed(span_key, at);
            self.storage.update_span_closed(span_key, at);
        }

        self.storage.update_instance_disconnected(instance_key, at);

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
                let instance_key = new_span_event.instance_key;

                if self
                    .span_key_map
                    .contains_key(&(instance_key, new_span_event.span_id))
                {
                    return Err(EngineInsertError::DuplicateSpanId);
                }

                let parent_key = new_create_event
                    .parent_id
                    .map(|span_id| {
                        self.span_key_map
                            .get(&(instance_key, span_id))
                            .copied()
                            .ok_or(EngineInsertError::UnknownParentSpanId)
                    })
                    .transpose()?;

                let span = Span {
                    instance_key: new_span_event.instance_key,
                    id: new_span_event.span_id,
                    created_at: new_span_event.timestamp,
                    closed_at: None,
                    parent_key,
                    follows: Vec::new(),
                    target: new_create_event.target.clone(),
                    name: new_create_event.name.clone(),
                    level: new_create_event
                        .level
                        .try_into()
                        .map_err(|_| EngineInsertError::UnknownLevel)?,
                    file_name: new_create_event.file_name.clone(),
                    file_line: new_create_event.file_line,
                    fields: new_create_event.fields.clone(),
                };

                let span_event = SpanEvent {
                    instance_key: new_span_event.instance_key,
                    timestamp: new_span_event.timestamp,
                    span_key: span.created_at,
                    kind: SpanEventKind::Create(CreateSpanEvent {
                        parent_key,
                        target: new_create_event.target,
                        name: new_create_event.name,
                        level: new_create_event
                            .level
                            .try_into()
                            .map_err(|_| EngineInsertError::UnknownLevel)?,
                        file_name: new_create_event.file_name,
                        file_line: new_create_event.file_line,
                        fields: new_create_event.fields,
                    }),
                };

                self.insert_span_bookeeping(&span);
                self.storage.insert_span(span);

                self.insert_span_event_bookeeping(&span_event);
                self.storage.insert_span_event(span_event);
            }
            NewSpanEventKind::Update(new_update_event) => {
                let span_key = self
                    .span_key_map
                    .get(&(new_span_event.instance_key, new_span_event.span_id))
                    .copied()
                    .ok_or(EngineInsertError::UnknownSpanId)?;

                let update_event = UpdateSpanEvent {
                    fields: new_update_event.fields.clone(),
                };

                let updated_an_indexed_attribute = self
                    .span_indexes
                    .attributes
                    .keys()
                    .any(|attribute| update_event.fields.contains_key(attribute));

                if updated_an_indexed_attribute {
                    for span_key in self.span_indexes.descendents[&span_key].clone() {
                        // check if nested span attribute changed
                        let ancestors = &self.span_ancestors[&span_key];
                        self.span_indexes.update_with_new_field_on_parent(
                            &self.token,
                            span_key,
                            ancestors,
                            span_key,
                            &update_event.fields,
                        );
                    }

                    for event_key in self.event_indexes.descendents[&span_key].clone() {
                        // check if nested event attribute changed
                        let ancestors = &self.event_ancestors[&event_key];
                        self.event_indexes.update_with_new_field_on_parent(
                            &self.token,
                            event_key,
                            ancestors,
                            span_key,
                            &update_event.fields,
                        );
                    }
                }

                let fields = &self
                    .span_ancestors
                    .get(&span_key)
                    .unwrap()
                    .0
                    .last()
                    .unwrap()
                    .1;

                fields
                    .borrow_mut(&mut self.token)
                    .extend(update_event.fields.clone());

                let span_event = SpanEvent {
                    instance_key: new_span_event.instance_key,
                    timestamp: new_span_event.timestamp,
                    span_key,
                    kind: SpanEventKind::Update(update_event),
                };

                self.storage
                    .update_span_fields(span_key, new_update_event.fields);

                self.insert_span_event_bookeeping(&span_event);
                self.storage.insert_span_event(span_event);
            }
            NewSpanEventKind::Follows(new_follows_event) => {
                let span_key = self
                    .span_key_map
                    .get(&(new_span_event.instance_key, new_span_event.span_id))
                    .copied()
                    .ok_or(EngineInsertError::UnknownSpanId)?;

                let follows_span_key = self
                    .span_key_map
                    .get(&(new_span_event.instance_key, new_follows_event.follows))
                    .copied()
                    .ok_or(EngineInsertError::UnknownSpanId)?;

                // TODO: check against circular following
                // TODO: check against duplicates

                let span_event = SpanEvent {
                    instance_key: new_span_event.instance_key,
                    timestamp: new_span_event.timestamp,
                    span_key,
                    kind: SpanEventKind::Follows(FollowsSpanEvent {
                        follows: follows_span_key,
                    }),
                };

                self.storage.update_span_follows(span_key, follows_span_key);

                self.insert_span_event_bookeeping(&span_event);
                self.storage.insert_span_event(span_event);
            }
            NewSpanEventKind::Enter => {
                let span_key = self
                    .span_key_map
                    .get(&(new_span_event.instance_key, new_span_event.span_id))
                    .copied()
                    .ok_or(EngineInsertError::UnknownSpanId)?;

                let span_event = SpanEvent {
                    instance_key: new_span_event.instance_key,
                    timestamp: new_span_event.timestamp,
                    span_key,
                    kind: SpanEventKind::Enter,
                };

                self.insert_span_event_bookeeping(&span_event);
                self.storage.insert_span_event(span_event);
            }
            NewSpanEventKind::Exit => {
                let span_key = self
                    .span_key_map
                    .get(&(new_span_event.instance_key, new_span_event.span_id))
                    .copied()
                    .ok_or(EngineInsertError::UnknownSpanId)?;

                let span_event = SpanEvent {
                    instance_key: new_span_event.instance_key,
                    timestamp: new_span_event.timestamp,
                    span_key,
                    kind: SpanEventKind::Exit,
                };

                self.insert_span_event_bookeeping(&span_event);
                self.storage.insert_span_event(span_event);
            }
            NewSpanEventKind::Close => {
                let span_key = self
                    .span_key_map
                    .get(&(new_span_event.instance_key, new_span_event.span_id))
                    .copied()
                    .ok_or(EngineInsertError::UnknownSpanId)?;

                let span_event = SpanEvent {
                    instance_key: new_span_event.instance_key,
                    timestamp: new_span_event.timestamp,
                    span_key,
                    kind: SpanEventKind::Close,
                };

                self.span_indexes
                    .update_with_closed(span_key, new_span_event.timestamp);

                self.storage
                    .update_span_closed(span_key, new_span_event.timestamp);

                self.insert_span_event_bookeeping(&span_event);
                self.storage.insert_span_event(span_event);
            }
        }

        Ok(span_event_key)
    }

    fn insert_span_bookeeping(&mut self, span: &Span) {
        let span_key = span.created_at;

        let mut ancestors = if let Some(parent_key) = span.parent_key {
            self.span_ancestors.get(&parent_key).unwrap().clone()
        } else {
            let (_, instance_fields) = &self.instances[&span.instance_key];

            let mut ancestors = Ancestors::new();
            ancestors
                .0
                .push((span.instance_key, instance_fields.clone()));
            ancestors
        };

        let current_fields = Rc::new(GhostCell::new(span.fields.clone()));
        ancestors.0.push((span_key, current_fields));

        self.span_key_map
            .insert((span.instance_key, span.id), span_key);
        self.span_id_map.insert(span_key, span.id);
        self.span_indexes
            .update_with_new_span(&self.token, span, &ancestors);
        self.span_ancestors.insert(span_key, ancestors);
    }

    fn insert_span_event_bookeeping(&mut self, span_event: &SpanEvent) {
        let timestamp = span_event.timestamp;
        let idx = self.span_event_ids.upper_bound_via_expansion(&timestamp);
        self.span_event_ids.insert(idx, timestamp);
    }

    pub fn insert_event(&mut self, mut new_event: NewEvent) -> Result<(), EngineInsertError> {
        let span_key = new_event
            .span_id
            .map(|span_id| {
                self.span_key_map
                    .get(&(new_event.instance_key, span_id))
                    .copied()
                    .ok_or(EngineInsertError::UnknownParentSpanId)
            })
            .transpose()?;

        let event_key = self.keys.register(now(), new_event.timestamp);
        new_event.timestamp = event_key;

        let event = Event {
            instance_key: new_event.instance_key,
            timestamp: new_event.timestamp,
            span_key,
            name: new_event.name,
            target: new_event.target,
            level: new_event
                .level
                .try_into()
                .map_err(|_| EngineInsertError::UnknownLevel)?,
            file_name: new_event.file_name,
            file_line: new_event.file_line,
            fields: new_event.fields,
        };

        self.insert_event_bookeeping(&event);
        self.storage.insert_event(event.clone());

        let mut remove = vec![];
        for (id, (filter, sender)) in &self.event_subscribers {
            if filter.matches(&self.token, &self.event_ancestors, &event) {
                let send_result = sender.send(self.render_event(&event));
                if send_result.is_err() {
                    remove.push(*id);
                }
            }
        }

        for id in remove {
            self.event_subscribers.remove(&id);
        }

        Ok(())
    }

    fn insert_event_bookeeping(&mut self, event: &Event) {
        let event_key = event.timestamp;

        let mut ancestors = if let Some(parent_key) = event.span_key {
            self.span_ancestors.get(&parent_key).unwrap().clone()
        } else {
            let (_, instance_fields) = &self.instances[&event.instance_key];

            let mut ancestors = Ancestors::new();
            ancestors
                .0
                .push((event.instance_key, instance_fields.clone()));
            ancestors
        };

        let current_fields = Rc::new(GhostCell::new(event.fields.clone()));
        ancestors.0.push((event_key, current_fields));

        self.event_indexes
            .update_with_new_event(&self.token, event, &ancestors);
        self.event_ancestors.insert(event_key, ancestors);
    }

    pub fn add_attribute_index(&mut self, name: String) {
        if !self.span_indexes.attributes.contains_key(&name) {
            let mut attr_index = AttributeIndex::new();
            for span in self.storage.get_all_spans() {
                let span_key = span.created_at;

                if let Some(value) = self.span_ancestors[&span_key].get_value(&name, &self.token) {
                    attr_index.add_entry(span_key, value);
                }
            }

            self.span_indexes
                .attributes
                .insert(name.clone(), attr_index);
        }

        if !self.event_indexes.attributes.contains_key(&name) {
            let mut attr_index = AttributeIndex::new();
            for event in self.storage.get_all_events() {
                let event_key = event.timestamp;

                if let Some(value) = self.event_ancestors[&event_key].get_value(&name, &self.token)
                {
                    attr_index.add_entry(event_key, value);
                }
            }

            self.event_indexes
                .attributes
                .insert(name.clone(), attr_index);
        }

        // TODO: persist indexes
    }

    pub fn subscribe_to_events(
        &mut self,
        filter: Vec<FilterPredicate>,
    ) -> (SubscriptionId, UnboundedReceiver<EventView>) {
        let mut filter = BasicEventFilter::And(
            filter
                .into_iter()
                .map(|p| {
                    BasicEventFilter::from_predicate(p, &self.instance_key_map, &self.span_key_map)
                        .unwrap()
                })
                .collect(),
        );
        filter.simplify();

        let id = self.next_subscriber_id;
        self.next_subscriber_id += 1;

        let (sender, receiver) = mpsc::unbounded_channel();

        self.event_subscribers.insert(id, (filter, sender));

        (id, receiver)
    }

    pub fn unsubscribe_from_events(&mut self, id: SubscriptionId) {
        self.event_subscribers.remove(&id);
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

#[derive(Clone)]
#[allow(clippy::type_complexity)]
struct Ancestors<'b>(Vec<(Timestamp, Rc<GhostCell<'b, BTreeMap<String, Value>>>)>);

impl<'b> Ancestors<'b> {
    fn new() -> Ancestors<'b> {
        Ancestors(Vec::new())
    }

    fn get_value<'a>(&'a self, attribute: &str, token: &'a GhostToken<'b>) -> Option<&'a Value> {
        self.0
            .iter()
            .rev()
            .find_map(move |(_, attributes)| attributes.borrow(token).get(attribute))
    }

    fn get_value_and_key<'a>(
        &'a self,
        attribute: &str,
        token: &'a GhostToken<'b>,
    ) -> Option<(&'a Value, Timestamp)> {
        self.0.iter().rev().find_map(move |(key, attributes)| {
            attributes.borrow(token).get(attribute).map(|v| (v, *key))
        })
    }

    fn has_parent(&self, timestamp: Timestamp) -> bool {
        self.0.iter().any(|(id, _)| *id == timestamp)
    }
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
    use models::{NewCreateSpanEvent, NewUpdateSpanEvent};

    use super::*;

    #[test]
    fn test_event_filters() {
        GhostToken::new(|token| {
            let mut engine = RawEngine::new(TransientStorage::new(), token);

            let instance_key = engine
                .insert_instance(NewInstance {
                    id: 1,
                    fields: BTreeMap::new(),
                })
                .unwrap();

            let simple = |id: u64, level: i32, attribute1: &str, attribute2: &str| -> NewEvent {
                NewEvent {
                    instance_key,
                    timestamp: id.try_into().unwrap(),
                    span_id: None,
                    name: "event".to_owned(),
                    target: "crate::storage::tests".to_owned(),
                    level,
                    file_name: None,
                    file_line: None,
                    fields: BTreeMap::from_iter([
                        ("attribute1".to_owned(), Value::Str(attribute1.to_owned())),
                        ("attribute2".to_owned(), Value::Str(attribute2.to_owned())),
                    ]),
                }
            };

            engine.add_attribute_index("attribute1".to_owned());
            engine.add_attribute_index("attribute2".to_owned());

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
                filter: FilterPredicate::parse("#level: >=WARN @attribute1: test @attribute2: A")
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
        })
    }

    #[test]
    fn test_span_filters() {
        GhostToken::new(|token| {
            let mut engine = RawEngine::new(TransientStorage::new(), token);

            let instance_key = engine
                .insert_instance(NewInstance {
                    id: 1,
                    fields: BTreeMap::new(),
                })
                .unwrap();

            let simple_open =
                |open: u64, level: i32, attribute1: &str, attribute2: &str| -> NewSpanEvent {
                    NewSpanEvent {
                        instance_key,
                        timestamp: Timestamp::new(open).unwrap(),
                        span_id: open.try_into().unwrap(),
                        kind: NewSpanEventKind::Create(NewCreateSpanEvent {
                            parent_id: None,
                            target: "crate::storage::tests".to_owned(),
                            name: "test".to_owned(),
                            level,
                            file_name: None,
                            file_line: None,
                            fields: BTreeMap::from_iter([
                                ("attribute1".to_owned(), Value::Str(attribute1.to_owned())),
                                ("attribute2".to_owned(), Value::Str(attribute2.to_owned())),
                            ]),
                        }),
                    }
                };

            let simple_close = |open: u64, close: u64| -> NewSpanEvent {
                NewSpanEvent {
                    instance_key,
                    timestamp: Timestamp::new(close).unwrap(),
                    span_id: open.try_into().unwrap(),
                    kind: NewSpanEventKind::Close,
                }
            };

            engine.add_attribute_index("attribute1".to_owned());
            engine.add_attribute_index("attribute2".to_owned());

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
                filter: FilterPredicate::parse("#level: >=WARN @attribute1: test @attribute2: A")
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
        });
    }

    #[test]
    fn event_found_with_nonindexed_instance_attribute() {
        GhostToken::new(|token| {
            let mut engine = RawEngine::new(TransientStorage::new(), token);

            let instance_key = engine
                .insert_instance(NewInstance {
                    id: 1,
                    fields: BTreeMap::from_iter([("attr1".to_owned(), Value::Str("A".to_owned()))]),
                })
                .unwrap();

            let now = now();
            engine
                .insert_event(NewEvent {
                    instance_key,
                    timestamp: now.saturating_add(1),
                    span_id: None,
                    name: "event".to_owned(),
                    target: "crate::storage::tests".to_owned(),
                    level: 4,
                    file_name: None,
                    file_line: None,
                    fields: BTreeMap::new(),
                })
                .unwrap();

            let events = engine.query_event(Query {
                filter: FilterPredicate::parse("@attr1: A").unwrap(),
                order: Order::Asc,
                limit: 5,
                start: now,
                end: now.saturating_add(2),
                previous: None,
            });

            assert_eq!(events.len(), 1);

            let events = engine.query_event(Query {
                filter: FilterPredicate::parse("@attr1: B").unwrap(),
                order: Order::Asc,
                limit: 5,
                start: now,
                end: now.saturating_add(2),
                previous: None,
            });

            assert_eq!(events.len(), 0);
        });
    }

    #[test]
    fn event_found_with_indexed_instance_attribute() {
        GhostToken::new(|token| {
            let mut engine = RawEngine::new(TransientStorage::new(), token);

            engine.add_attribute_index("attr1".to_owned());

            let instance_key = engine
                .insert_instance(NewInstance {
                    id: 1,
                    fields: BTreeMap::from_iter([("attr1".to_owned(), Value::Str("A".to_owned()))]),
                })
                .unwrap();

            let now = now();
            engine
                .insert_event(NewEvent {
                    instance_key,
                    timestamp: now.saturating_add(1),
                    span_id: None,
                    name: "event".to_owned(),
                    target: "crate::storage::tests".to_owned(),
                    level: 4,
                    file_name: None,
                    file_line: None,
                    fields: BTreeMap::new(),
                })
                .unwrap();

            let events = engine.query_event(Query {
                filter: FilterPredicate::parse("@attr1: A").unwrap(),
                order: Order::Asc,
                limit: 5,
                start: now,
                end: now.saturating_add(2),
                previous: None,
            });

            assert_eq!(events.len(), 1);

            let events = engine.query_event(Query {
                filter: FilterPredicate::parse("@attr1: B").unwrap(),
                order: Order::Asc,
                limit: 5,
                start: now,
                end: now.saturating_add(2),
                previous: None,
            });

            assert_eq!(events.len(), 0);
        });
    }

    #[test]
    fn event_found_with_nonindexed_inherent_attribute() {
        GhostToken::new(|token| {
            let mut engine = RawEngine::new(TransientStorage::new(), token);

            let instance_key = engine
                .insert_instance(NewInstance {
                    id: 1,
                    fields: BTreeMap::from_iter([("attr1".to_owned(), Value::Str("A".to_owned()))]),
                })
                .unwrap();

            let now = now();
            engine
                .insert_event(NewEvent {
                    instance_key,
                    timestamp: now.saturating_add(1),
                    span_id: None,
                    name: "event".to_owned(),
                    target: "crate::storage::tests".to_owned(),
                    level: 4,
                    file_name: None,
                    file_line: None,
                    fields: BTreeMap::from_iter([("attr1".to_owned(), Value::Str("B".to_owned()))]),
                })
                .unwrap();

            let events = engine.query_event(Query {
                filter: FilterPredicate::parse("@attr1: A").unwrap(),
                order: Order::Asc,
                limit: 5,
                start: now,
                end: now.saturating_add(2),
                previous: None,
            });

            assert_eq!(events.len(), 0);

            let events = engine.query_event(Query {
                filter: FilterPredicate::parse("@attr1: B").unwrap(),
                order: Order::Asc,
                limit: 5,
                start: now,
                end: now.saturating_add(2),
                previous: None,
            });

            assert_eq!(events.len(), 1);
        });
    }

    #[test]
    fn event_found_with_indexed_inherent_attribute() {
        GhostToken::new(|token| {
            let mut engine = RawEngine::new(TransientStorage::new(), token);

            engine.add_attribute_index("attr1".to_owned());

            let instance_key = engine
                .insert_instance(NewInstance {
                    id: 1,
                    fields: BTreeMap::from_iter([("attr1".to_owned(), Value::Str("A".to_owned()))]),
                })
                .unwrap();

            let now = now();
            engine
                .insert_event(NewEvent {
                    instance_key,
                    timestamp: now.saturating_add(1),
                    span_id: None,
                    name: "event".to_owned(),
                    target: "crate::storage::tests".to_owned(),
                    level: 4,
                    file_name: None,
                    file_line: None,
                    fields: BTreeMap::from_iter([("attr1".to_owned(), Value::Str("B".to_owned()))]),
                })
                .unwrap();

            let events = engine.query_event(Query {
                filter: FilterPredicate::parse("@attr1: A").unwrap(),
                order: Order::Asc,
                limit: 5,
                start: now,
                end: now.saturating_add(2),
                previous: None,
            });

            assert_eq!(events.len(), 0);

            let events = engine.query_event(Query {
                filter: FilterPredicate::parse("@attr1: B").unwrap(),
                order: Order::Asc,
                limit: 5,
                start: now,
                end: now.saturating_add(2),
                previous: None,
            });

            assert_eq!(events.len(), 1);
        });
    }

    #[test]
    fn event_found_with_nonindexed_span_attribute() {
        GhostToken::new(|token| {
            let mut engine = RawEngine::new(TransientStorage::new(), token);

            let instance_key = engine
                .insert_instance(NewInstance {
                    id: 1,
                    fields: BTreeMap::from_iter([("attr1".to_owned(), Value::Str("A".to_owned()))]),
                })
                .unwrap();

            engine
                .insert_span_event(NewSpanEvent {
                    instance_key,
                    timestamp: now(),
                    span_id: 1.try_into().unwrap(),
                    kind: NewSpanEventKind::Create(NewCreateSpanEvent {
                        parent_id: None,
                        target: "crate::storage::tests".to_owned(),
                        name: "test".to_owned(),
                        level: 4,
                        file_name: None,
                        file_line: None,
                        fields: BTreeMap::from_iter([(
                            "attr1".to_owned(),
                            Value::Str("C".to_owned()),
                        )]),
                    }),
                })
                .unwrap();

            let now = now();
            engine
                .insert_event(NewEvent {
                    instance_key,
                    timestamp: now.saturating_add(1),
                    span_id: SpanId::new(1),
                    name: "event".to_owned(),
                    target: "crate::storage::tests".to_owned(),
                    level: 4,
                    file_name: None,
                    file_line: None,
                    fields: BTreeMap::new(),
                })
                .unwrap();

            let events = engine.query_event(Query {
                filter: FilterPredicate::parse("@attr1: A").unwrap(),
                order: Order::Asc,
                limit: 5,
                start: now,
                end: now.saturating_add(2),
                previous: None,
            });

            assert_eq!(events.len(), 0);

            let events = engine.query_event(Query {
                filter: FilterPredicate::parse("@attr1: C").unwrap(),
                order: Order::Asc,
                limit: 5,
                start: now,
                end: now.saturating_add(2),
                previous: None,
            });

            assert_eq!(events.len(), 1);
        });
    }

    #[test]
    fn event_found_with_indexed_span_attribute() {
        GhostToken::new(|token| {
            let mut engine = RawEngine::new(TransientStorage::new(), token);

            engine.add_attribute_index("attr1".to_owned());

            let instance_key = engine
                .insert_instance(NewInstance {
                    id: 1,
                    fields: BTreeMap::from_iter([("attr1".to_owned(), Value::Str("A".to_owned()))]),
                })
                .unwrap();

            engine
                .insert_span_event(NewSpanEvent {
                    instance_key,
                    timestamp: now(),
                    span_id: 1.try_into().unwrap(),
                    kind: NewSpanEventKind::Create(NewCreateSpanEvent {
                        parent_id: None,
                        target: "crate::storage::tests".to_owned(),
                        name: "test".to_owned(),
                        level: 4,
                        file_name: None,
                        file_line: None,
                        fields: BTreeMap::from_iter([(
                            "attr1".to_owned(),
                            Value::Str("C".to_owned()),
                        )]),
                    }),
                })
                .unwrap();

            let now = now();
            engine
                .insert_event(NewEvent {
                    instance_key,
                    timestamp: now.saturating_add(1),
                    span_id: SpanId::new(1),
                    name: "event".to_owned(),
                    target: "crate::storage::tests".to_owned(),
                    level: 4,
                    file_name: None,
                    file_line: None,
                    fields: BTreeMap::new(),
                })
                .unwrap();

            let events = engine.query_event(Query {
                filter: FilterPredicate::parse("@attr1: A").unwrap(),
                order: Order::Asc,
                limit: 5,
                start: now,
                end: now.saturating_add(2),
                previous: None,
            });

            assert_eq!(events.len(), 0);

            let events = engine.query_event(Query {
                filter: FilterPredicate::parse("@attr1: C").unwrap(),
                order: Order::Asc,
                limit: 5,
                start: now,
                end: now.saturating_add(2),
                previous: None,
            });

            assert_eq!(events.len(), 1);
        });
    }

    #[test]
    fn event_found_with_nonindexed_updated_span_attribute() {
        GhostToken::new(|token| {
            let mut engine = RawEngine::new(TransientStorage::new(), token);

            let instance_key = engine
                .insert_instance(NewInstance {
                    id: 1,
                    fields: BTreeMap::from_iter([("attr1".to_owned(), Value::Str("A".to_owned()))]),
                })
                .unwrap();

            engine
                .insert_span_event(NewSpanEvent {
                    instance_key,
                    timestamp: now(),
                    span_id: 1.try_into().unwrap(),
                    kind: NewSpanEventKind::Create(NewCreateSpanEvent {
                        parent_id: None,
                        target: "crate::storage::tests".to_owned(),
                        name: "test".to_owned(),
                        level: 4,
                        file_name: None,
                        file_line: None,
                        fields: BTreeMap::new(),
                    }),
                })
                .unwrap();

            let now = now();
            engine
                .insert_event(NewEvent {
                    instance_key,
                    timestamp: now.saturating_add(1),
                    span_id: SpanId::new(1),
                    name: "event".to_owned(),
                    target: "crate::storage::tests".to_owned(),
                    level: 4,
                    file_name: None,
                    file_line: None,
                    fields: BTreeMap::new(),
                })
                .unwrap();

            engine
                .insert_span_event(NewSpanEvent {
                    instance_key,
                    timestamp: super::now(),
                    span_id: 1.try_into().unwrap(),
                    kind: NewSpanEventKind::Update(NewUpdateSpanEvent {
                        fields: BTreeMap::from_iter([(
                            "attr1".to_owned(),
                            Value::Str("C".to_owned()),
                        )]),
                    }),
                })
                .unwrap();

            let events = engine.query_event(Query {
                filter: FilterPredicate::parse("@attr1: A").unwrap(),
                order: Order::Asc,
                limit: 5,
                start: now,
                end: now.saturating_add(2),
                previous: None,
            });

            assert_eq!(events.len(), 0);

            let events = engine.query_event(Query {
                filter: FilterPredicate::parse("@attr1: C").unwrap(),
                order: Order::Asc,
                limit: 5,
                start: now,
                end: now.saturating_add(2),
                previous: None,
            });

            assert_eq!(events.len(), 1);
        });
    }

    #[test]
    fn event_found_with_indexed_updated_span_attribute() {
        GhostToken::new(|token| {
            let mut engine = RawEngine::new(TransientStorage::new(), token);

            engine.add_attribute_index("attr1".to_owned());

            let instance_key = engine
                .insert_instance(NewInstance {
                    id: 1,
                    fields: BTreeMap::from_iter([("attr1".to_owned(), Value::Str("A".to_owned()))]),
                })
                .unwrap();

            engine
                .insert_span_event(NewSpanEvent {
                    instance_key,
                    timestamp: now(),
                    span_id: 1.try_into().unwrap(),
                    kind: NewSpanEventKind::Create(NewCreateSpanEvent {
                        parent_id: None,
                        target: "crate::storage::tests".to_owned(),
                        name: "test".to_owned(),
                        level: 4,
                        file_name: None,
                        file_line: None,
                        fields: BTreeMap::new(),
                    }),
                })
                .unwrap();

            let now = now();
            engine
                .insert_event(NewEvent {
                    instance_key,
                    timestamp: now.saturating_add(1),
                    span_id: SpanId::new(1),
                    name: "event".to_owned(),
                    target: "crate::storage::tests".to_owned(),
                    level: 4,
                    file_name: None,
                    file_line: None,
                    fields: BTreeMap::new(),
                })
                .unwrap();

            engine
                .insert_span_event(NewSpanEvent {
                    instance_key,
                    timestamp: super::now(),
                    span_id: 1.try_into().unwrap(),
                    kind: NewSpanEventKind::Update(NewUpdateSpanEvent {
                        fields: BTreeMap::from_iter([(
                            "attr1".to_owned(),
                            Value::Str("C".to_owned()),
                        )]),
                    }),
                })
                .unwrap();

            let events = engine.query_event(Query {
                filter: FilterPredicate::parse("@attr1: A").unwrap(),
                order: Order::Asc,
                limit: 5,
                start: now,
                end: now.saturating_add(2),
                previous: None,
            });

            assert_eq!(events.len(), 0);

            let events = engine.query_event(Query {
                filter: FilterPredicate::parse("@attr1: C").unwrap(),
                order: Order::Asc,
                limit: 5,
                start: now,
                end: now.saturating_add(2),
                previous: None,
            });

            assert_eq!(events.len(), 1);
        });
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
