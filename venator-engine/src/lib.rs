//! The "engine" crate represents the core functionality to injest, store,
//! index, and query the events and spans. It does not provide functionality
//! outside of its Rust API.

mod filter;
mod index;
mod models;
mod storage;

use std::cell::{Cell, OnceCell};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::future::Future;
use std::sync::Arc;
use std::time::Instant;

use models::{AttributeTypeView, EventKey, FollowsSpanEvent};
use serde::Serialize;
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};
use tokio::sync::oneshot::{self, Sender as OneshotSender};

use filter::{
    BoundSearch, IndexedEventFilter, IndexedEventFilterIterator, IndexedSpanFilter,
    IndexedSpanFilterIterator,
};
use index::{EventIndexes, IndexExt, SpanIndexes};

pub use filter::input::{
    FilterPredicate, FilterPredicateSingle, FilterPropertyKind, ValuePredicate,
};
pub use filter::{
    BasicConnectionFilter, BasicEventFilter, BasicSpanFilter, FallibleFilterPredicate, InputError,
    Order, Query,
};
pub use models::{
    AncestorView, AttributeSourceView, AttributeView, Connection, ConnectionId, ConnectionKey,
    ConnectionView, CreateSpanEvent, Event, EventView, NewConnection, NewCreateSpanEvent, NewEvent,
    NewFollowsSpanEvent, NewSpanEvent, NewSpanEventKind, NewUpdateSpanEvent, Span, SpanEvent,
    SpanEventKey, SpanEventKind, SpanId, SpanKey, SpanView, StatsView, SubscriptionId, Timestamp,
    UpdateSpanEvent, Value, ValueOperator,
};
pub use storage::{CachedStorage, Storage, TransientStorage};

#[cfg(feature = "persist")]
pub use storage::FileStorage;

#[derive(Debug, Copy, Clone, Serialize)]
pub enum EngineInsertError {
    DuplicateConnectionId,
    DuplicateSpanId,
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
                    EngineCommand::QueryConnection(query, sender) => {
                        let connections = engine.query_connection(query);
                        let _ = sender.send(connections);
                    }
                    EngineCommand::QueryConnectionCount(query, sender) => {
                        let events = engine.query_connection_count(query);
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
                    EngineCommand::InsertConnection(connection, sender) => {
                        let res = engine.insert_connection(connection);
                        if let Err(err) = &res {
                            eprintln!("rejecting connection insert due to: {err:?}");
                        }
                        let _ = sender.send(res);
                    }
                    EngineCommand::DisconnectConnection(connection_id, sender) => {
                        let res = engine.disconnect_connection(connection_id);
                        if let Err(err) = &res {
                            eprintln!("rejecting connection disconnect due to: {err:?}");
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
    pub fn query_connection(&self, query: Query) -> impl Future<Output = Vec<ConnectionView>> {
        let (sender, receiver) = oneshot::channel();
        let _ = self
            .query_sender
            .send(EngineCommand::QueryConnection(query, sender));
        async move { receiver.await.unwrap() }
    }

    // The query is executed even if the returned future is not awaited
    pub fn query_connection_count(&self, query: Query) -> impl Future<Output = usize> {
        let (sender, receiver) = oneshot::channel();
        let _ = self
            .query_sender
            .send(EngineCommand::QueryConnectionCount(query, sender));
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

    pub fn insert_connection(
        &self,
        connection: NewConnection,
    ) -> impl Future<Output = Result<ConnectionKey, EngineInsertError>> {
        let (sender, receiver) = oneshot::channel();
        let _ = self
            .insert_sender
            .send(EngineCommand::InsertConnection(connection, sender));
        async move { receiver.await.unwrap() }
    }

    pub fn disconnect_connection(
        &self,
        id: ConnectionId,
    ) -> impl Future<Output = Result<(), EngineInsertError>> {
        let (sender, receiver) = oneshot::channel();
        let _ = self
            .insert_sender
            .send(EngineCommand::DisconnectConnection(id, sender));
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
}

enum EngineCommand {
    QueryConnection(Query, OneshotSender<Vec<ConnectionView>>),
    QueryConnectionCount(Query, OneshotSender<usize>),
    QuerySpan(Query, OneshotSender<Vec<SpanView>>),
    QuerySpanCount(Query, OneshotSender<usize>),
    QuerySpanEvent(Query, OneshotSender<Vec<SpanEvent>>),
    QueryEvent(Query, OneshotSender<Vec<EventView>>),
    QueryEventCount(Query, OneshotSender<usize>),
    QueryStats(OneshotSender<StatsView>),
    InsertConnection(
        NewConnection,
        OneshotSender<Result<ConnectionKey, EngineInsertError>>,
    ),
    DisconnectConnection(ConnectionId, OneshotSender<Result<(), EngineInsertError>>),
    InsertSpanEvent(
        NewSpanEvent,
        OneshotSender<Result<SpanKey, EngineInsertError>>,
    ),
    InsertEvent(NewEvent, OneshotSender<Result<(), EngineInsertError>>),
    Delete(DeleteFilter, OneshotSender<DeleteMetrics>),

    EventSubscribe(
        Vec<FilterPredicate>,
        OneshotSender<(SubscriptionId, UnboundedReceiver<EventView>)>,
    ),
    EventUnsubscribe(SubscriptionId, OneshotSender<()>),

    CopyDataset(Box<dyn Storage + Send>, OneshotSender<()>),
    GetStatus(OneshotSender<EngineStatusView>),
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
    pub connections: usize,
    pub spans: usize,
    pub span_events: usize,
    pub events: usize,
}

struct RawEngine<S> {
    storage: S,
    keys: KeyCache,
    connection_key_map: HashMap<ConnectionId, ConnectionKey>,
    connections: BTreeMap<ConnectionKey, Connection>,
    span_key_map: HashMap<(ConnectionKey, SpanId), SpanKey>,
    span_id_map: HashMap<SpanKey, SpanId>,
    span_indexes: SpanIndexes,
    span_event_ids: Vec<Timestamp>,
    span_events_by_span_ids: HashMap<SpanKey, Vec<Timestamp>>,
    event_indexes: EventIndexes,

    next_subscriber_id: usize,
    event_subscribers: HashMap<usize, (BasicEventFilter, UnboundedSender<EventView>)>,
}

impl<S: Storage> RawEngine<S> {
    fn new(storage: S) -> RawEngine<S> {
        let mut engine = RawEngine {
            storage,
            keys: KeyCache::new(),
            connection_key_map: HashMap::new(),
            connections: BTreeMap::new(),
            span_key_map: HashMap::new(),
            span_id_map: HashMap::new(),
            span_indexes: SpanIndexes::new(),
            span_event_ids: vec![],
            span_events_by_span_ids: HashMap::new(),
            event_indexes: EventIndexes::new(),

            next_subscriber_id: 0,
            event_subscribers: HashMap::new(),
        };

        let connections = engine.storage.get_all_connections().collect::<Vec<_>>();

        let mut connections_not_disconnected = vec![];
        for connection in connections {
            if connection.disconnected_at.is_none() {
                connections_not_disconnected.push(connection.key());
            }

            engine.insert_connection_bookeeping(&connection);
        }

        let spans = engine.storage.get_all_spans().collect::<Vec<_>>();

        let mut spans_not_closed = vec![];
        for span in spans {
            if span.closed_at.is_none() {
                spans_not_closed.push(span.key());
            }
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

        if !connections_not_disconnected.is_empty() || !spans_not_closed.is_empty() {
            let last_event = engine.event_indexes.all.last();
            let last_span_event = engine.span_event_ids.last();
            let last_at = match (last_event, last_span_event) {
                (Some(event), Some(span_event)) => Ord::max(*event, *span_event),
                (None, Some(span_event)) => *span_event,
                (Some(event), None) => *event,
                (None, None) => panic!("not possible to have open span but no span events"),
            };

            let at = last_at.saturating_add(1);

            for connection_key in connections_not_disconnected {
                engine
                    .storage
                    .update_connection_disconnected(connection_key, at);
            }

            for span_key in spans_not_closed {
                engine.span_indexes.update_with_closed(span_key, at);
                engine.storage.update_span_closed(span_key, at);
            }
        }

        engine
    }

    pub fn query_connection(&self, query: Query) -> Vec<ConnectionView> {
        let limit = query.limit;

        let mut filter = BasicConnectionFilter::And(
            query
                .filter
                .into_iter()
                .map(|p| BasicConnectionFilter::from_predicate(p).unwrap())
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

        let mut connections = self
            .connections
            .range(start..=end)
            .map(|(_key, connection)| connection)
            .collect::<Vec<_>>();

        if query.order == Order::Desc {
            connections.reverse();
        }

        connections
            .into_iter()
            .filter(|connection| {
                connection.connected_at <= query.end
                    && connection
                        .disconnected_at
                        .map(|d| d >= query.start)
                        .unwrap_or(true)
            })
            .filter(|connection| filter.matches(&self.storage, connection.key()))
            .take(limit)
            .map(|connection| self.render_connection(connection))
            .collect()
    }

    pub fn query_connection_count(&self, query: Query) -> usize {
        // TODO: make this better
        self.query_connection(query).len()
    }

    fn render_connection(&self, connection: &Connection) -> ConnectionView {
        let connection_id = connection.id;

        ConnectionView {
            id: connection_id.to_string(),
            connected_at: connection.connected_at,
            disconnected_at: connection.disconnected_at,
            attributes: connection
                .fields
                .iter()
                .map(|(name, value)| AttributeView {
                    name: name.to_owned(),
                    value: value.to_string(),
                    typ: value.to_type_view(),
                    source: AttributeSourceView::Connection {
                        connection_id: connection_id.to_string(),
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
        let connection = self.connections.get(&event.connection_key).unwrap();
        let connection_id = connection.id;

        let context = EventContext::with_event(event, &self.storage);

        let mut attributes =
            BTreeMap::<String, (AttributeSourceView, String, AttributeTypeView)>::new();
        for (attribute, value) in &context.event().fields {
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
                    let parent_id = *self.span_id_map.get(&parent.key()).unwrap();
                    attributes.insert(
                        attribute.to_owned(),
                        (
                            AttributeSourceView::Span {
                                span_id: format!("{connection_id}-{parent_id}"),
                            },
                            value.to_string(),
                            value.to_type_view(),
                        ),
                    );
                }
            }
        }
        for (attribute, value) in &context.connection().fields {
            if !attributes.contains_key(attribute) {
                attributes.insert(
                    attribute.to_owned(),
                    (
                        AttributeSourceView::Connection {
                            connection_id: connection_id.to_string(),
                        },
                        value.to_string(),
                        value.to_type_view(),
                    ),
                );
            }
        }

        EventView {
            connection_id: connection_id.to_string(),
            ancestors: {
                let mut ancestors = context
                    .parents()
                    .map(|parent| {
                        let parent_id = parent.id;

                        AncestorView {
                            id: format!("{connection_id}-{parent_id}"),
                            name: parent.name.clone(),
                        }
                    })
                    .collect::<Vec<_>>();

                ancestors.reverse();
                ancestors
            },
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
        let connection = self.connections.get(&span.connection_key).unwrap();
        let connection_id = connection.id;

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
                    let parent_id = *self.span_id_map.get(&parent.key()).unwrap();
                    attributes.insert(
                        attribute.to_owned(),
                        (
                            AttributeSourceView::Span {
                                span_id: format!("{connection_id}-{parent_id}"),
                            },
                            value.to_string(),
                            value.to_type_view(),
                        ),
                    );
                }
            }
        }
        for (attribute, value) in &context.connection().fields {
            if !attributes.contains_key(attribute) {
                attributes.insert(
                    attribute.to_owned(),
                    (
                        AttributeSourceView::Connection {
                            connection_id: connection_id.to_string(),
                        },
                        value.to_string(),
                        value.to_type_view(),
                    ),
                );
            }
        }

        SpanView {
            id: format!("{connection_id}-{}", span.id),
            ancestors: {
                let mut ancestors = context
                    .parents()
                    .map(|parent| {
                        let parent_id = parent.id;

                        AncestorView {
                            id: format!("{connection_id}-{parent_id}"),
                            name: parent.name.clone(),
                        }
                    })
                    .collect::<Vec<_>>();

                ancestors.reverse();
                ancestors
            },
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
        }
    }

    pub fn insert_connection(
        &mut self,
        connection: NewConnection,
    ) -> Result<ConnectionKey, EngineInsertError> {
        if self.connection_key_map.contains_key(&connection.id) {
            return Err(EngineInsertError::DuplicateConnectionId);
        }

        let now = now();
        let connection_key = self.keys.register(now, now);
        let connection = Connection {
            id: connection.id,
            connected_at: connection_key,
            disconnected_at: None,
            fields: connection.fields,
        };

        self.insert_connection_bookeeping(&connection);
        self.storage.insert_connection(connection);

        Ok(connection_key)
    }

    fn insert_connection_bookeeping(&mut self, connection: &Connection) {
        self.connection_key_map
            .insert(connection.id, connection.key());
        self.connections
            .insert(connection.key(), connection.clone());
    }

    pub fn disconnect_connection(
        &mut self,
        connection_id: ConnectionId,
    ) -> Result<(), EngineInsertError> {
        let now = now();
        let at = self.keys.register(now, now);

        let connection_key = *self
            .connection_key_map
            .get(&connection_id)
            .ok_or(EngineInsertError::UnknownConnectionId)?;

        let connection = self.storage.get_connection(connection_key).unwrap();

        if connection.disconnected_at.is_some() {
            return Err(EngineInsertError::AlreadyDisconnected);
        }

        let filter = IndexedSpanFilter::And(vec![
            IndexedSpanFilter::Single(&self.span_indexes.durations.open, None),
            IndexedSpanFilter::Single(
                self.span_indexes
                    .connections
                    .get(&connection_key)
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

        self.storage
            .update_connection_disconnected(connection_key, at);

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
                let connection_key = new_span_event.connection_key;

                if self
                    .span_key_map
                    .contains_key(&(connection_key, new_span_event.span_id))
                {
                    return Err(EngineInsertError::DuplicateSpanId);
                }

                let parent_key = new_create_event
                    .parent_id
                    .map(|span_id| {
                        self.span_key_map
                            .get(&(connection_key, span_id))
                            .copied()
                            .ok_or(EngineInsertError::UnknownParentSpanId)
                    })
                    .transpose()?;

                let span = Span {
                    connection_key: new_span_event.connection_key,
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
                    connection_key: new_span_event.connection_key,
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
                    .get(&(new_span_event.connection_key, new_span_event.span_id))
                    .copied()
                    .ok_or(EngineInsertError::UnknownSpanId)?;

                let update_event = UpdateSpanEvent {
                    fields: new_update_event.fields.clone(),
                };

                let descendent_spans = self
                    .span_indexes
                    .descendents
                    .get(&span_key)
                    .cloned()
                    .unwrap_or_default();

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
                    .descendents
                    .get(&span_key)
                    .cloned()
                    .unwrap_or_default();

                for event_key in descendent_events {
                    // check if nested event attribute changed
                    self.event_indexes.update_with_new_field_on_parent(
                        &EventContext::new(event_key, &self.storage),
                        span_key,
                        &update_event.fields,
                    );
                }

                let span_event = SpanEvent {
                    connection_key: new_span_event.connection_key,
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
                    .get(&(new_span_event.connection_key, new_span_event.span_id))
                    .copied()
                    .ok_or(EngineInsertError::UnknownSpanId)?;

                let follows_span_key = self
                    .span_key_map
                    .get(&(new_span_event.connection_key, new_follows_event.follows))
                    .copied()
                    .ok_or(EngineInsertError::UnknownSpanId)?;

                // TODO: check against circular following
                // TODO: check against duplicates

                let span_event = SpanEvent {
                    connection_key: new_span_event.connection_key,
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
                    .get(&(new_span_event.connection_key, new_span_event.span_id))
                    .copied()
                    .ok_or(EngineInsertError::UnknownSpanId)?;

                let span_event = SpanEvent {
                    connection_key: new_span_event.connection_key,
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
                    .get(&(new_span_event.connection_key, new_span_event.span_id))
                    .copied()
                    .ok_or(EngineInsertError::UnknownSpanId)?;

                let span_event = SpanEvent {
                    connection_key: new_span_event.connection_key,
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
                    .get(&(new_span_event.connection_key, new_span_event.span_id))
                    .copied()
                    .ok_or(EngineInsertError::UnknownSpanId)?;

                let span_event = SpanEvent {
                    connection_key: new_span_event.connection_key,
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

        self.span_key_map
            .insert((span.connection_key, span.id), span_key);
        self.span_id_map.insert(span_key, span.id);
        self.span_indexes
            .update_with_new_span(&SpanContext::with_span(span, &self.storage));
    }

    fn insert_span_event_bookeeping(&mut self, span_event: &SpanEvent) {
        let timestamp = span_event.timestamp;
        let idx = self.span_event_ids.upper_bound_via_expansion(&timestamp);
        self.span_event_ids.insert(idx, timestamp);

        let by_span_index = self
            .span_events_by_span_ids
            .entry(span_event.span_key)
            .or_default();
        let idx = by_span_index.upper_bound_via_expansion(&timestamp);
        by_span_index.insert(idx, timestamp);
    }

    pub fn insert_event(&mut self, mut new_event: NewEvent) -> Result<(), EngineInsertError> {
        let span_key = new_event
            .span_id
            .map(|span_id| {
                self.span_key_map
                    .get(&(new_event.connection_key, span_id))
                    .copied()
                    .ok_or(EngineInsertError::UnknownParentSpanId)
            })
            .transpose()?;

        let event_key = self.keys.register(now(), new_event.timestamp);
        new_event.timestamp = event_key;

        let event = Event {
            connection_key: new_event.connection_key,
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
        let context = EventContext::with_event(&event, &self.storage);
        for (id, (filter, sender)) in &self.event_subscribers {
            if filter.matches(&context) {
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
        self.event_indexes
            .update_with_new_event(&EventContext::with_event(event, &self.storage));
    }

    pub fn delete(&mut self, filter: DeleteFilter) -> DeleteMetrics {
        let connections =
            self.get_connections_in_range_filter(filter.start, filter.end, filter.inside);
        let root_spans =
            self.get_root_spans_in_range_filter(filter.start, filter.end, filter.inside);
        let root_events =
            self.get_root_events_in_range_filter(filter.start, filter.end, filter.inside);

        let spans_from_root_spans = root_spans
            .iter()
            .flat_map(|root| {
                self.span_indexes
                    .descendents
                    .get(root)
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
                    .descendents
                    .get(root)
                    .map(Vec::as_slice)
                    .unwrap_or_default()
                    .iter()
                    .cloned()
            })
            .collect::<Vec<EventKey>>();
        let span_events = spans_from_root_spans
            .iter()
            .flat_map(|span| {
                self.span_events_by_span_ids
                    .get(span)
                    .map(Vec::as_slice)
                    .unwrap_or_default()
                    .iter()
                    .cloned()
            })
            .collect::<Vec<SpanEventKey>>();

        if filter.dry_run {
            return DeleteMetrics {
                connections: connections.len(),
                spans: spans_from_root_spans.len(),
                span_events: span_events.len(),
                events: root_events.len() + events_from_root_spans.len(),
            };
        }

        let mut connections_to_delete = connections;
        let mut spans_to_delete = spans_from_root_spans;
        let mut span_events_to_delete = span_events;
        let mut events_to_delete = root_events;
        events_to_delete.extend(events_from_root_spans);

        connections_to_delete.sort(); // this should already be sorted in theory
        spans_to_delete.sort();
        span_events_to_delete.sort();
        events_to_delete.sort();

        // drop smaller scoped entities from storage first to avoid integrity
        // issues if things go wrong

        self.storage.drop_events(&events_to_delete);
        self.storage.drop_span_events(&span_events_to_delete);
        self.storage.drop_spans(&spans_to_delete);
        self.storage.drop_connections(&connections_to_delete);

        // remove smaller scoped entities from indexes last for some efficiency

        self.remove_connections_bookeeping(&connections_to_delete);
        self.remove_spans_bookeeping(&spans_to_delete);
        self.remove_span_events_bookeeping(&span_events_to_delete);
        self.remove_events_bookeeping(&events_to_delete);

        DeleteMetrics {
            connections: connections_to_delete.len(),
            spans: spans_to_delete.len(),
            span_events: span_events_to_delete.len(),
            events: events_to_delete.len(),
        }
    }

    pub fn get_connections_in_range_filter(
        &self,
        start: Timestamp,
        end: Timestamp,
        inside: bool,
    ) -> Vec<ConnectionKey> {
        self.connections
            .iter()
            .filter(|(_, connection)| {
                if inside {
                    connection.connected_at <= end
                        && connection.disconnected_at.unwrap_or(Timestamp::MAX) >= start
                } else {
                    connection.connected_at > end
                        || connection.disconnected_at.unwrap_or(Timestamp::MAX) < start
                }
            })
            .map(|(key, _)| *key)
            .collect()
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

        let indexed_filter = IndexedSpanFilter::build(Some(filter), &self.span_indexes);
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

        let indexed_filter = IndexedEventFilter::build(Some(filter), &self.event_indexes);
        let iter = IndexedEventFilterIterator::new_internal(indexed_filter, self);

        iter.collect()
    }

    fn remove_connections_bookeeping(&mut self, connections: &[ConnectionKey]) {
        for connection_key in connections {
            self.connections.remove(connection_key);
        }

        self.connection_key_map
            .retain(|_, key| !connections.contains(key));

        self.span_indexes.remove_connections(connections);
        self.event_indexes.remove_connections(connections);
    }

    fn remove_spans_bookeeping(&mut self, spans: &[SpanKey]) {
        for span_key in spans {
            self.span_id_map.remove(span_key);
        }

        self.span_key_map.retain(|_, key| !spans.contains(key));

        self.span_indexes.remove_spans(spans);

        self.event_indexes.remove_spans(spans);
        for span_key in spans {
            self.span_events_by_span_ids.remove(span_key);
        }
    }

    fn remove_span_events_bookeeping(&mut self, span_events: &[SpanEventKey]) {
        self.span_event_ids.remove_list_sorted(span_events);

        for span_index in self.span_events_by_span_ids.values_mut() {
            span_index.remove_list_sorted(span_events);
        }
    }

    fn remove_events_bookeeping(&mut self, events: &[EventKey]) {
        self.event_indexes.remove_events(events);
    }

    pub fn copy_dataset(&self, mut to: Box<dyn Storage + Send>) {
        let connections = self.storage.get_all_connections().collect::<Vec<_>>();

        for connection in connections {
            to.insert_connection((*connection).clone());
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
    ) -> (SubscriptionId, UnboundedReceiver<EventView>) {
        let mut filter = BasicEventFilter::And(
            filter
                .into_iter()
                .map(|p| {
                    BasicEventFilter::from_predicate(
                        p,
                        &self.connection_key_map,
                        &self.span_key_map,
                    )
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

struct EventContext<'a, S> {
    event_key: EventKey,
    storage: &'a S,
    event: RefOrDeferredArc<'a, Event>,
    parents: OnceCell<Vec<Arc<Span>>>,
    connection: OnceCell<Arc<Connection>>,
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
            connection: OnceCell::new(),
        }
    }

    fn with_event(event: &'a Event, storage: &'a S) -> EventContext<'a, S> {
        EventContext {
            event_key: event.key(),
            storage,
            event: RefOrDeferredArc::Ref(event),
            parents: OnceCell::new(),
            connection: OnceCell::new(),
        }
    }

    fn key(&self) -> EventKey {
        self.event_key
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
                let mut parent_key_next = event.span_key;

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

    fn connection(&self) -> &Connection {
        let event = self.event();

        self.connection
            .get_or_init(|| self.storage.get_connection(event.connection_key).unwrap())
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

        let connection = self.connection();
        if let Some(v) = connection.fields.get(attr) {
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

        let connection = self.connection();
        if let Some(v) = connection.fields.get(attr) {
            return Some((v, connection.key()));
        }

        None
    }

    #[allow(unused)]
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

        let connection = self.connection();
        for (attr, value) in &connection.fields {
            attributes.entry(&**attr).or_insert(value);
        }

        attributes.into_iter()
    }
}

struct SpanContext<'a, S> {
    span_key: SpanKey,
    storage: &'a S,
    span: RefOrDeferredArc<'a, Span>,
    parents: OnceCell<Vec<Arc<Span>>>,
    connection: OnceCell<Arc<Connection>>,
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
            connection: OnceCell::new(),
        }
    }

    fn with_span(span: &'a Span, storage: &'a S) -> SpanContext<'a, S> {
        SpanContext {
            span_key: span.key(),
            storage,
            span: RefOrDeferredArc::Ref(span),
            parents: OnceCell::new(),
            connection: OnceCell::new(),
        }
    }

    fn key(&self) -> SpanKey {
        self.span_key
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

    fn connection(&self) -> &Connection {
        let span = self.span();

        self.connection
            .get_or_init(|| self.storage.get_connection(span.connection_key).unwrap())
            .as_ref()
    }

    fn attribute(&self, attr: &str) -> Option<&Value> {
        let span = self.span();
        if let Some(v) = span.fields.get(attr) {
            return Some(v);
        }

        let parents = self.parents();
        for parent in parents {
            if let Some(v) = parent.fields.get(attr) {
                return Some(v);
            }
        }

        let connection = self.connection();
        if let Some(v) = connection.fields.get(attr) {
            return Some(v);
        }

        None
    }

    fn attribute_with_key(&self, attr: &str) -> Option<(&Value, Timestamp)> {
        let span = self.span();
        if let Some(v) = span.fields.get(attr) {
            return Some((v, span.key()));
        }

        let parents = self.parents();
        for parent in parents {
            if let Some(v) = parent.fields.get(attr) {
                return Some((v, parent.key()));
            }
        }

        let connection = self.connection();
        if let Some(v) = connection.fields.get(attr) {
            return Some((v, connection.key()));
        }

        None
    }

    #[allow(unused)]
    fn attributes(&self) -> impl Iterator<Item = (&str, &Value)> {
        let mut attributes = BTreeMap::new();

        let span = self.span();
        for (attr, value) in &span.fields {
            attributes.entry(&**attr).or_insert(value);
        }

        let parents = self.parents();
        for parent in parents {
            for (attr, value) in &parent.fields {
                attributes.entry(&**attr).or_insert(value);
            }
        }

        let connection = self.connection();
        for (attr, value) in &connection.fields {
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
    use models::{NewCreateSpanEvent, NewUpdateSpanEvent};

    use super::*;

    #[test]
    fn test_event_filters() {
        let mut engine = RawEngine::new(TransientStorage::new());

        let connection_key = engine
            .insert_connection(NewConnection {
                id: 1,
                fields: BTreeMap::new(),
            })
            .unwrap();

        let simple = |id: u64, level: i32, attribute1: &str, attribute2: &str| -> NewEvent {
            NewEvent {
                connection_key,
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

        let connection_key = engine
            .insert_connection(NewConnection {
                id: 1,
                fields: BTreeMap::new(),
            })
            .unwrap();

        let simple_open =
            |open: u64, level: i32, attribute1: &str, attribute2: &str| -> NewSpanEvent {
                NewSpanEvent {
                    connection_key,
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
                connection_key,
                timestamp: Timestamp::new(close).unwrap(),
                span_id: open.try_into().unwrap(),
                kind: NewSpanEventKind::Close,
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
    fn event_found_with_nonindexed_connection_attribute() {
        let mut engine = RawEngine::new(TransientStorage::new());

        let connection_key = engine
            .insert_connection(NewConnection {
                id: 1,
                fields: BTreeMap::from_iter([("attr1".to_owned(), Value::Str("A".to_owned()))]),
            })
            .unwrap();

        let now = now();
        engine
            .insert_event(NewEvent {
                connection_key,
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
    fn event_found_with_indexed_connection_attribute() {
        let mut engine = RawEngine::new(TransientStorage::new());

        let connection_key = engine
            .insert_connection(NewConnection {
                id: 1,
                fields: BTreeMap::from_iter([("attr1".to_owned(), Value::Str("A".to_owned()))]),
            })
            .unwrap();

        let now = now();
        engine
            .insert_event(NewEvent {
                connection_key,
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
    fn event_found_with_nonindexed_inherent_attribute() {
        let mut engine = RawEngine::new(TransientStorage::new());

        let connection_key = engine
            .insert_connection(NewConnection {
                id: 1,
                fields: BTreeMap::from_iter([("attr1".to_owned(), Value::Str("A".to_owned()))]),
            })
            .unwrap();

        let now = now();
        engine
            .insert_event(NewEvent {
                connection_key,
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
    fn event_found_with_indexed_inherent_attribute() {
        let mut engine = RawEngine::new(TransientStorage::new());

        let connection_key = engine
            .insert_connection(NewConnection {
                id: 1,
                fields: BTreeMap::from_iter([("attr1".to_owned(), Value::Str("A".to_owned()))]),
            })
            .unwrap();

        let now = now();
        engine
            .insert_event(NewEvent {
                connection_key,
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
    fn event_found_with_nonindexed_span_attribute() {
        let mut engine = RawEngine::new(TransientStorage::new());

        let connection_key = engine
            .insert_connection(NewConnection {
                id: 1,
                fields: BTreeMap::from_iter([("attr1".to_owned(), Value::Str("A".to_owned()))]),
            })
            .unwrap();

        engine
            .insert_span_event(NewSpanEvent {
                connection_key,
                timestamp: now(),
                span_id: 1.try_into().unwrap(),
                kind: NewSpanEventKind::Create(NewCreateSpanEvent {
                    parent_id: None,
                    target: "crate::storage::tests".to_owned(),
                    name: "test".to_owned(),
                    level: 4,
                    file_name: None,
                    file_line: None,
                    fields: BTreeMap::from_iter([("attr1".to_owned(), Value::Str("C".to_owned()))]),
                }),
            })
            .unwrap();

        let now = now();
        engine
            .insert_event(NewEvent {
                connection_key,
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
    fn event_found_with_indexed_span_attribute() {
        let mut engine = RawEngine::new(TransientStorage::new());

        let connection_key = engine
            .insert_connection(NewConnection {
                id: 1,
                fields: BTreeMap::from_iter([("attr1".to_owned(), Value::Str("A".to_owned()))]),
            })
            .unwrap();

        engine
            .insert_span_event(NewSpanEvent {
                connection_key,
                timestamp: now(),
                span_id: 1.try_into().unwrap(),
                kind: NewSpanEventKind::Create(NewCreateSpanEvent {
                    parent_id: None,
                    target: "crate::storage::tests".to_owned(),
                    name: "test".to_owned(),
                    level: 4,
                    file_name: None,
                    file_line: None,
                    fields: BTreeMap::from_iter([("attr1".to_owned(), Value::Str("C".to_owned()))]),
                }),
            })
            .unwrap();

        let now = now();
        engine
            .insert_event(NewEvent {
                connection_key,
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

        let connection_key = engine
            .insert_connection(NewConnection {
                id: 1,
                fields: BTreeMap::from_iter([("attr1".to_owned(), Value::Str("A".to_owned()))]),
            })
            .unwrap();

        engine
            .insert_span_event(NewSpanEvent {
                connection_key,
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
                connection_key,
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
                connection_key,
                timestamp: super::now(),
                span_id: 1.try_into().unwrap(),
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

        let connection_key = engine
            .insert_connection(NewConnection {
                id: 1,
                fields: BTreeMap::from_iter([("attr1".to_owned(), Value::Str("A".to_owned()))]),
            })
            .unwrap();

        engine
            .insert_span_event(NewSpanEvent {
                connection_key,
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
                connection_key,
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
                connection_key,
                timestamp: super::now(),
                span_id: 1.try_into().unwrap(),
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
