use std::future::Future;
use std::time::Instant;

use anyhow::Error as AnyError;
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};
use tokio::sync::oneshot::{self, Sender as OneshotSender};
use tracing::instrument;

use crate::filter::{FilterPredicate, Query};
use crate::storage::Storage;
use crate::{
    DeleteFilter, DeleteMetrics, EngineStatusView, EventView, InstanceId, NewEvent, NewResource,
    NewSpanEvent, ResourceKey, SpanEvent, SpanKey, SpanView, StatsView, SubscriptionId,
    SubscriptionResponse,
};

use super::SyncEngine;

/// Provides the core engine functionality with an async interface.
///
/// Internally this wraps a `SyncEngine` in a thread and coordinates with it via
/// message passing. As such, all the methods are cancel safe (i.e. an insert
/// call will complete whether it is polled or not).
#[derive(Clone)]
pub struct AsyncEngine {
    insert_sender: UnboundedSender<(tracing::Span, EngineCommand)>,
    query_sender: UnboundedSender<(tracing::Span, EngineCommand)>,
}

impl AsyncEngine {
    pub fn new<S: Storage + Send + 'static>(storage: S) -> AsyncEngine {
        let (insert_sender, mut insert_receiver) =
            mpsc::unbounded_channel::<(tracing::Span, EngineCommand)>();
        let (query_sender, mut query_receiver) =
            mpsc::unbounded_channel::<(tracing::Span, EngineCommand)>();

        std::thread::spawn(move || {
            let mut engine = SyncEngine::new(storage).unwrap();

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

            while let Some((tracing_span, cmd)) = recv() {
                let _entered_span = tracing_span.enter();

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
                        let metrics = engine.delete(filter).unwrap();
                        let _ = sender.send(metrics);
                    }
                    EngineCommand::SpanSubscribe(filter, sender) => {
                        let res = engine.subscribe_to_spans(filter).unwrap();
                        let _ = sender.send(res);
                    }
                    EngineCommand::SpanUnsubscribe(id, sender) => {
                        engine.unsubscribe_from_spans(id);
                        let _ = sender.send(());
                    }
                    EngineCommand::EventSubscribe(filter, sender) => {
                        let res = engine.subscribe_to_events(filter).unwrap();
                        let _ = sender.send(res);
                    }
                    EngineCommand::EventUnsubscribe(id, sender) => {
                        engine.unsubscribe_from_events(id);
                        let _ = sender.send(());
                    }
                    EngineCommand::CopyDataset(to, sender) => {
                        engine.copy_dataset(to).unwrap();
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
                        engine.save().unwrap();
                        let _ = sender.send(());
                    }
                }
                let cmd_elapsed = cmd_start.elapsed().as_millis();
                computed_ms_since_last_check += cmd_elapsed;
            }
        });

        AsyncEngine {
            insert_sender,
            query_sender,
        }
    }

    // The query is executed even if the returned future is not awaited
    #[instrument(skip_all)]
    pub fn query_span(&self, query: Query) -> impl Future<Output = Vec<SpanView>> {
        let (sender, receiver) = oneshot::channel();
        let _ = self.query_sender.send((
            tracing::Span::current(),
            EngineCommand::QuerySpan(query, sender),
        ));
        async move { receiver.await.unwrap() }
    }

    // The query is executed even if the returned future is not awaited
    #[instrument(skip_all)]
    pub fn query_span_count(&self, query: Query) -> impl Future<Output = usize> {
        let (sender, receiver) = oneshot::channel();
        let _ = self.query_sender.send((
            tracing::Span::current(),
            EngineCommand::QuerySpanCount(query, sender),
        ));
        async move { receiver.await.unwrap() }
    }

    // The query is executed even if the returned future is not awaited
    #[instrument(skip_all)]
    #[doc(hidden)]
    pub fn query_span_event(&self, query: Query) -> impl Future<Output = Vec<SpanEvent>> {
        let (sender, receiver) = oneshot::channel();
        let _ = self.query_sender.send((
            tracing::Span::current(),
            EngineCommand::QuerySpanEvent(query, sender),
        ));
        async move { receiver.await.unwrap() }
    }

    // The query is executed even if the returned future is not awaited
    #[instrument(skip_all)]
    pub fn query_event(&self, query: Query) -> impl Future<Output = Vec<EventView>> {
        let (sender, receiver) = oneshot::channel();
        let _ = self.query_sender.send((
            tracing::Span::current(),
            EngineCommand::QueryEvent(query, sender),
        ));
        async move { receiver.await.unwrap() }
    }

    // The query is executed even if the returned future is not awaited
    #[instrument(skip_all)]
    pub fn query_event_count(&self, query: Query) -> impl Future<Output = usize> {
        let (sender, receiver) = oneshot::channel();
        let _ = self.query_sender.send((
            tracing::Span::current(),
            EngineCommand::QueryEventCount(query, sender),
        ));
        async move { receiver.await.unwrap() }
    }

    // The query is executed even if the returned future is not awaited
    #[instrument(skip_all)]
    pub fn query_stats(&self) -> impl Future<Output = StatsView> {
        let (sender, receiver) = oneshot::channel();
        let _ = self
            .query_sender
            .send((tracing::Span::current(), EngineCommand::QueryStats(sender)));
        async move { receiver.await.unwrap() }
    }

    #[instrument(skip_all)]
    pub fn insert_resource(
        &self,
        resource: NewResource,
    ) -> impl Future<Output = Result<ResourceKey, AnyError>> {
        let (sender, receiver) = oneshot::channel();
        let _ = self.insert_sender.send((
            tracing::Span::current(),
            EngineCommand::InsertResource(resource, sender),
        ));
        async move { receiver.await.unwrap() }
    }

    #[instrument(skip_all)]
    pub fn disconnect_tracing_instance(
        &self,
        id: InstanceId,
    ) -> impl Future<Output = Result<(), AnyError>> {
        let (sender, receiver) = oneshot::channel();
        let _ = self.insert_sender.send((
            tracing::Span::current(),
            EngineCommand::DisconnectTracingInstance(id, sender),
        ));
        async move { receiver.await.unwrap() }
    }

    #[instrument(skip_all)]
    pub fn insert_span_event(
        &self,
        span_event: NewSpanEvent,
    ) -> impl Future<Output = Result<SpanKey, AnyError>> {
        let (sender, receiver) = oneshot::channel();
        let _ = self.insert_sender.send((
            tracing::Span::current(),
            EngineCommand::InsertSpanEvent(span_event, sender),
        ));
        async move { receiver.await.unwrap() }
    }

    #[instrument(skip_all)]
    pub fn insert_event(&self, event: NewEvent) -> impl Future<Output = Result<(), AnyError>> {
        let (sender, receiver) = oneshot::channel();
        let _ = self.insert_sender.send((
            tracing::Span::current(),
            EngineCommand::InsertEvent(event, sender),
        ));
        async move { receiver.await.unwrap() }
    }

    #[instrument(skip_all)]
    pub fn delete(&self, filter: DeleteFilter) -> impl Future<Output = DeleteMetrics> {
        let (sender, receiver) = oneshot::channel();
        let _ = self.insert_sender.send((
            tracing::Span::current(),
            EngineCommand::Delete(filter, sender),
        ));
        async move { receiver.await.unwrap() }
    }

    #[instrument(skip_all)]
    pub fn subscribe_to_spans(
        &self,
        filter: Vec<FilterPredicate>,
    ) -> impl Future<
        Output = (
            SubscriptionId,
            UnboundedReceiver<SubscriptionResponse<SpanView>>,
        ),
    > {
        let (sender, receiver) = oneshot::channel();
        let _ = self.query_sender.send((
            tracing::Span::current(),
            EngineCommand::SpanSubscribe(filter, sender),
        ));
        async move { receiver.await.unwrap() }
    }

    #[instrument(skip_all)]
    pub fn unsubscribe_from_spans(&self, id: SubscriptionId) -> impl Future<Output = ()> {
        let (sender, receiver) = oneshot::channel();
        let _ = self.query_sender.send((
            tracing::Span::current(),
            EngineCommand::SpanUnsubscribe(id, sender),
        ));
        async move { receiver.await.unwrap() }
    }

    #[instrument(skip_all)]
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
        let _ = self.query_sender.send((
            tracing::Span::current(),
            EngineCommand::EventSubscribe(filter, sender),
        ));
        async move { receiver.await.unwrap() }
    }

    #[instrument(skip_all)]
    pub fn unsubscribe_from_events(&self, id: SubscriptionId) -> impl Future<Output = ()> {
        let (sender, receiver) = oneshot::channel();
        let _ = self.query_sender.send((
            tracing::Span::current(),
            EngineCommand::EventUnsubscribe(id, sender),
        ));
        async move { receiver.await.unwrap() }
    }

    #[instrument(skip_all)]
    pub fn copy_dataset(&self, to: Box<dyn Storage + Send>) -> impl Future<Output = ()> {
        let (sender, receiver) = oneshot::channel();
        let _ = self.query_sender.send((
            tracing::Span::current(),
            EngineCommand::CopyDataset(to, sender),
        ));
        async move { receiver.await.unwrap() }
    }

    #[instrument(skip_all)]
    pub fn get_status(&self) -> impl Future<Output = EngineStatusView> {
        let (sender, receiver) = oneshot::channel();
        let _ = self
            .query_sender
            .send((tracing::Span::current(), EngineCommand::GetStatus(sender)));
        async move { receiver.await.unwrap() }
    }

    #[instrument(skip_all)]
    pub fn save(&self) -> impl Future<Output = ()> {
        let (sender, receiver) = oneshot::channel();
        let _ = self
            .insert_sender
            .send((tracing::Span::current(), EngineCommand::Save(sender)));
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
    InsertResource(NewResource, OneshotSender<Result<ResourceKey, AnyError>>),
    DisconnectTracingInstance(InstanceId, OneshotSender<Result<(), AnyError>>),
    InsertSpanEvent(NewSpanEvent, OneshotSender<Result<SpanKey, AnyError>>),
    InsertEvent(NewEvent, OneshotSender<Result<(), AnyError>>),
    Delete(DeleteFilter, OneshotSender<DeleteMetrics>),

    SpanSubscribe(
        Vec<FilterPredicate>,
        OneshotSender<(
            SubscriptionId,
            UnboundedReceiver<SubscriptionResponse<SpanView>>,
        )>,
    ),
    SpanUnsubscribe(SubscriptionId, OneshotSender<()>),
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
