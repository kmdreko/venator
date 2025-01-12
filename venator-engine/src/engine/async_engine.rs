use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::time::Instant;

use anyhow::{Context, Error as AnyError};
use tokio::sync::mpsc::{self, UnboundedSender};
use tokio::sync::oneshot::{self, Sender as OneshotSender};
use tracing::instrument;

use crate::filter::{FilterPredicate, Query};
use crate::storage::Storage;
use crate::subscription::Subscriber;
use crate::{
    ComposedEvent, ComposedSpan, DatasetStats, DeleteFilter, DeleteMetrics, EngineStatus,
    InstanceId, NewEvent, NewResource, NewSpanEvent, ResourceKey, SpanEvent, SpanKey,
    SubscriptionId,
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
    pub fn new<S: Storage + Send + 'static>(storage: S) -> Result<AsyncEngine, AnyError> {
        let (insert_sender, mut insert_receiver) =
            mpsc::unbounded_channel::<(tracing::Span, EngineCommand)>();
        let (query_sender, mut query_receiver) =
            mpsc::unbounded_channel::<(tracing::Span, EngineCommand)>();

        let mut engine = SyncEngine::new(storage)?;

        std::thread::spawn(move || {
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
                let panic_result = std::panic::catch_unwind(AssertUnwindSafe(|| match cmd {
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
                            tracing::warn!("rejecting resource insert due to: {err:?}");
                        }
                        let _ = sender.send(res);
                    }
                    EngineCommand::DisconnectTracingInstance(instance_id, sender) => {
                        let res = engine.disconnect_tracing_instance(instance_id);
                        if let Err(err) = &res {
                            tracing::warn!("rejecting disconnect due to: {err:?}");
                        }
                        let _ = sender.send(res);
                    }
                    EngineCommand::InsertSpanEvent(span_event, sender) => {
                        let res = engine.insert_span_event(span_event);
                        if let Err(err) = &res {
                            tracing::warn!("rejecting span event insert due to: {err:?}");
                        }
                        let _ = sender.send(res);
                    }
                    EngineCommand::InsertEvent(event, sender) => {
                        let res = engine.insert_event(event);
                        if let Err(err) = &res {
                            tracing::warn!("rejecting event insert due to: {err:?}");
                        }
                        let _ = sender.send(res);
                    }
                    EngineCommand::Delete(filter, sender) => {
                        let res = engine.delete(filter);
                        let _ = sender.send(res);
                    }
                    EngineCommand::SpanSubscribe(filter, sender) => {
                        let res = engine.subscribe_to_spans(filter);
                        let _ = sender.send(res);
                    }
                    EngineCommand::SpanUnsubscribe(id, sender) => {
                        engine.unsubscribe_from_spans(id);
                        let _ = sender.send(());
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
                        let res = engine.copy_dataset(to);
                        let _ = sender.send(res);
                    }
                    EngineCommand::GetStatus(sender) => {
                        let elapsed_ms = last_check.elapsed().as_millis();
                        let computed_ms = computed_ms_since_last_check;

                        last_check = Instant::now();
                        computed_ms_since_last_check = 0;

                        let load = computed_ms as f64 / elapsed_ms as f64;

                        let _ = sender.send(EngineStatus {
                            load: load.min(1.0) * 100.0,
                        });
                    }
                    EngineCommand::Save(sender) => {
                        let res = engine.save();
                        let _ = sender.send(res);
                    }
                }));

                if let Err(err) = panic_result {
                    tracing::error!("engine call panicked: {err:?}");
                }

                let cmd_elapsed = cmd_start.elapsed().as_millis();
                computed_ms_since_last_check += cmd_elapsed;
            }
        });

        Ok(AsyncEngine {
            insert_sender,
            query_sender,
        })
    }

    // The query is executed even if the returned future is not awaited
    #[instrument(skip_all)]
    pub fn query_span(
        &self,
        query: Query,
    ) -> impl Future<Output = Result<Vec<ComposedSpan>, AnyError>> {
        let (sender, receiver) = oneshot::channel();
        let _ = self.query_sender.send((
            tracing::Span::current(),
            EngineCommand::QuerySpan(query, sender),
        ));
        async move { receiver.await.context("failed to get result") }
    }

    // The query is executed even if the returned future is not awaited
    #[instrument(skip_all)]
    pub fn query_span_count(&self, query: Query) -> impl Future<Output = Result<usize, AnyError>> {
        let (sender, receiver) = oneshot::channel();
        let _ = self.query_sender.send((
            tracing::Span::current(),
            EngineCommand::QuerySpanCount(query, sender),
        ));
        async move { receiver.await.context("failed to get result") }
    }

    // The query is executed even if the returned future is not awaited
    #[instrument(skip_all)]
    #[doc(hidden)]
    pub fn query_span_event(
        &self,
        query: Query,
    ) -> impl Future<Output = Result<Vec<SpanEvent>, AnyError>> {
        let (sender, receiver) = oneshot::channel();
        let _ = self.query_sender.send((
            tracing::Span::current(),
            EngineCommand::QuerySpanEvent(query, sender),
        ));
        async move { receiver.await.context("failed to get result") }
    }

    // The query is executed even if the returned future is not awaited
    #[instrument(skip_all)]
    pub fn query_event(
        &self,
        query: Query,
    ) -> impl Future<Output = Result<Vec<ComposedEvent>, AnyError>> {
        let (sender, receiver) = oneshot::channel();
        let _ = self.query_sender.send((
            tracing::Span::current(),
            EngineCommand::QueryEvent(query, sender),
        ));
        async move { receiver.await.context("failed to get result") }
    }

    // The query is executed even if the returned future is not awaited
    #[instrument(skip_all)]
    pub fn query_event_count(&self, query: Query) -> impl Future<Output = Result<usize, AnyError>> {
        let (sender, receiver) = oneshot::channel();
        let _ = self.query_sender.send((
            tracing::Span::current(),
            EngineCommand::QueryEventCount(query, sender),
        ));
        async move { receiver.await.context("failed to get result") }
    }

    // The query is executed even if the returned future is not awaited
    #[instrument(skip_all)]
    pub fn query_stats(&self) -> impl Future<Output = Result<DatasetStats, AnyError>> {
        let (sender, receiver) = oneshot::channel();
        let _ = self
            .query_sender
            .send((tracing::Span::current(), EngineCommand::QueryStats(sender)));
        async move { receiver.await.context("failed to get result") }
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
        async move { receiver.await.context("failed to get result")? }
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
        async move { receiver.await.context("failed to get result")? }
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
        async move { receiver.await.context("failed to get result")? }
    }

    #[instrument(skip_all)]
    pub fn insert_event(&self, event: NewEvent) -> impl Future<Output = Result<(), AnyError>> {
        let (sender, receiver) = oneshot::channel();
        let _ = self.insert_sender.send((
            tracing::Span::current(),
            EngineCommand::InsertEvent(event, sender),
        ));
        async move { receiver.await.context("failed to get result")? }
    }

    #[instrument(skip_all)]
    pub fn delete(
        &self,
        filter: DeleteFilter,
    ) -> impl Future<Output = Result<DeleteMetrics, AnyError>> {
        let (sender, receiver) = oneshot::channel();
        let _ = self.insert_sender.send((
            tracing::Span::current(),
            EngineCommand::Delete(filter, sender),
        ));
        async move { receiver.await.context("failed to get result")? }
    }

    #[instrument(skip_all)]
    pub fn subscribe_to_spans(
        &self,
        filter: Vec<FilterPredicate>,
    ) -> impl Future<Output = Result<Subscriber<ComposedSpan>, AnyError>> {
        let (sender, receiver) = oneshot::channel();
        let _ = self.query_sender.send((
            tracing::Span::current(),
            EngineCommand::SpanSubscribe(filter, sender),
        ));
        async move { receiver.await.context("failed to get result")? }
    }

    #[instrument(skip_all)]
    pub fn unsubscribe_from_spans(
        &self,
        id: SubscriptionId,
    ) -> impl Future<Output = Result<(), AnyError>> {
        let (sender, receiver) = oneshot::channel();
        let _ = self.query_sender.send((
            tracing::Span::current(),
            EngineCommand::SpanUnsubscribe(id, sender),
        ));
        async move { receiver.await.context("failed to get result") }
    }

    #[instrument(skip_all)]
    pub fn subscribe_to_events(
        &self,
        filter: Vec<FilterPredicate>,
    ) -> impl Future<Output = Result<Subscriber<ComposedEvent>, AnyError>> {
        let (sender, receiver) = oneshot::channel();
        let _ = self.query_sender.send((
            tracing::Span::current(),
            EngineCommand::EventSubscribe(filter, sender),
        ));
        async move { receiver.await.context("failed to get result")? }
    }

    #[instrument(skip_all)]
    pub fn unsubscribe_from_events(
        &self,
        id: SubscriptionId,
    ) -> impl Future<Output = Result<(), AnyError>> {
        let (sender, receiver) = oneshot::channel();
        let _ = self.query_sender.send((
            tracing::Span::current(),
            EngineCommand::EventUnsubscribe(id, sender),
        ));
        async move { receiver.await.context("failed to get result") }
    }

    #[instrument(skip_all)]
    pub fn copy_dataset(
        &self,
        to: Box<dyn Storage + Send>,
    ) -> impl Future<Output = Result<(), AnyError>> {
        let (sender, receiver) = oneshot::channel();
        let _ = self.query_sender.send((
            tracing::Span::current(),
            EngineCommand::CopyDataset(to, sender),
        ));
        async move { receiver.await.context("failed to get result")? }
    }

    #[instrument(skip_all)]
    pub fn get_status(&self) -> impl Future<Output = Result<EngineStatus, AnyError>> {
        let (sender, receiver) = oneshot::channel();
        let _ = self
            .query_sender
            .send((tracing::Span::current(), EngineCommand::GetStatus(sender)));
        async move { receiver.await.context("failed to get result") }
    }

    #[instrument(skip_all)]
    pub fn save(&self) -> impl Future<Output = Result<(), AnyError>> {
        let (sender, receiver) = oneshot::channel();
        let _ = self
            .insert_sender
            .send((tracing::Span::current(), EngineCommand::Save(sender)));
        async move { receiver.await.context("failed to get result")? }
    }
}

enum EngineCommand {
    QuerySpan(Query, OneshotSender<Vec<ComposedSpan>>),
    QuerySpanCount(Query, OneshotSender<usize>),
    QuerySpanEvent(Query, OneshotSender<Vec<SpanEvent>>),
    QueryEvent(Query, OneshotSender<Vec<ComposedEvent>>),
    QueryEventCount(Query, OneshotSender<usize>),
    QueryStats(OneshotSender<DatasetStats>),
    InsertResource(NewResource, OneshotSender<Result<ResourceKey, AnyError>>),
    DisconnectTracingInstance(InstanceId, OneshotSender<Result<(), AnyError>>),
    InsertSpanEvent(NewSpanEvent, OneshotSender<Result<SpanKey, AnyError>>),
    InsertEvent(NewEvent, OneshotSender<Result<(), AnyError>>),
    Delete(DeleteFilter, OneshotSender<Result<DeleteMetrics, AnyError>>),

    SpanSubscribe(
        Vec<FilterPredicate>,
        OneshotSender<Result<Subscriber<ComposedSpan>, AnyError>>,
    ),
    SpanUnsubscribe(SubscriptionId, OneshotSender<()>),
    EventSubscribe(
        Vec<FilterPredicate>,
        OneshotSender<Result<Subscriber<ComposedEvent>, AnyError>>,
    ),
    EventUnsubscribe(SubscriptionId, OneshotSender<()>),

    CopyDataset(Box<dyn Storage + Send>, OneshotSender<Result<(), AnyError>>),
    GetStatus(OneshotSender<EngineStatus>),

    Save(OneshotSender<Result<(), AnyError>>),
}
