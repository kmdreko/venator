use std::panic::AssertUnwindSafe;
use std::thread::Builder as ThreadBuilder;
use std::time::Instant;

use anyhow::{anyhow, Context, Error as AnyError};
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::sync::oneshot::{self, Receiver as OneshotReceiver, Sender as OneshotSender};
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
/// message passing.
#[derive(Clone)]
pub struct AsyncEngine {
    sync_sender: Sender<(tracing::Span, EngineCommand)>,
    insert_sender: Sender<(tracing::Span, EngineCommand)>,
    query_sender: Sender<(tracing::Span, EngineCommand)>,
}

impl AsyncEngine {
    pub fn new<S: Storage + Send + 'static>(storage: S) -> Result<AsyncEngine, AnyError> {
        let (sync_sender, mut sync_receiver) = mpsc::channel::<(tracing::Span, EngineCommand)>(1);
        let (insert_sender, mut insert_receiver) =
            mpsc::channel::<(tracing::Span, EngineCommand)>(10000);
        let (query_sender, mut query_receiver) =
            mpsc::channel::<(tracing::Span, EngineCommand)>(10000);

        let mut engine = SyncEngine::new(storage)?;

        let _ = ThreadBuilder::new().name("engine".into()).spawn(move || {
            let mut last_check = Instant::now();
            let mut computed_ns_since_last_check: u128 = 0;

            fn recv(
                sync: &mut Receiver<(tracing::Span, EngineCommand)>,
                query: &mut Receiver<(tracing::Span, EngineCommand)>,
                insert: &mut Receiver<(tracing::Span, EngineCommand)>,
            ) -> Option<(tracing::Span, EngineCommand)> {
                futures::executor::block_on(async {
                    tokio::select! {
                        biased;
                        msg = sync.recv() => {
                            msg
                        }
                        msg = query.recv() => {
                            msg
                        }
                        msg = insert.recv() => {
                            msg
                        }
                    }
                })
            }

            while let Some((tracing_span, cmd)) = recv(
                &mut sync_receiver,
                &mut query_receiver,
                &mut insert_receiver,
            ) {
                let cmd_start = Instant::now();

                let entered_span = tracing_span.entered();
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
                    EngineCommand::CopyDataset(mut to, sender) => {
                        let res = engine.copy_dataset(&mut *to);
                        let _ = sender.send(res);
                    }
                    EngineCommand::GetStatus(sender) => {
                        let elapsed_ms = last_check.elapsed().as_nanos();
                        let computed_ms = computed_ns_since_last_check;

                        last_check = Instant::now();
                        computed_ns_since_last_check = 0;

                        let load = computed_ms as f64 / elapsed_ms as f64;

                        let _ = sender.send(EngineStatus {
                            load: load.min(1.0) * 100.0,
                        });
                    }
                    EngineCommand::Shutdown(sender) => {
                        // The shutdown process first prevents further inserts
                        // and handles the existing ones. And since the insert
                        // receiver is no longer receiving commands, it will
                        // exit the loop and therefore shut down the engine
                        // after any followup query commands have been handled.
                        insert_receiver.close();

                        while let Some((tracing_span, cmd)) = insert_receiver.blocking_recv() {
                            let _entered_span = tracing_span.enter();
                            match cmd {
                                EngineCommand::InsertResource(resource, sender) => {
                                    let res = engine.insert_resource(resource);
                                    let _ = sender.send(res);
                                }
                                EngineCommand::DisconnectTracingInstance(instance_id, sender) => {
                                    let res = engine.disconnect_tracing_instance(instance_id);
                                    let _ = sender.send(res);
                                }
                                EngineCommand::InsertSpanEvent(span_event, sender) => {
                                    let res = engine.insert_span_event(span_event);
                                    let _ = sender.send(res);
                                }
                                EngineCommand::InsertEvent(event, sender) => {
                                    let res = engine.insert_event(event);
                                    let _ = sender.send(res);
                                }
                                _ => tracing::warn!("ignoring unexpected command on shutdown"),
                            }
                        }

                        let res = engine.shutdown();
                        let _ = sender.send(res);
                    }
                    EngineCommand::Sync => {
                        let _ = engine.sync();
                    }
                }));

                if let Err(err) = panic_result {
                    tracing::error!("engine call panicked: {err:?}");
                }

                drop(entered_span);

                let cmd_elapsed = cmd_start.elapsed().as_nanos();
                computed_ns_since_last_check += cmd_elapsed;
            }
        });

        Ok(AsyncEngine {
            sync_sender,
            insert_sender,
            query_sender,
        })
    }

    #[instrument(skip_all)]
    pub async fn query_span(&self, query: Query) -> Result<Vec<ComposedSpan>, AnyError> {
        let (sender, receiver) = oneshot::channel();
        self.emit_query(EngineCommand::QuerySpan(query, sender))
            .await;
        receiver.await.context("failed to get result")
    }

    #[instrument(skip_all)]
    pub async fn query_span_count(&self, query: Query) -> Result<usize, AnyError> {
        let (sender, receiver) = oneshot::channel();
        self.emit_query(EngineCommand::QuerySpanCount(query, sender))
            .await;
        receiver.await.context("failed to get result")
    }

    #[doc(hidden)]
    #[instrument(skip_all)]
    pub async fn query_span_event(&self, query: Query) -> Result<Vec<SpanEvent>, AnyError> {
        let (sender, receiver) = oneshot::channel();
        self.emit_query(EngineCommand::QuerySpanEvent(query, sender))
            .await;
        receiver.await.context("failed to get result")
    }

    #[instrument(skip_all)]
    pub async fn query_event(&self, query: Query) -> Result<Vec<ComposedEvent>, AnyError> {
        let (sender, receiver) = oneshot::channel();
        self.emit_query(EngineCommand::QueryEvent(query, sender))
            .await;
        receiver.await.context("failed to get result")
    }

    #[instrument(skip_all)]
    pub async fn query_event_count(&self, query: Query) -> Result<usize, AnyError> {
        let (sender, receiver) = oneshot::channel();
        self.emit_query(EngineCommand::QueryEventCount(query, sender))
            .await;
        receiver.await.context("failed to get result")
    }

    #[instrument(skip_all)]
    pub async fn query_stats(&self) -> Result<DatasetStats, AnyError> {
        let (sender, receiver) = oneshot::channel();
        self.emit_query(EngineCommand::QueryStats(sender)).await;
        receiver.await.context("failed to get result")
    }

    #[instrument(skip_all)]
    #[allow(clippy::async_yields_async)]
    pub async fn insert_resource(
        &self,
        resource: NewResource,
    ) -> OneshotReceiver<Result<ResourceKey, AnyError>> {
        let (sender, receiver) = oneshot::channel();
        self.emit_insert(EngineCommand::InsertResource(resource, sender))
            .await;
        receiver
    }

    #[instrument(skip_all)]
    #[allow(clippy::async_yields_async)]
    pub async fn disconnect_tracing_instance(
        &self,
        id: InstanceId,
    ) -> OneshotReceiver<Result<(), AnyError>> {
        let (sender, receiver) = oneshot::channel();
        self.emit_insert(EngineCommand::DisconnectTracingInstance(id, sender))
            .await;
        receiver
    }

    #[instrument(skip_all)]
    #[allow(clippy::async_yields_async)]
    pub async fn insert_span_event(
        &self,
        span_event: NewSpanEvent,
    ) -> OneshotReceiver<Result<SpanKey, AnyError>> {
        let (sender, receiver) = oneshot::channel();
        self.emit_insert(EngineCommand::InsertSpanEvent(span_event, sender))
            .await;
        receiver
    }

    #[instrument(skip_all)]
    #[allow(clippy::async_yields_async)]
    pub async fn insert_event(&self, event: NewEvent) -> OneshotReceiver<Result<(), AnyError>> {
        let (sender, receiver) = oneshot::channel();
        self.emit_insert(EngineCommand::InsertEvent(event, sender))
            .await;
        receiver
    }

    #[instrument(skip_all)]
    pub async fn delete(&self, filter: DeleteFilter) -> Result<DeleteMetrics, AnyError> {
        let (sender, receiver) = oneshot::channel();
        self.emit_insert(EngineCommand::Delete(filter, sender))
            .await;
        receiver.await.context("failed to get result")?
    }

    #[instrument(skip_all)]
    pub async fn subscribe_to_spans(
        &self,
        filter: Vec<FilterPredicate>,
    ) -> Result<Subscriber<ComposedSpan>, AnyError> {
        let (sender, receiver) = oneshot::channel();
        self.emit_query(EngineCommand::SpanSubscribe(filter, sender))
            .await;
        receiver.await.context("failed to get result")?
    }

    #[instrument(skip_all)]
    pub async fn unsubscribe_from_spans(&self, id: SubscriptionId) -> Result<(), AnyError> {
        let (sender, receiver) = oneshot::channel();
        self.emit_query(EngineCommand::SpanUnsubscribe(id, sender))
            .await;
        receiver.await.context("failed to get result")
    }

    #[instrument(skip_all)]
    pub async fn subscribe_to_events(
        &self,
        filter: Vec<FilterPredicate>,
    ) -> Result<Subscriber<ComposedEvent>, AnyError> {
        let (sender, receiver) = oneshot::channel();
        self.emit_query(EngineCommand::EventSubscribe(filter, sender))
            .await;
        receiver.await.context("failed to get result")?
    }

    #[instrument(skip_all)]
    pub async fn unsubscribe_from_events(&self, id: SubscriptionId) -> Result<(), AnyError> {
        let (sender, receiver) = oneshot::channel();
        self.emit_query(EngineCommand::EventUnsubscribe(id, sender))
            .await;
        receiver.await.context("failed to get result")
    }

    #[instrument(skip_all)]
    pub async fn copy_dataset(&self, to: Box<dyn Storage + Send>) -> Result<(), AnyError> {
        let (sender, receiver) = oneshot::channel();
        self.emit_query(EngineCommand::CopyDataset(to, sender))
            .await;
        receiver.await.context("failed to get result")?
    }

    #[instrument(skip_all)]
    pub async fn get_status(&self) -> Result<EngineStatus, AnyError> {
        let (sender, receiver) = oneshot::channel();
        self.emit_query(EngineCommand::GetStatus(sender)).await;
        receiver.await.context("failed to get result")
    }

    #[instrument(skip_all)]
    pub async fn sync(&self) -> Result<(), AnyError> {
        self.sync_sender
            .send((tracing::Span::current(), EngineCommand::Sync))
            .await
            .map_err(|err| anyhow!("{err}"))
            .context("failed to send sync command, engine must have stopped")?;

        Ok(())
    }

    #[instrument(skip_all)]
    pub async fn shutdown(&self) -> Result<(), AnyError> {
        let (sender, receiver) = oneshot::channel();
        self.emit_insert(EngineCommand::Shutdown(sender)).await;
        receiver.await.context("failed to get result")?
    }

    async fn emit_query(&self, command: EngineCommand) {
        // ignore errors, any issue sending the commend will resurface an error
        // when the command's receiver awaits the response
        let _ = self
            .query_sender
            .send((tracing::Span::current(), command))
            .await;
    }

    async fn emit_insert(&self, command: EngineCommand) {
        // ignore errors, any issue sending the commend will resurface an error
        // when the command's receiver awaits the response
        let _ = self
            .insert_sender
            .send((tracing::Span::current(), command))
            .await;
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

    Sync,

    Shutdown(OneshotSender<Result<(), AnyError>>),
}
