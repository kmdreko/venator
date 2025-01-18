use std::collections::{BTreeMap, HashMap};

use anyhow::{anyhow, Context, Error as AnyError};
use tokio::sync::mpsc::{self};
use tracing::instrument;

use crate::context::{EventContext, SpanContext};
use crate::filter::{
    BasicEventFilter, BasicSpanFilter, BoundSearch, FilterPredicate, IndexedEventFilter,
    IndexedEventFilterIterator, IndexedSpanFilter, IndexedSpanFilterIterator, Query,
};
use crate::index::{EventIndexes, SpanEventIndexes, SpanIndexes};
use crate::models::{CloseSpanEvent, EnterSpanEvent, EventKey, FollowsSpanEvent};
use crate::storage::Storage;
use crate::subscription::{EventSubscription, SpanSubscription, Subscriber};
use crate::{
    ComposedEvent, ComposedSpan, CreateSpanEvent, DatasetStats, DeleteFilter, DeleteMetrics, Event,
    FullSpanId, InstanceId, NewEvent, NewResource, NewSpanEvent, NewSpanEventKind, Resource,
    ResourceKey, Span, SpanEvent, SpanEventKey, SpanEventKind, SpanKey, SubscriptionId, Timestamp,
    UpdateSpanEvent, ValueOperator,
};

/// Provides the core engine functionality.
pub struct SyncEngine<S> {
    pub(crate) storage: S,
    pub(crate) span_indexes: SpanIndexes,
    pub(crate) span_event_indexes: SpanEventIndexes,
    pub(crate) event_indexes: EventIndexes,

    resources: HashMap<ResourceKey, Resource>,

    next_subscriber_id: usize,
    span_subscribers: HashMap<usize, SpanSubscription>,
    event_subscribers: HashMap<usize, EventSubscription>,
}

impl<S: Storage> SyncEngine<S> {
    pub fn new(storage: S) -> Result<SyncEngine<S>, AnyError> {
        let mut engine = SyncEngine {
            storage,
            span_indexes: SpanIndexes::new(),
            span_event_indexes: SpanEventIndexes::new(),
            event_indexes: EventIndexes::new(),

            resources: HashMap::new(),

            next_subscriber_id: 0,
            span_subscribers: HashMap::new(),
            event_subscribers: HashMap::new(),
        };

        tracing::info!("initializing engine");

        let resources = engine
            .storage
            .get_all_resources()
            .context("failed to load resources")?
            .collect::<Vec<_>>();

        for resource in resources {
            let resource = resource.context("failed to load resource")?;
            engine.insert_resource_bookeeping(&resource);
        }

        let indexes_result = engine
            .storage
            .as_index_storage()
            .and_then(|s| s.get_indexes().transpose());

        match indexes_result {
            Some(Ok(indexes)) => {
                tracing::debug!("loaded indexes from storage");

                let (span_indexes, span_event_indexes, event_indexes) = indexes;

                engine.span_indexes = span_indexes;
                engine.span_event_indexes = span_event_indexes;
                engine.event_indexes = event_indexes;
            }
            Some(Err(err)) => {
                tracing::debug!(?err, "warn failed to load indexes from storage");

                let spans = engine
                    .storage
                    .get_all_spans()
                    .context("failed to load spans")?
                    .collect::<Vec<_>>();

                for span in spans {
                    let span = span.context("failed to load span")?;
                    engine.insert_span_bookeeping(&span);
                }

                let span_events = engine
                    .storage
                    .get_all_span_events()
                    .context("failed to load span events")?
                    .collect::<Vec<_>>();

                for span_event in span_events {
                    let span_event = span_event.context("failed to load span event")?;
                    engine.insert_span_event_bookeeping(&span_event);
                }

                let events = engine
                    .storage
                    .get_all_events()
                    .context("failed to load events")?
                    .collect::<Vec<_>>();

                for event in events {
                    let event = event.context("failed to load event")?;
                    engine.insert_event_bookeeping(&event);
                }
            }
            None => {
                tracing::debug!("no indexes from storage");

                let spans = engine
                    .storage
                    .get_all_spans()
                    .context("failed to load spans")?
                    .collect::<Vec<_>>();

                for span in spans {
                    let span = span.context("failed to load span")?;
                    engine.insert_span_bookeeping(&span);
                }

                let span_events = engine
                    .storage
                    .get_all_span_events()
                    .context("failed to load span events")?
                    .collect::<Vec<_>>();

                for span_event in span_events {
                    let span_event = span_event.context("failed to load span event")?;
                    engine.insert_span_event_bookeeping(&span_event);
                }

                let events = engine
                    .storage
                    .get_all_events()
                    .context("failed to load events")?
                    .collect::<Vec<_>>();

                for event in events {
                    let event = event.context("failed to load event")?;
                    engine.insert_event_bookeeping(&event);
                }
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
                engine
                    .storage
                    .update_span_closed(span_key, at, None)
                    .context("failed to close span")?;
            }
        }

        tracing::info!("loaded {} spans", engine.span_indexes.all.len());
        tracing::info!("loaded {} span events", engine.span_event_indexes.all.len());
        tracing::info!("loaded {} events", engine.event_indexes.all.len());

        Ok(engine)
    }

    #[instrument(level = tracing::Level::TRACE, skip_all)]
    pub fn query_event(&self, query: Query) -> Vec<ComposedEvent> {
        tracing::debug!(?query, "querying for events");

        let limit = query.limit;
        IndexedEventFilterIterator::new(query, self)
            .take(limit)
            .filter_map(|event_key| {
                self.storage
                    .get_event(event_key)
                    .inspect_err(|err| tracing::warn!(?err, "failed to load event"))
                    .ok()
            })
            .map(|event| EventContext::with_event(&event, &self.storage).render())
            .collect()
    }

    #[instrument(level = tracing::Level::TRACE, skip_all)]
    pub fn query_event_count(&self, query: Query) -> usize {
        tracing::debug!(?query, "querying for event counts");

        let event_iter = IndexedEventFilterIterator::new(query, self);

        match event_iter.size_hint() {
            (min, Some(max)) if min == max => min,
            _ => event_iter.count(),
        }
    }

    #[instrument(level = tracing::Level::TRACE, skip_all)]
    pub fn query_span(&self, query: Query) -> Vec<ComposedSpan> {
        tracing::debug!(?query, "querying for spans");

        let limit = query.limit;
        IndexedSpanFilterIterator::new(query, self)
            .take(limit)
            .filter_map(|span_key| {
                self.storage
                    .get_span(span_key)
                    .inspect_err(|err| tracing::warn!(?err, "failed to load span"))
                    .ok()
            })
            .map(|span| SpanContext::with_span(&span, &self.storage).render())
            .collect()
    }

    #[instrument(level = tracing::Level::TRACE, skip_all)]
    pub fn query_span_count(&self, query: Query) -> usize {
        tracing::debug!(?query, "querying for span counts");

        let span_iter = IndexedSpanFilterIterator::new(query, self);

        match span_iter.size_hint() {
            (min, Some(max)) if min == max => min,
            _ => span_iter.count(),
        }
    }

    #[instrument(level = tracing::Level::TRACE, skip_all)]
    #[doc(hidden)]
    pub fn query_span_event(&self, _query: Query) -> Vec<SpanEvent> {
        unimplemented!()
    }

    #[instrument(level = tracing::Level::TRACE, skip_all)]
    pub fn query_stats(&self) -> DatasetStats {
        tracing::debug!("querying for stats");

        let event_start = self.event_indexes.all.first().copied();
        let event_end = self.event_indexes.all.last().copied();
        let span_start = self.span_indexes.all.first().copied();
        let span_end = self.span_indexes.all.last().copied(); // TODO: not technically right, but maybe okay

        DatasetStats {
            start: crate::filter::merge(event_start, span_start, Ord::min),
            end: crate::filter::merge(event_end, span_end, Ord::max),
            total_events: self.event_indexes.all.len(),
            total_spans: self.span_indexes.all.len(),
        }
    }

    #[instrument(level = tracing::Level::TRACE, skip_all)]
    pub fn insert_resource(&mut self, resource: NewResource) -> Result<ResourceKey, AnyError> {
        tracing::debug!(?resource, "inserting resource");

        if let Some((key, _)) = self
            .resources
            .iter()
            .find(|(_, r)| r.attributes == resource.attributes)
        {
            return Ok(*key);
        }

        let keys = self.resources.keys().copied().collect::<Vec<_>>();
        let now = now();
        let resource_key = get_unique_timestamp(now, &keys);
        let resource = Resource {
            created_at: resource_key,
            attributes: resource.attributes,
        };

        self.insert_resource_bookeeping(&resource);
        self.storage
            .insert_resource(resource)
            .context("failed to insert resource")?;

        Ok(resource_key)
    }

    fn insert_resource_bookeeping(&mut self, resource: &Resource) {
        self.resources.insert(resource.key(), resource.clone());
    }

    #[instrument(level = tracing::Level::TRACE, skip_all)]
    pub fn disconnect_tracing_instance(&mut self, instance_id: InstanceId) -> Result<(), AnyError> {
        tracing::debug!(instance_id, "disconnecting tracing instance");

        let at = now();

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
            self.storage
                .update_span_closed(span_key, at, None)
                .context("failed to close span")?;
        }

        Ok(())
    }

    #[instrument(level = tracing::Level::TRACE, skip_all)]
    pub fn insert_span_event(
        &mut self,
        mut new_span_event: NewSpanEvent,
    ) -> Result<SpanEventKey, AnyError> {
        tracing::debug!(span_event = ?new_span_event, "inserting span event");

        let span_event_key =
            get_unique_timestamp(new_span_event.timestamp, &self.span_event_indexes.all);
        new_span_event.timestamp = span_event_key;

        match new_span_event.kind {
            NewSpanEventKind::Create(new_create_event) => {
                if self.span_indexes.ids.contains_key(&new_span_event.span_id) {
                    return Err(anyhow::anyhow!("duplicate span id"));
                }

                // parent may not yet exist, that is ok
                let parent_id = new_create_event.parent_id;
                let parent_key = parent_id.and_then(|id| self.span_indexes.ids.get(&id).copied());

                let created_at =
                    get_unique_timestamp(new_span_event.timestamp, &self.span_indexes.all);

                let span = Span {
                    kind: new_create_event.kind,
                    resource_key: new_create_event.resource_key,
                    id: new_span_event.span_id,
                    created_at,
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
                    instrumentation_attributes: new_create_event.instrumentation_attributes.clone(),
                    attributes: new_create_event.attributes.clone(),
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
                        instrumentation_attributes: new_create_event.instrumentation_attributes,
                        attributes: new_create_event.attributes,
                    }),
                };

                let (child_spans, child_events) = self.insert_span_bookeeping(&span);
                self.storage
                    .insert_span(span.clone())
                    .context("failed to insert span")?;
                self.storage
                    .update_span_parents(span.key(), &child_spans)
                    .context("failed to update span parents")?;
                self.storage
                    .update_event_parents(span.key(), &child_events)
                    .context("failed to update event parents")?;

                self.insert_span_event_bookeeping(&span_event);
                self.storage
                    .insert_span_event(span_event)
                    .context("failed to insert span event")?;

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

                    // update subscribers for events that may have been updated
                    // by a new parent
                    for event_key in descendent_events {
                        let context = EventContext::new(event_key, &self.storage);
                        for subscriber in self.event_subscribers.values_mut() {
                            subscriber.on_event(&context);
                        }
                    }

                    self.event_subscribers.retain(|_, s| s.connected());
                }

                if !self.span_subscribers.is_empty() {
                    for subscriber in self.span_subscribers.values_mut() {
                        subscriber.on_span(&SpanContext::with_span(&span, &self.storage));
                    }

                    let root = SpanContext::with_span(&span, &self.storage).trace_root();
                    let descendent_spans = self
                        .span_indexes
                        .traces
                        .get(&root)
                        .map(Vec::as_slice)
                        .unwrap_or_default()
                        .iter()
                        .copied()
                        .filter(|key| {
                            SpanContext::new(*key, &self.storage)
                                .parents()
                                .any(|p| p.key() == span.key())
                        });

                    // update subscribers for spans that may have been updated
                    // by a new parent
                    for span_key in descendent_spans {
                        let context = SpanContext::new(span_key, &self.storage);
                        for subscriber in self.span_subscribers.values_mut() {
                            subscriber.on_span(&context);
                        }
                    }

                    self.span_subscribers.retain(|_, s| s.connected());
                }
            }
            NewSpanEventKind::Update(new_update_event) => {
                let span_key = self
                    .span_indexes
                    .ids
                    .get(&new_span_event.span_id)
                    .copied()
                    .ok_or(anyhow!("unknown span id"))?;

                let span = SpanContext::new(span_key, &self.storage);
                let trace = span.trace_root();

                let update_event = UpdateSpanEvent {
                    attributes: new_update_event.attributes.clone(),
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
                        &update_event.attributes,
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
                        &update_event.attributes,
                    );
                }

                let span_event = SpanEvent {
                    timestamp: new_span_event.timestamp,
                    span_key,
                    kind: SpanEventKind::Update(update_event),
                };

                self.storage
                    .update_span_attributes(span_key, new_update_event.attributes)
                    .context("failed to update span attributes")?;

                self.insert_span_event_bookeeping(&span_event);
                self.storage
                    .insert_span_event(span_event)
                    .context("failed to insert span event")?;

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

                    // update subscribers for events that may have been updated
                    // by an updated parent
                    for event_key in descendent_events {
                        let context = EventContext::new(event_key, &self.storage);
                        for subscriber in self.event_subscribers.values_mut() {
                            subscriber.on_event(&context);
                        }
                    }

                    self.event_subscribers.retain(|_, s| s.connected());
                }

                if !self.span_subscribers.is_empty() {
                    let descendent_spans = self
                        .span_indexes
                        .traces
                        .get(&trace)
                        .map(Vec::as_slice)
                        .unwrap_or_default()
                        .iter()
                        .copied()
                        .filter(|key| {
                            SpanContext::new(*key, &self.storage)
                                .parents()
                                .any(|p| p.key() == span_key)
                        });

                    // update subscribers for spans that may have been updated
                    // by an updated parent
                    for span_key in descendent_spans {
                        let context = SpanContext::new(span_key, &self.storage);
                        for subscriber in self.span_subscribers.values_mut() {
                            subscriber.on_span(&context);
                        }
                    }

                    self.span_subscribers.retain(|_, s| s.connected());
                }
            }
            NewSpanEventKind::Follows(new_follows_event) => {
                let span_key = self
                    .span_indexes
                    .ids
                    .get(&new_span_event.span_id)
                    .copied()
                    .ok_or(anyhow!("unknown span id"))?;

                let FullSpanId::Tracing(instance_id, _) = new_span_event.span_id else {
                    return Err(anyhow!("invalid span kind, expected tracing"));
                };

                let follows_span_id = FullSpanId::Tracing(instance_id, new_follows_event.follows);
                let follows_span_key = self
                    .span_indexes
                    .ids
                    .get(&follows_span_id)
                    .copied()
                    .ok_or(anyhow!("unknown span id"))?;

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
                    .update_span_link(span_key, follows_span_id, BTreeMap::new())
                    .context("failed to update span link")?;

                self.insert_span_event_bookeeping(&span_event);
                self.storage
                    .insert_span_event(span_event)
                    .context("failed to insert span event")?;
            }
            NewSpanEventKind::Enter(new_enter_event) => {
                let span_key = self
                    .span_indexes
                    .ids
                    .get(&new_span_event.span_id)
                    .copied()
                    .ok_or(anyhow!("unknown span id"))?;

                let span_event = SpanEvent {
                    timestamp: new_span_event.timestamp,
                    span_key,
                    kind: SpanEventKind::Enter(EnterSpanEvent {
                        thread_id: new_enter_event.thread_id,
                    }),
                };

                self.insert_span_event_bookeeping(&span_event);
                self.storage
                    .insert_span_event(span_event)
                    .context("failed to insert span event")?;
            }
            NewSpanEventKind::Exit => {
                let span_key = self
                    .span_indexes
                    .ids
                    .get(&new_span_event.span_id)
                    .copied()
                    .ok_or(anyhow!("unknown span id"))?;

                let span_event = SpanEvent {
                    timestamp: new_span_event.timestamp,
                    span_key,
                    kind: SpanEventKind::Exit,
                };

                self.insert_span_event_bookeeping(&span_event);
                self.storage
                    .insert_span_event(span_event)
                    .context("failed to insert span event")?;
            }
            NewSpanEventKind::Close(new_close_event) => {
                let span_key = self
                    .span_indexes
                    .ids
                    .get(&new_span_event.span_id)
                    .copied()
                    .ok_or(anyhow!("unknown span id"))?;

                let busy = if let Some(busy) = new_close_event.busy {
                    Some(busy)
                } else {
                    let mut busy = 0;
                    let mut last_enter = None;
                    for span_event_key in &self.span_event_indexes.spans[&span_key] {
                        let Ok(span_event) = self.storage.get_span_event(*span_event_key) else {
                            tracing::warn!("failed to get span event, ignoring");
                            continue;
                        };
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
                    .update_span_closed(span_key, new_span_event.timestamp, busy)
                    .context("failed to close span")?;

                self.insert_span_event_bookeeping(&span_event);
                self.storage
                    .insert_span_event(span_event)
                    .context("failed to insert span event")?;
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
                &span.attributes,
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
                &span.attributes,
            );
        }

        (spans_to_update_parent, events_to_update_parent)
    }

    fn insert_span_event_bookeeping(&mut self, span_event: &SpanEvent) {
        self.span_event_indexes
            .update_with_new_span_event(span_event);
    }

    #[instrument(level = tracing::Level::TRACE, skip_all)]
    pub fn insert_event(&mut self, mut new_event: NewEvent) -> Result<(), AnyError> {
        let event_key = get_unique_timestamp(new_event.timestamp, &self.event_indexes.all);
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
            attributes: new_event.attributes,
        };

        self.insert_event_bookeeping(&event);
        self.storage
            .insert_event(event.clone())
            .context("failed to insert event")?;

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

    #[instrument(level = tracing::Level::TRACE, skip_all)]
    pub fn delete(&mut self, filter: DeleteFilter) -> Result<DeleteMetrics, AnyError> {
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
            return Ok(DeleteMetrics {
                spans: spans_from_root_spans.len(),
                span_events: span_events.len(),
                events: root_events.len() + events_from_root_spans.len(),
            });
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

        self.storage
            .drop_events(&events_to_delete)
            .context("failed to drop events")?;
        self.storage
            .drop_span_events(&span_events_to_delete)
            .context("failed to drop span events")?;
        self.storage
            .drop_spans(&spans_to_delete)
            .context("failed to drop spans")?;

        // remove smaller scoped entities from indexes last for some efficiency

        self.remove_spans_bookeeping(&spans_to_delete);
        self.remove_span_events_bookeeping(&span_events_to_delete);
        self.remove_events_bookeeping(&events_to_delete);

        Ok(DeleteMetrics {
            spans: spans_to_delete.len(),
            span_events: span_events_to_delete.len(),
            events: events_to_delete.len(),
        })
    }

    fn get_root_spans_in_range_filter(
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

    fn get_root_events_in_range_filter(
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

    #[instrument(level = tracing::Level::TRACE, skip_all)]
    pub fn copy_dataset(
        &self,
        mut target_storage: Box<dyn Storage + Send>,
    ) -> Result<(), AnyError> {
        let resources = self
            .storage
            .get_all_resources()
            .context("failed to get resources")?
            .collect::<Vec<_>>();

        for resource in resources {
            let resource = resource.context("failed to get resource")?;
            target_storage
                .insert_resource((*resource).clone())
                .context("failed to insert resource")?;
        }

        let spans = self
            .storage
            .get_all_spans()
            .context("failed to get spans")?
            .collect::<Vec<_>>();

        for span in spans {
            let span = span.context("failed to get span")?;
            target_storage
                .insert_span((*span).clone())
                .context("failed to insert span")?;
        }

        let span_events = self
            .storage
            .get_all_span_events()
            .context("failed to get span events")?
            .collect::<Vec<_>>();

        for span_event in span_events {
            let span_event = span_event.context("failed to get span event")?;
            target_storage
                .insert_span_event((*span_event).clone())
                .context("failed to insert span event")?;
        }

        let events = self
            .storage
            .get_all_events()
            .context("failed to get events")?
            .collect::<Vec<_>>();

        for event in events {
            let event = event.context("failed to get event")?;
            target_storage
                .insert_event((*event).clone())
                .context("failed to insert event")?;
        }

        Ok(())
    }

    #[instrument(level = tracing::Level::TRACE, skip_all)]
    pub fn subscribe_to_spans(
        &mut self,
        filter: Vec<FilterPredicate>,
    ) -> Result<Subscriber<ComposedSpan>, AnyError> {
        let mut filter = BasicSpanFilter::And(
            filter
                .into_iter()
                .map(|p| BasicSpanFilter::from_predicate(p, &self.span_indexes.ids))
                .collect::<Result<_, _>>()
                .context("invalid span filter")?,
        );
        filter.simplify();

        let id = self.next_subscriber_id;
        self.next_subscriber_id += 1;

        let (sender, receiver) = mpsc::unbounded_channel();

        self.span_subscribers
            .insert(id, SpanSubscription::new(filter, sender));

        Ok((id, receiver))
    }

    #[instrument(level = tracing::Level::TRACE, skip_all)]
    pub fn unsubscribe_from_spans(&mut self, id: SubscriptionId) {
        self.span_subscribers.remove(&id);
    }

    #[instrument(level = tracing::Level::TRACE, skip_all)]
    pub fn subscribe_to_events(
        &mut self,
        filter: Vec<FilterPredicate>,
    ) -> Result<Subscriber<ComposedEvent>, AnyError> {
        let mut filter = BasicEventFilter::And(
            filter
                .into_iter()
                .map(|p| BasicEventFilter::from_predicate(p, &self.span_indexes.ids))
                .collect::<Result<_, _>>()
                .context("invalid event filter")?,
        );
        filter.simplify();

        let id = self.next_subscriber_id;
        self.next_subscriber_id += 1;

        let (sender, receiver) = mpsc::unbounded_channel();

        self.event_subscribers
            .insert(id, EventSubscription::new(filter, sender));

        Ok((id, receiver))
    }

    #[instrument(level = tracing::Level::TRACE, skip_all)]
    pub fn unsubscribe_from_events(&mut self, id: SubscriptionId) {
        self.event_subscribers.remove(&id);
    }

    #[instrument(level = tracing::Level::TRACE, skip_all)]
    pub fn shutdown(&mut self) -> Result<(), AnyError> {
        if let Some(s) = self.storage.as_index_storage_mut() {
            s.update_indexes(
                &self.span_indexes,
                &self.span_event_indexes,
                &self.event_indexes,
            )
            .context("failed to update indexes")?;
        }

        Ok(())
    }
}

fn get_unique_timestamp(mut timestamp: Timestamp, existing: &[Timestamp]) -> Timestamp {
    let mut idx = existing.lower_bound(&timestamp);

    while idx < existing.len() && timestamp == existing[idx] {
        idx += 1;
        timestamp = timestamp.saturating_add(1);
    }

    timestamp
}

fn now() -> Timestamp {
    #[cfg(test)]
    return Timestamp::new(1000).unwrap();

    #[cfg(not(test))]
    {
        use std::time::{SystemTime, UNIX_EPOCH};
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("now should not be before the UNIX epoch")
            .as_micros();

        let timestamp = u64::try_from(timestamp)
            .expect("microseconds shouldn't exceed a u64 until the year 586,912 AD");

        Timestamp::new(timestamp).expect("now should not be at the UNIX epoch")
    }
}

#[cfg(test)]
mod tests {
    use crate::filter::Order;
    use crate::models::{
        Level, NewCloseSpanEvent, NewCreateSpanEvent, NewUpdateSpanEvent, SourceKind,
    };
    use crate::storage::TransientStorage;
    use crate::Value;

    use super::*;

    #[test]
    fn test_event_filters() {
        let mut engine = SyncEngine::new(TransientStorage::new()).unwrap();

        let resource_key = engine
            .insert_resource(NewResource {
                attributes: BTreeMap::new(),
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
                attributes: BTreeMap::from_iter([
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
        let mut engine = SyncEngine::new(TransientStorage::new()).unwrap();

        let resource_key = engine
            .insert_resource(NewResource {
                attributes: BTreeMap::new(),
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
                        instrumentation_attributes: BTreeMap::default(),
                        attributes: BTreeMap::from_iter([
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
        let mut engine = SyncEngine::new(TransientStorage::new()).unwrap();

        let resource_key = engine
            .insert_resource(NewResource {
                attributes: BTreeMap::from_iter([("attr1".to_owned(), Value::Str("A".to_owned()))]),
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
                attributes: BTreeMap::new(),
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
        let mut engine = SyncEngine::new(TransientStorage::new()).unwrap();

        let resource_key = engine
            .insert_resource(NewResource {
                attributes: BTreeMap::from_iter([("attr1".to_owned(), Value::Str("A".to_owned()))]),
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
                attributes: BTreeMap::from_iter([("attr1".to_owned(), Value::Str("B".to_owned()))]),
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
        let mut engine = SyncEngine::new(TransientStorage::new()).unwrap();

        let resource_key = engine
            .insert_resource(NewResource {
                attributes: BTreeMap::from_iter([("attr1".to_owned(), Value::Str("A".to_owned()))]),
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
                    instrumentation_attributes: BTreeMap::default(),
                    attributes: BTreeMap::from_iter([(
                        "attr1".to_owned(),
                        Value::Str("C".to_owned()),
                    )]),
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
                attributes: BTreeMap::new(),
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
        let mut engine = SyncEngine::new(TransientStorage::new()).unwrap();

        let resource_key = engine
            .insert_resource(NewResource {
                attributes: BTreeMap::from_iter([("attr1".to_owned(), Value::Str("A".to_owned()))]),
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
                    instrumentation_attributes: BTreeMap::default(),
                    attributes: BTreeMap::new(),
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
                attributes: BTreeMap::new(),
            })
            .unwrap();

        engine
            .insert_span_event(NewSpanEvent {
                timestamp: super::now(),
                span_id: FullSpanId::Tracing(1.try_into().unwrap(), 1),
                kind: NewSpanEventKind::Update(NewUpdateSpanEvent {
                    attributes: BTreeMap::from_iter([(
                        "attr1".to_owned(),
                        Value::Str("C".to_owned()),
                    )]),
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
        let mut engine = SyncEngine::new(TransientStorage::new()).unwrap();

        let resource_key = engine
            .insert_resource(NewResource {
                attributes: BTreeMap::from_iter([("attr1".to_owned(), Value::Str("A".to_owned()))]),
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
                    instrumentation_attributes: BTreeMap::default(),
                    attributes: BTreeMap::new(),
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
                attributes: BTreeMap::new(),
            })
            .unwrap();

        engine
            .insert_span_event(NewSpanEvent {
                timestamp: super::now(),
                span_id: FullSpanId::Tracing(1.try_into().unwrap(), 1),
                kind: NewSpanEventKind::Update(NewUpdateSpanEvent {
                    attributes: BTreeMap::from_iter([(
                        "attr1".to_owned(),
                        Value::Str("C".to_owned()),
                    )]),
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
}
