use tokio::sync::mpsc::UnboundedSender;

use crate::filter::BoundSearch;
use crate::models::EventKey;
use crate::{
    BasicEventFilter, BasicSpanFilter, EventContext, EventView, SpanContext, SpanKey, SpanView,
    Storage, Timestamp,
};

pub type SubscriptionId = usize;

pub enum SubscriptionResponse<T> {
    Add(T),
    Remove(Timestamp),
}

pub(crate) struct EventSubscription {
    filter: BasicEventFilter,
    sender: UnboundedSender<SubscriptionResponse<EventView>>,
    cache: Vec<EventKey>,
}

impl EventSubscription {
    pub(crate) fn new(
        filter: BasicEventFilter,
        sender: UnboundedSender<SubscriptionResponse<EventView>>,
    ) -> EventSubscription {
        EventSubscription {
            filter,
            sender,
            cache: Vec::new(),
        }
    }

    pub(crate) fn connected(&self) -> bool {
        !self.sender.is_closed()
    }

    /// This should be called when an event is created or was impacted by a
    /// change in a parent span.
    pub(crate) fn on_event<S: Storage>(&mut self, event: &EventContext<'_, S>) {
        if self.filter.matches(event) {
            let idx = self.cache.upper_bound_via_expansion(&event.key());
            if idx == 0 || self.cache[idx - 1] != event.key() {
                // the event was not visible by this subscription before
                self.cache.insert(idx, event.key());
            }

            // we emit an event regardless since we want the subscriber to have
            // fresh data
            let _ = self.sender.send(SubscriptionResponse::Add(event.render()));
        } else {
            let idx = self.cache.upper_bound_via_expansion(&event.key());
            if idx != 0 && self.cache[idx - 1] == event.key() {
                // the event was visible by this subscription before
                self.cache.remove(idx - 1);
                let _ = self.sender.send(SubscriptionResponse::Remove(event.key()));
            }

            // NOTE: There is wiggle room for error here if the subscriber pre-
            // loads an event before subscribing but after subscribing a parent
            // span update means the event shouldn't be shown. This code would
            // not emit a "remove" event.
            //
            // However, the likleyhood of that happening is low since only some
            // filters are even susceptible to the possibility (have negation)
            // and the window for opportunity is often short-lived.
        }
    }
}

pub(crate) struct SpanSubscription {
    filter: BasicSpanFilter,
    sender: UnboundedSender<SubscriptionResponse<SpanView>>,
    cache: Vec<SpanKey>,
}

impl SpanSubscription {
    pub(crate) fn new(
        filter: BasicSpanFilter,
        sender: UnboundedSender<SubscriptionResponse<SpanView>>,
    ) -> SpanSubscription {
        SpanSubscription {
            filter,
            sender,
            cache: Vec::new(),
        }
    }

    pub(crate) fn connected(&self) -> bool {
        !self.sender.is_closed()
    }

    /// This should be called when a span is created or was impacted by a change
    /// in a parent span.
    pub(crate) fn on_span<S: Storage>(&mut self, span: &SpanContext<'_, S>) {
        if self.filter.matches(span) {
            let idx = self.cache.upper_bound_via_expansion(&span.key());
            if idx == 0 || self.cache[idx - 1] != span.key() {
                // the span was not visible by this subscription before
                self.cache.insert(idx, span.key());
            }

            // we emit an span regardless since we want the subscriber to have
            // fresh data
            let _ = self.sender.send(SubscriptionResponse::Add(span.render()));
        } else {
            let idx = self.cache.upper_bound_via_expansion(&span.key());
            if idx != 0 && self.cache[idx - 1] == span.key() {
                // the span was visible by this subscription before
                self.cache.remove(idx - 1);
                let _ = self.sender.send(SubscriptionResponse::Remove(span.key()));
            }

            // NOTE: There is wiggle room for error here if the subscriber pre-
            // loads an span before subscribing but after subscribing a parent
            // span update means the span shouldn't be shown. This code would
            // not emit a "remove" event.
            //
            // However, the likleyhood of that happening is low since only some
            // filters are even susceptible to the possibility (have negation)
            // and the window for opportunity is often short-lived.
        }
    }
}
