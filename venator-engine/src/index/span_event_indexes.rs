use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::models::Timestamp;
use crate::util::{BoundSearch, IndexExt};
use crate::{SpanEvent, SpanEventKey, SpanKey};

#[derive(Serialize, Deserialize)]
pub(crate) struct SpanEventIndexes {
    pub all: Vec<Timestamp>,
    pub spans: HashMap<SpanKey, Vec<Timestamp>>,
}

impl SpanEventIndexes {
    pub fn new() -> SpanEventIndexes {
        SpanEventIndexes {
            all: Vec::new(),
            spans: HashMap::new(),
        }
    }

    pub fn update_with_new_span_event(&mut self, span_event: &SpanEvent) {
        let timestamp = span_event.timestamp;

        let idx = self.all.upper_bound_via_expansion(&timestamp);
        self.all.insert(idx, timestamp);

        let span_index = self.spans.entry(span_event.span_key).or_default();
        let idx = span_index.upper_bound_via_expansion(&timestamp);
        span_index.insert(idx, timestamp);
    }

    pub fn remove_span_events(&mut self, span_events: &[SpanEventKey]) {
        self.all.remove_list_sorted(span_events);

        for span_index in self.spans.values_mut() {
            span_index.remove_list_sorted(span_events);
        }
    }

    pub fn remove_spans(&mut self, spans: &[SpanKey]) {
        for span_key in spans {
            self.spans.remove(span_key);
        }
    }
}
