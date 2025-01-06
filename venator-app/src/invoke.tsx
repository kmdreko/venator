import { Channel, invoke } from "@tauri-apps/api/core";

export type Timestamp = number;
export type InstanceId = string;
export type FullSpanId = string;
export type TraceRoot = string;
export type Level = 0 | 1 | 2 | 3 | 4 | 5;
export type SourceKind = 'tracing' | 'opentelemetry';

export type Stats = {
    start?: Timestamp;
    end?: Timestamp;
    total_spans: number;
    total_events: number;
};

export type Comparator = 'Gt' | 'Gte' | 'Eq' | 'Lt' | 'Lte';

export type ValuePredicate =
    { value_kind: 'not', value: ValuePredicate } |
    { value_kind: 'comparison', value: [Comparator, string] } |
    { value_kind: 'and', value: ValuePredicate[] } |
    { value_kind: 'or', value: ValuePredicate[] };

export type Input = (ValidFilterPredicate | InvalidFilterPredicate) & { editable?: false };

export type ValidFilterPredicate = { input: 'valid' } & FilterPredicate;

export type InvalidFilterPredicate = { input: 'invalid', text: string, error: string };

export type FilterPredicate =
    { predicate_kind: 'single', predicate: FilterPredicateSingle } |
    { predicate_kind: 'and', predicate: Input[] } |
    { predicate_kind: 'or', predicate: Input[] };

export type FilterPredicateSingle = {
    text: string,
    property_kind?: string,
    property: string,
} & ValuePredicate;

export type EventFilter = {
    filter: FilterPredicate[];
    order: 'asc' | 'desc';
    limit?: number;
    start: Timestamp | null;
    end: Timestamp | null;
    previous?: Timestamp;
};

export type CountFilter = {
    filter: FilterPredicate[];
    start: Timestamp;
    end: Timestamp;
};

export type Event = {
    kind: SourceKind,
    ancestors: Ancestor[];
    timestamp: Timestamp;
    content: string;
    namespace: string | null;
    function: string | null;
    level: Level;
    file?: string;
    attributes: Attribute[];
};

export type SpanFilter = {
    filter: FilterPredicate[];
    order: 'asc' | 'desc';
    limit?: number;
    start: Timestamp | null;
    end: Timestamp | null;
    previous?: Timestamp;
};

export type Span = {
    kind: SourceKind,
    id: FullSpanId,
    ancestors: Ancestor[];
    created_at: Timestamp;
    closed_at: Timestamp | null;
    busy: number | null;
    name: string;
    namespace: string | null;
    function: string | null;
    level: Level;
    file?: string;
    attributes: Attribute[];
};

export type Ancestor = {
    id: FullSpanId,
    name: string,
};

export type Attribute = {
    name: string;
    value: string;
    type: 'f64' | 'i64' | 'u64' | 'i128' | 'u128' | 'bool' | 'string';
} & ({ source: 'resource' }
    | { source: 'span', span_id: FullSpanId }
    | { source: 'inherent' });

export type AppStatus = {
    ingress_message: string;
    ingress_error: string;
    ingress_connections: number;
    ingress_bytes_per_second: number;
    dataset_name: string;
    engine_load: number;
};

export type DeleteMetrics = {
    spans: number;
    span_events: number;
    events: number;
};

export type Session = {
    tabs: SessionTab[];
};

export type SessionTab = {
    kind: 'events' | 'spans' | 'trace';
    start: Timestamp;
    end: Timestamp;
    filter: string;
    columns: string[];
};

export type SubscriptionResponse<T> = {
    kind: 'add';
    entity: T;
} | {
    kind: 'remove';
    entity: Timestamp;
};

export async function getStats(): Promise<Stats> {
    console.debug("invoking 'get_stats'");
    return await invoke<Stats>("get_stats", {});
}

export async function getEvents(filter: EventFilter): Promise<Event[]> {
    console.debug("invoking 'get_events'");
    return await invoke<Event[]>("get_events", filter);
}

export async function getEventCount(filter: CountFilter): Promise<number> {
    console.debug("invoking 'get_event_count'");
    return await invoke<number>("get_event_count", filter);
}

export async function parseEventFilter(filter: string): Promise<Input[]> {
    console.debug("invoking 'parse_event_filter'");
    return await invoke<Input[]>("parse_event_filter", { filter });
}

export async function getSpans(filter: SpanFilter): Promise<Span[]> {
    console.debug("invoking 'get_spans'");
    return await invoke<Span[]>("get_spans", filter);
}

export async function getSpanCount(filter: CountFilter): Promise<number> {
    console.debug("invoking 'get_span_count'");
    return await invoke<number>("get_span_count", filter);
}

export async function parseSpanFilter(filter: string): Promise<Input[]> {
    console.debug("invoking 'parse_span_filter'");
    return await invoke<Input[]>("parse_span_filter", { filter });
}

export async function deleteEntities(start: Timestamp | null, end: Timestamp | null, inside: boolean, dryRun: boolean): Promise<DeleteMetrics> {
    console.debug("invoking 'delete_entities'");
    return await invoke<DeleteMetrics>("delete_entities", { start, end, inside, dryRun });
}

export async function subscribeToSpans(filter: FilterPredicate[], channel: Channel<SubscriptionResponse<Span>>): Promise<number> {
    console.debug("invoking 'subscribe_to_spans'");
    return await invoke<number>("subscribe_to_spans", { filter, channel });
}

export async function unsubscribeFromSpans(id: number): Promise<number> {
    console.debug("invoking 'unsubscribe_from_spans'");
    return await invoke<number>("unsubscribe_from_spans", { id });
}

export async function subscribeToEvents(filter: FilterPredicate[], channel: Channel<SubscriptionResponse<Event>>): Promise<number> {
    console.debug("invoking 'subscribe_to_events'");
    return await invoke<number>("subscribe_to_events", { filter, channel });
}

export async function unsubscribeFromEvents(id: number): Promise<number> {
    console.debug("invoking 'unsubscribe_from_events'");
    return await invoke<number>("unsubscribe_from_events", { id });
}

export async function createAttributeIndex(name: string): Promise<void> {
    console.debug("invoking 'create_attribute_index'");
    return await invoke<void>("create_attribute_index", { name });
}

export async function removeAttributeIndex(name: string): Promise<void> {
    console.debug("invoking 'remove_attribute_index'");
    return await invoke<void>("remove_attribute_index", { name });
}

export async function getStatus(): Promise<AppStatus> {
    console.debug("invoking 'get_status'");
    return await invoke<AppStatus>("get_status");
}

export async function loadSession(): Promise<Session> {
    console.debug("invoking 'load_session'");
    return await invoke<Session>("load_session");
}

export async function saveSession(session: Session): Promise<void> {
    console.debug("invoking 'save_session'");
    return await invoke<void>("save_session", { session });
}
