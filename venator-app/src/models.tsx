import { Span, Timestamp, Event, FullSpanId, Level } from "./invoke";

export type ScreenKind = "events" | "spans" | "trace";

export type Counts = [number, number, number, number, number, number];
export type Timespan = [Timestamp, Timestamp];

export type PartialFilter = {
    order: 'asc' | 'desc';
    limit?: number;
    start: Timestamp | null;
    end: Timestamp | null;
    previous?: Timestamp;
};

export type PartialEventCountFilter = {
    start: Timestamp;
    end: Timestamp;
};

export type PaginationFilter = {
    order: 'asc' | 'desc';
    limit?: number;
    previous?: Timestamp;
};

export type Entry = Event | Span;

export type PositionedSpan = {
    id: FullSpanId,
    created_at: Timestamp,
    closed_at: Timestamp | null,
    level: Level,
    slot: number,
};

export type PositionedConnection = {
    id: FullSpanId,
    connected_at: Timestamp,
    disconnected_at: Timestamp | null,
    slot: number,
};
