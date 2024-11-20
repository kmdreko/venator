import { Event, EventFilter, FilterPredicate, FullSpanId, getEventCount, getEvents, getConnections, getSpans, Input, Connection, ConnectionId, Span, SpanFilter, subscribeToEvents, Timestamp, unsubscribeFromEvents } from "../invoke";
import { Counts, PaginationFilter, PartialEventCountFilter, PartialFilter, PositionedConnection, PositionedSpan, Timespan } from "../models";
import { Channel } from "@tauri-apps/api/core";

type IsVolatile = boolean;

export class EventDataLayer {
    // the filter to use fetching events
    //
    // this is immutable, if the filter changes the cache should be re-created
    #filter: FilterPredicate[];

    // the timespan for which this datalayer contains events
    #range: Timespan;

    // the cached events, in 'asc' order
    #events: Event[];

    // the cached event counts by "{start}-{end}" and level
    #counts: { [range: string]: [Counts, IsVolatile] };

    #expandStartTask: Promise<void> | null;
    #expandEndTask: Promise<void> | null;

    #subscription: Promise<number> | null;

    constructor(filter: Input[]) {
        this.#filter = filter.filter(f => f.input == 'valid');
        this.#range = [0, 0];
        this.#events = [];
        this.#counts = {};
        this.#expandStartTask = null;
        this.#expandEndTask = null;
        this.#subscription = null;
    }

    subscribe = () => {
        if (this.#subscription != null) {
            return;
        }

        this.#subscription = (async () => {
            let channel = new Channel<Event>();
            channel.onmessage = e => this.#insertEvent(e);
            return await subscribeToEvents(this.#filter, channel);
        })();
    }

    unsubscribe = async () => {
        if (this.#subscription == null) {
            return;
        }

        let subscription = this.#subscription;
        this.#subscription = null;

        let id = await subscription;
        await unsubscribeFromEvents(id);
    }

    getEvents = async (filter: PartialFilter): Promise<Event[]> => {
        // don't try to cache unbounded shenanigans
        if (filter.start == null || filter.end == null) {
            return await getEvents({ filter: this.#filter, ...filter });
        }

        let start: number, end: number;
        if (filter.order == 'asc') {
            start = (filter.previous) ? filter.previous + 1 : filter.start;
            end = filter.end;
        } else {
            start = filter.start;
            end = filter.previous ? filter.previous - 1 : filter.end;
        }

        if (within(this.#range, start, end)) {
            // events are cached

            let startIndex = partitionPointEventsLower(this.#events, start);
            let endIndex = partitionPointEventsUpper(this.#events, end);

            if (filter.order == 'asc') {
                endIndex = startIndex + Math.min(endIndex - startIndex, 50);
            } else {
                startIndex = endIndex - Math.min(endIndex - startIndex, 50);
            }

            let cachedEvents = this.#events.slice(startIndex, endIndex);

            if (filter.order == 'desc') {
                cachedEvents.reverse();
            }

            if (this.#events.length - endIndex < 50 && end > this.#range[1] - (filter.end - filter.start)) {
                this.#expandEnd(filter.end - filter.start);
            }
            if (startIndex < 50 && start < this.#range[0] + (filter.end - filter.start)) {
                this.#expandStart(filter.end - filter.start);
            }

            return cachedEvents;
        } else if (overlaps(this.#range, start, end)) {
            // events are partially cached

            if (start < this.#range[0] && filter.order == 'asc') {
                if (this.#expandStartTask != null) {
                    await this.#expandStartTask;

                    if (start >= this.#range[0]) {
                        let startIndex = partitionPointEventsLower(this.#events, start);
                        let endIndex = partitionPointEventsUpper(this.#events, end);
                        let cachedEvents = this.#events.slice(startIndex, endIndex);

                        cachedEvents.splice(50);

                        this.#expandStart(filter.end - filter.start);

                        return cachedEvents;
                    }
                }

                // create query from start to range[0]
                // - if it returns 50, reset the cache
                // - if it returns less, append it to current cache

                let newFilter = {
                    filter: this.#filter,
                    ...filter,
                    end: this.#range[0] - 1,
                };
                let newEvents = await getEvents(newFilter);

                if (newEvents.length == 50) {
                    // reset the cache
                    this.#range = getRetrievedEventRange(newFilter, newEvents);
                    this.#events = [...newEvents];
                    this.#expandStartTask = null;
                    this.#expandEndTask = null;

                    this.#expandStart(filter.end - filter.start);
                    this.#expandEnd(filter.end - filter.start);

                    return newEvents;
                } else {
                    this.#range = [start, this.#range[1]];
                    this.#events = [...newEvents, ...this.#events]

                    let startIndex = partitionPointEventsLower(this.#events, start);
                    let endIndex = partitionPointEventsUpper(this.#events, end);
                    let cachedEvents = this.#events.slice(startIndex, endIndex);

                    cachedEvents.splice(50);

                    this.#expandEnd(filter.end - filter.start);

                    return cachedEvents;
                }
            }

            if (end > this.#range[1] && filter.order == 'desc') {
                if (this.#expandEndTask != null) {
                    await this.#expandEndTask;

                    if (end <= this.#range[1]) {
                        let startIndex = partitionPointEventsLower(this.#events, start);
                        let endIndex = partitionPointEventsUpper(this.#events, end);
                        let cachedEvents = this.#events.slice(startIndex, endIndex);

                        cachedEvents.reverse();
                        cachedEvents.splice(50);

                        this.#expandEnd(filter.end - filter.start);

                        return cachedEvents;
                    }
                }

                // create query from range[1] to end
                // - if it returns 50, reset the cache
                // - if it returns less, append it to current cache

                let newFilter = {
                    filter: this.#filter,
                    ...filter,
                    start: this.#range[1] + 1,
                };
                let newEvents = await getEvents(newFilter);

                if (newEvents.length == 50) {
                    // reset the cache
                    this.#range = getRetrievedEventRange(newFilter, newEvents);
                    this.#events = [...newEvents].reverse();
                    this.#expandStartTask = null;
                    this.#expandEndTask = null;

                    this.#expandStart(filter.end - filter.start);
                    this.#expandEnd(filter.end - filter.start);

                    return newEvents;
                } else {
                    this.#range = [this.#range[0], end];
                    this.#events = [...this.#events, ...newEvents.reverse()]

                    let startIndex = partitionPointEventsLower(this.#events, start);
                    let endIndex = partitionPointEventsUpper(this.#events, end);
                    let cachedEvents = this.#events.slice(startIndex, endIndex);

                    cachedEvents.reverse();
                    cachedEvents.splice(50);

                    this.#expandEnd(filter.end - filter.start);

                    return cachedEvents;
                }
            }

            if (start < this.#range[0] && filter.order == 'desc') {
                let endIndex = partitionPointEventsUpper(this.#events, end);
                let cachedEvents = this.#events.slice(0, endIndex);
                if (cachedEvents.length >= 50) {
                    cachedEvents.reverse();
                    cachedEvents.splice(50);
                    return cachedEvents;
                }

                await this.#expandStart(filter.end - filter.start);

                endIndex = partitionPointEventsUpper(this.#events, end);
                cachedEvents = this.#events.slice(0, endIndex);
                cachedEvents.reverse();
                cachedEvents.splice(50);

                this.#expandStart(filter.end - filter.start);

                return cachedEvents;
            }

            if (end > this.#range[1] && filter.order == 'asc') {
                let startIndex = partitionPointEventsLower(this.#events, start);
                let cachedEvents = this.#events.slice(startIndex);
                if (cachedEvents.length >= 50) {
                    cachedEvents.splice(50);
                    return cachedEvents;
                }

                await this.#expandEnd(filter.end - filter.start);

                startIndex = partitionPointEventsLower(this.#events, start);
                cachedEvents = this.#events.slice(startIndex);
                cachedEvents.splice(50);

                this.#expandEnd(filter.end - filter.start);

                return cachedEvents;
            }

            return [];
        } else {
            // there is no overlap, reset the cache

            let events = await getEvents({ filter: this.#filter, ...filter });

            let cachedEvents = [...events];
            if (filter.order == 'desc') {
                cachedEvents.reverse();
            }

            let cachedRange = getRetrievedEventRange(filter, events);

            this.#range = cachedRange;
            this.#events = cachedEvents;
            this.#expandStartTask = null;
            this.#expandEndTask = null;

            this.#expandStart(filter.end - filter.start);
            this.#expandEnd(filter.end - filter.start);

            return events;
        }
    }

    getEventCounts = async (filter: PartialEventCountFilter, wait?: boolean, cache?: boolean): Promise<Counts | null> => {
        let key = `${filter.start}-${filter.end}`;

        // the latest second should not be considered reliable
        let inVolatileRange = filter.end >= Date.now() * 1000 - 1000000;

        if (this.#counts[key] != undefined && !this.#counts[key][1]) {
            return this.#counts[key][0];
        }

        if (wait === false) {
            return this.#counts[key] != undefined ? this.#counts[key][0] : null;
        }

        let counts = await Promise.all([
            getEventCount({ filter: [...this.#filter, { predicate_kind: 'single', predicate: { text: '', property: "level", value_kind: 'comparison', value: ['Eq', "TRACE"] } }], ...filter }),
            getEventCount({ filter: [...this.#filter, { predicate_kind: 'single', predicate: { text: '', property: "level", value_kind: 'comparison', value: ['Eq', "DEBUG"] } }], ...filter }),
            getEventCount({ filter: [...this.#filter, { predicate_kind: 'single', predicate: { text: '', property: "level", value_kind: 'comparison', value: ['Eq', "INFO"] } }], ...filter }),
            getEventCount({ filter: [...this.#filter, { predicate_kind: 'single', predicate: { text: '', property: "level", value_kind: 'comparison', value: ['Eq', "WARN"] } }], ...filter }),
            getEventCount({ filter: [...this.#filter, { predicate_kind: 'single', predicate: { text: '', property: "level", value_kind: 'comparison', value: ['Eq', "ERROR"] } }], ...filter }),
        ]);

        // cache if enabled
        if (cache == undefined || cache == true) {
            this.#counts[key] = [counts, inVolatileRange];
        }

        return counts;
    }

    #insertEvent = (event: Event) => {
        let insertIdx = partitionPointEventsLower(this.#events, event.timestamp);
        this.#events.splice(insertIdx, 0, event);
    }

    #expandStart = (duration: number): Promise<void> => {
        if (this.#expandStartTask != null) {
            return this.#expandStartTask;
        }

        this.#expandStartTask = (async () => {
            let range = this.#range;
            let filter: EventFilter & PartialFilter = {
                filter: this.#filter,
                order: 'desc',
                // limit: 100, // TODO: use client-side limits
                start: range[0] - duration,
                end: range[0] - 1,
            };

            let newEvents = await getEvents(filter);

            // check if fetched events are still valid for the cache
            if (this.#range[0] != range[0]) {
                return;
            }

            let retrievedRange = getRetrievedEventRange(filter, newEvents);

            this.#range = [retrievedRange[0], this.#range[1]];
            this.#events = [...newEvents.reverse(), ...this.#events];
            this.#expandStartTask = null;
        })();

        return this.#expandStartTask;
    }

    #expandEnd = (duration: number): Promise<void> => {
        if (this.#expandEndTask != null) {
            return this.#expandEndTask;
        }

        this.#expandEndTask = (async () => {
            let range = this.#range;

            // do not expand range beyond "now"
            let end = Math.min(range[1] + duration, Date.now() * 1000);

            let filter: EventFilter & PartialFilter = {
                filter: this.#filter,
                order: 'asc',
                // limit: 100, // TODO: use client-side limits
                start: range[1] + 1,
                end,
            };

            let newEvents = await getEvents(filter);

            // check if fetched events are still valid for the cache
            if (this.#range[1] != range[1]) {
                return;
            }

            let retrievedRange = getRetrievedEventRange(filter, newEvents);

            this.#range = [this.#range[0], retrievedRange[1]];
            this.#events = [...this.#events, ...newEvents];
            this.#expandEndTask = null;
        })();

        return this.#expandEndTask;
    }
}

export class SpanDataLayer {
    // the filter to use fetching spans
    //
    // this is immutable, if the filter changes the cache should be re-created
    #filter: FilterPredicate[];

    // the timespan for which this datalayer contains spans
    #range: Timespan;

    // the cached spans, in 'asc' order
    //
    // this may have spans prior to `#range[0]` but they will overlap `#range`
    #spans: Span[];

    #slots: Timespan[][];
    #slotmap: { [span_id: FullSpanId]: number };

    #expandStartTask: Promise<void> | null;
    #expandEndTask: Promise<void> | null;

    constructor(filter: Input[]) {
        this.#filter = filter.filter(f => f.input == 'valid');
        this.#range = [0, 0];
        this.#spans = [];
        this.#slots = [];
        this.#slotmap = {};
        this.#expandStartTask = null;
        this.#expandEndTask = null;
    }

    subscribe = () => { }

    unsubscribe = async () => { }

    getPositionedSpans = async (filter: PartialFilter, wait?: boolean): Promise<PositionedSpan[] | null> => {
        let spans = await this.getSpans(filter, wait);
        return spans && spans.map(span => ({
            id: span.id,
            created_at: span.created_at,
            closed_at: span.closed_at,
            level: span.level,
            slot: this.#slotmap[span.id],
        }))
    }

    getSpans = async (filter: PartialFilter, wait?: boolean): Promise<Span[] | null> => {
        // don't try to cache unbounded shenanigans
        if (filter.start == null || filter.end == null) {
            return await getSpans({ filter: this.#filter, ...filter });
        }

        if (within(this.#range, filter.start, filter.end)) {
            return this.#getSpansInCache(filter);
        } else if (overlaps(this.#range, filter.start, filter.end)) {
            if (filter.start < this.#range[0] && filter.order == 'asc') {
                if (wait === false) {
                    return null;
                }

                if (this.#expandStartTask != null) {
                    await this.#expandStartTask;

                    if (filter.start >= this.#range[0]) {
                        return this.#getSpansInCache(filter);
                    }
                }

                // create query from start to range[0]
                // - if it returns 50, reset the cache
                // - if it returns less, append it to current cache

                let newFilter = {
                    filter: this.#filter,
                    ...filter,
                    end: this.#range[0] - 1,
                };
                let newSpans = await getSpans(newFilter);

                if (newSpans.length == 50) {
                    // reset the cache
                    this.#spans = [...newSpans];
                    this.#range = getRetrievedSpanRange(filter, newSpans);
                    this.#slots = [];
                    this.#slotmap = {};
                    this.#expandStartTask = null;
                    this.#expandEndTask = null;

                    this.#calculateSlots(newSpans);
                    this.#expandStart(filter.end - filter.start);
                    this.#expandEnd(filter.end - filter.start);

                    return newSpans;
                } else {
                    let retrievedRange = getRetrievedSpanRange(filter, newSpans);
                    let denseStartIndex = partitionPointSpansLower(this.#spans, this.#range[0]);

                    this.#range = [retrievedRange[0], this.#range[1]];
                    this.#spans = [...newSpans, ...this.#spans.slice(denseStartIndex)];
                    this.#calculateSlots(newSpans);

                    return this.#getSpansInCache(filter);
                }
            }

            if (filter.end > this.#range[1] && filter.order == 'asc') {
                let startIndex = partitionPointSpansLower(this.#spans, filter.previous ?? 0);
                let endIndex = partitionPointSpansUpper(this.#spans, filter.end);
                if (endIndex - startIndex >= 50) {
                    return this.#spans.slice(startIndex, startIndex + 50);
                }

                if (wait === false) {
                    return null;
                }

                await this.#expandEnd(filter.end - filter.start);

                return this.#getSpansInCache(filter);
            }


            if (filter.start < this.#range[0] && filter.order == 'desc') {
                let startIndex = partitionPointSpansLower(this.#spans, filter.start);
                let endIndex = partitionPointSpansUpper(this.#spans, filter.previous ?? filter.end);
                if (endIndex - startIndex >= 50) {
                    return this.#spans.slice(endIndex - 50, endIndex).reverse();
                }

                if (wait === false) {
                    return null;
                }

                await this.#expandStart(filter.end - filter.start);

                return this.#getSpansInCache(filter);
            }

            if (filter.end > this.#range[1] && filter.order == 'desc') {
                if (this.#expandEndTask != null) {
                    await this.#expandEndTask;

                    if (filter.start >= this.#range[0]) {
                        return this.#getSpansInCache(filter);
                    }
                }

                // create query from end to range[1]
                // - if it returns 50, reset the cache
                // - if it returns less, append it to current cache

                let newFilter = {
                    filter: this.#filter,
                    ...filter,
                    start: this.#range[1] + 1,
                };
                let newSpans = await getSpans(newFilter);

                if (newSpans.length == 50) {
                    // reset the cache
                    this.#spans = [...newSpans.reverse()];
                    this.#range = getRetrievedSpanRange(filter, newSpans);
                    this.#slots = [];
                    this.#slotmap = {};
                    this.#expandStartTask = null;
                    this.#expandEndTask = null;

                    this.#calculateSlots(newSpans);
                    this.#expandStart(filter.end - filter.start);
                    this.#expandEnd(filter.end - filter.start);

                    return newSpans;
                } else {
                    let retrievedRange = getRetrievedSpanRange(filter, newSpans);
                    let denseStartIndex = partitionPointSpansLower(newSpans, this.#range[1] + 1);

                    this.#range = [this.#range[0], retrievedRange[1]];
                    this.#spans = [...this.#spans, ...newSpans.slice(denseStartIndex)];
                    this.#calculateSlots(newSpans);

                    return this.#getSpansInCache(filter);
                }
            }

            console.warn("fallback");
            if (wait === false) {
                return null;
            }

            return await getSpans({ filter: this.#filter, ...filter });
        } else {
            if (wait === false) {
                return null;
            }

            // there is no overlap, reset the cache

            let spans = await getSpans({ filter: this.#filter, ...filter });

            if (filter.previous != null || spans.length == 0) {
                // don't start new cache with this set
                return spans;
            }

            let cachedSpans = [...spans];
            if (filter.order == 'desc') {
                cachedSpans.reverse();
            }

            let endOfRange = cachedSpans[cachedSpans.length - 1].created_at;
            if (endOfRange <= filter.start) {
                // don't start new cache with this set
                return spans;
            }

            this.#range = [filter.start, endOfRange];
            this.#spans = cachedSpans;
            this.#slots = [];
            this.#slotmap = {};
            this.#expandStartTask = null;
            this.#expandEndTask = null;

            this.#calculateSlots(cachedSpans);
            this.#expandStart(filter.end - filter.start);
            this.#expandEnd(filter.end - filter.start);

            return spans;
        }
    }

    #getSpansInCache = (filter: PartialFilter): Span[] => {
        // this is a helper function that will get the spans assuming they are
        // already in the cache

        // don't try to cache unbounded shenanigans
        if (filter.start == null || filter.end == null) {
            // this shouldn't happen
            return [];
        }

        let startIndex = partitionPointSpansLower(this.#spans, filter.start);
        let endIndex = partitionPointSpansUpper(this.#spans, filter.end);

        if (filter.order == 'asc') {
            if (!filter.previous || filter.previous < filter.start) {
                // beginning is sparse
                let preRangeStart = (filter.previous)
                    ? partitionPointSpansUpper(this.#spans, filter.previous)
                    : 0;

                let preRangeSpansInFilter = [];
                let preRangeEnd = startIndex;

                let preRangeSpans = this.#spans.slice(preRangeStart, preRangeEnd);
                for (let span of preRangeSpans) {
                    if (span.closed_at == null || span.closed_at >= filter.start) {
                        preRangeSpansInFilter.push(span);
                        if (preRangeSpansInFilter.length == 50) {
                            return preRangeSpansInFilter;
                        }
                    }
                }

                if ((endIndex - startIndex) + preRangeSpansInFilter.length > 50) {
                    endIndex = startIndex + 50 - preRangeSpansInFilter.length;
                }

                if (this.#spans.length - endIndex < 50 && filter.end > this.#range[1] - (filter.end - filter.start)) {
                    this.#expandEnd(filter.end - filter.start);
                }
                if (startIndex < 50 && Math.max(filter.start, filter.previous ?? 0) < this.#range[0] + (filter.end - filter.start)) {
                    this.#expandStart(filter.end - filter.start);
                }

                return [...preRangeSpansInFilter, ...this.#spans.slice(startIndex, endIndex)];
            } else {
                // beginning is dense
                if (filter.previous && filter.previous > filter.start) {
                    startIndex = partitionPointSpansUpper(this.#spans, filter.previous);
                }

                if ((endIndex - startIndex) > 50) {
                    endIndex = startIndex + 50;
                }

                if (this.#spans.length - endIndex < 50 && filter.end > this.#range[1] - (filter.end - filter.start)) {
                    this.#expandEnd(filter.end - filter.start);
                }
                if (startIndex < 50 && Math.max(filter.start, filter.previous ?? 0) < this.#range[0] + (filter.end - filter.start)) {
                    this.#expandStart(filter.end - filter.start);
                }

                return this.#spans.slice(startIndex, endIndex);
            }
        } else /* filter.order == 'desc' */ {
            if (!filter.previous || filter.previous > filter.start) {
                // beginning (end?) is dense
                let actualEndIndex = filter.previous
                    ? partitionPointSpansUpper(this.#spans, filter.previous)
                    : endIndex;

                if (actualEndIndex - startIndex >= 50) {
                    startIndex = actualEndIndex - 50;
                    return this.#spans.slice(startIndex, actualEndIndex).reverse();
                }

                // the rest is sparse
                let spans = this.#spans.slice(startIndex, actualEndIndex).reverse();
                let span_rev_idx = 0;
                for (let span of this.#spans.slice(0, startIndex).reverse()) {
                    if (span.closed_at == null || span.closed_at >= filter.start) {
                        spans.push(span);
                        if (spans.length == 50) {
                            startIndex = startIndex - span_rev_idx;

                            if (this.#spans.length - actualEndIndex < 50 && filter.end > this.#range[1] - (filter.end - filter.start)) {
                                this.#expandEnd(filter.end - filter.start);
                            }
                            if (startIndex < 50 && Math.max(filter.start, filter.previous ?? 0) < this.#range[0] + (filter.end - filter.start)) {
                                this.#expandStart(filter.end - filter.start);
                            }

                            return spans;
                        }
                    }

                    span_rev_idx += 1;
                }

                return spans;
            } else {
                // sparse only
                let actualEndIndex = partitionPointSpansUpper(this.#spans, filter.previous);

                let spans = [];
                let span_rev_idx = 0;
                for (let span of this.#spans.slice(0, actualEndIndex).reverse()) {
                    if (span.closed_at == null || span.closed_at >= filter.start) {
                        spans.push(span);
                        if (spans.length == 50) {
                            startIndex = actualEndIndex - span_rev_idx;

                            if (this.#spans.length - actualEndIndex < 50 && filter.end > this.#range[1] - (filter.end - filter.start)) {
                                this.#expandEnd(filter.end - filter.start);
                            }
                            if (startIndex < 50 && Math.max(filter.start, filter.previous ?? 0) < this.#range[0] + (filter.end - filter.start)) {
                                this.#expandStart(filter.end - filter.start);
                            }

                            return spans;
                        }
                    }

                    span_rev_idx += 1;
                }

                return spans;
            }
        }
    }

    #calculateSlots = (newSpans: Span[]) => {
        let spansToPosition = [...newSpans];
        spansToPosition.sort((a, b) => {
            let a_order = spanOrdering(a);
            let b_order = spanOrdering(b);

            if (a_order != b_order) {
                return a_order - b_order;
            } else {
                return a.created_at - b.created_at;
            }
        });

        outer: for (let span of spansToPosition) {
            if (this.#slotmap[span.id] != undefined) {
                continue;
            }

            let minSlot = 0;
            for (let parentSpan of span.ancestors.slice().reverse()) {
                let parentSlot = this.#slotmap[parentSpan.id];
                if (parentSlot != undefined) {
                    minSlot = parentSlot + 1;
                    break;
                }
            }

            for (let i = minSlot; i < this.#slots.length; i++) {
                if (putInSlot(this.#slots[i], span)) {
                    this.#slotmap[span.id] = i;
                    continue outer;
                }
            }

            this.#slotmap[span.id] = this.#slots.length;
            this.#slots.push([[span.created_at, span.closed_at ?? Infinity]]);
        }
    }

    #expandStart = (duration: number): Promise<void> => {
        if (this.#expandStartTask != null) {
            return this.#expandStartTask;
        }

        this.#expandStartTask = (async () => {
            let range = this.#range;
            let filter: SpanFilter & PartialFilter = {
                filter: [...this.#filter, {
                    predicate_kind: 'single',
                    predicate: {
                        text: '',
                        property: 'created',
                        value_kind: 'comparison',
                        value: ['Gte', `${range[0] - duration}`],
                    },
                }],
                order: 'desc',
                // limit: 100, // TODO: use client-side limits
                start: range[0] - duration,
                end: range[0] - 1,
            };

            let newSpans = await getSpans(filter);

            let retrievedRange = getRetrievedSpanRange(filter, newSpans);

            let newPreSpans = await getSpans({
                ...filter,
                filter: [...this.#filter, {
                    predicate_kind: 'single',
                    predicate: {
                        text: '',
                        property: 'created',
                        value_kind: 'comparison',
                        value: ['Lt', `${range[0] - duration}`],
                    },
                }],
                start: retrievedRange[0],
            });

            // check if fetched events are still valid for the cache
            if (this.#range[0] != range[0]) {
                return;
            }

            if (newPreSpans.length == 50) {
                console.error("woops, didn't get enough");
            }

            let allNewSpans = [...newPreSpans.reverse(), ...newSpans.reverse()];
            let denseStartIndex = partitionPointSpansLower(this.#spans, this.#range[0]);

            this.#range = [retrievedRange[0], this.#range[1]];
            this.#spans = [...allNewSpans, ...this.#spans.slice(denseStartIndex)];
            this.#calculateSlots(allNewSpans);
            this.#expandStartTask = null;
        })();

        return this.#expandStartTask;
    }

    #expandEnd = (duration: number): Promise<void> => {
        if (this.#expandEndTask != null) {
            return this.#expandEndTask;
        }

        this.#expandEndTask = (async () => {
            let range = this.#range;

            // do not expand range beyond "now"
            let end = Math.min(range[1] + duration, Date.now() * 1000);

            let filter: SpanFilter & PartialFilter = {
                filter: [...this.#filter, {
                    predicate_kind: 'single',
                    predicate: {
                        text: '',
                        property: 'created',
                        value_kind: 'comparison',
                        value: ['Gt', `${range[1]}`],
                    },
                }],
                order: 'asc',
                // limit: 100, // TODO: use client-side limits
                start: range[1] + 1,
                end,
            };

            let newSpans = await getSpans(filter);

            // check if fetched events are still valid for the cache
            if (this.#range[1] != range[1]) {
                return;
            }

            let retrievedRange = getRetrievedSpanRange(filter, newSpans);

            this.#range = [this.#range[0], retrievedRange[1]];
            this.#spans = [...this.#spans, ...newSpans];
            this.#calculateSlots(newSpans);
            this.#expandEndTask = null;
        })();

        return this.#expandEndTask;
    }
}

export class TraceDataLayer {
    // the filter to use fetching spans
    //
    // this is immutable, if the filter changes the cache should be re-created
    #filter: FilterPredicate[];

    // the cached events and spans in first-child order
    #entries: (Event | Span)[];

    #fetchTask: Promise<void> | null;

    constructor(filter: Input[]) {
        this.#filter = filter.filter(f => f.input == 'valid');
        this.#entries = [];
        this.#fetchTask = null;
    }

    subscribe = () => { }

    unsubscribe = async () => { }

    getEntries = async (filter: PaginationFilter): Promise<(Event | Span)[]> => {
        await this.#fetch();

        // this is a hack to support a "paginator" asking for more entries even
        // when we returned all we had in the first call
        if (filter.previous != undefined) {
            return [];
        }

        if (filter.order == 'asc') {
            return this.#entries.slice();
        } else {
            return this.#entries.slice().reverse();
        }
    }

    #fetch = (): Promise<void> => {
        if (this.#fetchTask != null) {
            return this.#fetchTask;
        }

        this.#fetchTask = (async () => {
            let spans = [];
            let previous: number | undefined;
            while (true) {
                let newSpans = await getSpans({
                    filter: this.#filter,
                    order: 'asc',
                    start: null,
                    end: null,
                    previous,
                });

                spans.push(...newSpans);

                if (newSpans.length != 50) {
                    break;
                }

                previous = newSpans[newSpans.length - 1].created_at;
            }

            let events = [];
            previous = undefined;
            while (true) {
                let newEvents = await getEvents({
                    filter: this.#filter,
                    order: 'asc',
                    start: null,
                    end: null,
                    previous,
                });

                events.push(...newEvents);

                if (newEvents.length != 50) {
                    break;
                }

                previous = newEvents[newEvents.length - 1].timestamp;
            }

            function getTimestamp(e: Event | Span): Timestamp {
                return (e as any).timestamp || (e as any).created_at;
            }

            let spansById = Object.fromEntries(spans.map(s => [s.id, s]));
            let ordering = (e: Event | Span) => e.ancestors.map(a => spansById[a.id]?.created_at ?? '*').concat(getTimestamp(e)).join('-');

            let entries = [...spans, ...events];
            entries.sort((a, b) => {
                let a_order = ordering(a);
                let b_order = ordering(b);

                return a_order < b_order ? -1 : 1;
            });

            this.#entries = entries;
        })();

        return this.#fetchTask;
    }
}

export class ConnectionDataLayer {
    // the filter to use fetching spans
    //
    // this is immutable, if the filter changes the cache should be re-created
    #filter: FilterPredicate[];

    // the cached events and spans in first-child order
    #connections: Connection[];

    #slotmap: { [connection_id: ConnectionId]: number };

    #fetchTask: Promise<void> | null;

    constructor(filter: Input[]) {
        this.#filter = filter.filter(f => f.input == 'valid');
        this.#connections = [];
        this.#slotmap = {};
        this.#fetchTask = null;
    }

    subscribe = () => { }

    unsubscribe = async () => { }

    getConnections = async (filter: PartialFilter): Promise<Connection[]> => {
        await this.#fetch();

        let startIndex = partitionPointConnectionsLower(this.#connections, filter.start ?? 1);
        let endIndex = partitionPointConnectionsUpper(this.#connections, filter.end ?? Infinity);

        let preRangeConnectionsInFilter = [];
        if (!filter.previous || filter.previous < (filter.start ?? 1)) {
            // beginning is sparse
            let preRangeStart = (filter.previous)
                ? partitionPointConnectionsUpper(this.#connections, filter.previous)
                : 0;

            let preRangeEnd = startIndex;

            let preRangeConnections = this.#connections.slice(preRangeStart, preRangeEnd);
            for (let connection of preRangeConnections) {
                if (connection.disconnected_at == null || connection.disconnected_at >= (filter.start ?? 1)) {
                    preRangeConnectionsInFilter.push(connection);
                    if (preRangeConnectionsInFilter.length == 50) {
                        return preRangeConnectionsInFilter;
                    }
                }
            }

            if ((endIndex - startIndex) + preRangeConnectionsInFilter.length > 50) {
                endIndex = startIndex + 50 - preRangeConnectionsInFilter.length;
            }

            return [...preRangeConnectionsInFilter, ...this.#connections.slice(startIndex, endIndex)];
        } else {
            // beginning is dense
            if (filter.previous && filter.previous > (filter.start ?? 1)) {
                startIndex = partitionPointConnectionsUpper(this.#connections, filter.previous);
            }

            if ((endIndex - startIndex) > 50) {
                endIndex = startIndex + 50;
            }

            return this.#connections.slice(startIndex, endIndex);
        }
    }

    getPositionedConnections = async (filter: PartialFilter): Promise<PositionedConnection[]> => {
        let connections = await this.getConnections(filter);

        return connections.map(i => ({
            id: i.id,
            connected_at: i.connected_at,
            disconnected_at: i.disconnected_at,
            slot: this.#slotmap[i.id],
        }))
    }

    #calculateSlots = (connections: Connection[]): { [connection_id: ConnectionId]: number } => {
        let connectionsToPosition = [...connections];
        let slots: Timespan[][] = [];
        let slotmap: { [connection_id: ConnectionId]: number } = {};
        connectionsToPosition.sort((a, b) => {
            let a_order = connectionOrdering(a);
            let b_order = connectionOrdering(b);

            if (a_order != b_order) {
                return a_order - b_order;
            } else {
                return a.connected_at - b.connected_at;
            }
        });

        outer: for (let connection of connectionsToPosition) {
            if (slotmap[connection.id] != undefined) {
                continue;
            }

            for (let i = 0; i < slots.length; i++) {
                if (putInConnectionSlot(slots[i], connection)) {
                    slotmap[connection.id] = i;
                    continue outer;
                }
            }

            slotmap[connection.id] = slots.length;
            slots.push([[connection.connected_at, connection.disconnected_at ?? Infinity]]);
        }

        return slotmap;
    }

    #fetch = (): Promise<void> => {
        if (this.#fetchTask != null) {
            return this.#fetchTask;
        }

        this.#fetchTask = (async () => {
            let connections = [];
            let previous: number | undefined;
            while (true) {
                let newConnections = await getConnections({
                    filter: this.#filter,
                    order: 'asc',
                    start: null,
                    end: null,
                    previous,
                });

                connections.push(...newConnections);

                if (newConnections.length != 50) {
                    break;
                }

                previous = newConnections[newConnections.length - 1].connected_at;
            }

            this.#slotmap = this.#calculateSlots(connections);
            this.#connections = connections;
        })();

        return this.#fetchTask;
    }
}

// this deduces the range of covered events based on the events retrieved and
// the filter used to get them
function getRetrievedEventRange(filter: PartialFilter, events: Event[]): Timespan {
    if (events.length != 50) { // TODO: change to filter.limit
        // this means the range was exhausted since the limit was not reached
        if (filter.order == 'asc') {
            return [filter.previous ? filter.previous + 1 : filter.start!, filter.end!];
        } else {
            return [filter.start!, filter.previous ? filter.previous - 1 : filter.end!];
        }
    } else {
        // the limit was hit, so we don't know if the range was exhausted
        if (filter.order == 'asc') {
            return [filter.previous ? filter.previous + 1 : filter.start!, events[events.length - 1].timestamp];
        } else {
            return [events[events.length - 1].timestamp, filter.previous ? filter.previous - 1 : filter.end!];
        }
    }
}

// this deduces the range of covered spans based on the spans retrieved and the
// filter used to get them
function getRetrievedSpanRange(filter: PartialFilter, spans: Span[]): Timespan {
    if (spans.length != 50) { // TODO: change to filter.limit
        // this means the range was exhausted since the limit was not reached
        if (filter.order == 'asc') {
            return [(filter.previous && filter.previous > filter.start!) ? filter.previous + 1 : filter.start!, filter.end!];
        } else {
            return [filter.start!, filter.previous ? filter.previous - 1 : filter.end!];
        }
    } else {
        // the limit was hit, so we don't know if the range was exhausted
        if (filter.order == 'asc') {
            return [(filter.previous && filter.previous > filter.start!) ? filter.previous + 1 : filter.start!, spans[spans.length - 1].created_at];
        } else {
            return [Math.max(spans[spans.length - 1].created_at, filter.start!), filter.previous ? filter.previous - 1 : filter.end!];
        }
    }
}

function within(range: Timespan, start: Timestamp, end: Timestamp): boolean {
    return (start >= range[0] && end <= range[1]);
}

function overlaps(range: Timespan, start: Timestamp, end: Timestamp): boolean {
    return (start >= range[0] && start - 1 <= range[1]) || (end + 1 >= range[0] && end <= range[1]);
}

// returns the index where an event with the timestamp should be if it exists
export function partitionPointEventsLower(events: Event[], timestamp: Timestamp): number {
    let start = 0;
    let end = events.length - 1;

    while (start <= end) {
        let mid = Math.floor((start + end) / 2);

        if (events[mid].timestamp == timestamp) {
            return mid;
        } else if (events[mid].timestamp > timestamp) {
            end = mid - 1;
        } else {
            start = mid + 1;
        }
    }

    return start;
}

export function partitionPointEventsUpper(events: Event[], timestamp: Timestamp): number {
    let start = 0;
    let end = events.length - 1;

    while (start <= end) {
        let mid = Math.floor((start + end) / 2);

        if (events[mid].timestamp == timestamp) {
            return mid + 1;
        } else if (events[mid].timestamp > timestamp) {
            end = mid - 1;
        } else {
            start = mid + 1;
        }
    }

    return start;
}

// returns the index where a span created at the timestamp should be if it
// exists 
export function partitionPointSpansLower(spans: Span[], timestamp: Timestamp): number {
    let start = 0;
    let end = spans.length - 1;

    while (start <= end) {
        let mid = Math.floor((start + end) / 2);

        if (spans[mid].created_at == timestamp) {
            return mid;
        } else if (spans[mid].created_at > timestamp) {
            end = mid - 1;
        } else {
            start = mid + 1;
        }
    }

    return start;
}

export function partitionPointSpansUpper(spans: Span[], timestamp: Timestamp): number {
    let start = 0;
    let end = spans.length - 1;

    while (start <= end) {
        let mid = Math.floor((start + end) / 2);

        if (spans[mid].created_at == timestamp) {
            return mid + 1;
        } else if (spans[mid].created_at > timestamp) {
            end = mid - 1;
        } else {
            start = mid + 1;
        }
    }

    return start;
}

// returns the index where a span created at the timestamp should be if it
// exists 
export function partitionPointEntriesLower(entries: (Event | Span)[], timestamp: Timestamp): number {
    let start = 0;
    let end = entries.length - 1;

    function getTimestamp(entry: Event | Span): Timestamp {
        return (entry as any).timestamp || (entry as any).created_at;
    }

    while (start <= end) {
        let mid = Math.floor((start + end) / 2);

        if (getTimestamp(entries[mid]) == timestamp) {
            return mid;
        } else if (getTimestamp(entries[mid]) > timestamp) {
            end = mid - 1;
        } else {
            start = mid + 1;
        }
    }

    return start;
}

export function partitionPointEntriesUpper(entries: (Event | Span)[], timestamp: Timestamp): number {
    let start = 0;
    let end = entries.length - 1;

    function getTimestamp(entry: Event | Span): Timestamp {
        return (entry as any).timestamp || (entry as any).created_at;
    }

    while (start <= end) {
        let mid = Math.floor((start + end) / 2);

        if (getTimestamp(entries[mid]) == timestamp) {
            return mid + 1;
        } else if (getTimestamp(entries[mid]) > timestamp) {
            end = mid - 1;
        } else {
            start = mid + 1;
        }
    }

    return start;
}

// returns the index where a span created at the timestamp should be if it
// exists 
export function partitionPointConnectionsLower(connections: Connection[], timestamp: Timestamp): number {
    let start = 0;
    let end = connections.length - 1;

    while (start <= end) {
        let mid = Math.floor((start + end) / 2);

        if (connections[mid].connected_at == timestamp) {
            return mid;
        } else if (connections[mid].connected_at > timestamp) {
            end = mid - 1;
        } else {
            start = mid + 1;
        }
    }

    return start;
}

export function partitionPointConnectionsUpper(connections: Connection[], timestamp: Timestamp): number {
    let start = 0;
    let end = connections.length - 1;

    while (start <= end) {
        let mid = Math.floor((start + end) / 2);

        if (connections[mid].connected_at == timestamp) {
            return mid + 1;
        } else if (connections[mid].connected_at > timestamp) {
            end = mid - 1;
        } else {
            start = mid + 1;
        }
    }

    return start;
}

// partitionPoint([] as any, 0) == 0
// partitionPoint([{ timestamp: 1 }] as any, 0) == 0
// partitionPoint([{ timestamp: 1 }] as any, 1) == 0
// partitionPoint([{ timestamp: 1 }] as any, 2) == 1
// partitionPoint([{ timestamp: 1 }, { timestamp: 3 }] as any, 2) == 1
// partitionPoint([{ timestamp: 1 }, { timestamp: 3 }] as any, 3) == 1
// partitionPoint([{ timestamp: 1 }, { timestamp: 3 }] as any, 4) == 2

function spanMeasure(span: Span): number {
    if (span.closed_at == null) {
        return 1;
    }

    let duration = span.closed_at - span.created_at;
    return 32 - Math.min(Math.log(duration), 31);
}

function spanOrdering(span: Span): number {
    return spanMeasure(span) * Math.pow(32, span.ancestors.length);
}

function putInSlot(slot: Timespan[], span: Span): boolean {
    if (slot.length == 0) {
        slot.push([span.created_at, span.closed_at ?? Infinity]);
        return true;
    }

    if (slot[0][0] > (span.closed_at ?? Infinity)) {
        slot.splice(0, 0, [span.created_at, span.closed_at ?? Infinity]);
        return true;
    }

    for (let i = 0; i < slot.length - 1; i++) {
        if (span.created_at > slot[i][1] && (span.closed_at ?? Infinity) < slot[i + 1][0]) {
            slot.splice(i + 1, 0, [span.created_at, span.closed_at ?? Infinity]);
            return true;
        }
    }

    if (span.created_at > slot[slot.length - 1][1]) {
        slot.push([span.created_at, span.closed_at ?? Infinity]);
        return true;
    }

    return false;
}

function connectionMeasure(connection: Connection): number {
    if (connection.disconnected_at == null) {
        return 1;
    }

    let duration = connection.disconnected_at - connection.connected_at;
    return 32 - Math.min(Math.log(duration), 31);
}

function connectionOrdering(connection: Connection): number {
    return connectionMeasure(connection);
}

function putInConnectionSlot(slot: Timespan[], connection: Connection): boolean {
    if (slot.length == 0) {
        slot.push([connection.connected_at, connection.disconnected_at ?? Infinity]);
        return true;
    }

    if (slot[0][0] > (connection.disconnected_at ?? Infinity)) {
        slot.splice(0, 0, [connection.connected_at, connection.disconnected_at ?? Infinity]);
        return true;
    }

    for (let i = 0; i < slot.length - 1; i++) {
        if (connection.connected_at > slot[i][1] && (connection.disconnected_at ?? Infinity) < slot[i + 1][0]) {
            slot.splice(i + 1, 0, [connection.connected_at, connection.disconnected_at ?? Infinity]);
            return true;
        }
    }

    if (connection.connected_at > slot[slot.length - 1][1]) {
        slot.push([connection.connected_at, connection.disconnected_at ?? Infinity]);
        return true;
    }

    return false;
}
