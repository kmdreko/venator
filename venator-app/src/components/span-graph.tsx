import { batch, createEffect, createSignal, For, Match, Show, Switch, untrack } from "solid-js";
import { Input, Span } from "../invoke";
import { PartialFilter, Timespan, PositionedSpan, Counts, PartialCountFilter } from "../models";
import { GraphContainer } from "./graph-container";

import "./span-graph.css";

type SpanGraphMode = "individualized" | "count";

export type SpanGraphProps = {
    filter: Input[],
    timespan: Timespan,
    updateTimespan: (new_timespan: Timespan) => void,
    hoveredRow: Span | null,

    setCount: (count: [number, boolean]) => void,

    getPositionedSpans: (filter: PartialFilter, wait?: boolean) => Promise<PositionedSpan[] | null>,
    getSpanCounts: (filter: PartialCountFilter, wait?: boolean, cache?: boolean) => Promise<Counts | null>,
};

export function SpanGraph(props: SpanGraphProps) {
    const [mode, setMode] = createSignal<SpanGraphMode>("individualized");

    function setCount(count: [number, boolean]) {
        if (count[0] > 500 && mode() == "individualized") {
            setMode("count");
        }

        if (count[0] < 100 && count[1] && mode() == "count") {
            setMode("individualized");
        }

        props.setCount(count);
    }

    return <Switch>
        <Match when={mode() == 'individualized'}>
            <SpanIndividualizedGraph {...props} setCount={setCount} mode={mode()} />
        </Match>
        <Match when={mode() == 'count'}>
            <SpanCountGraph {...props} setCount={setCount} mode={mode()} />
        </Match>
    </Switch>;
}

let CACHE_START_LAST = 0;
let CACHE_START_DELAY_MS = 250;

export type SpanIndividualizedGraphProps = {
    mode: SpanGraphMode,

    filter: Input[],
    timespan: Timespan,
    updateTimespan: (new_timespan: Timespan) => void,
    hoveredRow: Span | null,

    setCount: (count: [number, boolean]) => void,

    getPositionedSpans: (filter: PartialFilter, wait?: boolean) => Promise<PositionedSpan[] | null>,
};

function SpanIndividualizedGraph(props: SpanIndividualizedGraphProps) {
    const [spans, setSpans] = createSignal<PositionedSpan[]>([]);

    createEffect(async () => {
        let current_filter = props.filter;
        let current_timespan = props.timespan;
        let [start, end] = current_timespan;

        setSpans([]);

        let now = Date.now();
        let primed = await props.getPositionedSpans({ order: 'asc', start, end }, false);
        if (primed == null && now < CACHE_START_LAST + CACHE_START_DELAY_MS) {
            await new Promise(resolve => setTimeout(resolve, CACHE_START_DELAY_MS));
            if (props.timespan != current_timespan || current_filter != props.filter || props.mode != 'individualized') {
                return;
            }
        }

        let previous: number | undefined;

        CACHE_START_LAST = now;
        while (true) {
            let new_spans = (await props.getPositionedSpans({ order: 'asc', start, end, previous }))!;
            if (props.timespan != current_timespan || current_filter != props.filter || props.mode != 'individualized') {
                return;
            }

            let current_spans = untrack(spans);
            let updated_spans = current_spans.concat(new_spans);
            setSpans(updated_spans);
            if (new_spans.length < 50) {
                break;
            }

            props.setCount([updated_spans.length, false]);
            previous = new_spans[new_spans.length - 1].created_at;
        }

        props.setCount([untrack(spans).length, true]);
    });

    function spanPosition(span: PositionedSpan): { top: string, left: string, right: string } {
        let current_timespan = props.timespan;
        let [start, end] = current_timespan;
        let duration = end - start;

        let left = (span.created_at - start) / duration;
        let right = (span.closed_at == null) ? 0.0 : (end - span.closed_at) / duration;

        return {
            top: `${span.slot * 6 + 1}px`,
            left: `${left * 100}%`,
            right: `${right * 100}%`,
        };
    }

    return <GraphContainer {...props} height={10}>
        <For each={spans()}>
            {span => (<span class={`span-graph-bar level-${span.level}`} style={spanPosition(span)}></span>)}
        </For>
    </GraphContainer>;
}

export type SpanCountGraphProps = {
    mode: SpanGraphMode,

    filter: Input[],
    timespan: Timespan,
    updateTimespan: (new_timespan: Timespan) => void,
    hoveredRow: Span | null,

    getSpanCounts: (filter: PartialCountFilter, wait?: boolean, cache?: boolean) => Promise<Counts | null>,

    setCount: (count: [number, boolean]) => void,
};

export function SpanCountGraph(props: SpanCountGraphProps) {
    const [bars, setBars] = createSignal<[Timespan, Counts | null][]>([]);

    createEffect(async () => {
        let current_filter = props.filter;
        let current_timespan = props.timespan;

        const buckets = calcBucketSizes(current_timespan[0], current_timespan[1]);

        let initial_height = 1;
        let initial_bars = buckets.map(span => [span, null] as [Timespan, Counts | null]);
        let initial_count = 0;
        for (let i in buckets) {
            let [start, end] = buckets[i];
            let current_bar = await props.getSpanCounts({ start, end }, false);

            if (current_bar != null) {
                initial_bars[i] = [initial_bars[i][0], current_bar];

                let current_total = current_bar[0] + current_bar[1] + current_bar[2] + current_bar[3] + current_bar[4];
                if (current_total > initial_height) {
                    initial_height = current_total;
                }

                initial_count += current_total;
            }
        }

        batch(() => {
            props.setCount([initial_count, false]);
            setBars(initial_bars);
        });

        let count = 0;
        for (let i in buckets) {
            // await new Promise(resolve => setTimeout(resolve, 5));

            let [start, end] = buckets[i];
            let new_bar = (await props.getSpanCounts({ start, end }))!;
            if (start >= current_timespan[0] && end <= current_timespan[1]) {
                count += new_bar[0] + new_bar[1] + new_bar[2] + new_bar[3] + new_bar[4];
            } else {
                let p_start = Math.max(start, current_timespan[0]);
                let p_end = Math.min(end, current_timespan[1]);
                let partial_bar = (await props.getSpanCounts({ start: p_start, end: p_end }, true, false))!;
                count += partial_bar[0] + partial_bar[1] + partial_bar[2] + partial_bar[3] + partial_bar[4];
            }

            if (current_timespan != props.timespan || current_filter != props.filter || props.mode != 'count') {
                return;
            }

            let current_bars = untrack(bars);
            if (current_bars[i][1] == new_bar) continue;
            let updated_bars = [...current_bars];
            updated_bars[i] = [updated_bars[i][0], new_bar];
            setBars(updated_bars);
            props.setCount([count, false]);
        }

        props.setCount([count, true]);
    });

    function barHeightMax() {
        let height = 1;
        for (let current_bar of bars()) {
            let [_, count] = current_bar;
            if (count != null) {
                let current_total = count[0] + count[1] + count[2] + count[3] + count[4];
                if (current_total > height) {
                    height = current_total;
                }
            }
        }
        return height;
    }

    function barSize(): string {
        let current_bars = bars();
        if (current_bars.length == 0) {
            return '?';
        }

        let [bar_start, bar_end] = current_bars[0][0];
        let duration = bar_end - bar_start;

        const MILLISECOND = 1000;
        const SECOND = 1000000;
        const MINUTE = 60000000;
        const HOUR = 3600000000;
        const DAY = 86400000000;

        if (duration / DAY >= 1.0) {
            return `${duration / DAY} days`;
        }
        if (duration / HOUR >= 1.0) {
            return `${duration / HOUR} hours`;
        }
        if (duration / MINUTE >= 1.0) {
            return `${duration / MINUTE} minutes`;
        }
        if (duration / SECOND >= 1.0) {
            return `${duration / SECOND} seconds`;
        }

        return `${duration / MILLISECOND} milliseconds`;
    }

    function barStyle(barTimespan: Timespan) {
        let current_timespan = props.timespan;
        let [start, end] = current_timespan;
        let duration = end - start;

        let [bar_start, bar_end] = barTimespan;

        let left = (bar_start - start) / duration;
        let right = (end - bar_end) / duration;

        return {
            left: `calc(${left * 100}% + 2px)`,
            right: `calc(${right * 100}% + 2px)`,
        };
    }

    return <GraphContainer {...props} height={barHeightMax()} stats={{ "bar-width": barSize().toString() }}>
        <For each={bars()}>
            {([span, bar]) => {
                if (bar == null) {
                    return <span class="span-count-graph-bar" style={barStyle(span)}></span>;
                } else {
                    return <span class="span-count-graph-bar" style={barStyle(span)} title={`${bar[0] + bar[1] + bar[2] + bar[3] + bar[4]} total`}>
                        <Show when={bar[0] != 0}><div class="span-count-graph-bar-level-0" style={{ height: `${bar[0] / barHeightMax() * 100}%` }}></div></Show>
                        <Show when={bar[1] != 0}><div class="span-count-graph-bar-level-1" style={{ height: `${bar[1] / barHeightMax() * 100}%` }}></div></Show>
                        <Show when={bar[2] != 0}><div class="span-count-graph-bar-level-2" style={{ height: `${bar[2] / barHeightMax() * 100}%` }}></div></Show>
                        <Show when={bar[3] != 0}><div class="span-count-graph-bar-level-3" style={{ height: `${bar[3] / barHeightMax() * 100}%` }}></div></Show>
                        <Show when={bar[4] != 0}><div class="span-count-graph-bar-level-4" style={{ height: `${bar[4] / barHeightMax() * 100}%` }}></div></Show>
                        <Show when={bar[5] != 0}><div class="span-count-graph-bar-level-5" style={{ height: `${bar[5] / barHeightMax() * 100}%` }}></div></Show>
                    </span>;
                }
            }}
        </For>
    </GraphContainer>;
}

const BUCKET_COUNT = 40;
const BUCKET_SIZES = [
    1000,   // 1ms
    2000,   // 2ms
    5000,   // 5ms
    10000,  // 10ms
    20000,  // 20ms
    50000,  // 50ms
    100000, // 100ms
    200000, // 200ms
    500000, // 500ms
    1000000,     // 1s
    2000000,     // 2s
    5000000,     // 5s
    10000000,    // 10s
    30000000,    // 30s
    60000000,    // 1m
    120000000,   // 2m
    300000000,   // 5m
    600000000,   // 10m
    1800000000,  // 30m
    3600000000,  // 1h
    10800000000, // 3h
    21600000000, // 6h
    43200000000, // 12h
    86400000000, // 1d
    172800000000,// 2d
    259200000000,// 3d
    432000000000,// 5d
    864000000000,// 10d
];

function indexOfSmallest<T>(a: T[], f: (a: T) => number) {
    let lowest_id = 0;
    let lowest_value = f(a[0]);
    for (let i = 1; i < a.length; i++) {
        let current_value = f(a[i]);
        if (current_value < lowest_value) {
            lowest_id = i;
            lowest_value = current_value;
        }
    }
    return lowest_id;
}

function calcBucketSizes(start: number, end: number): Timespan[] {
    let duration = end - start;

    // get the bucket size that gives us closest to BUCKET_COUNT divisions
    let bucket_size_idx = indexOfSmallest(BUCKET_SIZES, size => Math.abs(BUCKET_COUNT - duration / size));
    let bucket_size = BUCKET_SIZES[bucket_size_idx];
    let bucket_start = Math.floor(start / bucket_size) * bucket_size;

    let buckets = [] as Timespan[];
    while (bucket_start < end) {
        let bucket_end = bucket_start + bucket_size;
        buckets.push([bucket_start, bucket_end]);
        bucket_start = bucket_end;
    }

    return buckets;
}