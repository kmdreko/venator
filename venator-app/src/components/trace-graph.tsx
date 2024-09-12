import { batch, createEffect, createSignal, For, Show } from "solid-js";
import { Timestamp, Span, Event } from "../invoke";
import { PaginationFilter, Timespan } from "../models";

import "./trace-graph.css";

export type TraceGraphProps = {
    timespan: Timespan | null,
    hoveredRow: Event | Span | null,

    setCount: (count: [number, boolean]) => void,

    getEntries: (filter: PaginationFilter) => Promise<(Event | Span)[]>,
};

export function TraceGraph(props: TraceGraphProps) {
    const [entries, setEntries] = createSignal<(Event | Span)[]>([]);
    const [barHeightMax, setBarHeightMax] = createSignal(10);
    const [barHeightMarkers, setBarHeightMarkers] = createSignal(1);
    const [barTimeMarkers, setBarTimeMarkers] = createSignal<[number, string][]>([]);
    const [mouseTime, setMouseTime] = createSignal<[number, number | null] | null>(null);

    function cursor(): [number, number | null] | null {
        let mouse = mouseTime();
        if (mouse != null) {
            return mouse;
        } else if (props.hoveredRow != null) {
            let start = (props.hoveredRow as Span).created_at ?? (props.hoveredRow as Event).timestamp;
            let end = (props.hoveredRow as Span).closed_at ?? (props.hoveredRow as Event).timestamp;
            return [start, end];
        } else {
            return null;
        }
    }

    function cursorStyle(cursor: [number, number | null]): { left: string, right: string } {
        let [start, end] = props.timespan!;
        let duration = end - start;

        let left = (cursor[0] - start) / duration;
        let right = (cursor[1] == null) ? 0 : (end - cursor[1]) / duration;

        return { left: `calc(${left * 100}% - 1px)`, right: `calc(${right * 100}% - 1px)` };
    }

    createEffect(async () => {
        let current_timespan = props.timespan;
        if (current_timespan == null) {
            return;
        }

        let entries = await props.getEntries({ order: 'asc' });

        let [start, end] = current_timespan;

        const time_markers = calcTimeMarkers(start, end);
        let [height, height_markers] = getHeightAndMarkers(Math.max(entries.length, 10));

        batch(() => {
            setEntries(entries);
            setBarTimeMarkers(time_markers);
            setBarHeightMax(height);
            setBarHeightMarkers(height_markers);
        });
    });

    function timestampMarkerOffset(timestamp: Timestamp): { left?: string } {
        let current_timespan = props.timespan;
        if (current_timespan == null) {
            return {} as any;
        }

        let [start, end] = current_timespan;
        let duration = end - start;

        let left = (timestamp - start) / duration;

        return { left: `${left * 100}%` };
    }

    function position(entry: Event | Span, i: number): { top?: string, left: string, right?: string, height: string } {
        let current_timespan = props.timespan;
        if (current_timespan == null) {
            return {} as any;
        }

        let [start, end] = current_timespan;
        let duration = end - start;

        let height = Math.min(Math.max(60 / barHeightMax(), 2), 4);
        let top = i / barHeightMax() * 56 / 60;

        if ((entry as any).timestamp != undefined) {
            let event = entry as Event;
            let left = (event.timestamp - start) / duration;

            return {
                top: `${top * 100}%`,
                left: `${left * 100}%`,
                height: `${height}px`,
            };
        } else {
            let span = entry as Span;
            let left = (span.created_at - start) / duration;
            let right = (span.closed_at == null) ? 0.0 : (end - span.closed_at) / duration;

            return {
                top: `${top * 100}%`,
                left: `${left * 100}%`,
                right: `${right * 100}%`,
                height: `${height}px`,
            };
        }
    }

    let mouse_set_requested: number | null;
    function mousemove(this: HTMLElement, e: MouseEvent) {
        let self = this;

        if (mouse_set_requested != null) {
            return;
        }

        mouse_set_requested = requestAnimationFrame(() => {
            mouse_set_requested = null;

            e.preventDefault();

            if (props.timespan == null) {
                return;
            }

            let [start, end] = props.timespan;
            let duration = end - start;
            let proportion = (e.pageX - self.offsetLeft) / self.offsetWidth;

            let time = proportion * duration + start;

            setMouseTime([time, time]);
        });
    }

    function mouseout(this: HTMLElement, _e: MouseEvent) {
        setMouseTime(null);
        if (mouse_set_requested != null) {
            cancelAnimationFrame(mouse_set_requested);
            mouse_set_requested = null;
        }
    }

    return <div class="trace-graph-container">
        <div class="trace-graph-stats">{barHeightMax()} max
            <Show when={mouseTime() != null}>
                {' '}- {formatTimestamp(mouseTime()![0])}
            </Show>
        </div>
        <div class="trace-graph" onmouseenter={mousemove} onmousemove={mousemove} onmouseleave={mouseout}>
            <div class="trace-graph-y-lines">
                <For each={Array(barHeightMarkers() + 1)}>
                    {() => <div class="trace-graph-y-line"></div>}
                </For>
            </div>
            <For each={entries()}>
                {(entry, i) => (<span class={`trace-graph-bar level-${entry.level}`} style={position(entry, i())}></span>)}
            </For>
            <Show when={cursor() != null}>
                <div class="trace-graph-cursor" style={cursorStyle(cursor()!)}></div>
            </Show>
        </div>
        <div class="trace-graph-x-axis">
            <For each={barTimeMarkers()}>
                {([time, display]) => <span class="trace-graph-x-axis-marker" style={timestampMarkerOffset(time)}>{display}</span>}
            </For>
        </div>
    </div>;
}

function formatTimestamp(timestamp: number): string {
    var datetime = new Date(timestamp / 1000);
    return datetime.getFullYear() + "-" + (datetime.getMonth() + 1).toString().padStart(2, '0') + "-" +
        datetime.getDate().toString().padStart(2, '0') + " " + datetime.getHours().toString().padStart(2, '0') + ":" +
        datetime.getMinutes().toString().padStart(2, '0') + ":" + datetime.getSeconds().toString().padStart(2, '0') + "." +
        datetime.getMilliseconds().toString().padStart(3, '0');
}

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

function calcTimeMarkers(start: number, end: number): [Timestamp, string][] {
    let duration = end - start;

    let bucket_size_idx = indexOfSmallest(BUCKET_SIZES, size => Math.abs(5 - duration / size));
    let bucket_size = BUCKET_SIZES[bucket_size_idx];

    let marker_timstamp = Math.floor(start / bucket_size) * bucket_size;

    let prev_components = getTimeComponents(marker_timstamp - bucket_size);

    let markers: [Timestamp, string][] = [];
    while (marker_timstamp < end) {
        let components = getTimeComponents(marker_timstamp);

        if (components[0] != prev_components[0]) {
            markers.push([marker_timstamp, components[0]]);
        } else if (components[1] != prev_components[1]) {
            markers.push([marker_timstamp, components[1]]);
        } else if (components[2] != prev_components[2]) {
            markers.push([marker_timstamp, components[2]]);
        } else {
            markers.push([marker_timstamp, components[3]]);
        }

        prev_components = components;
        marker_timstamp += bucket_size;
    }

    return markers;
}

function getTimeComponents(timestamp: Timestamp): [string, string, string, string] {
    let datetime = new Date(timestamp / 1000);

    return [
        `${datetime.getFullYear()}-${(datetime.getMonth() + 1).toString().padStart(2, '0')}-${datetime.getDate().toString().padStart(2, '0')}`,
        `${datetime.toLocaleTimeString(undefined, { hour: '2-digit', minute: '2-digit' })}`,
        `${datetime.getSeconds()}s`,
        `${datetime.getMilliseconds()}ms`,
    ];
}

function getHeightAndMarkers(n: number): [number, number] {
    if (n < 5) {
        return [n, n];
    }

    let scale = Math.pow(10, Math.floor(Math.log10(n)) - 1);

    // 10 <= basis < 100
    let basis = n / scale;

    let basis_delimeter = 2;
    if (basis > 60) {
        basis_delimeter = 20;
    } else if (basis > 30) {
        basis_delimeter = 10;
    } else if (basis > 12) {
        basis_delimeter = 5;
    }

    let delimeter = basis_delimeter * scale;
    let markers = Math.ceil(n / delimeter);
    let height = markers * delimeter;

    return [height, markers];
}
