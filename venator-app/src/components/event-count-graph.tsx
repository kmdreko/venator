import { batch, createEffect, createSignal, For, Show, untrack } from "solid-js";
import { Timestamp, Event, Input } from "../invoke";
import { Counts, PartialEventCountFilter, Timespan } from "../models";

import "./event-count-graph.css";

export type EventCountGraphProps = {
    filter: Input[],
    timespan: Timespan,
    updateTimespan: (new_timespan: Timespan) => void,
    hoveredRow: Event | null,

    getEventCounts: (filter: PartialEventCountFilter, wait?: boolean, cache?: boolean) => Promise<Counts | null>,

    setCount: (count: [number, boolean]) => void,
};

export function EventCountGraph(props: EventCountGraphProps) {
    const [bars, setBars] = createSignal<[Timespan, Counts | null][]>([]);
    const [barHeightMax, setBarHeightMax] = createSignal(1);
    const [barHeightMarkers, setBarHeightMarkers] = createSignal(10);
    const [barTimeMarkers, setBarTimeMarkers] = createSignal<[number, string][]>([]);
    const [mouseTime, setMouseTime] = createSignal<number | null>(null);
    const [zoomRange, setZoomRange] = createSignal<[number, number] | null>(null);

    function cursor() {
        return mouseTime() ?? props.hoveredRow?.timestamp;
    }

    function cursorStyle(cursor: number): { left: string } {
        let current_bars = bars();
        let first_bar_start = current_bars[0][0][0];
        let last_bar_end = current_bars[current_bars.length - 1][0][1];
        let bar_duration = last_bar_end - first_bar_start;

        let left = (cursor - first_bar_start) / bar_duration;

        return { left: `calc(${left * 100}% - 1px)` }
    }

    function selectionStyle([zstart, zend]: [number, number]): { left: string, right: string } {
        let [zrstart, zrend] = (zend > zstart) ? [zstart, zend] : [zend, zstart];

        let current_bars = bars();
        let first_bar_start = current_bars[0][0][0];
        let last_bar_end = current_bars[current_bars.length - 1][0][1];
        let bar_duration = last_bar_end - first_bar_start;

        let left = (zrstart - first_bar_start) / bar_duration;
        let right = (last_bar_end - zrend) / bar_duration;

        return {
            left: `${left * 100}%`,
            right: `${right * 100}%`,
        };
    }

    createEffect(async () => {
        let current_filter = props.filter;
        let current_timespan = props.timespan;

        const buckets = calcBucketSizes(current_timespan[0], current_timespan[1]);
        const time_markers = calcTimeMarkers(current_timespan[0], current_timespan[1]);

        let initial_height = 1;
        let initial_bars = buckets.map(span => [span, null] as [Timespan, Counts | null]);
        let initial_count = 0;
        for (let i in buckets) {
            let [start, end] = buckets[i];
            let current_bar = await props.getEventCounts({ start, end }, false);

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
            let [height, height_markers] = getHeightAndMarkers(initial_height);

            props.setCount([initial_count, false]);
            setBars(initial_bars);
            setBarTimeMarkers(time_markers);
            setBarHeightMax(height);
            setBarHeightMarkers(height_markers);
        });

        let count = 0;
        for (let i in buckets) {
            // await new Promise(resolve => setTimeout(resolve, 5));

            let [start, end] = buckets[i];
            let new_bar = (await props.getEventCounts({ start, end }))!;
            if (start >= current_timespan[0] && end <= current_timespan[1]) {
                count += new_bar[0] + new_bar[1] + new_bar[2] + new_bar[3] + new_bar[4];
            } else {
                let p_start = Math.max(start, current_timespan[0]);
                let p_end = Math.min(end, current_timespan[1]);
                let partial_bar = (await props.getEventCounts({ start: p_start, end: p_end }, true, false))!;
                count += partial_bar[0] + partial_bar[1] + partial_bar[2] + partial_bar[3] + partial_bar[4];
            }

            if (current_timespan != props.timespan || current_filter != props.filter) {
                return;
            }

            let current_bars = untrack(bars);
            if (current_bars[i][1] == new_bar) continue;
            let updated_bars = [...current_bars];
            updated_bars[i] = [updated_bars[i][0], new_bar];
            setBars(updated_bars);
            props.setCount([count, false]);

            let total = new_bar[0] + new_bar[1] + new_bar[2] + new_bar[3] + new_bar[4];
            if (total > untrack(barHeightMax)) {
                let [height, markers] = getHeightAndMarkers(total);
                setBarHeightMax(height);
                setBarHeightMarkers(markers);
            }
        }

        props.setCount([count, true]);
    });

    function offsets(): { left?: string, right?: string, width?: string } {
        let current_bars = bars();
        if (current_bars.length == 0) {
            return {};
        }

        let [start, end] = props.timespan;
        let duration = end - start;

        let first_bar_start = current_bars[0][0][0];
        let last_bar_end = current_bars[current_bars.length - 1][0][1];

        let left = (start - first_bar_start) / duration;
        let right = (last_bar_end - end) / duration;

        return {
            left: `-${left * 100}%`,
            width: `${(1 + left + right) * 100}%`,
        };
    }

    function timestampMarkerOffset(timestamp: Timestamp): { left?: string } {
        let [start, end] = props.timespan;
        let duration = end - start;

        let left = (timestamp - start) / duration;

        return { left: `${left * 100}%` };
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

    function wheel(this: HTMLElement, e: WheelEvent) {
        if (e.deltaY == 0.0) {
            return;
        }

        let bias = (e.pageX - this.offsetLeft) / this.offsetWidth;
        let scale = 1 + e.deltaY / 1000;
        let [start, end] = props.timespan;
        let duration = end - start;
        let middle = start * (1 - bias) + end * bias;
        let new_duration = duration * scale;
        let new_start = middle - new_duration * bias;
        let new_end = middle + new_duration * (1 - bias);
        props.updateTimespan([new_start, new_end]);

        e.preventDefault();
    }

    let move_requested: number | null;
    let prev_mouse_pos: [number, number] | null;
    function mousedrag(this: HTMLElement, e: MouseEvent) {
        let self = this;

        if (move_requested != null) {
            return;
        }

        move_requested = requestAnimationFrame(() => {
            move_requested = null;

            e.preventDefault();

            if ((e.buttons & 4) == 0) {
                prev_mouse_pos = null;
                return;
            }

            if (!prev_mouse_pos) {
                prev_mouse_pos = [e.screenX, e.screenY];
                return;
            }

            let delta_x = e.screenX - prev_mouse_pos[0];
            //let delta_y = e.screenY - prev_mouse_pos[1];
            prev_mouse_pos = [e.screenX, e.screenY];

            let [start, end] = props.timespan;
            let duration = end - start;
            let timespan_shift = -(delta_x / self.offsetWidth) * duration;
            let new_start = start + timespan_shift;
            let new_end = end + timespan_shift;

            props.updateTimespan([new_start, new_end]);
        })
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

            let proportion = (e.pageX - self.offsetLeft) / self.offsetWidth;

            let current_bars = bars();
            let first_bar_start = current_bars[0][0][0];
            let last_bar_end = current_bars[current_bars.length - 1][0][1];
            let bar_duration = last_bar_end - first_bar_start;

            let time = proportion * bar_duration + first_bar_start;

            setMouseTime(time);

            let existingZoom = zoomRange();
            if (existingZoom) {
                setZoomRange([existingZoom[0], time]);
            }
        });
    }

    function mouseout(this: HTMLElement, _e: MouseEvent) {
        setMouseTime(null);
        setZoomRange(null);
        if (mouse_set_requested != null) {
            cancelAnimationFrame(mouse_set_requested);
            mouse_set_requested = null;
        }
    }

    function mousedown(this: HTMLElement, e: MouseEvent) {
        if (e.button != 0) {
            return;
        }

        e.preventDefault();

        let proportion = (e.pageX - this.offsetLeft) / this.offsetWidth;

        let current_bars = bars();
        let first_bar_start = current_bars[0][0][0];
        let last_bar_end = current_bars[current_bars.length - 1][0][1];
        let bar_duration = last_bar_end - first_bar_start;

        let time = proportion * bar_duration + first_bar_start;
        setZoomRange([time, time]);
    }

    function mouseup(this: HTMLElement, e: MouseEvent) {
        if (e.button != 0) {
            return;
        }

        let range = zoomRange();
        if (range == null) {
            return;
        }

        let [new_start, new_end] = range;
        if (new_start == new_end) {
            setZoomRange(null);
            return;
        }

        let timespan: [number, number] = (new_end > new_start) ? [new_start, new_end] : [new_end, new_start];

        setZoomRange(null);
        props.updateTimespan(timespan);
    }

    return <div class="event-count-graph-container" onwheel={wheel} onmousemove={mousedrag}>
        <div class="event-count-graph-stats">
            <span class="stat-name">height:</span>
            <span class="stat-value">{barHeightMax()}</span>
            <span class="stat-name">bar-width:</span>
            <span class="stat-value">{barSize()}</span>
            <Show when={mouseTime() != null}>
                <span class="stat-name">cursor:</span>
                <span class="stat-value">{formatTimestamp(mouseTime()!)}</span>
            </Show>
        </div>
        <div class="event-count-graph" style={offsets()} onmouseenter={mousemove} onmousemove={mousemove} onmouseleave={mouseout} onmousedown={mousedown} onmouseup={mouseup}>
            <div class="event-count-graph-y-lines">
                <For each={Array(barHeightMarkers() + 1)}>
                    {() => <div class="event-count-graph-y-line"></div>}
                </For>
            </div>
            <For each={bars()}>
                {([_span, bar]) => {
                    if (bar == null) {
                        return <span class="event-count-graph-bar"></span>;
                    } else {
                        return <span class="event-count-graph-bar" title={`${bar[0] + bar[1] + bar[2] + bar[3] + bar[4]} total`}>
                            <Show when={bar[0] != 0}><div class="event-count-graph-bar-level-0" style={{ height: `${bar[0] / barHeightMax() * 100}%` }}></div></Show>
                            <Show when={bar[1] != 0}><div class="event-count-graph-bar-level-1" style={{ height: `${bar[1] / barHeightMax() * 100}%` }}></div></Show>
                            <Show when={bar[2] != 0}><div class="event-count-graph-bar-level-2" style={{ height: `${bar[2] / barHeightMax() * 100}%` }}></div></Show>
                            <Show when={bar[3] != 0}><div class="event-count-graph-bar-level-3" style={{ height: `${bar[3] / barHeightMax() * 100}%` }}></div></Show>
                            <Show when={bar[4] != 0}><div class="event-count-graph-bar-level-4" style={{ height: `${bar[4] / barHeightMax() * 100}%` }}></div></Show>
                        </span>;
                    }
                }}
            </For>
            <Show when={zoomRange() != null}>
                <div class="event-count-graph-selection" style={selectionStyle(zoomRange()!)}></div>
            </Show>
            <Show when={cursor() != null}>
                <div class="event-count-graph-cursor" style={cursorStyle(cursor()!)}></div>
            </Show>
        </div>
        <div class="event-count-graph-x-axis">
            <For each={barTimeMarkers()}>
                {([time, display]) => <span class="event-count-graph-x-axis-marker" style={timestampMarkerOffset(time)}>{display}</span>}
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
