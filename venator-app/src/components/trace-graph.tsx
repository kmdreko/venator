import { createEffect, createSignal, For } from "solid-js";
import { Span, Event } from "../invoke";
import { PaginationFilter, Timespan } from "../models";

import "./trace-graph.css";
import { GraphContainer } from "./graph-container";

export type TraceGraphProps = {
    timespan: Timespan | null,
    hoveredRow: Event | Span | null,

    setCount: (count: [number, boolean]) => void,

    getEntries: (filter: PaginationFilter) => Promise<(Event | Span)[]>,
};

export function TraceGraph(props: TraceGraphProps) {
    const [entries, setEntries] = createSignal<(Event | Span)[]>([]);

    createEffect(async () => {
        let current_timespan = props.timespan;
        if (current_timespan == null) {
            return;
        }

        let entries = await props.getEntries({ order: 'asc' });
        setEntries(entries);
    });

    function barHeightMax() {
        return Math.max(entries().length, 10);
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

    return <GraphContainer {...props} timespan={props.timespan!} updateTimespan={() => { }} height={barHeightMax()}>
        <For each={entries()}>
            {(entry, i) => (<span class={`trace-graph-bar level-${entry.level}`} style={position(entry, i())}></span>)}
        </For>
    </GraphContainer>;
}

