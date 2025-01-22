import { createEffect, createSignal, For, untrack } from "solid-js";
import { Input, Span } from "../invoke";
import { PartialFilter, Timespan, PositionedSpan } from "../models";
import { GraphContainer } from "./graph-container";

import "./span-graph.css";

export type SpanGraphProps = {
    filter: Input[],
    timespan: Timespan,
    updateTimespan: (new_timespan: Timespan) => void,
    hoveredRow: Span | null,

    setCount: (count: [number, boolean]) => void,

    getPositionedSpans: (filter: PartialFilter, wait?: boolean) => Promise<PositionedSpan[] | null>,
};

let CACHE_START_LAST = 0;
let CACHE_START_DELAY_MS = 250;

export function SpanGraph(props: SpanGraphProps) {
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
            if (props.timespan != current_timespan || current_filter != props.filter) {
                return;
            }
        }

        let previous: number | undefined;

        CACHE_START_LAST = now;
        while (true) {
            let new_spans = (await props.getPositionedSpans({ order: 'asc', start, end, previous }))!;
            if (props.timespan != current_timespan || current_filter != props.filter) {
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

