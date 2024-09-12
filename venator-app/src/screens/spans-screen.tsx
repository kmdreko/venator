import { createSignal, Show } from "solid-js";

import { SpanDetailPane } from "../components/detail-pane";
import { FilterInput } from "../components/filter-input";
import { ScreenHeader } from "../components/screen-header";
import { FilterPredicate, parseSpanFilter, Span } from '../invoke';
import { PartialFilter, PositionedSpan, Timespan } from "../models";
import { ColumnDef, INHERENT, Table } from "../components/table";
import { SpanGraph } from "../components/span-graph";

import './spans-screen.css';

export type SpansScreenProps = {
    filter: FilterPredicate[],
    setFilter: (filter: FilterPredicate[]) => void,
    timespan: Timespan,
    setTimespan: (timespan: Timespan) => void,

    columns: ColumnDef<Span>[],
    columnWidths: string[],
    columnUpdate: (i: number, def: ColumnDef<Span>) => void,
    columnUpdateWidth: (i: number, width: string) => void,
    columnInsert: (i: number, def: ColumnDef<Span>) => void,
    columnRemove: (i: number) => void,

    getSpans: (filter: PartialFilter, wait?: boolean) => Promise<Span[] | null>,
    getPositionedSpans: (filter: PartialFilter, wait?: boolean) => Promise<PositionedSpan[] | null>,
};

export function SpansScreen(props: SpansScreenProps) {
    const [selectedRow, setSelectedRow] = createSignal<Span | null>(null);
    const [hoveredRow, setHoveredRow] = createSignal<Span | null>(null);
    const [count, setCount] = createSignal<[number, boolean]>([0, false]);

    return (<div class="spans-screen">
        <ScreenHeader
            screenKind="spans"
            {...props}
            count={count()}
            countThresholds={[1000, 5000]}
            timeControlsEnabled={true}
            live={false}
            setLive={() => { }}
        />

        <FilterInput predicates={props.filter} updatePredicates={props.setFilter} parse={parseSpanFilter} />

        <SpanGraph
            filter={props.filter}
            timespan={props.timespan}
            updateTimespan={props.setTimespan}
            getPositionedSpans={props.getPositionedSpans}
            setCount={setCount}
            hoveredRow={hoveredRow()}
        />

        <div class="spans-screen-content">
            <Table<Span>
                timespan={props.timespan}
                columns={props.columns}
                columnWidths={props.columnWidths}
                columnUpdate={props.columnUpdate}
                columnUpdateWidth={props.columnUpdateWidth}
                columnInsert={props.columnInsert}
                columnRemove={props.columnRemove}
                columnDefault={INHERENT('name')}
                columnMin={3}
                selectedRow={selectedRow()}
                setSelectedRow={setSelectedRow}
                hoveredRow={hoveredRow()}
                setHoveredRow={setHoveredRow}
                getEntries={props.getSpans}
            />

            <Show when={selectedRow()}>
                {row => <SpanDetailPane timespan={props.timespan} span={row()} updateSelectedRow={setSelectedRow} />}
            </Show>
        </div>
    </div>);
}
