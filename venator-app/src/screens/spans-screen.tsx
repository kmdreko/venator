import { createSignal, Show } from "solid-js";

import { SpanDetailPane } from "../components/detail-pane";
import { FilterInput } from "../components/filter-input";
import { ScreenHeader } from "../components/screen-header";
import { Input, parseSpanFilter, Span } from '../invoke';
import { PartialFilter, PositionedSpan, Timespan } from "../models";
import { ColumnDef, getColumnDef, INHERENT, Table } from "../components/table";
import { SpanGraph } from "../components/span-graph";

import './spans-screen.css';

export type SpansScreenProps = {
    filter: Input[],
    setFilter: (filter: Input[]) => void,
    addToFilter: (filter: Input[]) => void,
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

    selected: Span | null,
    setSelected: (e: Span | null) => void,
};

export function SpansScreen(props: SpansScreenProps) {
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
                selectedRow={props.selected}
                setSelectedRow={props.setSelected}
                hoveredRow={hoveredRow()}
                setHoveredRow={setHoveredRow}
                getEntries={props.getSpans}
            />

            <Show when={props.selected}>
                {row => <SpanDetailPane
                    timespan={props.timespan}
                    span={row()}
                    updateSelectedRow={props.setSelected}
                    addToFilter={async f => props.addToFilter(await parseSpanFilter(f))}
                    addColumn={c => props.columnInsert(-1, getColumnDef(c))}
                />}
            </Show>
        </div>
    </div>);
}
