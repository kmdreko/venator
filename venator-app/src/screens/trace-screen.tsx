import { createEffect, createSignal, Show } from "solid-js";

import { EventDetailPane, SpanDetailPane } from "../components/detail-pane";
import { FilterInput } from "../components/filter-input";
import { ScreenHeader } from "../components/screen-header";
import { parseSpanFilter, Span, Event, Input } from '../invoke';
import { PaginationFilter, Timespan } from "../models";
import { ATTRIBUTE, ColumnDef, COMBINED, INHERENT, parseTraceColumn, Table } from "../components/table";
import { TraceGraph } from "../components/trace-graph";

import './trace-screen.css';
import { CollapsableContext } from "../context/collapsable";

export type TraceScreenProps = {
    raw_filter: Input[],
    filter: Input[],
    setFilter: (filter: Input[]) => void,
    addToFilter: (filter: Input[]) => void,
    timespan: Timespan | null,
    setTimespan: (timespan: Timespan) => void,

    columns: ColumnDef<Event | Span>[],
    columnWidths: string[],
    columnUpdate: (i: number, def: ColumnDef<Event | Span>) => void,
    columnUpdateWidth: (i: number, width: string) => void,
    columnMove: (i: number, to: number) => void,
    columnInsert: (i: number, def: ColumnDef<Event | Span>) => void,
    columnRemove: (i: number) => void,

    getEntries: (filter: PaginationFilter) => Promise<(Event | Span)[]>,

    collapsed: { [id: string]: true },
    setCollapsed: (id: string, collapsed: boolean) => void,

    selected: Event | Span | null,
    setSelected: (e: Event | Span | null) => void,
};

export function TraceScreen(props: TraceScreenProps) {
    const [hoveredRow, setHoveredRow] = createSignal<Event | Span | null>(null);
    const [count, setCount] = createSignal<[number, boolean]>([0, false]);

    async function getUncollapsedEntries(filter: PaginationFilter): Promise<(Event | Span)[]> {
        // this relies on the fact that props.getEntries mostly ignores the
        // filter and returns either all or none entries
        let entries = await props.getEntries(filter);

        return entries.filter(e => e.ancestors.every(a => props.collapsed[a.id] != true));
    }

    createEffect(() => {
        props.filter;
        props.getEntries({ order: 'asc' });
    });

    return (<div class="trace-screen">
        <ScreenHeader
            screenKind="trace"
            {...props}
            count={count()}
            countThresholds={[1000, 5000]}
            timeControlsEnabled={false}
            live={false}
            setLive={() => { }}
        />

        <FilterInput predicates={props.raw_filter} updatePredicates={props.setFilter} parse={parseSpanFilter} />

        <TraceGraph
            timespan={props.timespan}
            getEntries={getUncollapsedEntries}
            setCount={setCount}
            hoveredRow={hoveredRow()}
        />

        <div class="trace-screen-content">
            <Show when={props.timespan != null}>
                <CollapsableContext.Provider value={{
                    isCollapsed: id => props.collapsed[id] == true,
                    collapse: (id, c) => props.setCollapsed(id, c),
                }}>
                    <Table
                        timespan={props.timespan!}
                        columns={props.columns}
                        columnWidths={props.columnWidths}
                        columnUpdate={props.columnUpdate}
                        columnUpdateWidth={props.columnUpdateWidth}
                        columnMove={props.columnMove}
                        columnInsert={props.columnInsert}
                        columnRemove={props.columnRemove}
                        columnDefault={COMBINED(INHERENT('name'), ATTRIBUTE('message'))}
                        columnMin={3}
                        selectedRow={props.selected}
                        setSelectedRow={props.setSelected}
                        hoveredRow={hoveredRow()}
                        setHoveredRow={setHoveredRow}
                        getEntries={getUncollapsedEntries}
                        addToFilter={() => { }} // TODO: need way to ensure filter satisfies both events and spans
                        columnParser={parseTraceColumn}
                    />
                </CollapsableContext.Provider>
            </Show>
            <Show when={(props.selected as any)?.timestamp}>
                <EventDetailPane
                    timespan={props.timespan}
                    event={props.selected as Event}
                    updateSelectedRow={props.setSelected}
                    filter={props.filter}
                    addToFilter={() => { }} // TODO: need way to ensure filter satisfies both events and spans
                    addColumn={c => props.columnInsert(-1, parseTraceColumn(c))}
                />
            </Show>
            <Show when={(props.selected as any)?.created_at}>
                <SpanDetailPane
                    timespan={props.timespan}
                    span={props.selected as Span}
                    updateSelectedRow={props.setSelected}
                    filter={props.filter}
                    addToFilter={() => { }} // TODO: need way to ensure filter satisfies both events and spans
                    addColumn={c => props.columnInsert(-1, parseTraceColumn(c))}
                />
            </Show>
        </div>
    </div>);
}
