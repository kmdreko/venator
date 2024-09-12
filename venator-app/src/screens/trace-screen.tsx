import { createEffect, createSignal, Show } from "solid-js";

import { EventDetailPane, SpanDetailPane } from "../components/detail-pane";
import { FilterInput } from "../components/filter-input";
import { ScreenHeader } from "../components/screen-header";
import { FilterPredicate, parseSpanFilter, Span, Event } from '../invoke';
import { PaginationFilter, Timespan } from "../models";
import { ATTRIBUTE, ColumnDef, COMBINED, INHERENT, Table } from "../components/table";
import { TraceGraph } from "../components/trace-graph";

import './trace-screen.css';
import { CollapsableContext } from "../context/collapsable";

export type TraceScreenProps = {
    filter: FilterPredicate[],
    setFilter: (filter: FilterPredicate[]) => void,
    timespan: Timespan | null,
    setTimespan: (timespan: Timespan) => void,

    columns: ColumnDef<Event | Span>[],
    columnWidths: string[],
    columnUpdate: (i: number, def: ColumnDef<Event | Span>) => void,
    columnUpdateWidth: (i: number, width: string) => void,
    columnInsert: (i: number, def: ColumnDef<Event | Span>) => void,
    columnRemove: (i: number) => void,

    getEntries: (filter: PaginationFilter) => Promise<(Event | Span)[]>,

    collapsed: { [id: string]: true },
    setCollapsed: (id: string, collapsed: boolean) => void,
};

export function TraceScreen(props: TraceScreenProps) {
    const [selectedRow, setSelectedRow] = createSignal<Event | Span | null>(null);
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

        <FilterInput predicates={props.filter} updatePredicates={props.setFilter} parse={parseSpanFilter} />

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
                        columnInsert={props.columnInsert}
                        columnRemove={props.columnRemove}
                        columnDefault={COMBINED(INHERENT('name'), ATTRIBUTE('message'))}
                        columnMin={3}
                        selectedRow={selectedRow()}
                        setSelectedRow={setSelectedRow}
                        hoveredRow={hoveredRow()}
                        setHoveredRow={setHoveredRow}
                        getEntries={getUncollapsedEntries}
                    />
                </CollapsableContext.Provider>
            </Show>
            <Show when={(selectedRow() as any)?.timestamp}>
                <EventDetailPane timespan={props.timespan} event={selectedRow() as Event} updateSelectedRow={setSelectedRow} />
            </Show>
            <Show when={(selectedRow() as any)?.created_at}>
                <SpanDetailPane timespan={props.timespan} span={selectedRow() as Span} updateSelectedRow={setSelectedRow} />
            </Show>
        </div>
    </div>);
}
