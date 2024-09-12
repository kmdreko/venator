import { createSignal, Show } from "solid-js";

import { EventDetailPane } from "../components/detail-pane";
import { EventCountGraph } from "../components/event-count-graph";
import { FilterInput } from "../components/filter-input";
import { ScreenHeader } from "../components/screen-header";
import { Event, FilterPredicate, parseEventFilter } from '../invoke';
import { Counts, PartialEventCountFilter, PartialFilter, Timespan } from "../models";
import { ATTRIBUTE, ColumnDef, Table } from "../components/table";

import './events-screen.css';

export type EventsScreenProps = {
    filter: FilterPredicate[],
    setFilter: (filter: FilterPredicate[]) => void,
    timespan: Timespan,
    setTimespan: (timespan: Timespan) => void,

    columns: ColumnDef<Event>[],
    columnWidths: string[],
    columnUpdate: (i: number, def: ColumnDef<Event>) => void,
    columnUpdateWidth: (i: number, width: string) => void,
    columnInsert: (i: number, def: ColumnDef<Event>) => void,
    columnRemove: (i: number) => void,

    getEvents: (filter: PartialFilter) => Promise<Event[]>,
    getEventCounts: (filter: PartialEventCountFilter, wait?: boolean, cache?: boolean) => Promise<Counts | null>,

    live: boolean,
    setLive: (live: boolean) => void,
};

export function EventsScreen(props: EventsScreenProps) {
    const [selectedRow, setSelectedRow] = createSignal<Event | null>(null);
    const [hoveredRow, setHoveredRow] = createSignal<Event | null>(null);
    const [count, setCount] = createSignal<[number, boolean]>([0, false]);

    return (<div class="events-screen">
        <ScreenHeader
            screenKind="events"
            timespan={props.timespan}
            setTimespan={t => props.setTimespan(t)}
            count={count()}
            countThresholds={[1000, 10000]}
            timeControlsEnabled={true}
            live={props.live}
            setLive={props.setLive}
        />

        <FilterInput predicates={props.filter} updatePredicates={props.setFilter} parse={parseEventFilter} />

        <EventCountGraph
            filter={props.filter}
            timespan={props.timespan}
            updateTimespan={t => props.setTimespan(t)}
            getEventCounts={props.getEventCounts}
            setCount={setCount}
            hoveredRow={hoveredRow()}
        />

        <div class="events-screen-content">
            <Table<Event>
                timespan={props.timespan}
                columns={props.columns}
                columnWidths={props.columnWidths}
                columnUpdate={props.columnUpdate}
                columnUpdateWidth={props.columnUpdateWidth}
                columnInsert={props.columnInsert}
                columnRemove={props.columnRemove}
                columnDefault={ATTRIBUTE("message")}
                columnMin={3}
                selectedRow={selectedRow()}
                setSelectedRow={setSelectedRow}
                hoveredRow={hoveredRow()}
                setHoveredRow={setHoveredRow}
                getEntries={props.getEvents}
            />

            <Show when={selectedRow()}>
                {row => <EventDetailPane timespan={props.timespan} event={row()} updateSelectedRow={setSelectedRow} />}
            </Show>
        </div>
    </div>);
}
