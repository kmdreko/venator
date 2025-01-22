import { createSignal, Show } from "solid-js";

import { EventDetailPane } from "../components/detail-pane";
import { EventCountGraph } from "../components/event-count-graph";
import { FilterInput } from "../components/filter-input";
import { ScreenHeader } from "../components/screen-header";
import { Event, Input, parseEventFilter, Timestamp } from '../invoke';
import { Counts, PartialCountFilter, PartialFilter, Timespan } from "../models";
import { ColumnDef, CONTENT, parseEventColumn, Table } from "../components/table";

import './events-screen.css';

export type EventsScreenProps = {
    raw_filter: Input[],
    filter: Input[],
    setFilter: (filter: Input[]) => void,
    addToFilter: (filter: Input[]) => void,
    timespan: Timespan,
    setTimespan: (timespan: Timespan) => void,

    columns: ColumnDef<Event>[],
    columnWidths: string[],
    columnUpdate: (i: number, def: ColumnDef<Event>) => void,
    columnUpdateWidth: (i: number, width: string) => void,
    columnMove: (i: number, to: number) => void,
    columnInsert: (i: number, def: ColumnDef<Event>) => void,
    columnRemove: (i: number) => void,

    getEvents: (filter: PartialFilter) => Promise<Event[]>,
    getEventCounts: (filter: PartialCountFilter, wait?: boolean, cache?: boolean) => Promise<Counts | null>,

    live: boolean,
    setLive: (live: boolean) => void,

    selected: Event | null,
    setSelected: (e: Event | null) => void,
};

export function EventsScreen(props: EventsScreenProps) {
    const [hoveredRow, setHoveredRow] = createSignal<Event | null>(null);
    const [count, setCount] = createSignal<[number, boolean]>([0, false]);

    async function getTimestampBefore(timestamp: Timestamp) {
        let events = await props.getEvents({
            order: 'desc',
            start: null,
            end: timestamp,
            limit: 1,
        });

        if (events.length == 0) {
            return null;
        }

        return events[0].timestamp;
    }

    async function getTimestampAfter(timestamp: Timestamp) {
        let events = await props.getEvents({
            order: 'asc',
            start: timestamp,
            end: null,
            limit: 1,
        });

        if (events.length == 0) {
            return null;
        }

        return events[0].timestamp;
    }

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
            getTimestampBefore={getTimestampBefore}
            getTimestampAfter={getTimestampAfter}
        />

        <FilterInput predicates={props.raw_filter} updatePredicates={props.setFilter} parse={parseEventFilter} />

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
                columnMove={props.columnMove}
                columnInsert={props.columnInsert}
                columnRemove={props.columnRemove}
                columnDefault={CONTENT}
                columnMin={3}
                selectedRow={props.selected}
                setSelectedRow={props.setSelected}
                hoveredRow={hoveredRow()}
                setHoveredRow={setHoveredRow}
                getEntries={props.getEvents}
                addToFilter={async f => props.addToFilter(await parseEventFilter(f))}
                columnParser={parseEventColumn}
            />

            <Show when={props.selected}>
                {row => <EventDetailPane
                    timespan={props.timespan}
                    event={row()}
                    updateSelectedRow={props.setSelected}
                    filter={props.filter}
                    addToFilter={async f => props.addToFilter(await parseEventFilter(f))}
                    addColumn={c => props.columnInsert(-1, parseEventColumn(c))}
                />}
            </Show>
        </div>
    </div>);
}
