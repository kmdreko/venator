import { createSignal, Show } from "solid-js";

import { ConnectionDetailPane } from "../components/detail-pane";
import { FilterInput } from "../components/filter-input";
import { ScreenHeader } from "../components/screen-header";
import { Input, Connection, parseConnectionFilter, Timestamp } from '../invoke';
import { PartialFilter, PositionedConnection, Timespan } from "../models";
import { ColumnDef, INHERENT, parseConnectionColumn, Table } from "../components/table";
import { ConnectionGraph } from "../components/connection-graph";

import './connections-screen.css';

export type ConnectionsScreenProps = {
    raw_filter: Input[],
    filter: Input[],
    setFilter: (filter: Input[]) => void,
    addToFilter: (filter: Input[]) => void,
    timespan: Timespan,
    setTimespan: (timespan: Timespan) => void,

    columns: ColumnDef<Connection>[],
    columnWidths: string[],
    columnUpdate: (i: number, def: ColumnDef<Connection>) => void,
    columnUpdateWidth: (i: number, width: string) => void,
    columnMove: (i: number, to: number) => void,
    columnInsert: (i: number, def: ColumnDef<Connection>) => void,
    columnRemove: (i: number) => void,

    getConnections: (filter: PartialFilter, wait?: boolean) => Promise<Connection[] | null>,
    getPositionedConnections: (filter: PartialFilter, wait?: boolean) => Promise<PositionedConnection[] | null>,

    selected: Connection | null,
    setSelected: (e: Connection | null) => void,
};

export function ConnectionsScreen(props: ConnectionsScreenProps) {
    const [hoveredRow, setHoveredRow] = createSignal<Connection | null>(null);
    const [count, setCount] = createSignal<[number, boolean]>([0, false]);

    async function getTimestampBefore(timestamp: Timestamp) {
        // TODO: this gets the most recent "connected_at" and not the most
        // recent "disconnected_at" that a user is probably expecting, however 
        // at the moment that is more complicated to get and unclear what to
        // return if the timestamp is intersecting an connection

        let connections = await props.getConnections({
            order: 'desc',
            start: null,
            end: timestamp,
            limit: 1,
        });

        if (connections == null || connections.length == 0) {
            return null;
        }

        return connections[0].connected_at;
    }

    async function getTimestampAfter(timestamp: Timestamp) {
        // TODO: this will return a timestamp "before" the one provided if it
        // intersects an connection

        let connections = await props.getConnections({
            order: 'asc',
            start: timestamp,
            end: null,
            limit: 1,
        });

        if (connections == null || connections.length == 0) {
            return null;
        }

        return connections[0].connected_at;
    }

    return (<div class="connections-screen">
        <ScreenHeader
            screenKind="connections"
            {...props}
            count={count()}
            countThresholds={[1000, 5000]}
            timeControlsEnabled={true}
            live={false}
            setLive={() => { }}
            getTimestampBefore={getTimestampBefore}
            getTimestampAfter={getTimestampAfter}
        />

        <FilterInput predicates={props.raw_filter} updatePredicates={props.setFilter} parse={parseConnectionFilter} />

        <ConnectionGraph
            filter={props.filter}
            timespan={props.timespan}
            updateTimespan={props.setTimespan}
            getPositionedConnections={props.getPositionedConnections}
            setCount={setCount}
            hoveredRow={hoveredRow()}
        />

        <div class="connections-screen-content">
            <Table<Connection>
                timespan={props.timespan}
                columns={props.columns}
                columnWidths={props.columnWidths}
                columnUpdate={props.columnUpdate}
                columnUpdateWidth={props.columnUpdateWidth}
                columnMove={props.columnMove}
                columnInsert={props.columnInsert}
                columnRemove={props.columnRemove}
                columnDefault={INHERENT('id')}
                columnMin={2}
                selectedRow={props.selected}
                setSelectedRow={props.setSelected}
                hoveredRow={hoveredRow()}
                setHoveredRow={setHoveredRow}
                getEntries={props.getConnections}
                addToFilter={async f => props.addToFilter(await parseConnectionFilter(f))}
                columnParser={parseConnectionColumn}
            />

            <Show when={props.selected}>
                {row => <ConnectionDetailPane
                    timespan={props.timespan}
                    connection={row()}
                    updateSelectedRow={props.setSelected}
                    filter={props.filter}
                    addToFilter={async f => props.addToFilter(await parseConnectionFilter(f))}
                    addColumn={c => props.columnInsert(-1, parseConnectionColumn(c))}
                />}
            </Show>
        </div>
    </div>);
}
