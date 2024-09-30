import { createSignal, Show } from "solid-js";

import { InstanceDetailPane } from "../components/detail-pane";
import { FilterInput } from "../components/filter-input";
import { ScreenHeader } from "../components/screen-header";
import { Input, Instance, parseInstanceFilter } from '../invoke';
import { PartialFilter, PositionedInstance, Timespan } from "../models";
import { ColumnDef, INHERENT, Table } from "../components/table";
import { InstanceGraph } from "../components/instance-graph";

import './instances-screen.css';

export type InstancesScreenProps = {
    filter: Input[],
    setFilter: (filter: Input[]) => void,
    timespan: Timespan,
    setTimespan: (timespan: Timespan) => void,

    columns: ColumnDef<Instance>[],
    columnWidths: string[],
    columnUpdate: (i: number, def: ColumnDef<Instance>) => void,
    columnUpdateWidth: (i: number, width: string) => void,
    columnInsert: (i: number, def: ColumnDef<Instance>) => void,
    columnRemove: (i: number) => void,

    getInstances: (filter: PartialFilter, wait?: boolean) => Promise<Instance[] | null>,
    getPositionedInstances: (filter: PartialFilter, wait?: boolean) => Promise<PositionedInstance[] | null>,
};

export function InstancesScreen(props: InstancesScreenProps) {
    const [selectedRow, setSelectedRow] = createSignal<Instance | null>(null);
    const [hoveredRow, setHoveredRow] = createSignal<Instance | null>(null);
    const [count, setCount] = createSignal<[number, boolean]>([0, false]);

    return (<div class="instances-screen">
        <ScreenHeader
            screenKind="instances"
            {...props}
            count={count()}
            countThresholds={[1000, 5000]}
            timeControlsEnabled={true}
            live={false}
            setLive={() => { }}
        />

        <FilterInput predicates={props.filter} updatePredicates={props.setFilter} parse={parseInstanceFilter} />

        <InstanceGraph
            filter={props.filter}
            timespan={props.timespan}
            updateTimespan={props.setTimespan}
            getPositionedInstances={props.getPositionedInstances}
            setCount={setCount}
            hoveredRow={hoveredRow()}
        />

        <div class="instances-screen-content">
            <Table<Instance>
                timespan={props.timespan}
                columns={props.columns}
                columnWidths={props.columnWidths}
                columnUpdate={props.columnUpdate}
                columnUpdateWidth={props.columnUpdateWidth}
                columnInsert={props.columnInsert}
                columnRemove={props.columnRemove}
                columnDefault={INHERENT('id')}
                columnMin={3}
                selectedRow={selectedRow()}
                setSelectedRow={setSelectedRow}
                hoveredRow={hoveredRow()}
                setHoveredRow={setHoveredRow}
                getEntries={props.getInstances}
            />

            <Show when={selectedRow()}>
                {row => <InstanceDetailPane timespan={props.timespan} instance={row()} updateSelectedRow={setSelectedRow} />}
            </Show>
        </div>
    </div>);
}
