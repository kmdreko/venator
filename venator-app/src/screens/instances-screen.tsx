import { createSignal, Show } from "solid-js";

import { InstanceDetailPane } from "../components/detail-pane";
import { FilterInput } from "../components/filter-input";
import { ScreenHeader } from "../components/screen-header";
import { Input, Instance, parseInstanceFilter } from '../invoke';
import { PartialFilter, PositionedInstance, Timespan } from "../models";
import { ColumnDef, INHERENT, parseInstanceColumn, Table } from "../components/table";
import { InstanceGraph } from "../components/instance-graph";

import './instances-screen.css';

export type InstancesScreenProps = {
    raw_filter: Input[],
    filter: Input[],
    setFilter: (filter: Input[]) => void,
    addToFilter: (filter: Input[]) => void,
    timespan: Timespan,
    setTimespan: (timespan: Timespan) => void,

    columns: ColumnDef<Instance>[],
    columnWidths: string[],
    columnUpdate: (i: number, def: ColumnDef<Instance>) => void,
    columnUpdateWidth: (i: number, width: string) => void,
    columnMove: (i: number, to: number) => void,
    columnInsert: (i: number, def: ColumnDef<Instance>) => void,
    columnRemove: (i: number) => void,

    getInstances: (filter: PartialFilter, wait?: boolean) => Promise<Instance[] | null>,
    getPositionedInstances: (filter: PartialFilter, wait?: boolean) => Promise<PositionedInstance[] | null>,

    selected: Instance | null,
    setSelected: (e: Instance | null) => void,
};

export function InstancesScreen(props: InstancesScreenProps) {
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

        <FilterInput predicates={props.raw_filter} updatePredicates={props.setFilter} parse={parseInstanceFilter} />

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
                columnMove={props.columnMove}
                columnInsert={props.columnInsert}
                columnRemove={props.columnRemove}
                columnDefault={INHERENT('id')}
                columnMin={2}
                selectedRow={props.selected}
                setSelectedRow={props.setSelected}
                hoveredRow={hoveredRow()}
                setHoveredRow={setHoveredRow}
                getEntries={props.getInstances}
                addToFilter={async f => props.addToFilter(await parseInstanceFilter(f))}
                columnParser={parseInstanceColumn}
            />

            <Show when={props.selected}>
                {row => <InstanceDetailPane
                    timespan={props.timespan}
                    instance={row()}
                    updateSelectedRow={props.setSelected}
                    filter={props.filter}
                    addToFilter={async f => props.addToFilter(await parseInstanceFilter(f))}
                    addColumn={c => props.columnInsert(-1, parseInstanceColumn(c))}
                />}
            </Show>
        </div>
    </div>);
}
