import { For, useContext } from "solid-js";
import { ColumnData, defaultEventsScreen, defaultSpansScreen, ScreenData } from "../App";
import { Input, parseEventFilter, parseInstanceFilter, parseSpanFilter } from "../invoke";
import { NavigationContext } from "../context/navigation";
import { LogicalPosition } from "@tauri-apps/api/dpi";
import { Menu, MenuItemOptions } from "@tauri-apps/api/menu";
import { EventDataLayer, InstanceDataLayer, SpanDataLayer, TraceDataLayer } from "../utils/datalayer";
import { ATTRIBUTE, CONNECTED, CREATED, INHERENT, LEVEL, TIMESTAMP } from "./table";

import './tab-bar.css';
import eventsAddIcon from '../assets/event-add.svg';
import spansAddIcon from '../assets/span-add.svg';

export type TabBarProps = {
    screens: ScreenData[],
    column_datas: ColumnData[],
    active: number,
};

export function TabBar(props: TabBarProps) {
    let navigation = useContext(NavigationContext)!;

    function getTabPrefix(screen: ScreenData): string {
        if (screen.kind == 'events') {
            return 'Events';
        } else if (screen.kind == 'spans') {
            return 'Spans';
        } else if (screen.kind == 'trace') {
            return 'Trace';
        } else {
            return 'Instances';
        }
    }

    function stringifyFilter(filter: Input[]): string {
        let s = "";
        for (let predicate of filter) {
            s += ` ${predicate.text}`;
        }
        return s;
    }

    function getTabHovertext(screen: ScreenData): string {
        return getTabPrefix(screen) + ':' + stringifyFilter(screen.filter);
    }

    function onwheel(this: HTMLDivElement, e: WheelEvent) {
        if (Math.abs(e.deltaY) > 0) {
            e.preventDefault();
            this.scrollLeft += e.deltaY;
        }
    }

    function duplicateScreen(screen: ScreenData, column_data: ColumnData): [ScreenData, ColumnData] {
        if (screen.kind == 'events') {
            return [{
                kind: 'events',
                filter: [...screen.filter],
                timespan: screen.timespan,
                live: false,
                store: new EventDataLayer([...screen.filter]),
            }, {
                columns: [...column_data.columns],
                columnWidths: [...column_data.columnWidths],
            }];
        } else if (screen.kind == 'spans') {
            return [{
                kind: 'spans',
                filter: [...screen.filter],
                timespan: screen.timespan,
                live: false,
                store: new SpanDataLayer([...screen.filter]),
            }, {
                columns: [...column_data.columns],
                columnWidths: [...column_data.columnWidths],
            }];
        } else if (screen.kind == 'instances') {
            return [{
                kind: 'instances',
                filter: [...screen.filter],
                timespan: screen.timespan,
                live: false,
                store: new InstanceDataLayer([...screen.filter]),
            }, {
                columns: [...column_data.columns],
                columnWidths: [...column_data.columnWidths],
            }];
        } else {
            return [{
                kind: 'trace',
                filter: [...screen.filter],
                timespan: screen.timespan,
                live: false,
                store: new TraceDataLayer([...screen.filter]),
                collapsed: { ...screen.collapsed },
            }, {
                columns: [...column_data.columns],
                columnWidths: [...column_data.columnWidths],
            }];
        }
    }

    async function duplicateScreenAs(screen: ScreenData, screenKind: 'events' | 'spans' | 'instances'): Promise<[ScreenData, ColumnData]> {
        if (screenKind == 'events') {
            // TODO: put these default columns somewhere else
            let columns = [LEVEL, TIMESTAMP, ATTRIBUTE("message")];
            let columnWidths = columns.map(def => def.defaultWidth);

            let filterText = stringifyFilter(screen.filter);
            let filter = await parseEventFilter(filterText);

            if (filter.length == 0 || !filter[0].text.startsWith('#level:')) {
                filter.unshift({
                    text: "#level: >=TRACE",
                    input: 'valid',
                    property_kind: 'Inherent',
                    property: "level",
                    value_kind: 'comparison',
                    value: ['Gte', "TRACE"],
                    editable: false,
                });
            }

            return [{
                kind: 'events',
                filter,
                timespan: screen.timespan!,
                live: false,
                store: new EventDataLayer(filter),
            }, {
                columns: columns as any,
                columnWidths,
            }];
        } else if (screenKind == 'spans') {
            // TODO: put these default columns somewhere else
            let columns = [LEVEL, CREATED, INHERENT('name')];
            let columnWidths = columns.map(def => def.defaultWidth);

            let filterText = stringifyFilter(screen.filter);
            let filter = await parseSpanFilter(filterText);

            if (filter.length == 0 || !filter[0].text.startsWith('#level:')) {
                filter.unshift({
                    text: "#level: >=TRACE",
                    input: 'valid',
                    property_kind: 'Inherent',
                    property: "level",
                    value_kind: 'comparison',
                    value: ['Gte', "TRACE"],
                    editable: false,
                });
            }

            return [{
                kind: 'spans',
                filter,
                timespan: screen.timespan!,
                live: false,
                store: new SpanDataLayer(filter),
            }, {
                columns: columns as any,
                columnWidths,
            }];
        } else {
            // TODO: put these default columns somewhere else
            let columns = [CONNECTED, INHERENT('id')];
            let columnWidths = columns.map(def => def.defaultWidth);

            let filterText = stringifyFilter(screen.filter);
            let filter = await parseInstanceFilter(filterText);

            return [{
                kind: 'instances',
                filter,
                timespan: screen.timespan!,
                live: false,
                store: new InstanceDataLayer(filter),
            }, {
                columns: columns as any,
                columnWidths,
            }];
        }
    }

    function duplicationItems(screen: ScreenData, column_data: ColumnData): MenuItemOptions[] {
        if (screen.kind == 'events') {
            return [
                { text: "duplicate tab", action: () => navigation.createTab(...duplicateScreen(screen, column_data), true) },
                { text: "duplicate tab for spans", action: async () => navigation.createTab(...await duplicateScreenAs(screen, 'spans'), true) },
                { text: "duplicate tab for instances", action: async () => navigation.createTab(...await duplicateScreenAs(screen, 'instances'), true) },
            ];
        } else if (screen.kind == 'spans') {
            return [
                { text: "duplicate tab", action: () => navigation.createTab(...duplicateScreen(screen, column_data), true) },
                { text: "duplicate tab for events", action: async () => navigation.createTab(...await duplicateScreenAs(screen, 'events'), true) },
                { text: "duplicate tab for instances", action: async () => navigation.createTab(...await duplicateScreenAs(screen, 'instances'), true) },
            ];
        } else if (screen.kind == 'instances') {
            return [
                { text: "duplicate tab", action: () => navigation.createTab(...duplicateScreen(screen, column_data), true) },
                { text: "duplicate tab for events", action: async () => navigation.createTab(...await duplicateScreenAs(screen, 'events'), true) },
                { text: "duplicate tab for spans", action: async () => navigation.createTab(...await duplicateScreenAs(screen, 'spans'), true) },
            ];
        } else /* screen.kind == 'trace' */ {
            if (screen.timespan == null) {
                return [
                    { text: "duplicate tab", action: () => navigation.createTab(...duplicateScreen(screen, column_data), true) },
                ]
            } else {
                return [
                    { text: "duplicate tab", action: () => navigation.createTab(...duplicateScreen(screen, column_data), true) },
                    { text: "duplicate tab for events", action: async () => navigation.createTab(...await duplicateScreenAs(screen, 'events'), true) },
                    { text: "duplicate tab for spans", action: async () => navigation.createTab(...await duplicateScreenAs(screen, 'spans'), true) },
                ];
            }
        }
    }

    async function showContextMenu(e: MouseEvent, idx: number) {
        let screen = props.screens[idx];
        let column_data = props.column_datas[idx];
        let end = props.screens.length - 1;

        let menu = await Menu.new({
            items: [
                ...duplicationItems(screen, column_data),
                { item: 'Separator' },
                { text: "move left", enabled: idx != 0, action: () => navigation.moveTab(idx, idx - 1) },
                { text: "move far left", enabled: idx != 0, action: () => navigation.moveTab(idx, 0) },
                { text: "move right", enabled: idx != end, action: () => navigation.moveTab(idx, idx + 1) },
                { text: "move far right", enabled: idx != end, action: () => navigation.moveTab(idx, end) },
                { item: 'Separator' },
                { text: "close tab", action: () => navigation.removeTab(idx) },
                { text: "close all other tabs", action: () => navigation.removeAllOtherTabs(idx) },
            ]
        });
        await menu.popup(new LogicalPosition(e.clientX, e.clientY));
    }

    return (<div class="tabbar">
        <div class="tabs" onwheel={onwheel}>
            <For each={props.screens}>
                {(screen, idx) => (<div title={getTabHovertext(screen)} class="tab" classList={{ active: idx() == props.active }} onclick={() => navigation.activateTab(idx())} onauxclick={e => { if (e.button == 1) navigation.removeTab(idx()); }} oncontextmenu={e => showContextMenu(e, idx())}>
                    <span><b>{getTabPrefix(screen)}:</b>{stringifyFilter(screen.filter)}</span>
                    <button onclick={e => { navigation.removeTab(idx()); e.stopPropagation(); }}>X</button>
                </div>)}
            </For>
        </div>
        <button class="new-tab" onclick={async () => navigation.createTab(...await defaultEventsScreen(), true)}>
            <img src={eventsAddIcon} />
        </button>
        <button class="new-tab" onclick={async () => navigation.createTab(...await defaultSpansScreen(), true)}>
            <img src={spansAddIcon} />
        </button>
    </div>)
}
