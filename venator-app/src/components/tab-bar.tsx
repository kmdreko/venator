import { For, useContext } from "solid-js";
import { defaultEventsScreen, defaultSpansScreen, ScreenData } from "../App";
import { FilterPredicate } from "../invoke";
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

    function stringifyFilter(filter: FilterPredicate[]): string {
        let s = "";
        for (let predicate of filter) {
            s += ` ${predicate.text}`;
        }
        return s;
    }

    function onwheel(this: HTMLDivElement, e: WheelEvent) {
        if (Math.abs(e.deltaY) > 0) {
            e.preventDefault();
            this.scrollLeft += e.deltaY;
        }
    }

    function duplicateScreen(screen: ScreenData): ScreenData {
        if (screen.kind == 'events') {
            return {
                kind: 'events',
                filter: [...screen.filter],
                timespan: screen.timespan,
                live: false,
                store: new EventDataLayer([...screen.filter]),
                columns: [...screen.columns],
                columnWidths: [...screen.columnWidths],
            };
        } else if (screen.kind == 'spans') {
            return {
                kind: 'spans',
                filter: [...screen.filter],
                timespan: screen.timespan,
                live: false,
                store: new SpanDataLayer([...screen.filter]),
                columns: [...screen.columns],
                columnWidths: [...screen.columnWidths],
            };
        } else if (screen.kind == 'instances') {
            return {
                kind: 'instances',
                filter: [...screen.filter],
                timespan: screen.timespan,
                live: false,
                store: new InstanceDataLayer([...screen.filter]),
                columns: [...screen.columns],
                columnWidths: [...screen.columnWidths],
            };
        } else {
            return {
                kind: 'trace',
                filter: [...screen.filter],
                timespan: screen.timespan,
                live: false,
                store: new TraceDataLayer([...screen.filter]),
                collapsed: { ...screen.collapsed },
                columns: [...screen.columns],
                columnWidths: [...screen.columnWidths],
            }
        }
    }

    function duplicateScreenAs(screen: ScreenData, screenKind: 'events' | 'spans' | 'instances'): ScreenData {
        if (screenKind == 'events') {
            // TODO: put these default columns somewhere else
            let columns = [LEVEL, TIMESTAMP, ATTRIBUTE("message")];
            let columnWidths = columns.map(def => def.defaultWidth);

            return {
                kind: 'events',
                filter: [...screen.filter],
                timespan: screen.timespan!,
                live: false,
                store: new EventDataLayer([...screen.filter]),
                columns,
                columnWidths,
            };
        } else if (screenKind == 'spans') {
            // TODO: put these default columns somewhere else
            let columns = [LEVEL, CREATED, INHERENT('name')];
            let columnWidths = columns.map(def => def.defaultWidth);

            return {
                kind: 'spans',
                filter: [...screen.filter],
                timespan: screen.timespan!,
                live: false,
                store: new SpanDataLayer([...screen.filter]),
                columns,
                columnWidths,
            };
        } else {
            // TODO: put these default columns somewhere else
            let columns = [CONNECTED, INHERENT('id')];
            let columnWidths = columns.map(def => def.defaultWidth);

            return {
                kind: 'instances',
                filter: [...screen.filter],
                timespan: screen.timespan!,
                live: false,
                store: new InstanceDataLayer([...screen.filter]),
                columns,
                columnWidths,
            };
        }
    }

    function duplicationItems(screen: ScreenData): MenuItemOptions[] {
        if (screen.kind == 'events') {
            return [
                { text: "duplicate tab", action: () => navigation.createTab(duplicateScreen(screen), false) },
                // { text: "duplicate tab for spans", action: () => navigation.createTab(duplicateScreenAs(screen, 'spans'), false) },
                // { text: "duplicate tab for instances", action: () => navigation.createTab(duplicateScreenAs(screen, 'instances'), false) },
            ];
        } else if (screen.kind == 'spans') {
            return [
                { text: "duplicate tab", action: () => navigation.createTab(duplicateScreen(screen), false) },
                // { text: "duplicate tab for events", action: () => navigation.createTab(duplicateScreenAs(screen, 'events'), false) },
                // { text: "duplicate tab for instances", action: () => navigation.createTab(duplicateScreenAs(screen, 'instances'), false) },
            ];
        } else if (screen.kind == 'instances') {
            return [
                { text: "duplicate tab", action: () => navigation.createTab(duplicateScreen(screen), false) },
                // { text: "duplicate tab for events", action: () => navigation.createTab(duplicateScreenAs(screen, 'events'), false) },
                // { text: "duplicate tab for spans", action: () => navigation.createTab(duplicateScreenAs(screen, 'spans'), false) },
            ];
        } else /* screen.kind == 'trace' */ {
            if (screen.timespan == null) {
                return [
                    { text: "duplicate tab", action: () => navigation.createTab(duplicateScreen(screen), false) },
                ]
            } else {
                return [
                    { text: "duplicate tab", action: () => navigation.createTab(duplicateScreen(screen), false) },
                    { text: "duplicate tab for events", action: () => navigation.createTab(duplicateScreenAs(screen, 'events'), false) },
                    { text: "duplicate tab for spans", action: () => navigation.createTab(duplicateScreenAs(screen, 'spans'), false) },
                ];
            }
        }
    }

    async function showContextMenu(e: MouseEvent, idx: number) {
        let screen = props.screens[idx];

        let menu = await Menu.new({
            items: [
                ...duplicationItems(screen),
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
                {(screen, idx) => (<div class="tab" classList={{ active: idx() == props.active }} onclick={() => navigation.activateTab(idx())} onauxclick={e => { if (e.button == 1) navigation.removeTab(idx()); }} oncontextmenu={e => showContextMenu(e, idx())}>
                    <span><b>{getTabPrefix(screen)}:</b>{stringifyFilter(screen.filter)}</span>
                    <button onclick={e => { navigation.removeTab(idx()); e.stopPropagation(); }}>X</button>
                </div>)}
            </For>
        </div>
        <button class="new-tab" onclick={async () => navigation.createTab(await defaultEventsScreen(), true)}>
            <img src={eventsAddIcon} />
        </button>
        <button class="new-tab" onclick={async () => navigation.createTab(await defaultSpansScreen(), true)}>
            <img src={spansAddIcon} />
        </button>
    </div>)
}
