import { listen } from '@tauri-apps/api/event';
import { ask, message, save } from '@tauri-apps/plugin-dialog';
import { writeTextFile } from '@tauri-apps/plugin-fs';
import { EventsScreen } from "./screens/events-screen";
import { AppStatus, deleteEntities, Event, getEvents, getConnections, getSpans, getStats, getStatus, Input, Connection, Span, Timestamp, ValidFilterPredicate } from "./invoke";
import { batch, createSignal, Match, onMount, Show, Switch } from "solid-js";
import { Counts, PaginationFilter, PartialEventCountFilter, PartialFilter, PositionedConnection, PositionedSpan, Timespan } from "./models";
import { SpansScreen } from "./screens/spans-screen";
import { EventDataLayer, SpanDataLayer, TraceDataLayer, ConnectionDataLayer } from "./utils/datalayer";
import { NavigationContext } from "./context/navigation";
import { TraceScreen } from "./screens/trace-screen";
import { ATTRIBUTE, ColumnDef, CONNECTED, CREATED, INHERENT, LEVEL, TIMESTAMP } from "./components/table";
import { ConnectionsScreen } from "./screens/connections-screen";
import { TabBar } from "./components/tab-bar";
import { UndoHistory } from './utils/undo';

import "./App.css";

const HOUR = 3600000000;

export type ColumnData = {
    columns: ColumnDef<Span | Event | Connection>[],
    columnWidths: string[],
}

type EventsScreenData = {
    kind: 'events',
    filter: Input[],
    timespan: Timespan,
    live: boolean,
    store: EventDataLayer,
};

type SpansScreenData = {
    kind: 'spans',
    filter: Input[],
    timespan: Timespan,
    live: boolean,
    store: SpanDataLayer,
};

type TraceScreenData = {
    kind: 'trace',
    filter: Input[],
    timespan: Timespan | null,
    live: boolean,
    store: TraceDataLayer,
    collapsed: { [id: string]: true },
};

type ConnectionsScreenData = {
    kind: 'connections',
    filter: Input[],
    timespan: Timespan,
    live: boolean,
    store: ConnectionDataLayer,
};

export type ScreenData = EventsScreenData | SpansScreenData | TraceScreenData | ConnectionsScreenData;

export async function defaultEventsScreen(): Promise<[EventsScreenData, ColumnData]> {
    let stats = await getStats();

    let start;
    let end;
    if (stats.start == null) {
        let now = Date.now() * 1000;
        start = now - HOUR;
        end = now;
    } else {
        start = stats.start!;
        end = stats.end!;

        let duration = end - start;
        start -= duration * 0.05;
        end += duration * 0.05;

        [start, end] = normalizeTimespan([start, end]);
    }

    let filter: Input[] = [{
        input: 'valid',
        predicate_kind: 'single',
        predicate: {
            text: "#level: >=TRACE",
            property_kind: 'Inherent',
            property: "level",
            value_kind: 'comparison',
            value: ['Gte', "TRACE"],
        },
        editable: false,
    }];
    let columns = [LEVEL, TIMESTAMP, ATTRIBUTE("message")];
    let columnWidths = columns.map(def => def.defaultWidth);

    return [{
        kind: 'events',
        filter,
        timespan: [start, end],
        live: false,
        store: new EventDataLayer(filter),
    }, {
        columns: columns as any,
        columnWidths,
    }];
}

export async function defaultSpansScreen(): Promise<[SpansScreenData, ColumnData]> {
    let stats = await getStats();

    let start;
    let end;
    if (stats.start == null) {
        let now = Date.now() * 1000;
        start = now - HOUR;
        end = now;
    } else {
        start = stats.start!;
        end = stats.end!;

        let duration = end - start;
        start -= duration * 0.05;
        end += duration * 0.05;

        [start, end] = normalizeTimespan([start, end]);
    }

    let filter: Input[] = [{
        input: 'valid',
        predicate_kind: 'single',
        predicate: {
            text: "#level: >=TRACE",
            property_kind: 'Inherent',
            property: "level",
            value_kind: 'comparison',
            value: ['Gte', "TRACE"],
        },
        editable: false,
    }, {
        input: 'valid',
        predicate_kind: 'single',
        predicate: {
            text: "#parent: none",
            property_kind: 'Inherent',
            property: "parent",
            value_kind: 'comparison',
            value: ['Eq', "none"],
        },
    }];
    let columns = [LEVEL, CREATED, INHERENT('name')];
    let columnWidths = columns.map(def => def.defaultWidth);

    return [{
        kind: 'spans',
        filter,
        timespan: [start, end],
        live: false,
        store: new SpanDataLayer(filter),
    }, {
        columns: columns as any,
        columnWidths,
    }];
}

export async function defaultConnectionsScreen(): Promise<[ConnectionsScreenData, ColumnData]> {
    let stats = await getStats();

    let start;
    let end;
    if (stats.start == null) {
        let now = Date.now() * 1000;
        start = now - HOUR;
        end = now;
    } else {
        start = stats.start!;
        end = stats.end!;

        let duration = end - start;
        start -= duration * 0.05;
        end += duration * 0.05;

        [start, end] = normalizeTimespan([start, end]);
    }

    let columns = [CONNECTED, INHERENT('id')];
    let columnWidths = columns.map(def => def.defaultWidth);
    return [{
        kind: 'connections',
        filter: [],
        timespan: [start, end],
        live: false,
        store: new ConnectionDataLayer([]),
    }, {
        columns: columns as any,
        columnWidths,
    }];
}

function normalizeTimespan(new_timespan: Timespan): Timespan {
    let [new_start, new_end] = new_timespan;

    if (new_end <= new_start) {
        console.warn("attempted to set non-linear timespan");
    }

    const DAY = 86400000000;
    const MILLISECOND = 1000;

    let duration = new_end - new_start;
    if (duration > 60 * DAY) {
        duration = 60 * DAY;
        let middle = new_start / 2 + new_end / 2;

        new_start = middle - duration / 2;
        new_end = middle + duration / 2;
    }
    if (duration < 1 * MILLISECOND) {
        duration = 1 * MILLISECOND;
        let middle = new_start / 2 + new_end / 2;

        new_start = middle - duration / 2;
        new_end = middle + duration / 2;
    }

    new_start = Math.round(new_start);
    new_end = Math.round(new_end);

    const TIME_MIN = 1;
    const TIME_MAX = Date.now() * 1000;

    if (new_start < TIME_MIN) {
        let shift = TIME_MIN - new_start;
        new_start += shift;
        new_end += shift;
    }

    if (new_end > TIME_MAX) {
        let shift = new_end - TIME_MAX;
        new_start -= shift;
        new_end -= shift;
    }

    return [new_start, new_end];
}

function App() {
    let [screens, setScreens] = createSignal<ScreenData[]>([]);
    let [rawFilters, setRawFilters] = createSignal<Input[][]>([]);
    let [selectedRows, setSelectedRows] = createSignal<(Event | Span | Connection | null)[]>([]);
    let [columnDatas, setColumnDatas] = createSignal<ColumnData[]>([]);

    let [selectedScreen, setSelectedScreen] = createSignal<number | undefined>();
    let [status, setStatus] = createSignal<AppStatus | null>(null);

    let undoHistories: UndoHistory[] = [];
    let root_element = document.querySelector('#root')!;

    onMount(async () => {
        createTab(...await defaultEventsScreen(), true);
    });

    onMount(async () => {
        setStatus(await getStatus());

        setInterval(async () => setStatus(await getStatus()), 500);

        await listen('save-as-csv-clicked', async () => {
            let current_screen_idx = selectedScreen()!;
            let current_screen = screens()[current_screen_idx];

            if (current_screen.kind == 'trace') {
                await message('CSVs cannot be generated from the Trace view', { title: "Export Error", kind: 'error' });
                return;
            }

            function encodeCsvValue(v: string) {
                if (v.includes('"') || v.includes(',') || v.includes('\n')) {
                    let escaped = v.replace('"', '\\"').replace('\n', '\\n');
                    return `"${escaped}"`;
                } else {
                    return v;
                }
            }

            let columns = columnDatas()[current_screen_idx].columns;
            let timespan = current_screen.timespan!;
            let csvData = columns.map(c => c.headerText).map(encodeCsvValue).join(',') + '\n';

            if (current_screen.kind == 'events') {
                let previous: number | undefined;

                while (true) {
                    let events = await getEvents({
                        filter: current_screen.filter.filter(f => f.input == 'valid'),
                        start: timespan[0],
                        end: timespan[1],
                        order: 'asc', // TODO: use screen ordering
                        previous,
                    });

                    for (let e of events) {
                        csvData += columns.map(c => c.dataText(e)).map(encodeCsvValue).join(',') + '\n';
                    }

                    if (events.length < 50) {
                        break;
                    }

                    previous = events[events.length - 1].timestamp;
                }
            } else if (current_screen.kind == 'spans') {
                let previous: number | undefined;

                while (true) {
                    let spans = await getSpans({
                        filter: current_screen.filter.filter(f => f.input == 'valid'),
                        start: timespan[0],
                        end: timespan[1],
                        order: 'asc', // TODO: use screen ordering
                        previous,
                    });

                    for (let s of spans) {
                        csvData += columns.map(c => c.dataText(s)).map(encodeCsvValue).join(',') + '\n';
                    }

                    if (spans.length < 50) {
                        break;
                    }

                    previous = spans[spans.length - 1].created_at;
                }
            } else if (current_screen.kind == 'connections') {
                let previous: number | undefined;

                while (true) {
                    let connections = await getConnections({
                        filter: current_screen.filter.filter(f => f.input == 'valid'),
                        start: timespan[0],
                        end: timespan[1],
                        order: 'asc', // TODO: use screen ordering
                        previous,
                    });

                    for (let i of connections) {
                        csvData += columns.map(c => c.dataText(i)).map(encodeCsvValue).join(',') + '\n';
                    }

                    if (connections.length < 50) {
                        break;
                    }

                    previous = connections[connections.length - 1].connected_at;
                }
            }

            let file = await save({
                title: 'Export',
                filters: [{ name: "CSV", extensions: ['csv'] }],
            });

            if (file == undefined) {
                return;
            }

            await writeTextFile(file, csvData);
        });

        await listen('undo-clicked', () => {
            performUndo();
        });

        await listen('redo-clicked', () => {
            performRedo();
        });

        await listen('tab-new-events-clicked', () => {
            performNewEventsTab();
        });

        await listen('tab-new-spans-clicked', () => {
            performNewSpansTab();
        });

        await listen('tab-new-connections-clicked', () => {
            performNewConnectionsTab();
        });

        await listen('tab-duplicate-clicked', () => {
            performDuplicateTab();
        });

        await listen('tab-close-others-clicked', () => {
            performCloseAllTabs();
        });

        await listen('focus-filter-clicked', () => {
            performFocusFilter();
        });

        await listen('zoom-in-clicked', () => {
            performZoomIn();
        });

        await listen('zoom-out-clicked', () => {
            performZoomOut();
        });

        await listen('focus-clicked', () => {
            performFocus();
        });

        await listen('focus-all-clicked', () => {
            performFocusAll();
        });

        await listen('set-theme-light-clicked', () => {
            root_element.setAttribute('data-theme', 'light');
        });

        await listen('set-theme-dark-clicked', () => {
            root_element.setAttribute('data-theme', 'dark');
        });

        await listen('delete-all-clicked', async () => {
            let metrics = await deleteEntities(null, null, true, true);

            let answer = await ask(`This will delete ${metrics.connections} connections, ${metrics.spans} spans, and ${metrics.events} events. \n\n Proceed?`, {
                title: `Delete from ${status()?.dataset_name}`,
                kind: 'warning',
            });

            if (answer) {
                await deleteEntities(null, null, true, false);

                forceResetScreenFilters();
            }
        });

        await listen('delete-inside-clicked', async () => {
            let screen = screens()[selectedScreen()!];
            let timespan = screen.timespan!;

            let metrics = await deleteEntities(timespan[0], timespan[1], true, true);

            let answer = await ask(`This will delete ${metrics.connections} connections, ${metrics.spans} spans, and ${metrics.events} events. \n\n Proceed?`, {
                title: `Delete from ${status()?.dataset_name}`,
                kind: 'warning',
            });

            if (answer) {
                await deleteEntities(timespan[0], timespan[1], true, false);

                forceResetScreenFilters();
            }
        });

        await listen('delete-outside-clicked', async () => {
            let screen = screens()[selectedScreen()!];
            let timespan = screen.timespan!;

            let metrics = await deleteEntities(timespan[0], timespan[1], false, true);

            let answer = await ask(`This will delete ${metrics.connections} connections, ${metrics.spans} spans, and ${metrics.events} events. \n\n Proceed?`, {
                title: `Delete from ${status()?.dataset_name}`,
                kind: 'warning',
            });

            if (answer) {
                await deleteEntities(timespan[0], timespan[1], false, false);

                forceResetScreenFilters();
            }
        });

        document.onkeydown = function (e) {
            if (e.ctrlKey && !e.altKey && !e.shiftKey && e.key == 'z') {
                e.preventDefault();
                performUndo();
            }
            if (e.ctrlKey && !e.altKey && !e.shiftKey && e.key == 'y') {
                e.preventDefault();
                performRedo();
            }
            if (e.ctrlKey && !e.altKey && !e.shiftKey && e.key == 't') {
                e.preventDefault();
                performNewEventsTab();
            }
            if (e.ctrlKey && !e.altKey && !e.shiftKey && e.key == 'd') {
                e.preventDefault();
                performDuplicateTab();
            }
            if (e.ctrlKey && !e.altKey && !e.shiftKey && e.key == 'f') {
                e.preventDefault();
                performFocusFilter();
            }
            if (e.ctrlKey && !e.altKey && !e.shiftKey && e.key == '=') {
                e.preventDefault();
                performZoomIn();
            }
            if (e.ctrlKey && !e.altKey && !e.shiftKey && e.key == '-') {
                e.preventDefault();
                performZoomOut();
            }
            if (e.ctrlKey && !e.altKey && !e.shiftKey && e.key == 'g') {
                e.preventDefault();
                performFocus();
            }
            if (e.ctrlKey && !e.altKey && e.shiftKey && e.key == 'G') {
                e.preventDefault();
                performFocusAll();
            }
        }
    })

    async function getAndCacheEvents(screen: EventsScreenData, filter: PartialFilter): Promise<Event[]> {
        return await screen.store.getEvents(filter);
    }

    async function getAndCacheEventCounts(screen: EventsScreenData, filter: PartialEventCountFilter, wait?: boolean, cache?: boolean): Promise<Counts | null> {
        return await screen.store.getEventCounts(filter, wait, cache);
    }

    async function getAndCacheSpans(screen: SpansScreenData, filter: PartialFilter, wait?: boolean): Promise<Span[] | null> {
        return await screen.store.getSpans(filter, wait);
    }

    async function getAndCachePositionedSpans(screen: SpansScreenData, filter: PartialFilter, wait?: boolean): Promise<PositionedSpan[] | null> {
        return await screen.store.getPositionedSpans(filter, wait);
    }

    async function getEntries(screen: TraceScreenData, filter: PaginationFilter): Promise<(Event | Span)[]> {
        let entries = await screen.store.getEntries(filter);

        function getEndTimestamp(e: Event | Span): Timestamp {
            return (e as any).timestamp || ((e as any).closed_at ?? 0);
        }

        let max_closed_at = getEndTimestamp(entries[0]) ?? 0;
        for (let i = 1; i < entries.length; i++) {
            if (getEndTimestamp(entries[i]) > max_closed_at) {
                max_closed_at = getEndTimestamp(entries[i]);
            }
        }

        let timespan = getPaddedTimespan([(entries[0] as Span).created_at, max_closed_at || (Date.now() * 1000)]);

        if (screen.timespan == null) {
            let current_selected_screen = selectedScreen()!;
            let current_screens = screens();
            let updated_screens = [...current_screens];
            updated_screens[current_selected_screen] = {
                ...current_screens[current_selected_screen],
                timespan: normalizeTimespan(timespan),
                store: screen.store,
            } as any;
            setScreens(updated_screens);
        }

        return entries;
    }

    async function getAndCacheConnections(screen: ConnectionsScreenData, filter: PartialFilter): Promise<Connection[] | null> {
        return await screen.store.getConnections(filter);
    }

    async function getAndCachePositionedConnections(screen: ConnectionsScreenData, filter: PartialFilter): Promise<PositionedConnection[] | null> {
        return await screen.store.getPositionedConnections(filter);
    }

    function getPaddedTimespan(timespan: Timespan): Timespan {
        let now = Date.now() * 1000;

        let [created_at, closed_at] = timespan;
        let duration = closed_at - created_at;

        let start = Math.max(Math.floor(created_at - duration / 20), 1);
        let end = Math.min(Math.ceil(closed_at + duration / 20), now);

        return [start, end];
    }

    function getCurrentScreen(): ScreenData | null {
        let current_selected_screen = selectedScreen();
        if (current_selected_screen == undefined) {
            return null;
        }
        let current_screens = screens();
        return current_screens[current_selected_screen];
    }

    function getCurrentRawFilters(): Input[] | null {
        let current_selected_screen = selectedScreen();
        if (current_selected_screen == undefined) {
            return null;
        }
        let current_raw_filters = rawFilters();
        return current_raw_filters[current_selected_screen];
    }

    function getCurrentSelectedRow(): Event | Span | Connection | null {
        let current_selected_screen = selectedScreen();
        if (current_selected_screen == undefined) {
            return null;
        }
        let current_selected_rows = selectedRows();
        return current_selected_rows[current_selected_screen];
    }

    function getCurrentColumnData(): ColumnData | null {
        let current_selected_screen = selectedScreen();
        if (current_selected_screen == undefined) {
            return null;
        }
        let current_column_datas = columnDatas();
        return current_column_datas[current_selected_screen];
    }

    function setScreenFilter(filter: Input[]) {
        let current_selected_screen = selectedScreen()!;
        let current_screens = screens();
        let updated_screens = [...current_screens];

        let current_raw_filters = rawFilters();
        let updated_raw_filters = [...current_raw_filters];

        let valid_filter = filter.filter(f => f.input == 'valid');

        function filterText(filter: ValidFilterPredicate[]): string {
            let s = "";
            for (let predicate of filter) {
                s += ` ${stringifyNestedFilter(predicate)}`;
            }
            return s;
        }

        function stringifyNestedFilter(predicate: ValidFilterPredicate): string {
            let s = "";
            switch (predicate.predicate_kind) {
                case "single":
                    s += ` ${predicate.predicate.text}`;
                    break;
                case "and":
                    s += ` (`;
                    let and_predicates = predicate.predicate.filter(f => f.input == 'valid');
                    for (let i = 0; i < and_predicates.length; i++) {
                        if (i != 0) {
                            s += " AND ";
                        }
                        s += stringifyNestedFilter(and_predicates[i]);
                    }
                    s += ")";
                    break;
                case "or":
                    s += ` (`;
                    let or_predicates = predicate.predicate.filter(f => f.input == 'valid');
                    for (let i = 0; i < or_predicates.length; i++) {
                        if (i != 0) {
                            s += " OR ";
                        }
                        s += stringifyNestedFilter(or_predicates[i]);
                    }
                    s += ")";
                    break;
            }
            return s;
        }

        if (filterText(valid_filter) == filterText(current_screens[current_selected_screen].filter as ValidFilterPredicate[])) {
            // valid filter didn't change, only update raw_filter

            updated_raw_filters[current_selected_screen] = filter;
            undoHistories[current_selected_screen].updateWithFilter(filter);
            setRawFilters(updated_raw_filters);
        } else {
            // valid filter did change

            if (current_screens[current_selected_screen].live) {
                current_screens[current_selected_screen].store.unsubscribe();
            }

            updated_screens[current_selected_screen] = current_screens[current_selected_screen].kind == 'events'
                ? { ...current_screens[current_selected_screen], filter: valid_filter, store: new EventDataLayer(filter) }
                : current_screens[current_selected_screen].kind == 'spans'
                    ? { ...current_screens[current_selected_screen], filter: valid_filter, store: new SpanDataLayer(filter) }
                    : current_screens[current_selected_screen].kind == 'connections'
                        ? { ...current_screens[current_selected_screen], filter: valid_filter, store: new ConnectionDataLayer(filter) }
                        : { ...current_screens[current_selected_screen], filter: valid_filter };

            if (updated_screens[current_selected_screen].live) {
                updated_screens[current_selected_screen].store.subscribe();
            }

            updated_raw_filters[current_selected_screen] = filter;
            batch(() => {
                undoHistories[current_selected_screen].updateWithFilter(filter);
                setRawFilters(updated_raw_filters);
                setScreens(updated_screens);
            })
        }

    }

    function forceResetScreenFilters() {
        let current_screens = screens();
        let updated_screens = [...current_screens];
        for (let i = 0; i < updated_screens.length; i++) {
            let filter = [...current_screens[i].filter];

            switch (current_screens[i].kind) {
                case 'events':
                    updated_screens[i] = { ...current_screens[i], filter, store: new EventDataLayer(filter) as any };
                    break;
                case 'spans':
                    updated_screens[i] = { ...current_screens[i], filter, store: new SpanDataLayer(filter) as any };
                    break;
                case 'connections':
                    updated_screens[i] = { ...current_screens[i], filter, store: new ConnectionDataLayer(filter) as any };
                    break;
                case 'trace':
                    updated_screens[i] = { ...current_screens[i], filter };
                    break;
            }
        }

        setScreens(updated_screens);
    }

    function addToFilter(filter: Input[]) {
        let current_selected_screen = selectedScreen()!;
        let current_screens = screens();
        setScreenFilter([...current_screens[current_selected_screen].filter, ...filter]);
    }

    function setScreenTimespan(timespan: Timespan) {
        let normalizedTimespan = normalizeTimespan(timespan);

        let current_selected_screen = selectedScreen()!;
        let current_screens = screens();
        let updated_screens = [...current_screens];
        updated_screens[current_selected_screen] = {
            ...current_screens[current_selected_screen],
            timespan: normalizedTimespan,
        };

        if (updated_screens[current_selected_screen].live) {
            let duration = normalizedTimespan[1] - normalizedTimespan[0];
            let snap_dist = duration * 0.10;
            if (normalizedTimespan[1] < Date.now() * 1000 - snap_dist) {
                updated_screens[current_selected_screen].live = false;
            }
        }

        undoHistories[current_selected_screen].updateWithTimespan(normalizedTimespan);
        setScreens(updated_screens);
    }

    function setScreenLive(live: boolean) {
        let current_selected_screen = selectedScreen()!;
        let current_screens = screens();
        let updated_screens = [...current_screens];
        updated_screens[current_selected_screen] = {
            ...current_screens[current_selected_screen],
            live,
        };

        let store = (updated_screens[current_selected_screen] as EventsScreenData).store;
        if (live) {
            store.subscribe();
        } else {
            store.unsubscribe();
        }

        setScreens(updated_screens);
    }

    function setScreenSelected<T>(selected: T | null) {
        let current_selected_screen = selectedScreen()!;
        let current_rows = selectedRows();
        let updated_rows = [...current_rows];
        updated_rows[current_selected_screen] = selected as any;
        setSelectedRows(updated_rows);
    }

    function setCollapsed(id: string, collapsed: boolean) {
        let current_selected_screen = selectedScreen()!;
        let current_screens = screens();
        let updated_screens = [...current_screens];

        let current_collapsed = (current_screens[current_selected_screen] as TraceScreenData).collapsed;
        let updated_collapsed = { ...current_collapsed };
        if (collapsed) {
            updated_collapsed[id] = true;
        } else {
            delete updated_collapsed[id];
        }

        updated_screens[current_selected_screen] = {
            ...(current_screens[current_selected_screen] as TraceScreenData),
            collapsed: updated_collapsed,
        };
        setScreens(updated_screens);
    }

    async function removeScreen(idx: number) {
        let current_selected_screen = selectedScreen()!;

        let current_screens = screens();
        let updated_screens = [...current_screens];
        updated_screens.splice(idx, 1);

        let current_raw_filters = rawFilters();
        let updated_raw_filters = [...current_raw_filters];
        updated_raw_filters.splice(idx, 1);

        let current_rows = selectedRows();
        let updated_rows = [...current_rows];
        updated_rows.splice(idx, 1);

        let current_column_datas = columnDatas();
        let updated_column_datas = [...current_column_datas];
        updated_column_datas.splice(idx, 1);

        undoHistories.splice(idx, 1);

        if (updated_screens.length == 0) {
            let [screen, columnDatas] = await defaultEventsScreen();
            updated_screens = [screen];
            updated_raw_filters = [[...screen.filter]];
            updated_rows = [null];
            updated_column_datas = [columnDatas];
            undoHistories = [new UndoHistory({
                timespan: screen.timespan!,
                raw_filter: [...screen.filter],
                columns: [...columnDatas.columns as any],
                columnWidths: [...columnDatas.columnWidths],
            })];
        }

        let updated_selected_screen = (current_selected_screen > idx) ? current_selected_screen - 1 : current_selected_screen;
        if (updated_selected_screen == updated_screens.length) {
            updated_selected_screen -= 1;
        }

        batch(() => {
            setScreens(updated_screens);
            setRawFilters(updated_raw_filters);
            setSelectedRows(updated_rows);
            setColumnDatas(updated_column_datas);
            setSelectedScreen(updated_selected_screen);
        })
    }

    function removeAllOtherScreens(idx: number) {
        let selected_screen = screens()[idx];
        let selected_raw_filter = rawFilters()[idx];
        let selected_selected_row = selectedRows()[idx];
        let selected_column_data = columnDatas()[idx];

        batch(() => {
            let history = undoHistories[idx];
            undoHistories = [history];

            setScreens([selected_screen]);
            setRawFilters([selected_raw_filter]);
            setSelectedRows([selected_selected_row]);
            setColumnDatas([selected_column_data]);
            setSelectedScreen(0);
        })
    }

    function moveScreen(fromIdx: number, toIdx: number) {
        if (fromIdx == toIdx) {
            return;
        }

        let current_selected_screen = selectedScreen()!;

        let current_screens = screens();
        let updated_screens = [...current_screens];
        let [screen] = updated_screens.splice(fromIdx, 1);
        updated_screens.splice(toIdx, 0, screen);

        let current_raw_filters = rawFilters();
        let updated_raw_filters = [...current_raw_filters];
        let [rawFilter] = updated_raw_filters.splice(fromIdx, 1);
        updated_raw_filters.splice(toIdx, 0, rawFilter);

        let current_selected_rows = selectedRows();
        let updated_selected_rows = [...current_selected_rows];
        let [selectedRow] = updated_selected_rows.splice(fromIdx, 1);
        updated_selected_rows.splice(toIdx, 0, selectedRow);

        let current_column_datas = columnDatas();
        let updated_column_datas = [...current_column_datas];
        let [columnData] = updated_column_datas.splice(fromIdx, 1);
        updated_column_datas.splice(toIdx, 0, columnData);

        let updated_selected_screen = current_selected_screen;
        if (current_selected_screen == fromIdx) {
            updated_selected_screen = toIdx;
        } else if (current_selected_screen < fromIdx && current_selected_screen >= toIdx) {
            updated_selected_screen += 1;
        } else if (current_selected_screen > fromIdx && current_selected_screen <= toIdx) {
            updated_selected_screen -= 1;
        }

        batch(() => {
            let [history] = undoHistories.splice(fromIdx, 1);
            undoHistories.splice(toIdx, 0, history);

            setScreens(updated_screens);
            setRawFilters(current_raw_filters);
            setSelectedRows(updated_selected_rows);
            setColumnDatas(updated_column_datas);
            setSelectedScreen(updated_selected_screen);
        })
    }

    function createTab(screen: ScreenData, columns: ColumnData, navigate: boolean) {
        let current_screens = screens();
        let updated_screens = [...current_screens];
        updated_screens.push({
            ...screen,
            filter: screen.filter.filter(f => f.input == 'valid'),
        });

        let current_raw_filters = rawFilters();
        let updated_raw_filters = [...current_raw_filters];
        updated_raw_filters.push([...screen.filter]);

        let current_selected_rows = selectedRows();
        let updated_selected_rows = [...current_selected_rows];
        updated_selected_rows.push(null);

        let current_column_datas = columnDatas();
        let updated_column_datas = [...current_column_datas];
        updated_column_datas.push(columns);

        batch(() => {
            undoHistories.push(new UndoHistory({
                timespan: screen.timespan!,
                raw_filter: [...screen.filter],
                columns: [...columns.columns],
                columnWidths: [...columns.columnWidths],
            }));

            setScreens(updated_screens);
            setRawFilters(updated_raw_filters);
            setSelectedRows(updated_selected_rows);
            setColumnDatas(updated_column_datas);
            if (navigate) {
                setSelectedScreen(updated_screens.length - 1);
            }
        })
    }

    function setColumnWidth(i: number, width: string) {
        let current_selected_screen = selectedScreen()!;
        let current_column_datas = columnDatas();
        let updated_column_datas = [...current_column_datas];

        let widths = current_column_datas[current_selected_screen].columnWidths;
        widths.splice(i, 1, width);

        let current_columns = current_column_datas[current_selected_screen].columns;

        updated_column_datas[current_selected_screen] = {
            ...current_column_datas[current_selected_screen],
            columnWidths: widths
        };

        undoHistories[current_selected_screen].updateWithColumnData(current_columns, widths);
        setColumnDatas(updated_column_datas);
    }

    function moveColumn(fromIdx: number, toIdx: number) {
        let current_selected_screen = selectedScreen()!;
        let current_column_datas = columnDatas();
        let updated_column_datas = [...current_column_datas];

        let current_columns = current_column_datas[current_selected_screen].columns;
        let updated_columns = [...current_columns];
        let [column_data] = updated_columns.splice(fromIdx, 1);
        updated_columns.splice(toIdx, 0, column_data);

        // TODO: do something different with widths?
        let current_columns_widths = current_column_datas[current_selected_screen].columnWidths;

        updated_column_datas[current_selected_screen] = {
            ...current_column_datas[current_selected_screen],
            columns: updated_columns,
        }

        undoHistories[current_selected_screen].updateWithColumnData(updated_columns, current_columns_widths);
        setColumnDatas(updated_column_datas);
    }

    function setColumnDef<T>(i: number, def: ColumnDef<T>) {
        let current_selected_screen = selectedScreen()!;
        let current_column_datas = columnDatas();
        let updated_column_datas = [...current_column_datas];

        let defs = current_column_datas[current_selected_screen].columns;
        defs.splice(i, 1, def as any);

        let current_columns_widths = current_column_datas[current_selected_screen].columnWidths;

        updated_column_datas[current_selected_screen] = {
            ...current_column_datas[current_selected_screen],
            columns: defs as any
        };

        undoHistories[current_selected_screen].updateWithColumnData(defs, current_columns_widths);
        setColumnDatas(updated_column_datas);
    }

    function addColumnAfter<T>(i: number, def: ColumnDef<T>) {
        let current_selected_screen = selectedScreen()!;
        let current_column_datas = columnDatas();

        let existingColumns = current_column_datas[current_selected_screen].columns;
        let existingColumnWidths = current_column_datas[current_selected_screen].columnWidths;
        let updatedColumns = [...existingColumns];
        let updatedColumnWidths = [...existingColumnWidths];

        if (i == -1) {
            i = existingColumns.length - 1;
        }

        updatedColumns.splice(i + 1, 0, def as any);
        updatedColumnWidths.splice(i + 1, 0, def.defaultWidth);

        let updated_column_datas = [...current_column_datas];
        updated_column_datas[current_selected_screen] = {
            ...current_column_datas[current_selected_screen],
            columns: updatedColumns as any,
            columnWidths: updatedColumnWidths,
        };

        undoHistories[current_selected_screen].updateWithColumnData(updatedColumns, updatedColumnWidths);
        setColumnDatas(updated_column_datas);
    }

    function removeColumn(i: number) {
        let current_selected_screen = selectedScreen()!;
        let current_column_datas = columnDatas();

        let existingColumns = current_column_datas[current_selected_screen].columns;
        let existingColumnWidths = current_column_datas[current_selected_screen].columnWidths;
        let updatedColumns = [...existingColumns];
        let updatedColumnWidths = [...existingColumnWidths];
        updatedColumns.splice(i, 1);
        updatedColumnWidths.splice(i, 1);

        // if we remove the last column, ensure the new last column is reset to
        // its default width.
        if (i == updatedColumns.length) {
            updatedColumnWidths[updatedColumnWidths.length - 1] = updatedColumns[updatedColumns.length - 1].defaultWidth;
        }

        let updated_column_datas = [...current_column_datas];
        updated_column_datas[current_selected_screen] = {
            ...current_column_datas[current_selected_screen],
            columns: updatedColumns as any,
            columnWidths: updatedColumnWidths,
        };

        undoHistories[current_selected_screen].updateWithColumnData(updatedColumns, updatedColumnWidths);
        setColumnDatas(updated_column_datas);
    }

    function performUndo() {
        let current_selected_screen = selectedScreen()!;

        let data = undoHistories[current_selected_screen].undo();
        if (data == null) {
            return;
        }

        let filter = data.raw_filter.filter(f => f.input == 'valid');

        let current_screens = screens();
        let updated_screens = [...current_screens];
        switch (current_screens[current_selected_screen].kind) {
            case 'events':
                updated_screens[current_selected_screen] = { ...current_screens[current_selected_screen], timespan: data.timespan, filter, store: new EventDataLayer(filter) as any };
                break;
            case 'spans':
                updated_screens[current_selected_screen] = { ...current_screens[current_selected_screen], timespan: data.timespan, filter, store: new SpanDataLayer(filter) as any };
                break;
            case 'connections':
                updated_screens[current_selected_screen] = { ...current_screens[current_selected_screen], timespan: data.timespan, filter, store: new ConnectionDataLayer(filter) as any };
                break;
            case 'trace':
                updated_screens[current_selected_screen] = { ...current_screens[current_selected_screen], timespan: data.timespan, filter };
                break;
        }

        let current_raw_filters = rawFilters();
        let updated_raw_filters = [...current_raw_filters];
        updated_raw_filters[current_selected_screen] = [...data.raw_filter];

        let current_column_datas = columnDatas();
        let updated_column_datas = [...current_column_datas];
        updated_column_datas[current_selected_screen] = {
            columns: [...data.columns],
            columnWidths: [...data.columnWidths],
        };

        batch(() => {
            setScreens(updated_screens);
            setRawFilters(updated_raw_filters);
            setColumnDatas(updated_column_datas);
        });
    }

    function performRedo() {
        let current_selected_screen = selectedScreen()!;

        let data = undoHistories[current_selected_screen].redo();
        if (data == null) {
            return;
        }

        let filter = data.raw_filter.filter(f => f.input == 'valid');

        let current_screens = screens();
        let updated_screens = [...current_screens];
        switch (current_screens[current_selected_screen].kind) {
            case 'events':
                updated_screens[current_selected_screen] = { ...current_screens[current_selected_screen], timespan: data.timespan, filter, store: new EventDataLayer(filter) as any };
                break;
            case 'spans':
                updated_screens[current_selected_screen] = { ...current_screens[current_selected_screen], timespan: data.timespan, filter, store: new SpanDataLayer(filter) as any };
                break;
            case 'connections':
                updated_screens[current_selected_screen] = { ...current_screens[current_selected_screen], timespan: data.timespan, filter, store: new ConnectionDataLayer(filter) as any };
                break;
            case 'trace':
                updated_screens[current_selected_screen] = { ...current_screens[current_selected_screen], timespan: data.timespan, filter };
                break;
        }

        let current_raw_filters = rawFilters();
        let updated_raw_filters = [...current_raw_filters];
        updated_raw_filters[current_selected_screen] = [...data.raw_filter];

        let current_column_datas = columnDatas();
        let updated_column_datas = [...current_column_datas];
        updated_column_datas[current_selected_screen] = {
            columns: [...data.columns],
            columnWidths: [...data.columnWidths],
        };

        batch(() => {
            setScreens(updated_screens);
            setRawFilters(updated_raw_filters);
            setColumnDatas(updated_column_datas);
        });
    }

    async function performFocus() {
        let current_selected_screen = selectedScreen()!;
        let current_screens = screens();

        if (current_screens[current_selected_screen].kind == 'events') {
            let [start, end] = current_screens[current_selected_screen].timespan;

            let start_events = await getAndCacheEvents(current_screens[current_selected_screen], {
                order: 'asc',
                start,
                end: null,
                limit: 1,
            });

            let end_events = await getAndCacheEvents(current_screens[current_selected_screen], {
                order: 'desc',
                start: null,
                end,
                limit: 1,
            });

            let start_timestamp = start_events.length == 0 ? null : start_events[0].timestamp;
            let end_timestamp = end_events.length == 0 ? null : end_events[0].timestamp;

            if (start_timestamp == null || end_timestamp == null) {
                return;
            }

            let duration = end_timestamp - start_timestamp;
            let padded_start = start_timestamp - duration * 0.05;
            let padded_end = end_timestamp + duration * 0.05;

            setScreenTimespan([padded_start, padded_end]);
        } else if (current_screens[current_selected_screen].kind == 'spans') {
            let [start, end] = current_screens[current_selected_screen].timespan;

            let start_spans = await getAndCacheSpans(current_screens[current_selected_screen], {
                order: 'asc',
                start,
                end: null,
                limit: 1,
            });

            let end_spans = await getAndCacheSpans(current_screens[current_selected_screen], {
                order: 'desc',
                start: null,
                end,
                limit: 50,
            });

            // TODO: this will return a timestamp "before" the one provided if it
            // intersects a span
            let start_timestamp = (start_spans == null || start_spans.length == 0) ? null : start_spans[0].created_at;

            // TODO: this gets the most recent "closed_at" of the 50 most recent
            // spans by "opened_at" which isn't technically guaranteed to be
            // exactly what we want, but its close enough
            let end_timestamp = end_spans?.map(c => c.closed_at)?.reduce((prev, curr) => Math.max(prev || 0, curr || 0));

            if (start_timestamp == null || end_timestamp == null) {
                return;
            }

            let duration = end_timestamp - start_timestamp;
            let padded_start = start_timestamp - duration * 0.05;
            let padded_end = end_timestamp + duration * 0.05;

            setScreenTimespan([padded_start, padded_end]);
        } else if (current_screens[current_selected_screen].kind == 'connections') {
            let [start, end] = current_screens[current_selected_screen].timespan;

            let start_connections = await getAndCacheConnections(current_screens[current_selected_screen], {
                order: 'asc',
                start,
                end: null,
                limit: 1,
            });

            let end_connections = await getAndCacheConnections(current_screens[current_selected_screen], {
                order: 'desc',
                start: null,
                end,
                limit: 50,
            });

            // TODO: this will return a timestamp "before" the one provided if it
            // intersects an connection
            let start_timestamp = (start_connections == null || start_connections.length == 0) ? null : start_connections[0].connected_at;

            // TODO: this gets the most recent "disconnected_at" of the 50 most
            // recent connections by "connected_at" which isn't technically
            // guaranteed to be exactly what we want, but its close enough
            let end_timestamp = end_connections?.map(c => c.disconnected_at)?.reduce((prev, curr) => Math.max(prev || 0, curr || 0));

            if (start_timestamp == null || end_timestamp == null || end_timestamp == 0) {
                return;
            }

            let duration = end_timestamp - start_timestamp;
            let padded_start = start_timestamp - duration * 0.05;
            let padded_end = end_timestamp + duration * 0.05;

            setScreenTimespan([padded_start, padded_end]);
        }
    }

    async function performFocusAll() {
        let current_selected_screen = selectedScreen()!;
        let current_screens = screens();

        if (current_screens[current_selected_screen].kind == 'events') {
            let start_events = await getAndCacheEvents(current_screens[current_selected_screen], {
                order: 'asc',
                start: null,
                end: null,
                limit: 1,
            });

            let end_events = await getAndCacheEvents(current_screens[current_selected_screen], {
                order: 'desc',
                start: null,
                end: null,
                limit: 1,
            });

            let start_timestamp = start_events.length == 0 ? null : start_events[0].timestamp;
            let end_timestamp = end_events.length == 0 ? null : end_events[0].timestamp;

            if (start_timestamp == null || end_timestamp == null) {
                return;
            }

            let duration = end_timestamp - start_timestamp;
            let padded_start = start_timestamp - duration * 0.05;
            let padded_end = end_timestamp + duration * 0.05;

            setScreenTimespan([padded_start, padded_end]);
        } else if (current_screens[current_selected_screen].kind == 'spans') {
            let start_spans = await getAndCacheSpans(current_screens[current_selected_screen], {
                order: 'asc',
                start: null,
                end: null,
                limit: 1,
            });

            let end_spans = await getAndCacheSpans(current_screens[current_selected_screen], {
                order: 'desc',
                start: null,
                end: null,
                limit: 50,
            });

            // TODO: this will return a timestamp "before" the one provided if it
            // intersects a span
            let start_timestamp = (start_spans == null || start_spans.length == 0) ? null : start_spans[0].created_at;

            // TODO: this gets the most recent "closed_at" of the 50 most recent
            // spans by "opened_at" which isn't technically guaranteed to be
            // exactly what we want, but its close enough
            let end_timestamp = end_spans?.map(c => c.closed_at)?.reduce((prev, curr) => Math.max(prev || 0, curr || 0));

            if (start_timestamp == null || end_timestamp == null) {
                return;
            }

            let duration = end_timestamp - start_timestamp;
            let padded_start = start_timestamp - duration * 0.05;
            let padded_end = end_timestamp + duration * 0.05;

            setScreenTimespan([padded_start, padded_end]);
        } else if (current_screens[current_selected_screen].kind == 'connections') {
            let start_connections = await getAndCacheConnections(current_screens[current_selected_screen], {
                order: 'asc',
                start: null,
                end: null,
                limit: 1,
            });

            let end_connections = await getAndCacheConnections(current_screens[current_selected_screen], {
                order: 'desc',
                start: null,
                end: null,
                limit: 50,
            });

            // TODO: this will return a timestamp "before" the one provided if it
            // intersects an connection
            let start_timestamp = (start_connections == null || start_connections.length == 0) ? null : start_connections[0].connected_at;

            // TODO: this gets the most recent "disconnected_at" of the 50 most
            // recent connections by "connected_at" which isn't technically
            // guaranteed to be exactly what we want, but its close enough
            let end_timestamp = end_connections?.map(c => c.disconnected_at)?.reduce((prev, curr) => Math.max(prev || 0, curr || 0));

            if (start_timestamp == null || end_timestamp == null) {
                return;
            }

            let duration = end_timestamp - start_timestamp;
            let padded_start = start_timestamp - duration * 0.05;
            let padded_end = end_timestamp + duration * 0.05;

            setScreenTimespan([padded_start, padded_end]);
        }
    }

    async function performNewEventsTab() {
        createTab(...await defaultEventsScreen(), true);
    }

    async function performNewSpansTab() {
        createTab(...await defaultSpansScreen(), true);
    }

    async function performNewConnectionsTab() {
        createTab(...await defaultConnectionsScreen(), true);
    }

    function performDuplicateTab() {
        let current_selected_screen = selectedScreen()!;
        let current_screens = screens();
        let current_column_datas = columnDatas();

        createTab(current_screens[current_selected_screen], current_column_datas[current_selected_screen], true);
    }

    function performCloseAllTabs() {
        removeAllOtherScreens(selectedScreen()!);
    }

    function performFocusFilter() {
        document.getElementById('filter-input')?.focus();
    }

    function performZoomIn() {
        let current_selected_screen = selectedScreen()!;
        let current_screens = screens();
        let current_timespan = current_screens[current_selected_screen].timespan!;

        let bias = 0.5;
        let scale = 1 / 1.1;

        let [start, end] = current_timespan;
        let duration = end - start;
        let middle = start * (1 - bias) + end * bias;

        let new_duration = duration * scale;
        let new_start = middle - new_duration * bias;
        let new_end = middle + new_duration * (1 - bias);

        setScreenTimespan([new_start, new_end]);
    }

    function performZoomOut() {
        let current_selected_screen = selectedScreen()!;
        let current_screens = screens();
        let current_timespan = current_screens[current_selected_screen].timespan!;

        let bias = 0.5;
        let scale = 1.1;

        let [start, end] = current_timespan;
        let duration = end - start;
        let middle = start * (1 - bias) + end * bias;

        let new_duration = duration * scale;
        let new_start = middle - new_duration * bias;
        let new_end = middle + new_duration * (1 - bias);

        setScreenTimespan([new_start, new_end]);
    }

    return (<>
        <NavigationContext.Provider value={{
            createTab,
            removeTab: removeScreen,
            removeAllOtherTabs: removeAllOtherScreens,
            moveTab: moveScreen,
            activateTab: setSelectedScreen,
        }}>
            <TabBar screens={screens()} column_datas={columnDatas()} active={selectedScreen()!} />
            <div id="screen">
                <Show when={selectedScreen() != undefined} fallback={<>
                    <span style="display: block; text-align: center; margin: 50px 0; color: var(--text-light); font-size: 24px; width: 100%;">loading data...</span>
                </>}>
                    {(_idx) => (<Switch>
                        <Match when={getCurrentScreen()!.kind == 'events'}>
                            <EventsScreen
                                raw_filter={getCurrentRawFilters()!}
                                filter={getCurrentScreen()!.filter}
                                setFilter={setScreenFilter}
                                addToFilter={addToFilter}
                                timespan={(getCurrentScreen() as EventsScreenData).timespan}
                                setTimespan={setScreenTimespan}

                                columns={getCurrentColumnData()!.columns}
                                columnWidths={getCurrentColumnData()!.columnWidths}
                                columnUpdate={setColumnDef}
                                columnUpdateWidth={setColumnWidth}
                                columnMove={moveColumn}
                                columnInsert={addColumnAfter}
                                columnRemove={removeColumn}

                                getEvents={f => getAndCacheEvents(getCurrentScreen() as EventsScreenData, f)}
                                getEventCounts={(f, w) => getAndCacheEventCounts(getCurrentScreen() as EventsScreenData, f, w)}

                                live={(getCurrentScreen() as EventsScreenData).live}
                                setLive={setScreenLive}

                                selected={getCurrentSelectedRow() as any}
                                setSelected={setScreenSelected}
                            />
                        </Match>
                        <Match when={getCurrentScreen()!.kind == 'spans'}>
                            <SpansScreen
                                raw_filter={getCurrentRawFilters()!}
                                filter={getCurrentScreen()!.filter}
                                setFilter={setScreenFilter}
                                addToFilter={addToFilter}
                                timespan={(getCurrentScreen() as SpansScreenData).timespan}
                                setTimespan={setScreenTimespan}

                                columns={getCurrentColumnData()!.columns}
                                columnWidths={getCurrentColumnData()!.columnWidths}
                                columnUpdate={setColumnDef}
                                columnUpdateWidth={setColumnWidth}
                                columnMove={moveColumn}
                                columnInsert={addColumnAfter}
                                columnRemove={removeColumn}

                                getSpans={(f, w) => getAndCacheSpans(getCurrentScreen() as SpansScreenData, f, w)}
                                getPositionedSpans={(f, w) => getAndCachePositionedSpans(getCurrentScreen() as SpansScreenData, f, w)}

                                selected={getCurrentSelectedRow() as any}
                                setSelected={setScreenSelected}
                            />
                        </Match>
                        <Match when={getCurrentScreen()!.kind == 'trace'}>
                            <TraceScreen
                                raw_filter={getCurrentRawFilters()!}
                                filter={getCurrentScreen()!.filter}
                                setFilter={setScreenFilter}
                                addToFilter={addToFilter}
                                timespan={getCurrentScreen()!.timespan}
                                setTimespan={setScreenTimespan}

                                columns={getCurrentColumnData()!.columns}
                                columnWidths={getCurrentColumnData()!.columnWidths}
                                columnUpdate={setColumnDef}
                                columnUpdateWidth={setColumnWidth}
                                columnMove={moveColumn}
                                columnInsert={addColumnAfter}
                                columnRemove={removeColumn}

                                getEntries={f => getEntries(getCurrentScreen() as TraceScreenData, f)}

                                collapsed={(getCurrentScreen() as TraceScreenData).collapsed}
                                setCollapsed={setCollapsed}

                                selected={getCurrentSelectedRow() as any}
                                setSelected={setScreenSelected}
                            />
                        </Match>
                        <Match when={getCurrentScreen()!.kind == 'connections'}>
                            <ConnectionsScreen
                                raw_filter={getCurrentRawFilters()!}
                                filter={getCurrentScreen()!.filter}
                                setFilter={setScreenFilter}
                                addToFilter={addToFilter}
                                timespan={(getCurrentScreen() as ConnectionsScreenData).timespan}
                                setTimespan={setScreenTimespan}

                                columns={getCurrentColumnData()!.columns}
                                columnWidths={getCurrentColumnData()!.columnWidths}
                                columnUpdate={setColumnDef}
                                columnUpdateWidth={setColumnWidth}
                                columnMove={moveColumn}
                                columnInsert={addColumnAfter}
                                columnRemove={removeColumn}

                                getConnections={f => getAndCacheConnections(getCurrentScreen() as ConnectionsScreenData, f)}
                                getPositionedConnections={f => getAndCachePositionedConnections(getCurrentScreen() as ConnectionsScreenData, f)}

                                selected={getCurrentSelectedRow() as any}
                                setSelected={setScreenSelected}
                            />
                        </Match>
                    </Switch>)}
                </Show>
            </div>
        </NavigationContext.Provider>

        <div id="statusbar">
            <Show when={status()}>
                {s => <>
                    <span class="statusbar-region">
                        <span style="padding: 0 4px;">using {s().dataset_name}</span>
                        -
                        <span style="padding: 0 4px;" title={s().ingress_error}>{s().ingress_message}</span>
                    </span>
                    <span class="statusbar-region">
                        <span style="padding: 0 4px;">{formatBytesPerSecond(s().ingress_bytes_per_second)}</span>
                        -
                        <span style="padding: 0 4px;" title={s().ingress_error}>connections: {s().ingress_connections}</span>
                        -
                        <span style="padding: 0 4px;">load: {s().engine_load.toFixed(1)}%</span>
                    </span>
                </>}
            </Show>
        </div>
    </>);
}

function formatBytesPerSecond(bytes: number): string {
    if (bytes > 1000000) {
        return (bytes / 1000000).toFixed(1) + ' MB/s';
    } else if (bytes > 1000) {
        return (bytes / 1000).toFixed(1) + ' KB/s';
    } else {
        return bytes.toFixed(1) + ' B/s';
    }
}

export default App;
