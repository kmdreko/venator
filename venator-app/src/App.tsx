import { EventsScreen } from "./screens/events-screen";
import { Event, getStats, Input, Instance, Span, Timestamp } from "./invoke";
import { batch, createSignal, Match, onMount, Show, Switch } from "solid-js";
import { Counts, PaginationFilter, PartialEventCountFilter, PartialFilter, PositionedInstance, PositionedSpan, Timespan } from "./models";
import { SpansScreen } from "./screens/spans-screen";
import { EventDataLayer, SpanDataLayer, TraceDataLayer, InstanceDataLayer } from "./utils/datalayer";
import { NavigationContext } from "./context/navigation";
import { TraceScreen } from "./screens/trace-screen";
import { ATTRIBUTE, ColumnDef, CONNECTED, CREATED, INHERENT, LEVEL, TIMESTAMP } from "./components/table";
import { InstancesScreen } from "./screens/instances-screen";
import { TabBar } from "./components/tab-bar";

import "./App.css";

const HOUR = 3600000000;

type EventsScreenData = {
    kind: 'events',
    filter: Input[],
    timespan: Timespan,
    selected: Event | null,
    live: boolean,
    store: EventDataLayer,
    columns: ColumnDef<Event>[],
    columnWidths: string[],
};

type SpansScreenData = {
    kind: 'spans',
    filter: Input[],
    timespan: Timespan,
    selected: Span | null,
    live: boolean,
    store: SpanDataLayer,
    columns: ColumnDef<Span>[],
    columnWidths: string[],
};

type TraceScreenData = {
    kind: 'trace',
    filter: Input[],
    timespan: Timespan | null,
    selected: Event | Span | null,
    live: boolean,
    store: TraceDataLayer,
    collapsed: { [id: string]: true },
    columns: ColumnDef<Event | Span>[],
    columnWidths: string[],
};

type InstancesScreenData = {
    kind: 'instances',
    filter: Input[],
    timespan: Timespan,
    selected: Instance | null,
    live: boolean,
    store: InstanceDataLayer,
    columns: ColumnDef<Instance>[],
    columnWidths: string[],
};

export type ScreenData = EventsScreenData | SpansScreenData | TraceScreenData | InstancesScreenData;

export async function defaultEventsScreen(): Promise<EventsScreenData> {
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
    }

    let filter: Input[] = [{
        text: "#level: >=TRACE",
        input: 'valid',
        property_kind: 'Inherent',
        property: "level",
        value_kind: 'comparison',
        value: ['Gte', "TRACE"],
        editable: false,
    }];
    let columns = [LEVEL, TIMESTAMP, ATTRIBUTE("message")];
    let columnWidths = columns.map(def => def.defaultWidth);

    return {
        kind: 'events',
        filter,
        timespan: [start, end],
        selected: null,
        live: false,
        store: new EventDataLayer(filter),
        columns,
        columnWidths,
    };
}

export async function defaultSpansScreen(): Promise<SpansScreenData> {
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
    }

    let filter: Input[] = [{
        text: "#level: >=TRACE",
        input: 'valid',
        property_kind: 'Inherent',
        property: "level",
        value_kind: 'comparison',
        value: ['Gte', "TRACE"],
        editable: false,
    }, {
        text: "#parent: none",
        input: 'valid',
        property_kind: 'Inherent',
        property: "parent",
        value_kind: 'comparison',
        value: ['Eq', "none"],
    }];
    let columns = [LEVEL, CREATED, INHERENT('name')];
    let columnWidths = columns.map(def => def.defaultWidth);

    return {
        kind: 'spans',
        filter,
        timespan: [start, end],
        selected: null,
        live: false,
        store: new SpanDataLayer(filter),
        columns,
        columnWidths,
    };
}

export async function defaultInstancesScreen(): Promise<InstancesScreenData> {
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
    }

    let columns = [CONNECTED, INHERENT('id')];
    let columnWidths = columns.map(def => def.defaultWidth);
    return {
        kind: 'instances',
        filter: [],
        timespan: [start, end],
        selected: null,
        live: false,
        store: new InstanceDataLayer([]),
        columns,
        columnWidths,
    };
}

function App() {
    let [screens, setScreens] = createSignal<ScreenData[]>([]);
    let [selectedScreen, setSelectedScreen] = createSignal<number | undefined>();

    onMount(async () => {
        createTab(await defaultEventsScreen(), true);
    });

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

    async function getAndCacheInstances(screen: InstancesScreenData, filter: PartialFilter): Promise<Instance[] | null> {
        return await screen.store.getInstances(filter);
    }

    async function getAndCachePositionedInstances(screen: InstancesScreenData, filter: PartialFilter): Promise<PositionedInstance[] | null> {
        return await screen.store.getPositionedInstances(filter);
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

    function setScreenFilter(filter: Input[]) {
        let current_selected_screen = selectedScreen()!;
        let current_screens = screens();
        let updated_screens = [...current_screens];

        if (current_screens[current_selected_screen].live) {
            current_screens[current_selected_screen].store.unsubscribe();
        }

        updated_screens[current_selected_screen] = current_screens[current_selected_screen].kind == 'events'
            ? { ...current_screens[current_selected_screen], filter, store: new EventDataLayer(filter) }
            : current_screens[current_selected_screen].kind == 'spans'
                ? { ...current_screens[current_selected_screen], filter, store: new SpanDataLayer(filter) }
                : current_screens[current_selected_screen].kind == 'instances'
                    ? { ...current_screens[current_selected_screen], filter, store: new InstanceDataLayer(filter) }
                    : { ...current_screens[current_selected_screen], filter };

        if (updated_screens[current_selected_screen].live) {
            updated_screens[current_selected_screen].store.subscribe();
        }

        setScreens(updated_screens);
    }

    function addToFilter(filter: Input[]) {
        let current_selected_screen = selectedScreen()!;
        let current_screens = screens();

        setScreenFilter([...current_screens[current_selected_screen].filter, ...filter]);
    }

    function setScreenTimespan(timespan: Timespan) {
        let current_selected_screen = selectedScreen()!;
        let current_screens = screens();
        let updated_screens = [...current_screens];
        updated_screens[current_selected_screen] = {
            ...current_screens[current_selected_screen],
            timespan: normalizeTimespan(timespan),
        };
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
        setScreens(updated_screens);
    }

    function setScreenSelected<T>(selected: T | null) {
        let current_selected_screen = selectedScreen()!;
        let current_screens = screens();
        let updated_screens = [...current_screens];
        updated_screens[current_selected_screen] = {
            ...current_screens[current_selected_screen],
            selected: selected as any,
        };
        setScreens(updated_screens);
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

    function removeScreen(idx: number) {
        let current_selected_screen = selectedScreen()!;
        let current_screens = screens();
        let updated_screens = [...current_screens];
        updated_screens.splice(idx, 1);

        if (updated_screens.length == 0) {
            let filter: Input[] = [{
                text: "#level: >=TRACE",
                input: 'valid',
                property_kind: 'Inherent',
                property: "level",
                value_kind: 'comparison',
                value: ['Gte', "TRACE"],
                editable: false,
            }];
            let columns = [LEVEL, TIMESTAMP, ATTRIBUTE("message")];
            let columnWidths = columns.map(def => def.defaultWidth);
            let now = Date.now() * 1000;
            updated_screens = [{
                kind: 'events',
                filter,
                timespan: [now - 5 * 60 * 1000000, now],
                selected: null,
                live: false,
                store: new EventDataLayer(filter),
                columns,
                columnWidths,
            }];
        }

        let updated_selected_screen = (current_selected_screen > idx) ? current_selected_screen - 1 : current_selected_screen;
        if (updated_selected_screen == updated_screens.length) {
            updated_selected_screen -= 1;
        }

        batch(() => {
            setScreens(updated_screens);
            setSelectedScreen(updated_selected_screen);
        })
    }

    function removeAllOtherScreens(idx: number) {
        let current_screens = screens();
        let selected_screen = current_screens[idx];

        batch(() => {
            setScreens([selected_screen]);
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

        let updated_selected_screen = current_selected_screen;
        if (current_selected_screen == fromIdx) {
            updated_selected_screen = toIdx;
        } else if (current_selected_screen < fromIdx && current_selected_screen >= toIdx) {
            updated_selected_screen += 1;
        } else if (current_selected_screen > fromIdx && current_selected_screen <= toIdx) {
            updated_selected_screen -= 1;
        }

        batch(() => {
            setScreens(updated_screens);
            setSelectedScreen(updated_selected_screen);
        })
    }

    function createTab(screen: ScreenData, navigate: boolean) {
        let current_screens = screens();
        let updated_screens = [...current_screens];
        updated_screens.push(screen);
        batch(() => {
            setScreens(updated_screens);
            if (navigate) {
                setSelectedScreen(updated_screens.length - 1);
            }
        })
    }

    function setColumnWidth(i: number, width: string) {
        let current_selected_screen = selectedScreen()!;
        let current_screens = screens();
        let updated_screens = [...current_screens];

        let widths = current_screens[current_selected_screen].columnWidths;
        widths.splice(i, 1, width);

        updated_screens[current_selected_screen] = {
            ...current_screens[current_selected_screen],
            columnWidths: widths
        };
        setScreens(updated_screens);
    }

    function setColumnDef<T>(i: number, def: ColumnDef<T>) {
        let current_selected_screen = selectedScreen()!;
        let current_screens = screens();
        let updated_screens = [...current_screens];

        let defs = current_screens[current_selected_screen].columns;
        defs.splice(i, 1, def as any);

        updated_screens[current_selected_screen] = {
            ...current_screens[current_selected_screen],
            columns: defs as any
        };
        setScreens(updated_screens);
    }

    function addColumnAfter<T>(i: number, def: ColumnDef<T>) {
        let current_selected_screen = selectedScreen()!;
        let current_screens = screens();

        let existingColumns = current_screens[current_selected_screen].columns;
        let existingColumnWidths = current_screens[current_selected_screen].columnWidths;
        let updatedColumns = [...existingColumns];
        let updatedColumnWidths = [...existingColumnWidths];

        if (i == -1) {
            i = existingColumns.length - 1;
        }

        updatedColumns.splice(i + 1, 0, def as any);
        updatedColumnWidths.splice(i + 1, 0, def.defaultWidth);

        let updated_screens = [...current_screens];
        updated_screens[current_selected_screen] = {
            ...current_screens[current_selected_screen],
            columns: updatedColumns as any,
            columnWidths: updatedColumnWidths,
        };
        setScreens(updated_screens);
    }

    function removeColumn(i: number) {
        let current_selected_screen = selectedScreen()!;
        let current_screens = screens();

        let existingColumns = current_screens[current_selected_screen].columns;
        let existingColumnWidths = current_screens[current_selected_screen].columnWidths;
        let updatedColumns = [...existingColumns];
        let updatedColumnWidths = [...existingColumnWidths];
        updatedColumns.splice(i, 1);
        updatedColumnWidths.splice(i, 1);

        // if we remove the last column, ensure the new last column is reset to
        // its default width.
        if (i == updatedColumns.length) {
            updatedColumnWidths[updatedColumnWidths.length - 1] = updatedColumns[updatedColumns.length - 1].defaultWidth;
        }

        let updated_screens = [...current_screens];
        updated_screens[current_selected_screen] = {
            ...current_screens[current_selected_screen],
            columns: updatedColumns as any,
            columnWidths: updatedColumnWidths,
        };
        setScreens(updated_screens);
    }

    return (<>
        <NavigationContext.Provider value={{
            createTab,
            removeTab: removeScreen,
            removeAllOtherTabs: removeAllOtherScreens,
            moveTab: moveScreen,
            activateTab: setSelectedScreen,
        }}>
            <TabBar screens={screens()} active={selectedScreen()!} />
            <div id="screen">
                <Show when={getCurrentScreen()}>
                    {screen => (<Switch>
                        <Match when={screen().kind == 'events'}>
                            <EventsScreen
                                filter={screen().filter}
                                setFilter={setScreenFilter}
                                addToFilter={addToFilter}
                                timespan={(screen() as EventsScreenData).timespan}
                                setTimespan={setScreenTimespan}

                                columns={(screen() as EventsScreenData).columns}
                                columnWidths={(screen() as EventsScreenData).columnWidths}
                                columnUpdate={setColumnDef}
                                columnUpdateWidth={setColumnWidth}
                                columnInsert={addColumnAfter}
                                columnRemove={removeColumn}

                                getEvents={f => getAndCacheEvents(screen() as EventsScreenData, f)}
                                getEventCounts={(f, w) => getAndCacheEventCounts(screen() as EventsScreenData, f, w)}

                                live={(screen() as EventsScreenData).live}
                                setLive={live => {
                                    let store = (screen() as EventsScreenData).store;
                                    if (live) {
                                        store.subscribe();
                                    } else {
                                        store.unsubscribe();
                                    }
                                    setScreenLive(live);
                                }}

                                selected={(screen() as EventsScreenData).selected}
                                setSelected={setScreenSelected}
                            />
                        </Match>
                        <Match when={screen().kind == 'spans'}>
                            <SpansScreen
                                filter={screen().filter}
                                setFilter={setScreenFilter}
                                addToFilter={addToFilter}
                                timespan={(screen() as SpansScreenData).timespan}
                                setTimespan={setScreenTimespan}

                                columns={(screen() as SpansScreenData).columns}
                                columnWidths={(screen() as SpansScreenData).columnWidths}
                                columnUpdate={setColumnDef}
                                columnUpdateWidth={setColumnWidth}
                                columnInsert={addColumnAfter}
                                columnRemove={removeColumn}

                                getSpans={(f, w) => getAndCacheSpans(screen() as SpansScreenData, f, w)}
                                getPositionedSpans={(f, w) => getAndCachePositionedSpans(screen() as SpansScreenData, f, w)}

                                selected={(screen() as SpansScreenData).selected}
                                setSelected={setScreenSelected}
                            />
                        </Match>
                        <Match when={screen().kind == 'trace'}>
                            <TraceScreen
                                filter={screen().filter}
                                setFilter={setScreenFilter}
                                addToFilter={addToFilter}
                                timespan={screen().timespan}
                                setTimespan={setScreenTimespan}

                                columns={(screen() as TraceScreenData).columns}
                                columnWidths={(screen() as TraceScreenData).columnWidths}
                                columnUpdate={setColumnDef}
                                columnUpdateWidth={setColumnWidth}
                                columnInsert={addColumnAfter}
                                columnRemove={removeColumn}

                                getEntries={f => getEntries(screen() as TraceScreenData, f)}

                                collapsed={(screen() as TraceScreenData).collapsed}
                                setCollapsed={setCollapsed}

                                selected={(screen() as TraceScreenData).selected}
                                setSelected={setScreenSelected}
                            />
                        </Match>
                        <Match when={screen().kind == 'instances'}>
                            <InstancesScreen
                                filter={screen().filter}
                                setFilter={setScreenFilter}
                                addToFilter={addToFilter}
                                timespan={(screen() as InstancesScreenData).timespan}
                                setTimespan={setScreenTimespan}

                                columns={(screen() as InstancesScreenData).columns}
                                columnWidths={(screen() as InstancesScreenData).columnWidths}
                                columnUpdate={setColumnDef}
                                columnUpdateWidth={setColumnWidth}
                                columnInsert={addColumnAfter}
                                columnRemove={removeColumn}

                                getInstances={f => getAndCacheInstances(screen() as InstancesScreenData, f)}
                                getPositionedInstances={f => getAndCachePositionedInstances(screen() as InstancesScreenData, f)}

                                selected={(screen() as InstancesScreenData).selected}
                                setSelected={setScreenSelected}
                            />
                        </Match>
                    </Switch>)}
                </Show>
            </div>
        </NavigationContext.Provider>

        <div id="statusbar">
            <span style="padding: 0 4px;">Listening on 0.0.0.0:8362</span>
        </div>
    </>);
}

export default App;
