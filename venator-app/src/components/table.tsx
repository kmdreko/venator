import { batch, createEffect, createSignal, For, JSX, Show, useContext } from "solid-js";
import { PartialFilter, Timespan } from "../models";
import { Event, Instance, Span, Timestamp } from "../invoke";

import './table.css';
import { CollapsableContext } from "../context/collapsable";
import { Dynamic } from "solid-js/web";

export type ColumnHeaderComponent = (props: ColumnHeaderProps) => JSX.Element;
export type ColumnHeaderProps = {
    last: boolean,
    order: 'asc' | 'desc',
    n: number,
    orderToggle: () => void,
    setWidth: (width: string) => void,
    setProperty: (property: string) => void,
    addColumn: () => void,
    delColumn: () => void,
}

export type ColumnDataComponent<T> = (props: ColumnDataProps<T>) => JSX.Element;
export type ColumnDataProps<T> = {
    entry: T,
    selected: boolean,
    hovered: boolean,
    timespan: Timespan,
    onClick: (e: MouseEvent) => void,
    onHover: (e: MouseEvent, enter: boolean) => void,
}

export type ColumnDef<T> = {
    defaultWidth: string,
    header: ColumnHeaderComponent,
    headerText: string,
    data: ColumnDataComponent<T>,
};

export const LEVEL: ColumnDef<Event | Span> = {
    defaultWidth: "17px",
    header: (props) => {
        return <div class="header level" style={`z-index: ${props.n}`}></div>;
    },
    headerText: "",
    data: (props) => {
        return <div class="data" classList={{ selected: props.selected, hovered: props.hovered }} onclick={props.onClick} onmouseenter={e => props.onHover(e, true)} onmouseleave={e => props.onHover(e, false)}>
            <div class={`level-${props.entry.level}`}></div>
        </div>;
    },
};

export const TIMESTAMP: ColumnDef<Event> = {
    defaultWidth: "176px",
    header: (props) => {
        return (<div class="header" style={`z-index: ${props.n}`}>
            <div class="header-text">#timestamp</div>
            <button onclick={props.orderToggle}>{props.order == 'asc' ? "▼" : "▲"}</button>
        </div>);
    },
    headerText: "#timestamp",
    data: (props) => {
        return <div class="data" classList={{ selected: props.selected, hovered: props.hovered }} onclick={props.onClick} onmouseenter={e => props.onHover(e, true)} onmouseleave={e => props.onHover(e, false)}>
            {formatTimestamp(props.entry.timestamp)}
        </div>;
    },
};

export const CREATED: ColumnDef<Span> = {
    defaultWidth: "176px",
    header: (props) => {
        return (<div class="header" style={`z-index: ${props.n}`}>
            <div class="header-text">#created</div>
            <button onclick={props.orderToggle}>{props.order == 'asc' ? "▼" : "▲"}</button>
        </div>);
    },
    headerText: "#created",
    data: (props) => {
        return <div class="data" classList={{ selected: props.selected, hovered: props.hovered }} onclick={props.onClick} onmouseenter={e => props.onHover(e, true)} onmouseleave={e => props.onHover(e, false)}>
            {formatTimestamp(props.entry.created_at)}
        </div>;
    },
};

export const CONNECTED: ColumnDef<Instance> = {
    defaultWidth: "176px",
    header: (props) => {
        return (<div class="header" style={`z-index: ${props.n}`}>
            <div class="header-text">#connected</div>
            <button onclick={props.orderToggle}>{props.order == 'asc' ? "▼" : "▲"}</button>
        </div>);
    },
    headerText: "#connected",
    data: (props) => {
        return <div class="data" classList={{ selected: props.selected, hovered: props.hovered }} onclick={props.onClick} onmouseenter={e => props.onHover(e, true)} onmouseleave={e => props.onHover(e, false)}>
            {formatTimestamp(props.entry.connected_at)}
        </div>;
    },
};

export const ATTRIBUTE = (attribute: string): ColumnDef<Event | Span | Instance> => ({
    defaultWidth: "minmax(100px, 1fr)",
    header: (props) => {
        return (<ResizeableHeader n={props.n} enabled={!props.last} onchange={props.setWidth} onremove={props.delColumn}>
            <EditableHeaderText onchange={props.setProperty}>
                @{attribute}
            </EditableHeaderText>
            <button onclick={props.addColumn}>+</button>
        </ResizeableHeader>);
    },
    headerText: `@${attribute}`,
    data: (props) => {
        return <div class="data" classList={{ selected: props.selected, hovered: props.hovered }} onclick={props.onClick} onmouseenter={e => props.onHover(e, true)} onmouseleave={e => props.onHover(e, false)}>
            {props.entry.attributes.find(a => a.name == attribute)?.value ?? '---'}
        </div>;
    },
});

export const INHERENT = (inherent: string): ColumnDef<Event | Span | Instance> => ({
    defaultWidth: "minmax(100px, 1fr)",
    header: (props) => {
        return (<ResizeableHeader n={props.n} enabled={!props.last} onchange={props.setWidth} onremove={props.delColumn}>
            <EditableHeaderText onchange={props.setProperty}>
                #{inherent}
            </EditableHeaderText>
            <button onclick={props.addColumn}>+</button>
        </ResizeableHeader>);
    },
    headerText: `#${inherent}`,
    data: (props) => {
        return <div class="data" classList={{ selected: props.selected, hovered: props.hovered }} onclick={props.onClick} onmouseenter={e => props.onHover(e, true)} onmouseleave={e => props.onHover(e, false)}>
            {(props.entry as any)[inherent] ?? '---'}
        </div>;
    },
});

export const DURATION: ColumnDef<Span | Instance> = {
    defaultWidth: "minmax(100px, 1fr)",
    header: (props) => {
        return (<ResizeableHeader n={props.n} enabled={!props.last} onchange={props.setWidth} onremove={props.delColumn}>
            <EditableHeaderText onchange={props.setProperty}>
                #duration
            </EditableHeaderText>
            <button onclick={props.addColumn}>+</button>
        </ResizeableHeader>);
    },
    headerText: '#duration',
    data: (props) => {
        function renderedDuration(e: Span | Instance) {
            let start: number = (e as any).created_at ?? (e as any).connected_at;
            let end: number | null = (e as any).closed_at ?? (e as any).disconnected_at;
            if (end == null) {
                return '---';
            }

            let duration = end - start;

            const MILLISECOND = 1000;
            const SECOND = 1000000;
            const MINUTE = 60000000;
            const HOUR = 3600000000;
            const DAY = 86400000000;

            if (duration / DAY >= 1.0)
                return `${(duration / DAY).toPrecision(3)}d`;
            else if (duration / HOUR >= 1.0)
                return `${(duration / HOUR).toPrecision(3)}h`;
            else if (duration / MINUTE >= 1.0)
                return `${(duration / MINUTE).toPrecision(3)}min`;
            else if (duration / SECOND >= 1.0)
                return `${(duration / SECOND).toPrecision(3)}s`;
            else
                return `${(duration / MILLISECOND).toPrecision(3)}ms`;
        }

        return <div class="data" classList={{ selected: props.selected, hovered: props.hovered }} onclick={props.onClick} onmouseenter={e => props.onHover(e, true)} onmouseleave={e => props.onHover(e, false)}>
            {renderedDuration(props.entry)}
        </div>;
    },
};

export const UNKNOWN = (property: string): ColumnDef<Event | Span | Instance> => ({
    defaultWidth: "minmax(100px, 1fr)",
    header: (props) => {
        return (<ResizeableHeader n={props.n} enabled={!props.last} onchange={props.setWidth} onremove={props.delColumn}>
            <EditableHeaderText onchange={props.setProperty}>
                {property}
            </EditableHeaderText>
            <button onclick={props.addColumn}>+</button>
        </ResizeableHeader>);
    },
    headerText: `${property}`,
    data: (props) => {
        return <div class="data" classList={{ selected: props.selected, hovered: props.hovered }} onclick={props.onClick} onmouseenter={e => props.onHover(e, true)} onmouseleave={e => props.onHover(e, false)}>
            ---
        </div>;
    },
});

export const TIMESPAN: ColumnDef<Event | Span> = {
    defaultWidth: "300px",
    header: (props) => {
        return (<ResizeableHeader n={props.n} enabled={!props.last} onchange={props.setWidth} onremove={() => { }}>
            <div class="header-text">timespan</div>
            <button onclick={props.orderToggle}>{props.order == 'asc' ? "▼" : "▲"}</button>
        </ResizeableHeader>);
    },
    headerText: "timespan",
    data: (props) => {
        function position(entry: Event | Span): { left: string, right?: string } {
            let current_timespan = props.timespan;
            let [start, end] = current_timespan;
            let duration = end - start;

            if ((entry as any).timestamp != undefined) {
                let event = entry as Event;
                let left = (event.timestamp - start) / duration;

                return {
                    left: `${left * 100}%`,
                };
            } else {
                let span = entry as Span;
                let left = (span.created_at - start) / duration;
                let right = (span.closed_at == null) ? 0.0 : (end - span.closed_at) / duration;

                return {
                    left: `${left * 100}%`,
                    right: `${right * 100}%`,
                };
            }
        }

        return (<div class="data" classList={{ selected: props.selected, hovered: props.hovered }} onclick={props.onClick} onmouseenter={e => props.onHover(e, true)} onmouseleave={e => props.onHover(e, false)}>
            <div class="time-bar" style={{ ...position(props.entry as Span) }}></div>
        </div>);
    },
};

export const COLLAPSABLE: ColumnDef<Event | Span> = {
    defaultWidth: "22px",
    header: (props) => {
        return <div class="header collapsable" style={`z-index: ${props.n}`}></div>;
    },
    headerText: "",
    data: (props) => {
        let context = useContext(CollapsableContext);

        function collapsed(): boolean {
            let id: string = (props.entry as any).id;
            return context?.isCollapsed(id) ?? false;
        }
        function toggle() {
            let id: string = (props.entry as any).id;
            if (context == undefined) {
                return;
            }

            context.collapse(id, !context.isCollapsed(id));
        }
        return (props.entry as any).id == undefined
            ? (<div class="data" classList={{ selected: props.selected, hovered: props.hovered }} onclick={props.onClick} onmouseenter={e => props.onHover(e, true)} onmouseleave={e => props.onHover(e, false)}></div>)
            : (<div class="data" classList={{ selected: props.selected, hovered: props.hovered }} onclick={toggle} onmouseenter={e => props.onHover(e, true)} onmouseleave={e => props.onHover(e, false)}>
                {collapsed() ? '⏶' : '⏷'}
            </div>);
    },
};

export const COMBINED = (spanDef: ColumnDef<Span>, eventDef: ColumnDef<Event>): ColumnDef<Span | Event> => ({
    defaultWidth: spanDef.defaultWidth,
    header: (props) => {
        return (<ResizeableHeader n={props.n} enabled={!props.last} onchange={props.setWidth} onremove={props.delColumn}>
            <div class="header-text">{spanDef.headerText} / {eventDef.headerText}</div>
            <button onclick={props.addColumn}>+</button>
        </ResizeableHeader>);
    },
    headerText: `${spanDef.headerText} / ${eventDef.headerText}`,
    data: (props) => {
        if ((props.entry as any).timestamp != undefined) {
            return eventDef.data(props as ColumnDataProps<Event>);
        } else {
            return spanDef.data(props as ColumnDataProps<Span>);
        }
    },
});

function formatTimestamp(timestamp: number): string {
    var datetime = new Date(timestamp / 1000);
    return datetime.getFullYear() + "-" + (datetime.getMonth() + 1).toString().padStart(2, '0') + "-" +
        datetime.getDate().toString().padStart(2, '0') + " " + datetime.getHours().toString().padStart(2, '0') + ":" +
        datetime.getMinutes().toString().padStart(2, '0') + ":" + datetime.getSeconds().toString().padStart(2, '0') + "." +
        datetime.getMilliseconds().toString().padStart(3, '0');
}

let CACHE_START_LAST = 0;
let CACHE_START_DELAY_MS = 250;

export type TableProps<T> = {
    timespan: Timespan,

    columns: ColumnDef<T>[],
    columnWidths: string[],
    columnUpdate: (i: number, def: ColumnDef<T>) => void,
    columnUpdateWidth: (i: number, width: string) => void,
    columnInsert: (i: number, def: ColumnDef<T>) => void,
    columnRemove: (i: number) => void,
    columnDefault: ColumnDef<T>,
    columnMin: number,

    selectedRow: T | null,
    setSelectedRow: (e: T | null) => void,
    hoveredRow: T | null,
    setHoveredRow: (e: T | null) => void,

    getEntries: (filter: PartialFilter, wait?: boolean) => Promise<T[] | null>,
};

export function getColumnDef<T extends Event | Span | Instance>(property: string): ColumnDef<T> {
    if (property == 'name' || property == '#name') {
        return INHERENT('name');
    }
    if (property == 'target' || property == '#target') {
        return INHERENT('target');
    }
    if (property == 'file' || property == '#file') {
        return INHERENT('file');
    }
    if (property == 'duration' || property == '#duration') {
        return DURATION as any;
    }

    if (property.startsWith('#')) {
        return UNKNOWN(property);
    }

    if (property.startsWith('@')) {
        return ATTRIBUTE(property.slice(1));
    }

    return ATTRIBUTE(property);
}

export function Table<T extends Event | Span | Instance>(props: TableProps<T>) {
    const [entries, setEntries] = createSignal([] as T[]);
    const [status, setStatus] = createSignal('partial' as 'partial' | 'loading' | 'done');
    const [order, setOrder] = createSignal('asc' as 'asc' | 'desc');

    var table_wrapper: any;
    var table_bottom: any;

    createEffect(async () => {
        let current_order = order();
        let current_timespan = props.timespan;
        let [start, end] = current_timespan;

        let now = Date.now();
        let primed = await props.getEntries({ order: current_order, start, end }, false);
        if (primed == null && now < CACHE_START_LAST + CACHE_START_DELAY_MS) {
            await new Promise(resolve => setTimeout(resolve, CACHE_START_DELAY_MS));
            if (props.timespan != current_timespan) {
                return;
            }
        }

        CACHE_START_LAST = now;
        let events = (await props.getEntries({ order: current_order, start, end }))!;

        if (current_timespan != props.timespan) {
            return;
        }

        batch(() => {
            setEntries(events);
            setStatus((events.length == 50) ? 'partial' : 'done');
        })
    })

    async function loadMore() {
        if (status() != 'partial') {
            return;
        }

        let current_timespan = props.timespan;
        let [start, end] = current_timespan;

        setStatus('loading');
        let current_order = order();
        let current_entries = entries();
        let last_entry = current_entries[current_entries.length - 1];
        let new_events = (await props.getEntries({ order: current_order, start, end, previous: getTimestamp(last_entry) }))!;
        setEntries(current_entries.concat(new_events));
        setStatus((new_events.length == 50) ? 'partial' : 'done');
    }

    function getTimestamp(e: T): Timestamp {
        return (e as any).timestamp || (e as any).created_at || (e as any).connected_at;
    }

    function toggleOrder() {
        let current_order = order();
        let new_order: 'asc' | 'desc';
        if (current_order == 'asc') {
            new_order = 'desc';
        } else {
            new_order = 'asc';
        }
        setOrder(new_order);
    }

    function onClickRow(_e: MouseEvent, entry: T) {
        let current_selected_entry = props.selectedRow;
        if (current_selected_entry == entry) {
            props.setSelectedRow(null);
        } else {
            props.setSelectedRow(entry);
        }
    }

    function onHoverRow(_e: MouseEvent, entry: T, enter: boolean) {
        if (enter) {
            props.setHoveredRow(entry);
        } else if (props.hoveredRow == entry) {
            props.setHoveredRow(null);
        }
    }

    function isSelected(row: T): boolean {
        let selected = props.selectedRow;
        if (selected == null) {
            return false;
        }

        return getTimestamp(selected) == getTimestamp(row);
    }

    function isHovered(row: T): boolean {
        let hovered = props.hoveredRow;
        if (hovered == null) {
            return false;
        }

        return getTimestamp(hovered) == getTimestamp(row);
    }

    function getGridTemplateColumns(): string {
        return props.columnWidths.join(' ');
    }

    function removeColumn(i: number) {
        if (props.columns.length == props.columnMin) {
            props.columnUpdate(i, props.columnDefault);
        } else {
            props.columnRemove(i);
        }
    }

    let trailer = <div style="padding-left: 4px; white-space: nowrap" ref={table_bottom}>{(status() == 'partial') ? 'load more entries'
        : (status() == 'loading') ? 'loading more entries'
            : 'no more entries'}</div>;

    let trailerObserver = new IntersectionObserver((entries, _ob) => {
        for (let e of entries) {
            if (e.isIntersecting) {
                loadMore();
            }
        }
    });

    trailerObserver.observe(trailer as Element);

    return (<div ref={table_wrapper} id="table" style={{ ['grid-template-columns']: getGridTemplateColumns() }}>
        <For each={props.columns}>
            {(column, i) => (<Dynamic component={column.header}
                n={props.columns.length - i()}
                order={order()}
                orderToggle={toggleOrder}
                last={i() == props.columns.length - 1}
                setWidth={(w: string) => props.columnUpdateWidth(i(), w)}
                setProperty={(p: string) => props.columnUpdate(i(), getColumnDef(p))}
                addColumn={() => props.columnInsert(i(), props.columnDefault)}
                delColumn={() => removeColumn(i())}
            />)}
        </For>
        <For each={entries()}>
            {row => {
                return (<For each={props.columns}>
                    {column => (<Dynamic
                        component={column.data}
                        entry={row}
                        selected={isSelected(row)}
                        hovered={isHovered(row)}
                        timespan={props.timespan}
                        onClick={(e: MouseEvent) => onClickRow(e, row)}
                        onHover={(e: MouseEvent, enter: boolean) => onHoverRow(e, row, enter)}
                    />)}
                </For>);
            }}
        </For>
        {trailer}
    </div>);
}

type EditableHeaderTextProps = {
    onchange: (value: string) => void,
    children: JSX.Element,
}

function EditableHeaderText(props: EditableHeaderTextProps) {
    function onblur(this: HTMLDivElement) {
        props.onchange(this.innerText);
    }

    function onkeydown(this: HTMLDivElement, e: KeyboardEvent) {
        if (e.key == "Enter") {
            e.preventDefault();
            this.blur();
        }
    }

    return (<div class="header-text">
        <div contenteditable onblur={onblur} onkeydown={onkeydown}>{props.children}</div>
    </div>);
}

type ResizeableHeaderProps = {
    n: number,
    enabled: boolean,
    onchange: (value: string) => void,
    onremove: () => void,
    children: JSX.Element,
}

function ResizeableHeader(props: ResizeableHeaderProps) {
    let [dragging, setDragging] = createSignal<boolean>(false);

    let header_ref!: HTMLTableCellElement;

    function ongrab(e: MouseEvent) {
        setDragging(true);
        e.preventDefault();
    }

    let frameHandle: number | undefined;
    function onmove(e: MouseEvent) {
        if (!dragging()) {
            return;
        }

        if (frameHandle != undefined) {
            return;
        }

        frameHandle = requestAnimationFrame(() => {
            let mousex = e.clientX;
            let headerx = header_ref.getBoundingClientRect().left;

            let width = Math.max(mousex - headerx + 2, 100);
            props.onchange(`${width}px`);
            frameHandle = undefined;
        })
    }

    function onrelease() {
        setDragging(false);
    }

    function onclick(e: MouseEvent) {
        if (e.button == 1) {
            props.onremove();
        }
    }

    function onmousedown(e: MouseEvent) {
        if (e.button == 1) {
            e.preventDefault()
        }
    }

    return (<div class="header" style={`z-index: ${props.n}`} ref={header_ref} onauxclick={onclick} onmousedown={onmousedown}>
        {props.children}
        <Show when={props.enabled}>
            <div class="grabber" classList={{ grabbed: dragging() }} onmousedown={ongrab} onmousemove={onmove} onmouseup={onrelease} onmouseleave={onrelease}></div>
        </Show>
    </div>);
}
