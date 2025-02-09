import { batch, createEffect, createSignal, For, JSX, Show, untrack, useContext } from "solid-js";
import { LogicalPosition } from "@tauri-apps/api/dpi";
import { PartialFilter, Timespan } from "../models";
import { Event, Span, Timestamp } from "../invoke";
import { CollapsableContext } from "../context/collapsable";
import { Dynamic } from "solid-js/web";
import { Menu } from "@tauri-apps/api/menu";
import { writeText } from "@tauri-apps/plugin-clipboard-manager";
import { createVirtualizer } from "@tanstack/solid-virtual";

import './table.css';

export type ColumnHeaderComponent = (props: ColumnHeaderProps) => JSX.Element;
export type ColumnHeaderProps = {
    last: boolean,
    order: 'asc' | 'desc',
    n: number,
    total: number,
    min: number,
    orderToggle: () => void,
    setWidth: (width: string) => void,
    setProperty: (property: string) => void,
    moveColumn: (offset: number) => void,
    addColumn: () => void,
    delColumn: () => void,
}

export type ColumnDataComponent<T> = (props: ColumnDataProps<T>) => JSX.Element;
export type ColumnDataProps<T> = {
    style: JSX.CSSProperties,
    entry: T,
    selected: boolean,
    hovered: boolean,
    timespan: Timespan,
    onClick: (e: MouseEvent) => void,
    onHover: (e: MouseEvent, enter: boolean) => void,
    addToFilter: (filter: string) => void,
}

export type ColumnDef<T> = {
    defaultWidth: string,
    header: ColumnHeaderComponent,
    headerText: string,
    data: ColumnDataComponent<T>,
    dataText: (t: T) => string,
};

function getNavigationOptions(idx: number, end: number, min: number, move: (to: number) => void) {
    let minIdx = min - 1;
    let maxIdx = end - 1;

    if (idx < minIdx) {
        return [];
    }

    return [
        { text: "move left", enabled: idx != minIdx, action: () => move(idx - 1) },
        { text: "move far left", enabled: idx != minIdx, action: () => move(minIdx) },
        { text: "move right", enabled: idx != maxIdx, action: () => move(idx + 1) },
        { text: "move far right", enabled: idx != maxIdx, action: () => move(maxIdx) },
    ];
}

function levelText(entity: Event | Span) {
    switch (entity.level) {
        case 0:
            return 'TRACE';
        case 1:
            return 'DEBUG';
        case 2:
            return 'INFO';
        case 3:
            return 'WARN';
        case 4:
            return 'ERROR';
        case 5:
            return 'FATAL';
    }
}

export const LEVEL: ColumnDef<Event | Span> = {
    defaultWidth: "17px",
    header: (props) => {
        return <div class="header level" style={`z-index: ${props.n}`}></div>;
    },
    headerText: "#level",
    data: (props) => {
        return <div class="data" style={{ ...props.style }} classList={{ selected: props.selected, hovered: props.hovered }} title={levelText(props.entry)} onclick={props.onClick} onmouseenter={e => props.onHover(e, true)} onmouseleave={e => props.onHover(e, false)}>
            <div class={`level-${props.entry.level}`}></div>
        </div>;
    },
    dataText: (entity) => levelText(entity),
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
        return <div class="data" style={{ ...props.style }} classList={{ selected: props.selected, hovered: props.hovered }} onclick={props.onClick} onmouseenter={e => props.onHover(e, true)} onmouseleave={e => props.onHover(e, false)}>
            {formatTimestamp(props.entry.timestamp)}
        </div>;
    },
    dataText: (event) => formatTimestamp(event.timestamp),
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
        return <div class="data" style={{ ...props.style }} classList={{ selected: props.selected, hovered: props.hovered }} onclick={props.onClick} onmouseenter={e => props.onHover(e, true)} onmouseleave={e => props.onHover(e, false)}>
            {formatTimestamp(props.entry.created_at)}
        </div>;
    },
    dataText: (span) => formatTimestamp(span.created_at),
};

export const CLOSED: ColumnDef<Span> = {
    defaultWidth: "176px",
    header: (props) => {
        async function showContextMenu(e: MouseEvent) {
            let menu = await Menu.new({
                items: [
                    { text: "duplicate column", action: () => props.addColumn() },
                    { item: 'Separator' },
                    ...getNavigationOptions(props.total - props.n, props.total, props.min, props.moveColumn),
                    { item: 'Separator' },
                    { text: "remove column", action: () => props.delColumn() },
                    { text: "remove all other columns", enabled: false, action: () => { } },
                ]
            });
            await menu.popup(new LogicalPosition(e.clientX, e.clientY));
        }

        return (<ResizeableHeader n={props.n} enabled={!props.last} onchange={props.setWidth} onremove={props.delColumn} oncontextmenu={showContextMenu}>
            <EditableHeaderText onchange={props.setProperty}>
                #closed
            </EditableHeaderText>
            <button onclick={props.addColumn}>+</button>
        </ResizeableHeader>);
    },
    headerText: "#closed",
    data: (props) => {
        return <div class="data" style={{ ...props.style }} classList={{ selected: props.selected, hovered: props.hovered }} onclick={props.onClick} onmouseenter={e => props.onHover(e, true)} onmouseleave={e => props.onHover(e, false)}>
            {props.entry.closed_at ? formatTimestamp(props.entry.closed_at) : '---'}
        </div>;
    },
    dataText: (span) => span.closed_at ? formatTimestamp(span.closed_at) : '',
};

export const CONTENT: ColumnDef<Event> = {
    defaultWidth: "minmax(100px, 1fr)",
    header: (props) => {
        async function showContextMenu(e: MouseEvent) {
            let menu = await Menu.new({
                items: [
                    { text: "duplicate column", action: () => props.addColumn() },
                    { item: 'Separator' },
                    ...getNavigationOptions(props.total - props.n, props.total, props.min, props.moveColumn),
                    { item: 'Separator' },
                    { text: "remove column", action: () => props.delColumn() },
                    { text: "remove all other columns", enabled: false, action: () => { } },
                ]
            });
            await menu.popup(new LogicalPosition(e.clientX, e.clientY));
        }

        return (<ResizeableHeader n={props.n} enabled={!props.last} onchange={props.setWidth} onremove={props.delColumn} oncontextmenu={showContextMenu}>
            <EditableHeaderText onchange={props.setProperty}>
                #content
            </EditableHeaderText>
            <button onclick={props.addColumn}>+</button>
        </ResizeableHeader>);
    },
    headerText: "#content",
    data: (props) => {
        let value = props.entry.content;

        async function showContextMenu(e: MouseEvent) {
            let shortValue = value ? value.length > 16 ? value.slice(0, 14) + ".." : value : '';

            function escape(s: string): string {
                return s.replace(/"/g, '\\"');
            }

            function include() {
                let predicate = `#content:"${escape(value!)}"`;
                props.addToFilter(predicate);
            }

            function includeAll() {
                let predicate = `#content:*`;
                props.addToFilter(predicate);
            }

            function exclude() {
                let predicate = `#content:!"${escape(value!)}"`;
                props.addToFilter(predicate);
            }

            function excludeAll() {
                let predicate = `#content:!*`;
                props.addToFilter(predicate);
            }

            let menu = await Menu.new({
                items: [
                    { text: "copy value", action: () => writeText(value ?? '') },
                    { item: 'Separator' },
                    { text: `include #content:${shortValue} in filter`, enabled: value != null, action: include },
                    { text: `include all #content in filter`, action: includeAll },
                    { text: `exclude #content:${shortValue} from filter`, enabled: value != null, action: exclude },
                    { text: `exclude all #content from filter`, action: excludeAll },
                ]
            });
            await menu.popup(new LogicalPosition(e.clientX, e.clientY));
        }

        return <div class="data" style={{ ...props.style }} classList={{ selected: props.selected, hovered: props.hovered }} onclick={props.onClick} onmouseenter={e => props.onHover(e, true)} onmouseleave={e => props.onHover(e, false)} oncontextmenu={showContextMenu}>
            {value}
        </div>;
    },
    dataText: (entity) => entity.content,
};

export const ATTRIBUTE = (attribute: string): ColumnDef<Event | Span> => ({
    defaultWidth: "minmax(100px, 1fr)",
    header: (props) => {
        async function showContextMenu(e: MouseEvent) {
            let menu = await Menu.new({
                items: [
                    { text: "duplicate column", action: () => props.addColumn() },
                    { item: 'Separator' },
                    ...getNavigationOptions(props.total - props.n, props.total, props.min, props.moveColumn),
                    { item: 'Separator' },
                    { text: "remove column", action: () => props.delColumn() },
                    { text: "remove all other columns", enabled: false, action: () => { } },
                ]
            });
            await menu.popup(new LogicalPosition(e.clientX, e.clientY));
        }

        return (<ResizeableHeader n={props.n} enabled={!props.last} onchange={props.setWidth} onremove={props.delColumn} oncontextmenu={showContextMenu}>
            <EditableHeaderText onchange={props.setProperty}>
                @{attribute}
            </EditableHeaderText>
            <button onclick={props.addColumn}>+</button>
        </ResizeableHeader>);
    },
    headerText: `@${attribute}`,
    data: (props) => {
        let value = props.entry.attributes.find(a => a.name == attribute)?.value;

        async function showContextMenu(e: MouseEvent) {
            let shortName = attribute.length > 16 ? attribute.slice(0, 14) + ".." : attribute;
            let shortValue = value ? value.length > 16 ? value.slice(0, 14) + ".." : value : '';

            function escape(s: string): string {
                return s.replace(/"/g, '\\"');
            }

            function include() {
                let predicate = `@${attribute}:"${escape(value!)}"`;
                props.addToFilter(predicate);
            }

            function includeAll() {
                let predicate = `@${attribute}:*`;
                props.addToFilter(predicate);
            }

            function exclude() {
                let predicate = `@${attribute}:!"${escape(value!)}"`;
                props.addToFilter(predicate);
            }

            function excludeAll() {
                let predicate = `@${attribute}:!*`;
                props.addToFilter(predicate);
            }

            let menu = await Menu.new({
                items: [
                    { text: "copy value", action: () => writeText(value ?? '') },
                    { item: 'Separator' },
                    { text: `include @${shortName}:${shortValue} in filter`, enabled: value != null, action: include },
                    { text: `include all @${shortName} in filter`, action: includeAll },
                    { text: `exclude @${shortName}:${shortValue} from filter`, enabled: value != null, action: exclude },
                    { text: `exclude all @${shortName} from filter`, action: excludeAll },
                ]
            });
            await menu.popup(new LogicalPosition(e.clientX, e.clientY));
        }

        return <div class="data" style={{ ...props.style }} classList={{ selected: props.selected, hovered: props.hovered }} onclick={props.onClick} onmouseenter={e => props.onHover(e, true)} onmouseleave={e => props.onHover(e, false)} oncontextmenu={showContextMenu}>
            {value ?? '---'}
        </div>;
    },
    dataText: (entity) => entity.attributes.find(a => a.name == attribute)?.value ?? '',
});

export const INHERENT = (inherent: string): ColumnDef<Event | Span> => ({
    defaultWidth: "minmax(100px, 1fr)",
    header: (props) => {
        async function showContextMenu(e: MouseEvent) {
            let menu = await Menu.new({
                items: [
                    { text: "duplicate column", action: () => props.addColumn() },
                    { item: 'Separator' },
                    ...getNavigationOptions(props.total - props.n, props.total, props.min, props.moveColumn),
                    { item: 'Separator' },
                    { text: "remove column", action: () => props.delColumn() },
                    { text: "remove all other columns", enabled: false, action: () => { } },
                ]
            });
            await menu.popup(new LogicalPosition(e.clientX, e.clientY));
        }

        return (<ResizeableHeader n={props.n} enabled={!props.last} onchange={props.setWidth} onremove={props.delColumn} oncontextmenu={showContextMenu}>
            <EditableHeaderText onchange={props.setProperty}>
                #{inherent}
            </EditableHeaderText>
            <button onclick={props.addColumn}>+</button>
        </ResizeableHeader>);
    },
    headerText: `#${inherent}`,
    data: (props) => {
        let value = (props.entry as any)[inherent] as string | undefined;

        async function showContextMenu(e: MouseEvent) {
            let shortValue = value ? value.length > 16 ? value.slice(0, 14) + ".." : value : '';

            function escape(s: string): string {
                return s.replace(/"/g, '\\"');
            }

            function include() {
                let predicate = `#${inherent}:"${escape(value!)}"`;
                props.addToFilter(predicate);
            }

            function exclude() {
                let predicate = `#${inherent}:!"${escape(value!)}"`;
                props.addToFilter(predicate);
            }

            let menu = await Menu.new({
                items: [
                    { text: "copy value", action: () => writeText(value ?? '') },
                    { item: 'Separator' },
                    { text: `include #${inherent}:${shortValue} in filter`, enabled: value != null && inherent != 'id', action: include },
                    { text: `exclude #${inherent}:${shortValue} from filter`, enabled: value != null && inherent != 'id', action: exclude },
                ]
            });
            await menu.popup(new LogicalPosition(e.clientX, e.clientY));
        }

        return <div class="data" style={{ ...props.style }} classList={{ selected: props.selected, hovered: props.hovered }} onclick={props.onClick} onmouseenter={e => props.onHover(e, true)} onmouseleave={e => props.onHover(e, false)} oncontextmenu={showContextMenu}>
            {(props.entry as any)[inherent] ?? '---'}
        </div>;
    },
    dataText: (entity) => (entity as any)[inherent] ?? '',
});

function renderedParent(e: Event | Span) {
    let parent = e.ancestors[0];
    if (parent == null) {
        return null;
    } else {
        return parent.name;
    }
}

export const PARENT: ColumnDef<Event | Span> = {
    defaultWidth: "minmax(100px, 1fr)",
    header: (props) => {
        async function showContextMenu(e: MouseEvent) {
            let menu = await Menu.new({
                items: [
                    { text: "duplicate column", action: () => props.addColumn() },
                    { item: 'Separator' },
                    ...getNavigationOptions(props.total - props.n, props.total, props.min, props.moveColumn),
                    { item: 'Separator' },
                    { text: "remove column", action: () => props.delColumn() },
                    { text: "remove all other columns", enabled: false, action: () => { } },
                ]
            });
            await menu.popup(new LogicalPosition(e.clientX, e.clientY));
        }

        return (<ResizeableHeader n={props.n} enabled={!props.last} onchange={props.setWidth} onremove={props.delColumn} oncontextmenu={showContextMenu}>
            <EditableHeaderText onchange={props.setProperty}>
                #parent
            </EditableHeaderText>
            <button onclick={props.addColumn}>+</button>
        </ResizeableHeader>);
    },
    headerText: '#parent',
    data: (props) => {
        let value = renderedParent(props.entry);

        function parentTitle(e: Event | Span) {
            let parent = e.ancestors[0];
            if (parent == null) {
                return 'none';
            } else {
                return parent.id;
            }
        }

        async function showContextMenu(e: MouseEvent) {
            let shortValue = value ? value.length > 16 ? value.slice(0, 14) + ".." : value : '';

            function escape(s: string): string {
                return s.replace(/"/g, '\\"');
            }

            function include() {
                let predicate = `#parent:"${escape(value!)}"`;
                props.addToFilter(predicate);
            }

            function exclude() {
                let predicate = `#parent:!"${escape(value!)}"`;
                props.addToFilter(predicate);
            }

            let menu = await Menu.new({
                items: [
                    { text: "copy value", action: () => writeText(value ?? '') },
                    { item: 'Separator' },
                    { text: `include #parent:${shortValue} in filter`, enabled: value != null, action: include },
                    { text: `exclude #parent:${shortValue} from filter`, enabled: value != null, action: exclude },
                ]
            });
            await menu.popup(new LogicalPosition(e.clientX, e.clientY));
        }

        return <div class="data" style={{ ...props.style }} classList={{ selected: props.selected, hovered: props.hovered }} title={parentTitle(props.entry)} onclick={props.onClick} onmouseenter={e => props.onHover(e, true)} onmouseleave={e => props.onHover(e, false)} oncontextmenu={showContextMenu}>
            {renderedParent(props.entry) ?? 'none'}
        </div>;
    },
    dataText: (entity) => renderedParent(entity) ?? '',
};

function renderedDuration(span: Span) {
    let start: number = span.created_at;
    let end: number | null = span.closed_at;
    if (end == null) {
        return null;
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

export const DURATION: ColumnDef<Span> = {
    defaultWidth: "minmax(100px, 1fr)",
    header: (props) => {
        async function showContextMenu(e: MouseEvent) {
            let menu = await Menu.new({
                items: [
                    { text: "duplicate column", action: () => props.addColumn() },
                    { item: 'Separator' },
                    ...getNavigationOptions(props.total - props.n, props.total, props.min, props.moveColumn),
                    { item: 'Separator' },
                    { text: "remove column", action: () => props.delColumn() },
                    { text: "remove all other columns", enabled: false, action: () => { } },
                ]
            });
            await menu.popup(new LogicalPosition(e.clientX, e.clientY));
        }

        return (<ResizeableHeader n={props.n} enabled={!props.last} onchange={props.setWidth} onremove={props.delColumn} oncontextmenu={showContextMenu}>
            <EditableHeaderText onchange={props.setProperty}>
                #duration
            </EditableHeaderText>
            <button onclick={props.addColumn}>+</button>
        </ResizeableHeader>);
    },
    headerText: '#duration',
    data: (props) => {
        async function showContextMenu(e: MouseEvent) {
            let menu = await Menu.new({
                items: [
                    { text: "copy value", action: () => writeText(renderedDuration(props.entry) ?? '') },
                ]
            });
            await menu.popup(new LogicalPosition(e.clientX, e.clientY));
        }

        return <div class="data" style={{ ...props.style }} classList={{ selected: props.selected, hovered: props.hovered }} onclick={props.onClick} onmouseenter={e => props.onHover(e, true)} onmouseleave={e => props.onHover(e, false)} oncontextmenu={showContextMenu}>
            {renderedDuration(props.entry) ?? '---'}
        </div>;
    },
    dataText: (entity) => renderedDuration(entity) ?? '',
};

export const UNKNOWN = (property: string): ColumnDef<Event | Span> => ({
    defaultWidth: "minmax(100px, 1fr)",
    header: (props) => {
        async function showContextMenu(e: MouseEvent) {
            let menu = await Menu.new({
                items: [
                    { text: "duplicate column", action: () => props.addColumn() },
                    { item: 'Separator' },
                    ...getNavigationOptions(props.total - props.n, props.total, props.min, props.moveColumn),
                    { item: 'Separator' },
                    { text: "remove column", action: () => props.delColumn() },
                    { text: "remove all other columns", enabled: false, action: () => { } },
                ]
            });
            await menu.popup(new LogicalPosition(e.clientX, e.clientY));
        }

        return (<ResizeableHeader n={props.n} enabled={!props.last} onchange={props.setWidth} onremove={props.delColumn} oncontextmenu={showContextMenu}>
            <EditableHeaderText onchange={props.setProperty} title="unknown property">
                {property}
            </EditableHeaderText>
            <button onclick={props.addColumn}>+</button>
        </ResizeableHeader>);
    },
    headerText: `${property}`,
    data: (props) => {
        return <div class="data" style={{ ...props.style }} classList={{ selected: props.selected, hovered: props.hovered }} onclick={props.onClick} onmouseenter={e => props.onHover(e, true)} onmouseleave={e => props.onHover(e, false)}>
            ---
        </div>;
    },
    dataText: () => '',
});

export const TIMESPAN: ColumnDef<Event | Span> = {
    defaultWidth: "300px",
    header: (props) => {
        return (<ResizeableHeader n={props.n} enabled={!props.last} onchange={props.setWidth} onremove={() => { }}>
            <div class="header-text">timespan</div>
            <button onclick={props.orderToggle}>{props.order == 'asc' ? "▼" : "▲"}</button>
        </ResizeableHeader>);
    },
    headerText: "#timespan",
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

        let kind = (props.entry as any).timestamp != undefined ? 'event' : 'span';

        return (<div class="data" style={{ ...props.style }} classList={{ selected: props.selected, hovered: props.hovered }} onclick={props.onClick} onmouseenter={e => props.onHover(e, true)} onmouseleave={e => props.onHover(e, false)}>
            <div class={`time-bar time-bar-${props.entry.level} time-bar-${kind}`} style={{ ...position(props.entry as Span) }}></div>
        </div>);
    },
    dataText: () => '',
};

export const COLLAPSABLE: ColumnDef<Event | Span> = {
    defaultWidth: "20px",
    header: (props) => {
        return <div class="header collapsable" style={`z-index: ${props.n}`}></div>;
    },
    headerText: "#collapsable",
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
            ? (<div class="data" style={{ ...props.style }} classList={{ selected: props.selected, hovered: props.hovered }} onclick={props.onClick} onmouseenter={e => props.onHover(e, true)} onmouseleave={e => props.onHover(e, false)}></div>)
            : (<div class="data collapser" style={{ ...props.style }} classList={{ selected: props.selected, hovered: props.hovered }} onclick={toggle} onmouseenter={e => props.onHover(e, true)} onmouseleave={e => props.onHover(e, false)}>
                {collapsed() ? '⏶' : '⏷'}
            </div>);
    },
    dataText: () => '',
};

export const COMBINED = (spanDef: ColumnDef<Span>, eventDef: ColumnDef<Event>): ColumnDef<Span | Event> => ({
    defaultWidth: spanDef.defaultWidth,
    header: (props) => {
        function desc() {
            return `using '${spanDef.headerText}' for spans and '${eventDef.headerText}' for events`;
        }

        async function showContextMenu(e: MouseEvent) {
            let menu = await Menu.new({
                items: [
                    { text: "duplicate column", action: () => props.addColumn() },
                    { item: 'Separator' },
                    ...getNavigationOptions(props.total - props.n, props.total, props.min, props.moveColumn),
                    { item: 'Separator' },
                    { text: "remove column", action: () => props.delColumn() },
                    { text: "remove all other columns", enabled: false, action: () => { } },
                ]
            });
            await menu.popup(new LogicalPosition(e.clientX, e.clientY));
        }

        return (<ResizeableHeader n={props.n} enabled={!props.last} onchange={props.setWidth} onremove={props.delColumn} oncontextmenu={showContextMenu}>
            <EditableHeaderText onchange={props.setProperty} title={desc()}>
                {spanDef.headerText} / {eventDef.headerText}
            </EditableHeaderText>
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
    dataText: (entity) => {
        if ((entity as any).timestamp != undefined) {
            return eventDef.dataText(entity as Event);
        } else {
            return spanDef.dataText(entity as Span);
        }
    }
});

function formatTimestamp(timestamp: number): string {
    var datetime = new Date(timestamp / 1000);
    return datetime.getFullYear() + "-" + (datetime.getMonth() + 1).toString().padStart(2, '0') + "-" +
        datetime.getDate().toString().padStart(2, '0') + " " + datetime.getHours().toString().padStart(2, '0') + ":" +
        datetime.getMinutes().toString().padStart(2, '0') + ":" + datetime.getSeconds().toString().padStart(2, '0') + "." +
        datetime.getMilliseconds().toString().padStart(3, '0');
}

export function parseEventColumn(property: string, internal: boolean = false): ColumnDef<Event> {
    // if (property == 'instance' || property == '#instance') {
    //     return INSTANCE;
    // }
    if ((property == 'level' || property == '#level') && internal) {
        return LEVEL;
    }
    if ((property == 'timestamp' || property == '#timestamp') && internal) {
        return TIMESTAMP;
    }

    if (property == 'parent' || property == '#parent') {
        return PARENT;
    }
    if (property == 'content' || property == '#content') {
        return CONTENT;
    }
    if (property == 'target' || property == '#target') {
        return INHERENT('target');
    }
    if (property == 'file' || property == '#file') {
        return INHERENT('file');
    }

    if (property.startsWith('#')) {
        return UNKNOWN(property);
    }

    if (property.startsWith('@')) {
        return ATTRIBUTE(property.slice(1));
    }

    return ATTRIBUTE(property);
}

export function parseSpanColumn(property: string, internal: boolean = false): ColumnDef<Span> {
    // if (property == 'instance' || property == '#instance') {
    //     return INSTANCE;
    // }
    if ((property == 'level' || property == '#level') && internal) {
        return LEVEL;
    }
    if ((property == 'created' || property == '#created') && internal) {
        return CREATED;
    }

    if (property == 'closed' || property == '#closed') {
        return CLOSED;
    }
    if (property == 'duration' || property == '#duration') {
        return DURATION;
    }
    if (property == 'name' || property == '#name') {
        return INHERENT('name');
    }
    if (property == 'parent' || property == '#parent') {
        return PARENT;
    }
    if (property == 'target' || property == '#target') {
        return INHERENT('target');
    }
    if (property == 'file' || property == '#file') {
        return INHERENT('file');
    }

    if (property.startsWith('#')) {
        return UNKNOWN(property);
    }

    if (property.startsWith('@')) {
        return ATTRIBUTE(property.slice(1));
    }

    return ATTRIBUTE(property);
}

export function parseTraceColumn(property: string, internal: boolean = false): ColumnDef<Event | Span> {
    if ((property == 'collapsable' || property == '#collapsable') && internal) {
        return COLLAPSABLE;
    }
    if ((property == 'timespan' || property == '#timespan') && internal) {
        return TIMESPAN;
    }

    if (property.includes('/')) {
        let idx = property.indexOf('/');
        let span_property = property.slice(0, idx).trim();
        let event_property = property.slice(idx + 1).trim();
        return COMBINED(
            parseSpanColumn(span_property, internal),
            parseEventColumn(event_property, internal),
        );
    }

    let span_def = parseSpanColumn(property, internal);
    let event_def = parseEventColumn(property, internal);
    if (span_def.headerText == event_def.headerText) {
        return span_def as ColumnDef<Event | Span>;
    } else {
        return COMBINED(span_def, event_def);
    }
}

let CACHE_START_LAST = 0;
let CACHE_START_DELAY_MS = 250;

export type TableProps<T> = {
    timespan: Timespan,

    columns: ColumnDef<T>[],
    columnWidths: string[],
    columnUpdate: (i: number, def: ColumnDef<T>) => void,
    columnUpdateWidth: (i: number, width: string) => void,
    columnMove: (i: number, to: number) => void,
    columnInsert: (i: number, def: ColumnDef<T>) => void,
    columnRemove: (i: number) => void,
    columnDefault: ColumnDef<T>,
    columnMin: number,
    columnParser: (property: string) => ColumnDef<T>,

    selectedRow: T | null,
    setSelectedRow: (e: T | null) => void,
    hoveredRow: T | null,
    setHoveredRow: (e: T | null) => void,

    getEntries: (filter: PartialFilter, wait?: boolean) => Promise<T[] | null>,
    addToFilter: (filter: string) => void,
};

export function Table<T extends Event | Span>(props: TableProps<T>) {
    const [entries, setEntries] = createSignal([] as T[]);
    const [status, setStatus] = createSignal('loading' as 'partial' | 'loading' | 'done');
    const [order, setOrder] = createSignal('asc' as 'asc' | 'desc');

    var table_wrapper: any;

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

        let t = (trailer as HTMLDivElement);
        if (t.getBoundingClientRect().top <= document.body.clientHeight) {
            setTimeout(loadMore, 0);
        }
    })

    async function loadMore() {
        if (untrack(status) != 'partial') {
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

        let new_status = (new_events.length == 50) ? 'partial' as const : 'done' as const;
        setStatus(new_status);

        if (new_status == 'partial') {
            let t = (trailer as HTMLDivElement);
            if (t.getBoundingClientRect().top <= document.body.clientHeight) {
                setTimeout(loadMore, 0);
            }
        }
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

    let trailer = <div style="padding-left: 4px; white-space: nowrap; color: var(--text-light)">{(status() == 'partial') ? 'load more entries'
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

    let rowVirtualizer = createVirtualizer({
        count: entries().length,
        getScrollElement: () => table_wrapper,
        estimateSize: () => 21,
        paddingStart: 23,
        paddingEnd: -23,
    });

    createEffect(() => {
        let rows = entries().length;
        rowVirtualizer.setOptions({
            ...rowVirtualizer.options,
            count: rows,
        });
        rowVirtualizer.measure();
    });

    return (<div ref={table_wrapper} id="table">
        <div id="table-headers" style={{ ['grid-template-columns']: getGridTemplateColumns() }}>
            <For each={props.columns}>
                {(column, i) => (<Dynamic component={column.header}
                    n={props.columns.length - i()}
                    total={props.columns.length}
                    min={props.columnMin}
                    order={order()}
                    orderToggle={toggleOrder}
                    last={i() == props.columns.length - 1}
                    setWidth={(w: string) => props.columnUpdateWidth(i(), w)}
                    setProperty={(p: string) => props.columnUpdate(i(), props.columnParser(p))}
                    moveColumn={(to: number) => props.columnMove(i(), to)}
                    addColumn={() => props.columnInsert(i(), props.columnDefault)}
                    delColumn={() => removeColumn(i())}
                />)}
            </For>
        </div>
        <div id="table-inner" style={{ height: `${rowVirtualizer.getTotalSize()}px`, ['grid-template-columns']: getGridTemplateColumns() }}>
            {rowVirtualizer.getVirtualItems().map((virt) => {
                let offset = rowVirtualizer.getVirtualIndexes()[0];
                let row = entries()[virt.index];
                if (row == undefined) {
                    return <></>;
                }
                return (<For each={props.columns}>
                    {column => (<Dynamic
                        component={column.data}
                        style={{
                            transform: `translateY(${offset * 21}px)`,
                        }}
                        entry={row}
                        selected={isSelected(row)}
                        hovered={isHovered(row)}
                        timespan={props.timespan}
                        onClick={(e: MouseEvent) => onClickRow(e, row)}
                        onHover={(e: MouseEvent, enter: boolean) => onHoverRow(e, row, enter)}
                        addToFilter={props.addToFilter}
                    />)}
                </For>);
            })}
        </div>
        {trailer}
    </div>);
}

type EditableHeaderTextProps = {
    onchange: (value: string) => void,
    children: JSX.Element,
    title?: string,
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

    return (<div class="header-text" title={props.title}>
        <div contenteditable onblur={onblur} onkeydown={onkeydown}>{props.children}</div>
    </div>);
}

type ResizeableHeaderProps = {
    n: number,
    enabled: boolean,
    onchange: (value: string) => void,
    onremove: () => void,
    oncontextmenu?: (e: MouseEvent) => void,
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

    return (<div class="header" style={`z-index: ${props.n}`} ref={header_ref} onauxclick={onclick} onclick={onclick} onmousedown={onmousedown} oncontextmenu={props.oncontextmenu}>
        {props.children}
        <Show when={props.enabled}>
            <div class="grabber" classList={{ grabbed: dragging() }} onmousedown={ongrab} onmousemove={onmove} onmouseup={onrelease} onmouseleave={onrelease}></div>
        </Show>
    </div>);
}
