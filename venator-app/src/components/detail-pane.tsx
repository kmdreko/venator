import { createEffect, createMemo, createSignal, For, Match, Show, Switch, useContext } from 'solid-js';
import { writeText } from '@tauri-apps/plugin-clipboard-manager';
import { Menu } from '@tauri-apps/api/menu';
import { LogicalPosition } from '@tauri-apps/api/dpi';
import { Ancestor, Attribute, Event, FilterPredicate, FullSpanId, getEventCount, getSpanCount, Input, Span, TraceRoot } from '../invoke'
import { Timespan } from '../models';
import { NavigationContext } from '../context/navigation';
import { ColumnData, ScreenData } from '../App';
import { COLLAPSABLE, COMBINED, CONTENT, INHERENT, TIMESPAN } from './table';
import { TraceDataLayer } from '../utils/datalayer';

import "./detail-pane.css";
import spanIcon from '../assets/span.svg';
import resourceIcon from '../assets/resource.svg';
import traceIcon from "../assets/trace.svg";

export type EventDetailPaneProps = {
    event: Event,
    timespan: Timespan | null,
    updateSelectedRow: (event: Event | null) => void,
    filter: Input[],
    addToFilter: (filter: string) => void,
    addColumn: (column: string) => void,
}

export function EventDetailPane(props: EventDetailPaneProps) {
    let [width, setWidth] = createSignal<number>(500);
    let [inFilter, setInFilter] = createSignal<boolean>(true);

    let traceRoot = createMemo(() => {
        return getEventTraceRoot(props.event);
    });

    let navigation = useContext(NavigationContext);

    function eventInTimespan(): boolean {
        if (props.timespan == null) {
            return false;
        }

        return (props.event.timestamp >= props.timespan[0] && props.event.timestamp <= props.timespan[1])
    }

    createEffect(async () => {
        let countAtTimestamp = await getEventCount({
            filter: props.filter.filter(f => f.input == 'valid'),
            start: props.event.timestamp,
            end: props.event.timestamp,
        });
        setInFilter(countAtTimestamp == 1);
    })

    function onClickHeader(e: MouseEvent) {
        if (e.button == 1) {
            props.updateSelectedRow(null);
        }
    }

    async function showGrabberContextMenu(e: MouseEvent) {
        let menu = await Menu.new({
            items: [{ text: "reset width", action: () => setWidth(500) }]
        });
        await menu.popup(new LogicalPosition(e.clientX, e.clientY));
    }

    let startingX = 0;
    let startingWidth = 0;
    function ongrab(e: MouseEvent) {
        e.preventDefault();

        startingX = e.clientX;
        startingWidth = width();

        document.addEventListener('mousemove', ongrabmove);
        document.addEventListener('mouseup', ongrabrelease);
    }

    let dragRequested: number | null;
    function ongrabmove(e: MouseEvent) {
        if (dragRequested != null)
            return;

        dragRequested = requestAnimationFrame(() => {
            dragRequested = null;
            setWidth(startingWidth + startingX - e.clientX);
        });
    }

    function ongrabrelease(_e: MouseEvent) {
        document.removeEventListener('mousemove', ongrabmove);
        document.removeEventListener('mouseup', ongrabrelease);
    }

    function onclicktracebutton(e: MouseEvent) {
        if (e.button != 0) {
            return;
        }

        navigation?.createTab(...createDefaultTraceScreen(traceRoot()!), true)
    }

    return (<>
        <div id="detail-pane-grabber" onmousedown={ongrab} oncontextmenu={showGrabberContextMenu}></div>
        <div id="detail-pane" style={`width: ${width()}px; min-width: ${width()}px;`}>
            <div id="detail-header" onauxclick={onClickHeader} onclick={onClickHeader}>
                <span>event details</span>
                <button onclick={() => props.updateSelectedRow(null)}>X</button>
            </div>
            <div id="detail-info">
                <div id="detail-info-head">
                    <div id="detail-info-head-data">
                        <DetailedLevel level={props.event.level} />
                        <DetailedTimestamp timestamp={props.event.timestamp} />
                        <Show when={!inFilter()}>
                            <span style="color: #555555; margin: 0 4px;">not in filter</span>
                        </Show>
                        <Show when={!eventInTimespan()}>
                            <span style="color: #555555; margin: 0 4px;">not in timeframe</span>
                        </Show>
                    </div>
                    <div id="detail-info-head-controls">
                        <Show when={props.event.ancestors.length > 0}>
                            <button onclick={onclicktracebutton}>
                                <img src={traceIcon} style="width:16px;height:16px" title="open root trace in new tab" />
                            </button>
                        </Show>
                    </div>
                </div>
                <div id="detail-info-meta">
                    <Show when={props.event.namespace != null}>
                        <DetailedMeta name={"namespace"} value={props.event.namespace!} addToFilter={props.addToFilter} addColumn={props.addColumn} />
                    </Show>
                    <Show when={props.event.function != null}>
                        <DetailedMeta name={"function"} value={props.event.function!} addToFilter={props.addToFilter} addColumn={props.addColumn} />
                    </Show>
                    <DetailedMeta name={"file"} value={props.event.file} addToFilter={props.addToFilter} addColumn={props.addColumn} />
                    <DetailedMetaParents ancestors={props.event.ancestors} />
                </div>
                <DetailedPrimary message={props.event.content}></DetailedPrimary>
                <DetailAttributes attributes={props.event.attributes} addToFilter={props.addToFilter} addColumn={props.addColumn} />
            </div>
        </div>
    </>);
}

export type SpanDetailPaneProps = {
    span: Span,
    timespan: Timespan | null,
    updateSelectedRow: (span: Span | null) => void,
    filter: Input[],
    addToFilter: (filter: string) => void,
    addColumn: (column: string) => void,
}

export function SpanDetailPane(props: SpanDetailPaneProps) {
    let [width, setWidth] = createSignal<number>(500);
    let [inFilter, setInFilter] = createSignal<boolean>(true);

    let traceRoot = createMemo(() => {
        return getSpanTraceRoot(props.span);
    });

    let navigation = useContext(NavigationContext);

    function spanInTimespan(): boolean {
        if (props.timespan == null) {
            return false;
        }

        if (props.span.created_at > props.timespan[1]) {
            return false;
        }

        if (props.span.closed_at != null && props.span.closed_at < props.timespan[0]) {
            return false;
        }

        return true;
    }

    createEffect(async () => {
        let countAtTimestamp = await getSpanCount({
            filter: props.filter.filter(f => f.input == 'valid').map(f => f as FilterPredicate).concat({
                predicate_kind: 'single',
                predicate: {
                    property_kind: 'Inherent',
                    property: 'created',
                    value_kind: 'comparison',
                    value: ['Eq', `${props.span.created_at}`],
                },
            } as FilterPredicate),
            start: props.span.created_at,
            end: props.span.created_at,
        });
        setInFilter(countAtTimestamp == 1);
    })

    function onClickHeader(e: MouseEvent) {
        if (e.button == 1) {
            props.updateSelectedRow(null);
        }
    }

    async function showGrabberContextMenu(e: MouseEvent) {
        let menu = await Menu.new({
            items: [{ text: "reset width", action: () => setWidth(500) }]
        });
        await menu.popup(new LogicalPosition(e.clientX, e.clientY));
    }

    let startingX = 0;
    let startingWidth = 0;
    function ongrab(e: MouseEvent) {
        e.preventDefault();

        startingX = e.clientX;
        startingWidth = width();

        document.addEventListener('mousemove', ongrabmove);
        document.addEventListener('mouseup', ongrabrelease);
    }

    let dragRequested: number | null;
    function ongrabmove(e: MouseEvent) {
        if (dragRequested != null)
            return;

        dragRequested = requestAnimationFrame(() => {
            dragRequested = null;
            setWidth(startingWidth + startingX - e.clientX);
        });
    }

    function ongrabrelease(_e: MouseEvent) {
        document.removeEventListener('mousemove', ongrabmove);
        document.removeEventListener('mouseup', ongrabrelease);
    }

    function onclicktracebutton(e: MouseEvent) {
        if (e.button != 0) {
            return;
        }

        navigation?.createTab(...createDefaultTraceScreen(traceRoot()), true)
    }

    return (<>
        <div id="detail-pane-grabber" onmousedown={ongrab} oncontextmenu={showGrabberContextMenu}></div>
        <div id="detail-pane" style={`width: ${width()}px; min-width: ${width()}px;`}>
            <div id="detail-header" onauxclick={onClickHeader} onclick={onClickHeader}>
                span details
                <button onclick={() => props.updateSelectedRow(null)}>X</button>
            </div>
            <div id="detail-info">
                <div id="detail-info-head">
                    <div id="detail-info-head-data">
                        <DetailedLevel level={props.span.level} />
                        <DetailedTimestamp timestamp={props.span.created_at} />
                        <Show when={!inFilter()}>
                            <span style="color: #555555; margin: 0 4px;">not in filter</span>
                        </Show>
                        <Show when={!spanInTimespan()}>
                            <span style="color: #555555; margin: 0 4px;">not in timeframe</span>
                        </Show>
                    </div>
                    <div id="detail-info-head-controls">
                        <button onclick={onclicktracebutton}>
                            <img src={traceIcon} style="width:16px;height:16px" title="open root trace in new tab" />
                        </button>
                    </div>
                </div>
                <Show when={props.span.closed_at != null}>
                    <DetailedDuration duration={props.span.closed_at! - props.span.created_at} busy={props.span.busy} />
                </Show>
                <div id="detail-info-meta">
                    <DetailedMetaId value={props.span.id} created_at={props.span.created_at} closed_at={props.span.closed_at} />
                    <Show when={props.span.namespace != null}>
                        <DetailedMeta name={"namespace"} value={props.span.namespace!} addToFilter={props.addToFilter} addColumn={props.addColumn} />
                    </Show>
                    <Show when={props.span.function != null}>
                        <DetailedMeta name={"function"} value={props.span.function!} addToFilter={props.addToFilter} addColumn={props.addColumn} />
                    </Show>
                    <DetailedMeta name={"file"} value={props.span.file} addToFilter={props.addToFilter} addColumn={props.addColumn} />
                    <DetailedMetaParents ancestors={props.span.ancestors} name={props.span.name} id={props.span.id} />
                </div>
                <DetailedPrimary message={props.span.name}></DetailedPrimary>
                <DetailAttributes attributes={props.span.attributes} addToFilter={props.addToFilter} addColumn={props.addColumn} />
            </div>
        </div>
    </>);
}

export function DetailedLevel(props: { level: number }) {
    return (<Switch>
        <Match when={props.level == 0}>
            <div class="detailed-level-0">TRACE</div>
        </Match>
        <Match when={props.level == 1}>
            <div class="detailed-level-1">DEBUG</div>
        </Match>
        <Match when={props.level == 2}>
            <div class="detailed-level-2">INFO</div>
        </Match>
        <Match when={props.level == 3}>
            <div class="detailed-level-3">WARN</div>
        </Match>
        <Match when={props.level == 4}>
            <div class="detailed-level-4">ERROR</div>
        </Match>
        <Match when={props.level == 5}>
            <div class="detailed-level-5">FATAL</div>
        </Match>
    </Switch>);
}

export function DetailedTimestamp(props: { timestamp: number }) {
    return (<div class="detailed-timestamp">
        {(new Date(props.timestamp / 1000)).toISOString()}
    </div>);
}

export function DetailedDuration(props: { duration: number, busy: number | null }) {
    function renderedDuration(duration: number) {
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

    function busyPortion() {
        if (props.busy == null) {
            return 0.96;
        } else {
            return 0.96 * Math.min(props.busy / props.duration, 1.0);
        }
    }

    return (<div class="detailed-duration">
        <div class="bar">
            <Show when={props.busy != null}>
                <div class="busy-bar" style={{ left: `${100 * (1 - busyPortion() / 2) - 50}%`, right: `${100 * (1 - busyPortion() / 2) - 50}%` }}></div>
            </Show>
        </div>
        <span class="total">total: {renderedDuration(props.duration)}</span>
        <Show when={props.busy != null}>
            <span class="busy">busy: {renderedDuration(props.busy!)}</span>
        </Show>
    </div>);
}

export function DetailedMetaId(props: { value: string, created_at: number, closed_at: number | null }) {
    return (<div class="detailed-meta-id">
        <b>#id:</b>
        &nbsp;
        <span style="font-family: 'Noto Sans Mono', monospace; font-weight: 500;">{props.value}</span>
    </div>);
}

export function DetailedMeta(props: { name: string, value: string | undefined, addToFilter: (filter: string) => void, addColumn: (column: string) => void }) {
    async function showInherentContextMenu(e: MouseEvent, property: string, value: string | undefined) {
        let shortValue = value == undefined ? '---' : value.length > 16 ? value.slice(0, 14) + ".." : value;

        function escape(s: string): string {
            return s.replace(/"/g, '\\"');
        }

        function include() {
            let predicate = `#${property}:"${escape(value!)}"`;
            props.addToFilter(predicate);
        }

        function exclude() {
            let predicate = `#${property}:!"${escape(value!)}"`;
            props.addToFilter(predicate);
        }

        let menu = await Menu.new({
            items: [
                { text: "copy value", action: () => writeText(value!), enabled: value != undefined },
                { item: 'Separator' },
                { text: `include #${property}:${shortValue} in filter`, action: include, enabled: value != undefined },
                { text: `exclude #${property}:${shortValue} from filter`, action: exclude, enabled: value != undefined },
                { item: 'Separator' },
                { text: `add column for #${property}`, action: () => props.addColumn(`#${property}`) },
            ]
        });
        await menu.popup(new LogicalPosition(e.clientX, e.clientY));
    }

    return (<div class="detailed-meta" oncontextmenu={e => showInherentContextMenu(e, props.name, props.value)}>
        <b>#{props.name + ':'}</b>
        &nbsp;
        <span style="font-family: 'Noto Sans Mono', monospace; font-weight: 500;">{props.value}</span>
    </div>);
}

export function DetailedMetaParents(props: { id?: FullSpanId, name?: string, ancestors: Ancestor[] }) {
    let [collapsed, setCollapsed] = createSignal(true);

    return (<>
        <div class="detailed-meta" onclick={() => setCollapsed(!collapsed())}>
            <b>#stack:</b>
            &nbsp;
            <span style="font-family: 'Noto Sans Mono', monospace; font-weight: 500;">{props.ancestors.length + (props.name ? 1 : 0)}</span>
        </div>
        <Show when={!collapsed()}>
            <Show when={props.name}>
                <div class="detailed-meta-parent">
                    {props.name}
                    &nbsp;
                    <b style="font-family: Inter, Avenir, Helvetica, Arial, sans-serif;">{props.ancestors.length == 0 ? "(this, root)" : "(this)"}</b>
                </div>
            </Show>
            <For each={props.ancestors.slice().reverse()}>
                {(ancestor, i) => <div class="detailed-meta-parent">
                    {ancestor.name}
                    <Show when={i() == props.ancestors.length - 1}>
                        &nbsp;
                        <b style="font-family: Inter, Avenir, Helvetica, Arial, sans-serif;">(root)</b>
                    </Show>
                </div>}
            </For>
        </Show>
    </>);
}

export function DetailedPrimary(props: { message: string }) {
    return (<div class="detail-info-primary">
        {props.message}
    </div>);
}

export function DetailAttributes(props: { attributes: Attribute[], addToFilter: (filter: string) => void, addColumn: (column: string) => void }) {
    return (<div id="detail-info-attributes">
        <For each={props.attributes}>
            {attr => <DetailAttribute attr={attr} addToFilter={props.addToFilter} addColumn={props.addColumn}></DetailAttribute>}
        </For>
    </div>);
}

function DetailAttribute(props: { attr: Attribute, addToFilter: (filter: string) => void, addColumn: (column: string) => void }) {
    let [hovered, setHovered] = createSignal<boolean>(false);
    let [collapsed, setCollapsed] = createSignal<boolean>(true);

    async function showAttributeContextMenu(e: MouseEvent, attr: Attribute) {
        let shortName = attr.name.length > 16 ? attr.name.slice(0, 14) + ".." : attr.name;
        let shortValue = attr.value.length > 16 ? attr.value.slice(0, 14) + ".." : attr.value;

        function escape(s: string): string {
            return s.replace(/"/g, '\\"');
        }

        function include() {
            let predicate = `@${attr.name}:"${escape(attr.value)}"`;
            props.addToFilter(predicate);
        }

        function includeAll() {
            let predicate = `@${attr.name}:*`;
            props.addToFilter(predicate);
        }

        function exclude() {
            let predicate = `@${attr.name}:!"${escape(attr.value)}"`;
            props.addToFilter(predicate);
        }

        function excludeAll() {
            let predicate = `@${attr.name}:!*`;
            props.addToFilter(predicate);
        }

        function copySource() {
            if (attr.source == 'resource') {
                return [];
            } else if (attr.source == 'span') {
                return [{ text: "copy span id", action: () => writeText(attr.span_id) }];
            } else {
                return [];
            }
        }

        let menu = await Menu.new({
            items: [
                { text: "copy value", action: () => writeText(attr.value) },
                { text: "copy name", action: () => writeText(attr.name) },
                ...copySource(),
                { item: 'Separator' },
                { text: `include @${shortName}:${shortValue} in filter`, action: include },
                { text: `include all @${shortName} in filter`, action: includeAll },
                { text: `exclude @${shortName}:${shortValue} from filter`, action: exclude },
                { text: `exclude all @${shortName} from filter`, action: excludeAll },
                { item: 'Separator' },
                { text: `add column for @${shortName}`, action: () => props.addColumn(`@${attr.name}`) },
            ]
        });
        await menu.popup(new LogicalPosition(e.clientX, e.clientY));
    }

    function sourceIcon(attr: Attribute) {
        if (attr.source == 'span') {
            return spanIcon;
        } else /*if (attr.source == 'resource')*/ {
            return resourceIcon;
        }
    }

    function sourceName(attr: Attribute): string {
        if (attr.source == 'span') {
            return `from span ${attr.span_id}`;
        } else if (attr.source == 'resource') {
            return `from root`;
        } else {
            return '';
        }
    }

    function valueType(attr: Attribute): string {
        switch (attr.type) {
            case 'f64':
                return "number (f64)";
            case 'i64':
                return "integer (i64)";
            case 'u64':
                return "integer (u64)";
            case 'i128':
                return "integer (i128)";
            case 'u128':
                return "integer (u128)";
            case 'bool':
                return "boolean";
            case 'string':
                return "string";
            default:
                return '';
        }
    }

    function valueClass(attr: Attribute): string {
        switch (attr.type) {
            case 'f64':
            case 'i64':
            case 'u64':
            case 'i128':
            case 'u128':
                return "value-type-number";
            case 'bool':
                return "value-type-boolean";
            case 'string':
                return "value-type-string";
            default:
                return '';
        }
    }

    function onmouseenter() {
        setHovered(true);
    }

    function onmouseleave() {
        setHovered(false);
    }

    function onvalueclick() {
        setCollapsed(prev => !prev);
    }

    return (<>
        <span class="detail-info-attributes-source" classList={{ hovered: hovered() }} onmouseenter={onmouseenter} onmouseleave={onmouseleave} oncontextmenu={e => showAttributeContextMenu(e, props.attr)} >
            <Show when={props.attr.source != 'inherent'}>
                <img src={sourceIcon(props.attr)} style="width:8px;height:8px;padding:0 2px;" title={sourceName(props.attr)}></img>
            </Show>
        </span>
        <span class="detail-info-attributes-name" classList={{ hovered: hovered() }} onmouseenter={onmouseenter} onmouseleave={onmouseleave} oncontextmenu={e => showAttributeContextMenu(e, props.attr)} >@{props.attr.name}</span>
        <span style="font-weight: bold; padding: 0 4px; cursor: pointer; user-select: none;" classList={{ hovered: hovered() }} onmouseenter={onmouseenter} onmouseleave={onmouseleave} oncontextmenu={e => showAttributeContextMenu(e, props.attr)} onclick={onvalueclick} >{collapsed() ? '-' : '+'}</span>
        <span class="detail-info-attributes-value" classList={{ hovered: hovered(), [valueClass(props.attr)]: true }} onmouseenter={onmouseenter} onmouseleave={onmouseleave} oncontextmenu={e => showAttributeContextMenu(e, props.attr)} title={valueType(props.attr)} >
            <Show when={collapsed()} fallback={<>{props.attr.value}</>}>
                <span style="position: absolute; width: 100%; text-overflow: ellipsis; white-space: nowrap; overflow: clip;">
                    {props.attr.value}
                </span>
            </Show>
        </span>
    </>);
}

function getEventTraceRoot(event: Event): TraceRoot | null {
    if (event.ancestors.length == 0) {
        return null;
    }

    let root_parent_id = event.ancestors[0].id;
    if (root_parent_id.startsWith("tracing")) {
        return root_parent_id;
    } else {
        return root_parent_id.slice(0, root_parent_id.lastIndexOf('-'));
    }
}

function getSpanTraceRoot(span: Span): TraceRoot {
    let root_parent_id;
    if (span.ancestors.length == 0) {
        root_parent_id = span.id
    } else {
        root_parent_id = span.ancestors[0].id
    }

    if (root_parent_id.startsWith("tracing")) {
        return root_parent_id;
    } else {
        return root_parent_id.slice(0, root_parent_id.lastIndexOf('-'));
    }
}

function createDefaultTraceScreen(root: TraceRoot): [ScreenData, ColumnData] {
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
            text: `#trace: ${root}`,
            property_kind: 'Inherent',
            property: "trace",
            value_kind: 'comparison',
            value: ['Eq', root],
        },
        editable: false,
    }];

    let columns = [COLLAPSABLE, TIMESPAN, COMBINED(INHERENT('name'), CONTENT)];
    let columnWidths = columns.map(def => def.defaultWidth);

    return [{
        kind: 'trace',
        filter,
        timespan: null,
        live: false,
        store: new TraceDataLayer(filter),
        collapsed: {},
    }, {
        columns: columns as any,
        columnWidths,
    }];
}
