import { createSignal, For, Match, Show, Switch, useContext } from 'solid-js';
import { writeText } from '@tauri-apps/plugin-clipboard-manager';
import { Menu } from '@tauri-apps/api/menu';
import { LogicalPosition } from '@tauri-apps/api/dpi';
import { Ancestor, Attribute, Event, FullSpanId, Input, Instance, Span } from '../invoke'
import { Timespan } from '../models';
import { NavigationContext } from '../context/navigation';
import { ScreenData } from '../App';
import { ATTRIBUTE, COLLAPSABLE, COMBINED, INHERENT, TIMESPAN } from './table';
import { TraceDataLayer } from '../utils/datalayer';

import "./detail-pane.css";
import spanIcon from '../assets/span.svg';
import instanceIcon from '../assets/instance.svg';

export type EventDetailPaneProps = {
    event: Event,
    timespan: Timespan | null,
    updateSelectedRow: (event: Event | null) => void,
    addToFilter: (filter: string) => void,
    addColumn: (column: string) => void,
}

export function EventDetailPane(props: EventDetailPaneProps) {
    function eventInTimespan(): boolean {
        if (props.timespan == null) {
            return false;
        }

        return (props.event.timestamp >= props.timespan[0] && props.event.timestamp <= props.timespan[1])
    }

    function onClickHeader(e: MouseEvent) {
        if (e.button == 1) {
            props.updateSelectedRow(null);
        }
    }

    return (<div id="detail-pane">
        <div id="detail-header" onauxclick={onClickHeader}>
            <span>event details</span>
            <button onclick={() => props.updateSelectedRow(null)}>X</button>
        </div>
        <div id="detail-info">
            <div id="detail-info-head">
                <DetailedLevel level={props.event.level} />
                <DetailedTimestamp timestamp={props.event.timestamp} />
                <Show when={!eventInTimespan()}>
                    <span style="color: #555555; margin: 0 4px;">not in view</span>
                </Show>
            </div>
            <div id="detail-info-meta">
                <DetailedMeta name={"target"} value={props.event.target} addToFilter={props.addToFilter} addColumn={props.addColumn} />
                <DetailedMeta name={"file"} value={props.event.file} addToFilter={props.addToFilter} addColumn={props.addColumn} />
                <DetailedMetaParents ancestors={props.event.ancestors} />
                <DetailedMeta name={"instance"} value={props.event.instance_id} addToFilter={props.addToFilter} addColumn={props.addColumn} />
            </div>
            <DetailedPrimary message={props.event.attributes.find(a => a.name == 'message')?.value!}></DetailedPrimary>
            <DetailAttributes attributes={props.event.attributes} addToFilter={props.addToFilter} addColumn={props.addColumn} />
        </div>
    </div>);
}

export type SpanDetailPaneProps = {
    span: Span,
    timespan: Timespan | null,
    updateSelectedRow: (span: Span | null) => void,
    addToFilter: (filter: string) => void,
    addColumn: (column: string) => void,
}

export function SpanDetailPane(props: SpanDetailPaneProps) {
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

    function onClickHeader(e: MouseEvent) {
        if (e.button == 1) {
            props.updateSelectedRow(null);
        }
    }

    return (<div id="detail-pane">
        <div id="detail-header" onauxclick={onClickHeader}>span details</div>
        <div id="detail-info">
            <div id="detail-info-head">
                <DetailedLevel level={props.span.level} />
                <DetailedTimestamp timestamp={props.span.created_at} />
                <Show when={props.span.closed_at != null}>
                    <DetailedDuration duration={props.span.closed_at! - props.span.created_at} />
                </Show>
                <Show when={!spanInTimespan()}>
                    <span style="color: #555555; margin: 0 4px;">not in view</span>
                </Show>
            </div>
            <div id="detail-info-meta">
                <DetailedMetaId value={props.span.id} created_at={props.span.created_at} closed_at={props.span.closed_at} />
                <DetailedMeta name={"target"} value={props.span.target} addToFilter={props.addToFilter} addColumn={props.addColumn} />
                <DetailedMeta name={"file"} value={props.span.file} addToFilter={props.addToFilter} addColumn={props.addColumn} />
                <DetailedMetaParents ancestors={props.span.ancestors} name={props.span.name} id={props.span.id} />
                <DetailedMeta name={"instance"} value={props.span.id.substring(0, props.span.id.indexOf('-'))} addToFilter={props.addToFilter} addColumn={props.addColumn} />
            </div>
            <DetailedPrimary message={props.span.name}></DetailedPrimary>
            <DetailAttributes attributes={props.span.attributes} addToFilter={props.addToFilter} addColumn={props.addColumn} />
        </div>
    </div>);
}

export type InstanceDetailPaneProps = {
    instance: Instance,
    timespan: Timespan | null,
    updateSelectedRow: (instance: Instance | null) => void,
    addToFilter: (filter: string) => void,
    addColumn: (column: string) => void,
}

export function InstanceDetailPane(props: InstanceDetailPaneProps) {
    function instanceInTimespan(): boolean {
        if (props.timespan == null) {
            return false;
        }

        if (props.instance.connected_at > props.timespan[1]) {
            return false;
        }

        if (props.instance.disconnected_at != null && props.instance.disconnected_at < props.timespan[0]) {
            return false;
        }

        return true;
    }

    function onClickHeader(e: MouseEvent) {
        if (e.button == 1) {
            props.updateSelectedRow(null);
        }
    }

    return (<div id="detail-pane">
        <div id="detail-header" onauxclick={onClickHeader}>instance details</div>
        <div id="detail-info">
            <div id="detail-info-head">
                <DetailedTimestamp timestamp={props.instance.connected_at} />
                <Show when={props.instance.disconnected_at != null}>
                    <DetailedDuration duration={props.instance.disconnected_at! - props.instance.connected_at} />
                </Show>
                <Show when={!instanceInTimespan()}>
                    <span style="color: #555555; margin: 0 4px;">not in view</span>
                </Show>
            </div>
            <div id="detail-info-meta">
                <DetailedMeta name={"id"} value={props.instance.id} addToFilter={props.addToFilter} addColumn={props.addColumn} />
            </div>
            <DetailAttributes attributes={props.instance.attributes} addToFilter={props.addToFilter} addColumn={props.addColumn} />
        </div>
    </div>);
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
    </Switch>);
}

export function DetailedTimestamp(props: { timestamp: number }) {
    return (<div class="detailed-timestamp">
        {(new Date(props.timestamp / 1000)).toISOString()}
    </div>);
}

export function DetailedDuration(props: { duration: number }) {
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

    return (<div class="detailed-duration">
        {renderedDuration(props.duration)}
    </div>);
}

export function DetailedMetaId(props: { value: string, created_at: number, closed_at: number | null }) {
    let navigation = useContext(NavigationContext);

    async function showcontext(e: MouseEvent) {
        let menu = await Menu.new({
            items: [
                {
                    text: "open trace in new tab", action: () => {
                        navigation?.createTab(createDefaultTraceScreen(props.value), true)
                    }
                },
            ]
        });
        await menu.popup(new LogicalPosition(e.clientX, e.clientY));
    }

    return (<div class="detailed-meta-id" oncontextmenu={showcontext}>
        <b>#id</b>
        &nbsp;
        <span style="font-family: monospace;">{props.value}</span>
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
        <span style="font-family: monospace;">{props.value}</span>
    </div>);
}

export function DetailedMetaParents(props: { id?: FullSpanId, name?: string, ancestors: Ancestor[] }) {
    let navigation = useContext(NavigationContext);

    let [collapsed, setCollapsed] = createSignal(true);

    async function showcontext(e: MouseEvent, id: FullSpanId) {
        let menu = await Menu.new({
            items: [
                {
                    text: "open trace in new tab", action: () => {
                        navigation?.createTab(createDefaultTraceScreen(id), true)
                    }
                },
            ]
        });
        await menu.popup(new LogicalPosition(e.clientX, e.clientY));
    }

    return (<>
        <div class="detailed-meta" onclick={() => setCollapsed(!collapsed())}>
            <b>#stack:</b>
            &nbsp;
            <span style="font-family: monospace;">{props.ancestors.length + (props.name ? 1 : 0)}</span>
        </div>
        <Show when={!collapsed()}>
            <Show when={props.name}>
                <div class="detailed-meta-parent" oncontextmenu={e => showcontext(e, props.id!)}>
                    {props.name}
                    &nbsp;
                    <b style="font-family: Inter, Avenir, Helvetica, Arial, sans-serif;">{props.ancestors.length == 0 ? "(this, root)" : "(this)"}</b>
                </div>
            </Show>
            <For each={props.ancestors.slice().reverse()}>
                {(ancestor, i) => <div class="detailed-meta-parent" oncontextmenu={e => showcontext(e, ancestor.id)}>
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
            if (attr.source == 'instance') {
                return [{ text: "copy instance id", action: () => writeText(attr.instance_id) }];
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
                // { item: 'Separator' },
                // { text: `add index on @${shortName}`, action: () => { } },
            ]
        });
        await menu.popup(new LogicalPosition(e.clientX, e.clientY));
    }

    function sourceIcon(attr: Attribute) {
        if (attr.source == 'span') {
            return spanIcon;
        } else /*if (attr.source == 'instance')*/ {
            return instanceIcon;
        }
    }

    function sourceName(attr: Attribute): string {
        if (attr.source == 'span') {
            return `from span ${attr.span_id}`;
        } else if (attr.source == 'instance') {
            return `from instance ${attr.instance_id}`;
        } else {
            return '';
        }
    }

    return (<table id="detail-info-attributes">
        <tbody>
            <For each={props.attributes.filter(a => a.name != 'message')}>
                {attr => (<tr oncontextmenu={e => showAttributeContextMenu(e, attr)}>
                    <td class="detail-info-attributes-source">
                        <Show when={attr.source != 'inherent'}>
                            <img src={sourceIcon(attr)} style="width:8px;height:8px;padding:0 2px;" title={sourceName(attr)}></img>
                        </Show>
                    </td>
                    <td class="detail-info-attributes-name">@{attr.name}</td>
                    <td class="detail-info-attributes-value">: {attr.value}</td>
                </tr>)}
            </For>
        </tbody>
    </table>);
}

function createDefaultTraceScreen(spanId: FullSpanId): ScreenData {
    let filter: Input[] = [{
        text: "#level: >=TRACE",
        input: 'valid',
        property_kind: 'Inherent',
        property: "level",
        value_kind: 'comparison',
        value: ['Gte', "TRACE"],
        editable: false,
    }, {
        text: `#stack: ${spanId}`,
        input: 'valid',
        property_kind: 'Inherent',
        property: "stack",
        value_kind: 'comparison',
        value: ['Eq', spanId],
        editable: false,
    }];

    let columns = [COLLAPSABLE, TIMESPAN, COMBINED(INHERENT('name'), ATTRIBUTE('message'))];
    let columnWidths = columns.map(def => def.defaultWidth);

    return {
        kind: 'trace',
        filter,
        timespan: null,
        selected: null,
        live: false,
        store: new TraceDataLayer(filter),
        collapsed: {},
        columns,
        columnWidths,
    };
}
