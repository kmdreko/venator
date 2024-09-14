import { For, useContext } from "solid-js";
import { defaultEventsScreen, defaultSpansScreen, ScreenData } from "../App";
import { FilterPredicate } from "../invoke";
import { NavigationContext } from "../context/navigation";

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

    return (<div class="tabbar">
        <div class="tabs" onwheel={onwheel}>
            <For each={props.screens}>
                {(screen, idx) => (<div class="tab" classList={{ active: idx() == props.active }} onclick={() => navigation.activateTab(idx())} onauxclick={e => { if (e.button == 1) navigation.removeTab(idx()); }}>
                    <span><b>{getTabPrefix(screen)}:</b>{stringifyFilter(screen.filter)}</span>
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
