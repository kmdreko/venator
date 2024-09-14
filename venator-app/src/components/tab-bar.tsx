import { For, useContext } from "solid-js";
import { defaultEventsScreen, defaultInstancesScreen, defaultSpansScreen, ScreenData } from "../App";
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

    return (<div id="tabs">
        <For each={props.screens}>
            {(screen, idx) => (<div class={(idx() == props.active) ? "selected-tab" : "tab"} onclick={() => navigation.activateTab(idx())} onauxclick={e => { if (e.button == 1) navigation.removeTab(idx()); }}>
                <b>{getTabPrefix(screen)}:</b>{stringifyFilter(screen.filter)}
            </div>)}
        </For>
        <button id="new-tab" onclick={async () => navigation.createTab(await defaultEventsScreen(), true)}>
            <img src={eventsAddIcon} style="width:16px; height:16px;" />
        </button>
        <button id="new-tab" onclick={async () => navigation.createTab(await defaultSpansScreen(), true)}>
            <img src={spansAddIcon} style="width:16px; height:16px;" />
        </button>
        <button id="new-tab" onclick={async () => navigation.createTab(await defaultInstancesScreen(), true)} style="color: white">+</button>
    </div>)
}