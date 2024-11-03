import { createEffect, onCleanup, untrack, createSignal } from 'solid-js';
import { Timespan } from '../models';

import './time-controls.css';
import playIcon from '../assets/live-play.svg';
import pauseIcon from '../assets/live-pause.svg';

export type TimeControlProps = {
    enabled: boolean,
    timespan: Timespan | null,
    updateTimespan: (timespan: Timespan) => void,
    live: boolean,
    setLive: (live: boolean) => void,
};

export function TimeControls(props: TimeControlProps) {
    const [updateTimeout, setUpdateTimeout] = createSignal<number | undefined>(undefined);

    createEffect(() => {
        props.timespan;

        let existing_handle = untrack(updateTimeout);
        if (existing_handle != undefined) {
            clearTimeout(existing_handle);
            setUpdateTimeout(undefined);
        }

        if (props.live) {
            setUpdateTimeout(setTimeout(() => {
                let timespan = props.timespan;
                if (timespan == null) {
                    return;
                }

                let [start, end] = timespan;
                let duration = end - start;

                let now = Date.now() * 1000;
                props.updateTimespan([now - duration, now]);
            }, 100));
        }
    });

    onCleanup(() => {
        if (updateTimeout() != undefined) {
            clearTimeout(updateTimeout());
        }
    })

    function renderedStartTime() {
        let current_timespan = props.timespan;
        if (current_timespan == null) {
            return '---';
        }

        let [start, _end] = current_timespan;

        return (new Date(start / 1000)).toLocaleString();
    }

    function renderedDuration() {
        let current_timespan = props.timespan;
        if (current_timespan == null) {
            return '---';
        }

        let [start, end] = current_timespan;
        let duration = end - start;

        const MILLISECOND = 1000;
        const SECOND = 1000000;
        const MINUTE = 60000000;
        const HOUR = 3600000000;
        const DAY = 86400000000;

        if (duration / DAY >= 1.0)
            return `${(duration / DAY).toPrecision(3)} days`;
        else if (duration / HOUR >= 1.0)
            return `${(duration / HOUR).toPrecision(3)} hours`;
        else if (duration / MINUTE >= 1.0)
            return `${(duration / MINUTE).toPrecision(3)} minutes`;
        else if (duration / SECOND >= 1.0)
            return `${(duration / SECOND).toPrecision(3)} seconds`;
        else
            return `${(duration / MILLISECOND).toPrecision(3)} milliseconds`;
    }

    function decStartTime() {
        let current_timespan = props.timespan!;
        let [start, end] = current_timespan;
        let duration = end - start;
        let shift = duration / 10;

        let new_start = start - shift;
        let new_end = end - shift;
        props.updateTimespan([new_start, new_end]);
    }

    function incStartTime() {
        let current_timespan = props.timespan!;
        let [start, end] = current_timespan;
        let duration = end - start;
        let shift = duration / 10;

        let new_start = start + shift;
        let new_end = end + shift;
        props.updateTimespan([new_start, new_end]);
    }

    function decDuration() {
        let current_timespan = props.timespan!;
        let bias = 0.5;
        let scale = 1 / 1.1;

        let [start, end] = current_timespan;
        let duration = end - start;
        let middle = start * (1 - bias) + end * bias;

        let new_duration = duration * scale;
        let new_start = middle - new_duration * bias;
        let new_end = middle + new_duration * (1 - bias);
        props.updateTimespan([new_start, new_end]);
    }

    function incDuration() {
        let current_timespan = props.timespan!;
        let bias = 0.5;
        let scale = 1.1;

        let [start, end] = current_timespan;
        let duration = end - start;
        let middle = start * (1 - bias) + end * bias;

        let new_duration = duration * scale;
        let new_start = middle - new_duration * bias;
        let new_end = middle + new_duration * (1 - bias);
        props.updateTimespan([new_start, new_end]);
    }

    function enabled() {
        return props.enabled && props.timespan != null;
    }

    return <div class="time-controls" classList={{ enabled: enabled() }}>
        <div class="time-control">
            <button class="left" onclick={enabled() ? decStartTime : () => { }}>&lt;</button>
            <div class="main" style="width: 150px;">
                {renderedStartTime()}
            </div>
            <button class="right" onclick={enabled() ? incStartTime : () => { }}>&gt;</button>
        </div>
        <div class="time-control">
            <button class="left" onclick={enabled() ? decDuration : () => { }}>-</button>
            <div class="main" style="width: 100px;">
                {renderedDuration()}
            </div>
            <button class="right" onclick={enabled() ? incDuration : () => { }}>+</button>
        </div>
        <button classList={{ "live": true, "active": props.live }} onclick={() => props.setLive(!props.live)}>
            <img src={props.live ? pauseIcon : playIcon} style="width:14px;height:14px;" />
        </button>
    </div>;
}
