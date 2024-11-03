import { createEffect, onCleanup, untrack, createSignal } from 'solid-js';
import { Timespan } from '../models';

import './time-controls.css';
import playIcon from '../assets/live-play.svg';
import pauseIcon from '../assets/live-pause.svg';
import { Menu } from '@tauri-apps/api/menu';
import { LogicalPosition } from '@tauri-apps/api/dpi';
import { Timestamp } from '../invoke';

export type TimeControlProps = {
    enabled: boolean,
    timespan: Timespan | null,
    updateTimespan: (timespan: Timespan) => void,
    live: boolean,
    setLive: (live: boolean) => void,
    getTimestampBefore: (timestamp: Timestamp) => Promise<Timestamp | null>,
    getTimestampAfter: (timestamp: Timestamp) => Promise<Timestamp | null>,
};

export function TimeControls(props: TimeControlProps) {
    const [updateTimeout, setUpdateTimeout] = createSignal<number | undefined>(undefined);
    const [dateParseError, setDateParseError] = createSignal(false);
    const [durationParseError, setDurationParseError] = createSignal(false);

    createEffect(() => {
        props.timespan;

        setDateParseError(false);
        setDurationParseError(false);

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

    async function decStartTimeToData() {
        let current_timespan = props.timespan!;
        let [start, end] = current_timespan;
        let duration = end - start;

        let new_end = await props.getTimestampBefore(end);
        if (new_end == null) {
            return;
        }

        let padded_end = new_end + duration * 0.05;
        let padded_start = padded_end - duration;

        console.log(padded_start, padded_end, duration);
        props.updateTimespan([padded_start, padded_end])
    }

    async function incStartTimeToData() {
        let current_timespan = props.timespan!;
        let [start, end] = current_timespan;
        let duration = end - start;

        let new_start = await props.getTimestampAfter(start);
        if (new_start == null) {
            return;
        }

        let padded_start = new_start - duration * 0.05;
        let padded_end = padded_start + duration;

        console.log(padded_start, padded_end, duration);
        props.updateTimespan([padded_start, padded_end])
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

    function focusoutOnEnter(this: HTMLDivElement, e: KeyboardEvent) {
        if (e.key == "Enter") {
            e.preventDefault();
            this.blur();
        }
    }

    function onDateTimeBlur(this: HTMLDivElement) {
        let timestamp = Date.parse(this.innerText) * 1000;
        if (isNaN(timestamp)) {
            setDateParseError(true);
            return;
        }

        let [start, end] = props.timespan!;
        let duration = end - start;
        props.updateTimespan([timestamp, timestamp + duration]);
        setDateParseError(false);
    }

    function onDurationBlur(this: HTMLDivElement) {
        let text = this.innerText;
        let whitespace = text.indexOf(' ');
        if (whitespace == -1) {
            setDurationParseError(true);
            return;
        }

        let value = parseFloat(text.substring(0, whitespace));
        if (isNaN(value)) {
            setDurationParseError(true);
            return;
        }

        let unit = text.substring(whitespace).trim().toLowerCase();
        let scale = 1;
        if (unit == "d" || unit == "day" || unit == "days") {
            scale = 86400000000;
        } else if (unit == "h" || unit == "hour" || unit == "hours") {
            scale = 3600000000;
        } else if (unit == "m" || unit == "minute" || unit == "minutes") {
            scale = 60000000;
        } else if (unit == "s" || unit == "second" || unit == "seconds") {
            scale = 1000000;
        } else if (unit == "ms" || unit == "millisecond" || unit == "milliseconds") {
            scale = 1000;
        } else {
            setDurationParseError(true);
            return;
        }

        let duration = value * scale;
        let [start, _] = props.timespan!;
        setDurationParseError(false);
        props.updateTimespan([start, start + duration]);
    }

    async function showLeftContextMenu(e: MouseEvent) {
        let menu = await Menu.new({
            items: [
                { text: "go left", action: decStartTime },
                { text: "go left to data", action: decStartTimeToData },
            ]
        });
        await menu.popup(new LogicalPosition(e.clientX, e.clientY));
    }

    async function showRightContextMenu(e: MouseEvent) {
        let menu = await Menu.new({
            items: [
                { text: "go right", action: incStartTime },
                { text: "go right to data", action: incStartTimeToData },
            ]
        });
        await menu.popup(new LogicalPosition(e.clientX, e.clientY));
    }

    return <div class="time-controls" classList={{ enabled: enabled() }}>
        <div class="time-control">
            <button class="left" onclick={enabled() ? decStartTime : () => { }} onauxclick={showLeftContextMenu}>&lt;</button>
            <div contenteditable={enabled() ? "plaintext-only" : false} onblur={onDateTimeBlur} onkeypress={focusoutOnEnter} class="main" classList={{ error: dateParseError() }} style="width: 150px;">
                {renderedStartTime()}
            </div>
            <button class="right" onclick={enabled() ? incStartTime : () => { }} onauxclick={showRightContextMenu}>&gt;</button>
        </div>
        <div class="time-control">
            <button class="left" onclick={enabled() ? decDuration : () => { }}>-</button>
            <div contenteditable={enabled() ? "plaintext-only" : false} onblur={onDurationBlur} onkeypress={focusoutOnEnter} class="main" classList={{ error: durationParseError() }} style="width: 100px;">
                {renderedDuration()}
            </div>
            <button class="right" onclick={enabled() ? incDuration : () => { }}>+</button>
        </div>
        <button classList={{ "live": true, "active": props.live }} onclick={() => props.setLive(!props.live)}>
            <img src={props.live ? pauseIcon : playIcon} style="width:14px;height:14px;" />
        </button>
    </div>;
}
