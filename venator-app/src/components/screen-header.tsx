import { ScreenKind, Timespan } from "../models";
import { TimeControls } from "./time-controls";

import "./screen-header.css";
import eventIcon from '../assets/event.svg';
import spanIcon from '../assets/span.svg';
import traceIcon from '../assets/trace.svg';
import instanceIcon from '../assets/instance.svg';

export type ScreenHeaderProps = {
    screenKind: ScreenKind,
    timespan: Timespan | null,
    setTimespan: (timespan: Timespan) => void,
    timeControlsEnabled: boolean,
    count: [number, boolean],
    countThresholds: [number, number],
    live: boolean,
    setLive: (live: boolean) => void,
};

export function ScreenHeader(props: ScreenHeaderProps) {
    function headerText() {
        if (props.screenKind == 'events') {
            return 'Events';
        } else if (props.screenKind == 'spans') {
            return 'Spans';
        } else if (props.screenKind == 'instances') {
            return 'Instances';
        } else {
            return 'Trace';
        }
    }

    function headerIcon() {
        if (props.screenKind == 'events') {
            return eventIcon;
        } else if (props.screenKind == 'spans') {
            return spanIcon;
        } else if (props.screenKind == 'instances') {
            return instanceIcon;
        } else {
            return traceIcon;
        }
    }

    let countText = () => {
        let [count, exact] = props.count;
        return `${count}${exact && !props.live ? '' : '+'}`;
    };

    let countTextClasses = () => {
        let [count] = props.count;
        if (count >= props.countThresholds[1]) {
            return { "error-text": true }
        }
        if (count >= props.countThresholds[0]) {
            return { "warn-text": true }
        }
        return {};
    }

    return (<div class="screen-header">
        <h1>
            <img src={headerIcon()} style="width:16px;height:16px" />
            &nbsp;
            <span>{headerText()}</span>
            <span class="sub-header" classList={countTextClasses()}>{countText()} in view</span>
        </h1>
        <TimeControls
            timespan={props.timespan}
            updateTimespan={t => props.setTimespan(t)}
            enabled={props.timeControlsEnabled}
            live={props.live}
            setLive={props.setLive}
        />
    </div>);
}
