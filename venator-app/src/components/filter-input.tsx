import { createEffect, createSignal, For, Match, Switch } from "solid-js";
import { Input, InvalidFilterPredicate, ValidFilterPredicate } from "../invoke";

import './filter-input.css';
import { Menu } from "@tauri-apps/api/menu";
import { LogicalPosition } from "@tauri-apps/api/dpi";
import { writeText } from "@tauri-apps/plugin-clipboard-manager";

export type FilterInputProps = {
    predicates: Input[],
    updatePredicates: (predicates: Input[]) => void,
    parse: (p: string) => Promise<Input[]>,
};

export function FilterInput(props: FilterInputProps) {
    let [localPredicates, setLocalPredicates] = createSignal<Input[]>([...props.predicates]);

    let input_e!: HTMLDivElement;

    createEffect(() => {
        let p = props.predicates;

        if (getUneditableLength(props.predicates) == p.length) {
            input_e.innerText = ' ';
        } else {
            input_e.innerText = '';
        }

        setLocalPredicates([...p]);
    })

    async function onblur(this: HTMLInputElement) {
        let new_predicates = await props.parse(this.innerText);
        let uneditable_predicates = props.predicates.slice(0, getUneditableLength(props.predicates));

        props.updatePredicates([...uneditable_predicates, ...new_predicates]);
    }

    function remove(i: number) {
        let current_predicates = props.predicates;
        if (current_predicates[i].editable == false) {
            return;
        }

        let updated_predicates = [...current_predicates];
        updated_predicates.splice(i, 1);

        props.updatePredicates(updated_predicates);
    }

    function update(i: number, newPredicates: Input[]) {
        let current_predicates = props.predicates;
        let updated_predicates = [...current_predicates];
        updated_predicates.splice(i, 1, ...newPredicates);
        props.updatePredicates(updated_predicates);
    }

    function onkeydown(this: HTMLDivElement, e: KeyboardEvent) {
        if (e.key === "Enter") {
            e.preventDefault();
            this.blur();
        }
    }

    function onmousedown(e: MouseEvent) {
        if (e.button == 1 || e.button == 2) {
            e.preventDefault();
        }
    }

    function getUneditableLength(p: Input[]): number {
        for (let i = 0; i < p.length; i++) {
            if (p[i].editable !== false) {
                return i;
            }
        }

        return p.length;
    }

    return (<div class="filter-input-container">
        <For each={localPredicates().slice(0, getUneditableLength(localPredicates()))}>
            {(predicate, i) => <Switch>
                <Match when={predicate.input == 'valid'}>
                    <FilterInputPredicate predicate={predicate as ValidFilterPredicate} remove={() => remove(i())} update={p => update(i(), p)} parse={props.parse} />
                    <span class="spacer">{'  '}</span>
                </Match>
                <Match when={predicate.input == 'invalid'}>
                    <InvalidFilterInputPredicate predicate={predicate as InvalidFilterPredicate} remove={() => remove(i())} update={p => update(i(), p)} parse={props.parse} />
                </Match>
            </Switch>}
        </For>
        <span ref={input_e} class="filter-input" contenteditable="plaintext-only" onfocusout={onblur} onkeydown={onkeydown} onmousedown={onmousedown}>
            {' '}
            {localPredicates().slice(getUneditableLength(localPredicates())).map((predicate, i) => {
                return (<Switch>
                    <Match when={predicate.input == 'valid'}>
                        <FilterInputPredicate predicate={predicate as ValidFilterPredicate} remove={() => remove(i + getUneditableLength(localPredicates()))} update={p => update(i + getUneditableLength(localPredicates()), p)} parse={props.parse} />
                        <span class="spacer">{'  '}</span>
                    </Match>
                    <Match when={predicate.input == 'invalid'}>
                        <InvalidFilterInputPredicate predicate={predicate as InvalidFilterPredicate} remove={() => remove(i + getUneditableLength(localPredicates()))} update={p => update(i + getUneditableLength(localPredicates()), p)} parse={props.parse} />
                        <span class="spacer">{'  '}</span>
                    </Match>
                </Switch>);
            })}
        </span>
    </div>);
}

export function InvalidFilterInputPredicate(props: { predicate: InvalidFilterPredicate, remove: () => void, update: (p: Input[]) => void, parse: (p: string) => Promise<Input[]> }) {
    function onclick(e: MouseEvent) {
        if (e.button == 1) {
            e.preventDefault();
            e.stopPropagation();
            props.remove();
        }
    }

    async function showContextMenu(e: MouseEvent) {
        e.preventDefault();

        let menu = await Menu.new({
            items: [
                { text: "copy", action: () => writeText(props.predicate.text.trim()) },
                { text: "remove", action: () => props.remove() },
            ]
        });
        await menu.popup(new LogicalPosition(e.clientX, e.clientY));
    }

    return (<span class="predicate attribute-predicate error" onauxclick={onclick} oncontextmenu={showContextMenu} title={props.predicate.error}>
        {props.predicate.text}
    </span>);
}

export function FilterInputPredicate(props: { predicate: ValidFilterPredicate, remove: () => void, update: (p: Input[]) => void, parse: (p: string) => Promise<Input[]> }) {
    return (<Switch>
        <Match when={props.predicate.property == "level" && props.predicate.property_kind == 'Inherent'}>
            <FilterInputLevelPredicate predicate={props.predicate as any} remove={props.remove} update={props.update} />
        </Match>
        <Match when={props.predicate.property_kind == 'Inherent'}>
            <FilterInputMetaPredicate predicate={props.predicate} remove={props.remove} update={props.update} parse={props.parse} />
        </Match>
        <Match when={props.predicate.property_kind == 'Attribute'}>
            <FilterInputAttributePredicate predicate={props.predicate} remove={props.remove} update={props.update} parse={props.parse} />
        </Match>
    </Switch>);
}

export function FilterInputLevelPredicate(props: { predicate: ValidFilterPredicate & { value_kind: 'comparison' }, remove: () => void, update: (p: Input[]) => void }) {
    function wheel(e: WheelEvent) {
        if (e.deltaY < 0.0) {
            if (props.predicate.value[1] == "TRACE") {
                props.update([{ ...props.predicate, value: ['Gte', "DEBUG"], text: "#level: >=DEBUG" }])
            } else if (props.predicate.value[1] == "DEBUG") {
                props.update([{ ...props.predicate, value: ['Gte', "INFO"], text: "#level: >=INFO" }])
            } else if (props.predicate.value[1] == "INFO") {
                props.update([{ ...props.predicate, value: ['Gte', "WARN"], text: "#level: >=WARN" }])
            } else if (props.predicate.value[1] == "WARN") {
                props.update([{ ...props.predicate, value: ['Gte', "ERROR"], text: "#level: >=ERROR" }])
            }
        } else if (e.deltaY > 0.0) {
            if (props.predicate.value[1] == "DEBUG") {
                props.update([{ ...props.predicate, value: ['Gte', "TRACE"], text: "#level: >=TRACE" }])
            } else if (props.predicate.value[1] == "INFO") {
                props.update([{ ...props.predicate, value: ['Gte', "DEBUG"], text: "#level: >=DEBUG" }])
            } else if (props.predicate.value[1] == "WARN") {
                props.update([{ ...props.predicate, value: ['Gte', "INFO"], text: "#level: >=INFO" }])
            } else if (props.predicate.value[1] == "ERROR") {
                props.update([{ ...props.predicate, value: ['Gte', "WARN"], text: "#level: >=WARN" }])
            }
        }
    }

    return (<Switch>
        <Match when={props.predicate.value[1] == "TRACE"}>
            <span class="predicate level-predicate-0" onwheel={wheel}>
                {props.predicate.text}
            </span>
        </Match>
        <Match when={props.predicate.value[1] == "DEBUG"}>
            <span class="predicate level-predicate-1" onwheel={wheel}>
                {props.predicate.text}
            </span>
        </Match>
        <Match when={props.predicate.value[1] == "INFO"}>
            <span class="predicate level-predicate-2" onwheel={wheel}>
                {props.predicate.text}
            </span>
        </Match>
        <Match when={props.predicate.value[1] == "WARN"}>
            <span class="predicate level-predicate-3" onwheel={wheel}>
                {props.predicate.text}
            </span>
        </Match>
        <Match when={props.predicate.value[1] == "ERROR"}>
            <span class="predicate level-predicate-4" onwheel={wheel}>
                {props.predicate.text}
            </span>
        </Match>
    </Switch>);
}

export function FilterInputMetaPredicate(props: { predicate: ValidFilterPredicate & Input, remove: () => void, update: (p: Input[]) => void, parse: (p: string) => Promise<Input[]> }) {
    function onclick(e: MouseEvent) {
        if (e.button == 1) {
            e.preventDefault();
            e.stopPropagation();
            props.remove();
        }
    }

    async function showContextMenu(e: MouseEvent) {
        e.preventDefault();

        let menu = await Menu.new({
            items: [
                { text: "copy", action: () => writeText(props.predicate.text.trim()) },
                { text: "remove", enabled: props.predicate.editable, action: () => props.remove() },
            ]
        });
        await menu.popup(new LogicalPosition(e.clientX, e.clientY));
    }

    return (<span class="predicate meta-predicate" onauxclick={onclick} oncontextmenu={showContextMenu}>
        {props.predicate.text}
    </span>);
}

export function FilterInputAttributePredicate(props: { predicate: ValidFilterPredicate & Input, remove: () => void, update: (p: Input[]) => void, parse: (p: string) => Promise<Input[]> }) {
    function onclick(e: MouseEvent) {
        if (e.button == 1) {
            e.preventDefault();
            e.stopPropagation();
            props.remove();
        }
    }

    async function showContextMenu(e: MouseEvent) {
        e.preventDefault();

        let menu = await Menu.new({
            items: [
                { text: "copy", action: () => writeText(props.predicate.text.trim()) },
                { text: "remove", enabled: props.predicate.editable, action: () => props.remove() },
            ]
        });
        await menu.popup(new LogicalPosition(e.clientX, e.clientY));
    }

    return (<span class="predicate attribute-predicate" onauxclick={onclick} oncontextmenu={showContextMenu}>
        {props.predicate.text}
    </span>);
}
