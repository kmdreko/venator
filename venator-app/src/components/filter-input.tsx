import { createEffect, createSignal, For, Match, Show, Switch } from "solid-js";
import { Comparator, FilterPredicateSingle, Input, InvalidFilterPredicate, ValidFilterPredicate } from "../invoke";

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
        if ((updated_predicates[i] as any).predicate.length != undefined) {
            (updated_predicates[i] as ValidFilterPredicate).predicate = newPredicates;
        } else {
            updated_predicates.splice(i, 1, ...newPredicates);
        }
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

    return (<div class="filter-input-container" spellcheck={false}>
        <For each={localPredicates().slice(0, getUneditableLength(localPredicates()))}>
            {(predicate, i) => <>
                <FilterPredicate predicate={predicate} remove={() => { }} update={p => update(i(), p)} />
                <span class="spacer">{'  '}</span>
            </>}
        </For>
        <span ref={input_e} id="filter-input" class="filter-input" contenteditable="plaintext-only" onfocusout={onblur} onkeydown={onkeydown} onmousedown={onmousedown}>
            {' '}
            {localPredicates().slice(getUneditableLength(localPredicates())).map((predicate, i) => {
                return (<>
                    <FilterPredicate predicate={predicate} remove={() => remove(i + getUneditableLength(localPredicates()))} update={p => update(i + getUneditableLength(localPredicates()), p)} />
                    <span class="spacer">{'  '}</span>
                </>);
            })}
        </span>
    </div>);
}

export function FilterPredicate(props: { predicate: Input, remove: () => void, update: (p: Input[]) => void }) {
    return (<Switch>
        <Match when={props.predicate.input == 'valid'}>
            <FilterInputPredicate predicate={props.predicate as ValidFilterPredicate} remove={props.remove} update={props.update} />
        </Match>
        <Match when={props.predicate.input == 'invalid'}>
            <InvalidFilterInputPredicate predicate={props.predicate as InvalidFilterPredicate} remove={props.remove} />
        </Match>
    </Switch>);
}

export function InvalidFilterInputPredicate(props: { predicate: InvalidFilterPredicate, remove: () => void }) {
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

    return (<span class="predicate attribute-predicate error" onauxclick={onclick} onclick={onclick} oncontextmenu={showContextMenu} title={props.predicate.error}>
        {props.predicate.text}
    </span>);
}

export function FilterInputPredicate(props: { predicate: ValidFilterPredicate, remove: () => void, update: (p: Input[]) => void }) {
    function remove(i: number) {
        let predicates = props.predicate.predicate as Input[];
        predicates.splice(i, 1);
        props.update(predicates);
    }

    function update(i: number, p: Input[]) {
        let predicates = props.predicate.predicate as Input[];
        (predicates[i] as ValidFilterPredicate).predicate = p;
        props.update(predicates);
    }

    return (<Switch>
        <Match when={props.predicate.predicate_kind == 'single'}>
            <FilterInputPredicateSingle predicate={props.predicate as any} remove={props.remove} update={props.update} />
        </Match>
        <Match when={props.predicate.predicate_kind == 'and'}>
            <span class="grouper">{'('}</span>
            <For each={props.predicate.predicate as Input[]}>
                {(p, i) => <>
                    <span class="spacer">{'  '}</span>
                    <Show when={i() != 0}>
                        <span class="grouper">AND</span>
                        <span class="spacer">{'  '}</span>
                    </Show>
                    <FilterPredicate predicate={p} remove={() => remove(i())} update={p => update(i(), p)} />
                </>}
            </For>
            <span class="spacer">{'  '}</span>
            <span class="grouper">{')'}</span>
        </Match>
        <Match when={props.predicate.predicate_kind == 'or'}>
            {<>
                <span class="grouper">{'('}</span>
                <For each={props.predicate.predicate as Input[]}>
                    {(p, i) => <>
                        <span class="spacer">{'  '}</span>
                        <Show when={i() != 0}>
                            <span class="grouper">OR</span>
                            <span class="spacer">{'  '}</span>
                        </Show>
                        <FilterPredicate predicate={p} remove={() => remove(i())} update={p => update(i(), p)} />
                    </>}
                </For>
                <span class="spacer">{'  '}</span>
                <span class="grouper">{')'}</span>
            </>}
        </Match>
    </Switch>);
}

export function FilterInputPredicateSingle(props: { predicate: ValidFilterPredicate & { predicate: FilterPredicateSingle }, remove: () => void, update: (p: Input[]) => void }) {
    return (<Switch>
        <Match when={props.predicate.predicate.property == "level" && props.predicate.predicate.property_kind == 'Inherent'}>
            <FilterInputLevelPredicate predicate={props.predicate as any} remove={props.remove} update={props.update} />
        </Match>
        <Match when={props.predicate.predicate.property_kind == 'Inherent'}>
            <FilterInputMetaPredicate predicate={props.predicate} remove={props.remove} />
        </Match>
        <Match when={props.predicate.predicate.property_kind == 'Attribute'}>
            <FilterInputAttributePredicate predicate={props.predicate} remove={props.remove} />
        </Match>
    </Switch>);
}

export function FilterInputLevelPredicate(props: { predicate: { predicate: FilterPredicateSingle & { value_kind: 'comparison' } } & ValidFilterPredicate & Input, remove: () => void, update: (p: Input[]) => void }) {
    function updateValue(v: string) {
        function text(cmp: Comparator) {
            switch (cmp) {
                case "Gt":
                    return `#level: >${v}`;
                case "Gte":
                    return `#level: >=${v}`;
                case "Eq":
                    return `#level: ${v}`;
                case "Lt":
                    return `#level: <${v}`;
                case "Lte":
                    return `#level: <=${v}`;
            }
        }

        props.update([{
            ...props.predicate,
            predicate_kind: 'single',
            predicate: {
                ...props.predicate.predicate,
                value: [props.predicate.predicate.value[0], v],
                text: text(props.predicate.predicate.value[0]),
            }
        }])
    }

    function wheel(e: WheelEvent) {
        if (e.deltaY < 0.0) {
            if (props.predicate.predicate.value[1] == "TRACE") {
                updateValue("DEBUG");
            } else if (props.predicate.predicate.value[1] == "DEBUG") {
                updateValue("INFO");
            } else if (props.predicate.predicate.value[1] == "INFO") {
                updateValue("WARN");
            } else if (props.predicate.predicate.value[1] == "WARN") {
                updateValue("ERROR");
            } else if (props.predicate.predicate.value[1] == "ERROR") {
                updateValue("FATAL");
            }
        } else if (e.deltaY > 0.0) {
            if (props.predicate.predicate.value[1] == "DEBUG") {
                updateValue("TRACE");
            } else if (props.predicate.predicate.value[1] == "INFO") {
                updateValue("DEBUG");
            } else if (props.predicate.predicate.value[1] == "WARN") {
                updateValue("INFO");
            } else if (props.predicate.predicate.value[1] == "ERROR") {
                updateValue("WARN");
            } else if (props.predicate.predicate.value[1] == "FATAL") {
                updateValue("ERROR");
            }
        }
    }

    function onclick(e: MouseEvent) {
        if (e.button == 1) {
            e.preventDefault();
            e.stopPropagation();
            props.remove();
        }
    }

    async function showContextMenu(e: MouseEvent) {
        e.preventDefault();

        let value = props.predicate.predicate.value[1];

        let menu = await Menu.new({
            items: [
                { text: "copy", action: () => writeText(props.predicate.predicate.text.trim()) },
                { text: "remove", enabled: props.predicate.editable, action: () => props.remove() },
                { item: 'Separator' },
                { text: 'ERROR', enabled: value != 'ERROR', action: () => updateValue('ERROR') },
                { text: 'WARN', enabled: value != 'WARN', action: () => updateValue('WARN') },
                { text: 'INFO', enabled: value != 'INFO', action: () => updateValue('INFO') },
                { text: 'DEBUG', enabled: value != 'DEBUG', action: () => updateValue('DEBUG') },
                { text: 'TRACE', enabled: value != 'TRACE', action: () => updateValue('TRACE') },
                { text: 'FATAL', enabled: value != 'FATAL', action: () => updateValue('FATAL') },
            ]
        });
        await menu.popup(new LogicalPosition(e.clientX, e.clientY));
    }

    return (<Switch>
        <Match when={props.predicate.predicate.value[1] == "TRACE"}>
            <span class="predicate level-predicate-0" onauxclick={onclick} onclick={onclick} onwheel={wheel} oncontextmenu={showContextMenu}>
                {props.predicate.predicate.text}
            </span>
        </Match>
        <Match when={props.predicate.predicate.value[1] == "DEBUG"}>
            <span class="predicate level-predicate-1" onauxclick={onclick} onclick={onclick} onwheel={wheel} oncontextmenu={showContextMenu}>
                {props.predicate.predicate.text}
            </span>
        </Match>
        <Match when={props.predicate.predicate.value[1] == "INFO"}>
            <span class="predicate level-predicate-2" onauxclick={onclick} onclick={onclick} onwheel={wheel} oncontextmenu={showContextMenu}>
                {props.predicate.predicate.text}
            </span>
        </Match>
        <Match when={props.predicate.predicate.value[1] == "WARN"}>
            <span class="predicate level-predicate-3" onauxclick={onclick} onclick={onclick} onwheel={wheel} oncontextmenu={showContextMenu}>
                {props.predicate.predicate.text}
            </span>
        </Match>
        <Match when={props.predicate.predicate.value[1] == "ERROR"}>
            <span class="predicate level-predicate-4" onauxclick={onclick} onclick={onclick} onwheel={wheel} oncontextmenu={showContextMenu}>
                {props.predicate.predicate.text}
            </span>
        </Match>
        <Match when={props.predicate.predicate.value[1] == "FATAL"}>
            <span class="predicate level-predicate-5" onauxclick={onclick} onclick={onclick} onwheel={wheel} oncontextmenu={showContextMenu}>
                {props.predicate.predicate.text}
            </span>
        </Match>
    </Switch>);
}

export function FilterInputMetaPredicate(props: { predicate: { predicate: FilterPredicateSingle } & Input, remove: () => void }) {
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
                { text: "copy", action: () => writeText(props.predicate.predicate.text.trim()) },
                { text: "remove", enabled: props.predicate.editable, action: () => props.remove() },
            ]
        });
        await menu.popup(new LogicalPosition(e.clientX, e.clientY));
    }

    return (<span class="predicate meta-predicate" onauxclick={onclick} onclick={onclick} oncontextmenu={showContextMenu}>
        {props.predicate.predicate.text}
    </span>);
}

export function FilterInputAttributePredicate(props: { predicate: { predicate: FilterPredicateSingle } & Input, remove: () => void }) {
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
                { text: "copy", action: () => writeText(props.predicate.predicate.text.trim()) },
                { text: "remove", enabled: props.predicate.editable, action: () => props.remove() },
            ]
        });
        await menu.popup(new LogicalPosition(e.clientX, e.clientY));
    }

    return (<span class="predicate attribute-predicate" onauxclick={onclick} onclick={onclick} oncontextmenu={showContextMenu}>
        {props.predicate.predicate.text}
    </span>);
}
