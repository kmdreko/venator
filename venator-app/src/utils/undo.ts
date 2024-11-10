import { ColumnDef } from "../components/table";
import { Input, Connection, Event, Span } from "../invoke";
import { Timespan } from "../models";

const UNDO_DEBOUNCE_PERIOD_MS = 2000;

export type UndoDataKind = 'root' | 'timespan' | 'filter' | 'columns';

export type UndoData = {
    timespan: Timespan,
    raw_filter: Input[],
    columns: ColumnDef<Span | Event | Connection>[],
    columnWidths: string[],
};

export type UndoRecord = UndoData & {
    at: number,
    kind: UndoDataKind,
};

export class UndoHistory {
    data: UndoRecord[];
    current: number;

    constructor(data: UndoData) {
        this.data = [{
            ...data,
            at: Date.now() - UNDO_DEBOUNCE_PERIOD_MS,
            kind: 'root',
        }];
        this.current = 0;
    }


    undo(): UndoRecord | null {
        if (this.current == 0) {
            return null;
        }

        this.current -= 1;
        return this.data[this.current];
    }

    redo(): UndoRecord | null {
        if (this.current == this.data.length - 1) {
            return null;
        }

        this.current += 1;
        return this.data[this.current];
    }

    updateWithTimespan(timespan: Timespan) {
        let now = Date.now();
        let current = this.data[this.current];

        if (current.kind == 'timespan' && now - current.at < UNDO_DEBOUNCE_PERIOD_MS) {
            this.data.splice(this.current + 1);

            current.timespan = timespan;
            current.at = now;
        } else {
            this.data.splice(this.current + 1);
            this.data.push({
                ...current,
                timespan,
                at: now,
                kind: 'timespan',
            });
            this.current += 1;
        }
    }

    updateWithFilter(raw_filter: Input[]) {
        let now = Date.now();
        let current = this.data[this.current];

        // don't debounce the filter

        this.data.splice(this.current + 1);
        this.data.push({
            ...current,
            raw_filter: [...raw_filter],
            at: now,
            kind: 'filter',
        });
        this.current += 1;
    }

    updateWithColumnData(columns: ColumnDef<Span | Event | Connection>[], columnWidths: string[]) {
        let now = Date.now();
        let current = this.data[this.current];

        if (current.kind == 'columns' && now - current.at < UNDO_DEBOUNCE_PERIOD_MS) {
            this.data.splice(this.current + 1);

            current.columns = [...columns];
            current.columnWidths = [...columnWidths];
            current.at = now;
        } else {
            this.data.splice(this.current + 1);
            this.data.push({
                ...current,
                columns: [...columns],
                columnWidths: [...columnWidths],
                at: now,
                kind: 'columns',
            });
            this.current += 1;
        }
    }
}
