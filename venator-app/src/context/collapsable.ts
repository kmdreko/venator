import { createContext } from "solid-js";

import { FullSpanId } from "../invoke";

export const CollapsableContext = createContext<Collapsable>();

export type Collapsable = {
    isCollapsed: (id: FullSpanId) => boolean,
    collapse: (id: FullSpanId, collapse: boolean) => void,
    areAnyCollapsed: () => boolean,
    expandAll: () => void,
    collapseAll: () => void,
}
