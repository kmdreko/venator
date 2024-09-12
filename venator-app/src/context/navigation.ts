import { createContext } from "solid-js";

import { ScreenData } from "../App";

export const NavigationContext = createContext<Navigation>();

export type Navigation = {
    createTab: (data: ScreenData, navigate: boolean) => void,
}
