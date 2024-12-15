# Venator Interface

The user interface is split into three meta regions:
- the tab bar
- the main screen
- the status bar


## Tab Bar

The tabs should be self-explanatory but I'll explain regardless. The app can
have multiple "screens" open simulaneouosly; the most lit one is what is being
shown and the unlit ones are backgrounded. You can switch to a tab by clicking
on it. You can open a new tab by clicking one of the buttons on the right (one
for an events screen, one for a spans screen) or by using the shortcut `CTRL+T`.
You can close a tab by clicking on `X` when its active, or by middle-clicking.
Hovering over a tab will show the screen type and full filter. More options are
available via `View` in the menu and right-clicking a tab.


## Status Bar

The status bar at the bottom of the screen shows information of the application
overall.

The bottom left shows the database file that is open (or the "default dataset"
if launched by default) as well as if it is listening for connections and if so
what address and port.

The bottom right shows the running metrics. The first metric is the bytes per
second being received from established connections. The next metric shows the
number of connections currently established. The last metric shows the load on
the underlying engine that is handling incomming data and responding to queries.


## Main Screen

The main screen is where you can view events, spans, and other entities based on
timeframe and filter. It is composed of a few key components:
- the time controls
- the filter input
- the graph
- the table
- the details panel (collapsable)


### Time Controls

These controls affect the timeframe that affects the graph and table results of
the screen. It is split between the starting point and the duration of the time
frame with an additional button for listening to live events.

The starting point controls shows the currently set starting point and has
buttons for shifting the starting point before or after in time. The main field
can also be edited manually to set a specific time.

The duration controls shows the currently set duration and has buttons for
reducing or expanding the duration. The main field can also be edited manually
to set a specific duration.


### Filter Input

The filter input is where you can specify `property: value` predicates for
narrowing down the events, spans, etc that are being shown. See [filter syntax](./filter-syntax.md)
for details.

An empty event or span screen will include a permanent `#level` filter for only
showing entities at or above the specified log level. You can also hover and
scroll on this predicate to increase or decrease the level.

When editing the filter, any non-permanent predicates are undecorated and shown
as a single text input. When hitting `Enter` or clicking off, the filter will be
parsed and applied to the graph and table results. The predicates will be
decorated based on their type (white for attributes, gray for inherent fields).
The predicates will be highlighted in red if the value was invalid or the whole
input will be highlighted red if there was a syntax error that meant the filter
couldn't be properly split into predicates. Individual predicates can be right-
clicked to copy or remove, or middle-clicked to remove them.

Options in the table or details panel can add predicates to the filter.


### Graph

The graph can take a few different forms based on the type of main screen. For
events it will show bars of aggregated counts by log level. For spans it will 
show them individually spread across their timeframe stacked on top of eachother
(up to 10 high). For traces it will show all spans and events stacked on top of
eachother (squished to accomodate them all).

Hovering your cursor over the graph will show the hovered timestamp in the top.
Clicking and dragging will highlight a timeframe and zoom to it when released.
You can also scroll on the graph to zoom in and out. Middle-click dragging will
allow you to pan the timeframe left and right.


### Table

The table shows the actual events or spans being queried by the filter and
timeframe. By default it will show columns for the level, when it occurred
(`#timestamp` for events, `#created_at` for spans) and a default column
(`@message` for events, `#name` for spans). The timing column also includes a
toggle for controlling the sort order.

Each of the non-fixed columns have a `+` button in the header to create a new
column. The column header can be edited to change the property which uses the
same `#` and `@` syntax as the filter. You can also right-click on the header to
access various options or middle-click to remove it.

The table cells contain the value for the entity and property of the row and
column. If a value does not exist, it will show as `---`. Clicking on a row will
show or hide the details panel for that entity. You can also right-click the
cell to access various options for that value or property.


### Details Panel

This panel opens alongside the table and shows all information about the entity
selected. The top of the panel shows the inherent properties along with a
`#stack` property that can be expanded or collapsed on-click to show parent
spans.

The highlighted section shows the primary data for the entity (`@message` for
events and `#name` for spans).

The bottom shows all the attributes and their values. The far left icon will
indicate where that value came from (a missing icon means it is directly on the
entity, a span icon means it came from a parent span, and a resource icon means
it was provided by the root resource). You can hover over this icon for details
or the right-click will include a `copy * id` option when available. The
right-click menu shows many options for adding to the filter or even adding a
column to the table. An attribute value that is too long will be cut off, but
you can toggle the `-` after the attribute name to expand it.
