# Venator Filter Syntax

The filter is composed of `property: value` predicates that an event or span
must satisfy to be shown.

Properties come in two kinds:

- *inherent* properties start with `#` and correspond to values built-in to
    the instrumentation and reporting of events and spans. The available
    inherent properties are:
    - `#level`: 
    - `#instance`: 
    - `#parent`: 
    - `#stack`: 
    - `#target`: 
    - `#file`: 

- *attribute* properties start with `@` and are user-defined structured logging
    fields that can be provided on events and spans. Nested events and spans
    inheret the attributes of their parent span(s) and root connection unless
    overridden.

Values can take a few different forms:

- if the value matches `true` and `false` it will match boolean values as well
    as literal strings with those exact characters.
- if the value can be parsed as an integer like `42` it will match integer and
    float values that equal the value as well as literal strings with those
    exact characters.
- if the value can be parsed as a float like `6.09` or `-2.44e9` it will match
    float values that equal it as well as literal strings with those exact
    characters.
- if the value starts and ends with `/` like `/[0-9a-f]{32}/` it will be parsed
    as a regex and will match string values satifying that regex.
- if the value contains a `*` it will be interpretted as a wildcard (unless
    escaped like `\*`) and will match strings where `*` can satisfy any number
    of characters.
- if the value starts and ends with `"` then it is interpretted literally
    (except `*`s still mean a wildcard) and will not try to parse other symbols

Values can also have operators applied to them:

- `!value` will match values that do __not__ satisfy that value (can have other
    operators as well)
- `<value` will match values less than that value (lexicographical comparison
    for strings; numerical comparison for integers, floats, and booleans)
- `>value` will match values greater than that value (lexicographical comparison
    for strings; numerical comparison for integers, floats, and booleans)
- `<=value` will match values less than or equal to that value (lexicographical
    comparison for strings; numerical comparison for integers, floats, and
    booleans)
- `>=value` will match values greater than or equal to that value (lexicographical
    comparison for strings; numerical comparison for integers, floats, and
    booleans)
- `(value1 AND value2 ...)` will match values only if all are satisfied
- `(value1 OR value2 ...)` will match values if any are satisfied


## FAQ


### How to filter for a value with spaces?

You can surround a value with quotes `"` to include characters that would
otherwise be misinterpretted - like spaces, `:`, `!`, and other operators:

```
@name: "John Titor"  @category: "decor:lighting"
```

Note that quotes may be automatically removed when its not warranted and may
be automatically added if a value was able to be parsed but includes special
characters out of an abundance of clarity.

This goes for attributes as well:

```
@"first name": John  @"category:name": lighting
```


### How to exclude a value?

You can use `!` to negate a filter which will work on values:

```
@name: !John
```

It is worth noting however that the results will also include events or spans
that do not have that property. To only get results that have the property
set but are *not* a particular value, you can combine it with an "exists"
filter:

```
@name: (* AND !John)
```


### How to filter for a property that exists?

A wildcard will typically only filter for values that are strings, however a
bare `*` will include any value, so it can serve as an "exists" filter:

```
@name: *
```


### How to filter for a range?

The best way is to use an `AND` group with `>`/`>=` and `<`/`<=` comparison
operators:

```
#duration: (>1s AND <10s)
```


### How to filter for value that starts or ends with something?

You can use wildcards at the end or beginning of a string value to get "starts
with" or "ends with" behavior:

```
@name: "John *"  @message: "* items were found"
```


### How to filter for value in different properties?

You can use an `(... OR ...)` grouping around property-value pairs to find
entities that may satisfy one predicate or another:

```
(@name: John OR @legacy_name: John)
```


### How to filter for a specific type of value?

Not supported currently.
