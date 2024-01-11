# Structured Data Shorthand Syntax

[![Docs](https://godoc.org/github.com/danielgtaylor/shorthand?status.svg)](https://pkg.go.dev/github.com/danielgtaylor/shorthand?tab=doc) [![Go Report Card](https://goreportcard.com/badge/github.com/danielgtaylor/shorthand)](https://goreportcard.com/report/github.com/danielgtaylor/shorthand) [![CI](https://github.com/danielgtaylor/shorthand/workflows/CI/badge.svg?branch=main)](https://github.com/danielgtaylor/shorthand/actions?query=workflow%3ACI+branch%3Amain++) [![codecov](https://codecov.io/gh/danielgtaylor/shorthand/branch/main/graph/badge.svg)](https://codecov.io/gh/danielgtaylor/shorthand)

Shorthand is a superset and friendlier variant of JSON designed with several use-cases in mind:

| Use Case             | Example                                          |
| -------------------- | ------------------------------------------------ |
| CLI arguments/input  | `my-cli post 'foo.bar[0]{baz: 1, hello: world}'` |
| Patch operations     | `name: undefined, item.tags[]: appended`         |
| Query language       | `items[created before 2022-01-01].{id, tags}`    |
| Configuration format | `{json.save.autoFormat: true}`                   |

The shorthand syntax supports the following features, described in more detail with examples below:

- Superset of JSON (valid JSON is valid shorthand)
  - Optional commas, quotes, and sometimes colons
  - Support for comments & trailing commas
- Automatic type coercion
  - Support for bytes, datetimes, and maps with non-string keys
- Nested object & array creation
- Loading values from files
- Editing existing data
  - Appending & inserting to arrays
  - Unsetting properties
  - Moving properties & items
- Querying, array filtering, and field selection

The following are all completely valid shorthand and result in the same output:

```
foo.bar[]{baz: 1, hello: world}
```

```
{
  // This is a comment
  foo.bar[]{
    baz: 1
    hello: world
  }
}
```

```json
{
  "foo": {
    "bar": [
      {
        "baz": 1,
        "hello": "world"
      }
    ]
  }
}
```

This library has excellent test coverage to ensure correctness and is additionally fuzz tested to prevent panics.

## Alternatives & Inspiration

The CLI shorthand syntax is not the only one you can use to generate data for CLI commands. Here are some alternatives:

- [jo](https://github.com/jpmens/jo)
- [jarg](https://github.com/jdp/jarg)

For example, the shorthand example given above could be rewritten as:

```sh
$ jo -p foo=$(jo -p bar=$(jo -a $(jo -p baz=1 hello=world)))
```

The shorthand syntax implementation described herein uses those and the following for inspiration:

- [YAML](http://yaml.org/)
- [W3C HTML JSON Forms](https://www.w3.org/TR/html-json-forms/)
- [jq](https://stedolan.github.io/jq/)
- [JMESPath](http://jmespath.org/)

It seems reasonable to ask, why create a new syntax?

1. Built-in. No extra executables required. Your tool ships ready-to-go.
2. No need to use sub-shells to build complex structured data.
3. Syntax is closer to YAML & JSON and mimics how you do queries using tools like `jq` and `jmespath`.
4. It's _optional_, so you can use your favorite tool/language instead, while at the same time it provides a minimum feature set everyone will have in common.

## Features in Depth

You can use the included `j` executable to try out the shorthand format examples below. Examples are shown in JSON, but the shorthand parses into structured data that can be marshalled as other formats, like YAML or TOML if you prefer.

```sh
go get -u github.com/danielgtaylor/shorthand/cmd/j
```

Also feel free to use this tool to generate structured data for input to other commands.

Here is a diagram overview of the language syntax, which is similar to [JSON's syntax](https://www.json.org/json-en.html) but adds a few things:

<!--
https://tabatkins.github.io/railroad-diagrams/generator.html

Diagram(
    Choice(0,
      Sequence('//', NonTerminal('comment')),
      Sequence(
        '{',
        OneOrMore(
          Sequence(
            OneOrMore(Sequence(NonTerminal('string'), Optional(Sequence('[', Optional('^'), ZeroOrMore('0-9'), ']'))), '.'),
            Choice(0,
              Sequence(':', NonTerminal('value')),
              Sequence('^', NonTerminal('query')),
              NonTerminal('object'),
            ),
          ),
          Choice(0, ',', '\\n'),
        ),
        '}'
      ),
      Sequence(
        '[',
        OneOrMore(NonTerminal('value'), Choice(0, ',', '\\n')),
        ']'
      ),
      'undefined',
      'null',
      'true',
      'false',
      NonTerminal('integer'),
      NonTerminal('float'),
      Stack(
        Sequence(
          NonTerminal('YYYY'),
          '-',
          NonTerminal('MM'),
          '-',
          NonTerminal('DD'),
        ),
          Sequence(
          'T',
          NonTerminal('hh'),
          ':',
          NonTerminal('mm'),
          ':',
          NonTerminal('ss'),
          NonTerminal('zone')
        ),
      ),
      Sequence('%', NonTerminal('base64')),
      Sequence('@', NonTerminal('filename')),
      NonTerminal('string'),
    ),
)
-->

![shorthand-syntax](https://user-images.githubusercontent.com/106826/198850895-a1a8481a-2c63-484c-9bf2-ce472effa8c3.svg)

Note:

- `string` can be quoted (with `"`) or unquoted.
- The `query` syntax in the diagram above is described below in the [Querying](#querying) section.

### Keys & Values

At its most basic, a structure is built out of key & value pairs. They are separated by commas:

```sh
$ j hello: world, question: how are you?
{
  "hello": "world",
  "question": "how are you?"
}
```

### Types

Shorthand supports the standard JSON types, but adds some of its own as well to better support binary formats and its query features.

| Type      | Description                                                      |
| --------- | ---------------------------------------------------------------- |
| `null`    | JSON `null`                                                      |
| `boolean` | Either `true` or `false`                                         |
| `number`  | JSON number, e.g. `1`, `2.5`, or `1.4e5`                         |
| `string`  | Quoted or unquoted strings, e.g. `hello` or `"hello"`            |
| `bytes`   | `%`-prefixed, unquoted, base64-encoded binary data, e.g. `%wg==` |
| `time`    | Date/time in ISO8601, e.g. `2022-01-01T12:00:00Z`                |
| `array`   | JSON array, e.g. `[1, 2, 3]`                                     |
| `object`  | JSON object, e.g. `{"hello": "world"}`                           |

### Type Coercion

Well-known values like `null`, `true`, and `false` get converted to their respective types automatically. Numbers, bytes, and times also get converted. Similar to YAML, anything that doesn't fit one of those is treated as a string. This automatic coercion can be disabled by just wrapping your value in quotes.

```sh
# With coercion
$ j empty: null, bool: true, num: 1.5, string: hello
{
  "bool": true,
  "empty": null,
  "num": 1.5,
  "string": "hello"
}

# As strings
$ j empty: "null", bool: "true", num: "1.5", string: "hello"
{
  "bool": "true",
  "empty": "null",
  "num": "1.5",
  "string": "hello"
}

# Passing the empty string
$ j blank1: , blank2: ""
{
  "blank1": "",
  "blank2": ""
}
```

### Objects

Nested objects use a `.` separator when specifying the key.

```sh
$ j foo.bar.baz: 1
{
  "foo": {
    "bar": {
      "baz": 1
    }
  }
}
```

Properties of nested objects can be grouped by placing them inside `{` and `}`. The `:` becomes optional for nested objects, so `foo.bar: {...}` is equivalent to `foo.bar{...}`.

```sh
$ j foo.bar{id: 1, count.clicks: 5}
{
  "foo": {
    "bar": {
      "count": {
        "clicks": 5
      },
      "id": 1
    }
  }
}
```

### Arrays

Arrays are surrounded by square brackets like in JSON:

```sh
# Simple array
$ j [1, 2, 3]
[
  1,
  2,
  3
]
```

Array indexes use square brackets `[` and `]` to specify the zero-based index to set an item. If the index is out of bounds then `null` values are added as necessary to fill the array. Use an empty index `[]` to append to the an existing array. If the item is not an array, then a new one will be created.

```sh
# Nested arrays
$ j [0][2][0]: 1
[
  [
    null,
    null,
    [
      1
    ]
  ]
]

# Appending arrays
$ j a[]: 1, a[]: 2, a[]: 3
{
  "a": [
    1,
    2,
    3
  ]
}
```

### Loading from Files

Sometimes a field makes more sense to load from a file than to be specified on the commandline. The `@` preprocessor lets you load structured data, text, and bytes depending on the file extension and whether all bytes are valid UTF-8:

```sh
# Load a file's value as a parameter
$ j foo: @hello.txt
{
  "foo": "hello, world"
}

# Load structured data
$ j foo: @hello.json
{
  "foo": {
    "hello": "world"
  }
}
```

Remember, it's possible to disable this behavior with quotes:

```sh
$ j 'twitter: "@user"'
{
  "twitter": "@user"
}
```

### Patch (Partial Update)

Partial updates are supported on existing data, which can be used to implement HTTP `PATCH`, templating, and other similar features. The suggested content type for HTTP `PATCH` is `application/shorthand-patch`. This feature combines the best of both:

- [JSON Merge Patch](https://datatracker.ietf.org/doc/html/rfc7386)
- [JSON Patch](https://www.rfc-editor.org/rfc/rfc6902)

Partial updates support:

- Appending arrays via `[]`
- Inserting before via `[^index]`
- Removing fields or array items via `undefined`
- Moving/swapping fields or array items via `^`
  - The right hand side is a path to the value to swap. See Querying below for the path syntax.

Note: When sending shorthand patches file loading via `@` should be disabled as the files will not exist on the server.

Some examples:

```sh
# First, let's create some data we'll modify later
$ j id: 1, tags: [a, b, c] >data.json

# Now let's append to the tags array
$ j <data.json 'tags[]: d'
{
  "id": 1,
  "tags": [
    "a",
    "b",
    "c",
    "d"
  ]
}

# Array item insertion (prepend the array)
$ j <data.json 'tags[^0]: z'
{
  "id": 1,
  "tags": [
    "z",
    "a",
    "b",
    "c"
  ]
}

# Remove stuff
$ j <data.json 'id: undefined, tags[1]: undefined'
{
  "tags": [
    "a",
    "c"
  ]
}

# Rename the ID property, and swap the first/last array items
$ j <data.json 'id ^ name, tags[0] ^ tags[-1]'
{
  "name": 1,
  "tags": [
    "c",
    "b",
    "a"
  ]
}
```

### Querying

A data query language is included, which allows you to query, filter, and select fields to return. This functionality is used by the patch move operations described above and is similar to tools like:

- [jq](https://stedolan.github.io/jq/)
- [JMESPath](http://jmespath.org/)
- [JSON Path](https://www.ietf.org/archive/id/draft-ietf-jsonpath-base-06.html)

The query language supports:

- Paths for objects & arrays `foo.items.name`
- Wildcards for unknown props `foo.*.name`
- Array indexing & slicing `foo.items[1:2].name`
  - Including negative indexes `foo.items[-1].name`
- Array filtering via [mexpr](https://github.com/danielgtaylor/mexpr) `foo.items[name.lower startsWith d]`
- Object property selection `foo.{created, names: items.name}`
- Recursive search `foo..name`
- Stopping processing with a pipe `|`
- Flattening nested arrays `[]`

The query syntax is recursive and looks like this:

<!--
Diagram(
  Stack(
    OneOrMore(Sequence(
      Choice(1,
        Skip(),
        NonTerminal('string'),
        '*',
      ),
      Optional(
        Sequence(
          '[',
          Choice(1,
            Skip(),
            NonTerminal('number'),
            NonTerminal('slice'),
            NonTerminal('filter')
          ),
          ']',
        )
      ),
      Optional('|'),
    ), Choice(0, '.', '..')),
    Optional(
      Sequence(
        '.',
        '{',
        OneOrMore(
          Sequence(
            NonTerminal('string'),
            Optional(
              Sequence(':', NonTerminal('query')),
              'skip'
            ),
          ),
          ','
        ),
        '}',
      ), 'skip',
    ),
  )
)
-->

![shorthand-query-syntax](https://user-images.githubusercontent.com/106826/198693468-fadf8d48-8223-4dd9-a2cb-a1651e342fc5.svg)

The `filter` syntax is described in the documentation for [mexpr](https://github.com/danielgtaylor/mexpr).

Examples:

```sh
# First, let's make a complex file to query
$ j 'users: [{id: 1, age: 5, friends: [a, b]}, {id: 2, age: 6, friends: [b, c]}, {id: 3, age: 5, friends: [c, d]}]' >data.json

# Query for each user's ID
$ j <data.json -q 'users.id'
[
  1,
  2,
  3
]

# Get the users who are friends with `b`
$ j <data.json -q 'users[friends contains b].id'
[
  1,
  2
]

# Get the ID & age of users who are friends with `b`
$ j <data.json -q 'users[friends contains b].{id, age}'
[
  {
    "age": null,
    "id": 1
  },
  {
    "age": null,
    "id": 2
  }
]
```

## Library Usage

Aside from `Marshal` and `Unmarshal` functions, the `GetInput` function provides an all-in-one quick and simple way to get input from both stdin and passed arguments for CLI applications:

```go
package main

import (
  "fmt"
  "github.com/danielgtaylor/shorthand/v2"
)

func main() {
  result, err := shorthand.GetInput(os.Args[1:])
  if err != nil {
    panic(err)
  }

  fmt.Println(result)
}
```

It's also possible to get the shorthand representation of an input, for example:

```go
example := map[string]interface{}{
  "hello": "world",
  "labels": []interface{}{
    "one",
    "two",
  },
}

// Prints "hello: world, labels: [one, two]"
fmt.Println(shorthand.MarshalCLI(example))
```

## Benchmarks

Shorthand v2 has been completely rewritten from the ground up and is over 20 times faster than v1, putting it at a similar speed/efficiency as the standard library's `encoding/json` package and faster than the popular YAML package while supporting some compelling additional features:

```sh
# Comparing new (V2) vs. old (V1)
BenchmarkShorthandV2-12     309817    2482 ns/op    1888 B/op    54 allocs/op
BenchmarkShorthandV1-12      14670   83901 ns/op   36436 B/op   745 allocs/op

# Comparing JSON & YAML to Shorthand
BenchmarkMinJSON-10         825459    1446 ns/op    1808 B/op    31 allocs/op
BenchmarkFormattedJSON-10   707174    1658 ns/op    1712 B/op    30 allocs/op

BenchmarkYAML-10            107493   11053 ns/op   12100 B/op   140 allocs/op

BenchmarkShorthand-10       477285    2389 ns/op    1888 B/op    54 allocs/op
BenchmarkPretty-10          403887    2848 ns/op    1888 B/op    54 allocs/op
BenchmarkParse-10          1277103     938 ns/op     160 B/op    12 allocs/op
BenchmarkApply-10           811148    1421 ns/op    1733 B/op    42 allocs/op

# Comparing Shorthand get path to JMESPath
BenchmarkGetJMESPathSimple-10  414164   2790 ns/op   5799 B/op   74 allocs/op
BenchmarkGetPathSimple-10     4332314    276 ns/op    224 B/op    5 allocs/op

BenchmarkGetJMESPath-10        224778   5289 ns/op   9374 B/op   119 allocs/op
BenchmarkGetPath-10            793628   1437 ns/op   1192 B/op    27 allocs/op

BenchmarkGetJMESPathFlat-10   1743403  688.6 ns/op    632 B/op    14 allocs/op
BenchmarkGetPathFlat-10       1964098  610.3 ns/op    560 B/op    12 allocs/op
```

## Design & Implementation

The shorthand syntax is implemented as a custom parser split into two pieces: `parse.go` to parse a shorthand input into a set of operations and `apply.go` to provide the mechanism for applying those operations on an existing input (or `nil`). Every operation will either set, delete, or swap some value or path. For example:

```
# Input
foo.bar{id: 1, tags: [{value: a}, {value: b}]}

# Parsed
[
  [OpSet, "foo.bar.id", 1],
  [OpSet, "foo.bar.tags[0].value", "a"],
  [OpSet, "foo.bar.tags[1].value", "b"]
]

# Existing
{"foo": {"baz": 2}}

# Applied Output JSON
{
  "foo": {
    "bar": {
      "id": 1,
      "tags: [
        {"value": "a"},
        {"value": "b"}
      ]
    },
    "baz": 2
  }
}
```

This simplifies the code to apply changes, as it can process each operation independently.

The file `get.go` provides an implementation of query parsing. It also utilizes [danielgtaylor/mexpr](https://github.com/danielgtaylor/mexpr), a top-down operator precedence (Pratt) parser for simple filter expressions.

No special steps are necessary to test local changes to the grammar. You can just run the included `j` utility to test:

```sh
$ go run ./cmd/j your: new feature here
```
