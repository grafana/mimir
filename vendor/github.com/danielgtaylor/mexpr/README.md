# MicroExpr

[![Go Reference](https://pkg.go.dev/badge/github.com/danielgtaylor/mexpr.svg)](https://pkg.go.dev/github.com/danielgtaylor/mexpr) [![Go Report Card](https://goreportcard.com/badge/github.com/danielgtaylor/mexpr)](https://goreportcard.com/report/github.com/danielgtaylor/mexpr) ![GitHub tag (latest by date)](https://img.shields.io/github/v/tag/danielgtaylor/mexpr)

A small & fast dependency-free library for parsing micro expressions.

This library was originally built for use in templating languages (e.g. for-loop variable selection, if-statement evaluation) so is minimal in what it supports by design. If you need a more full-featured expression parser, check out [antonmedv/expr](https://github.com/antonmedv/expr) instead.

Features:

- Fast, low-allocation parser and runtime
  - Many simple expressions are zero-allocation
- Type checking during parsing
- Simple
  - Easy to learn
  - Easy to read
  - No hiding complex branching logic in expressions
- Intuitive, e.g. `"id" + 1` => `"id1"`
- Useful error messages, example:
  ```
  missing right operand
    not (1- <= 5)
    ......^
  ```
- Fuzz tested to prevent crashes

## Usage

Try it out on the [Go Playground](https://play.golang.org/p/Z0UcEBgfxu_r)! You can find many [example expressions in the tests](https://github.com/danielgtaylor/mexpr/blob/main/interpreter_test.go#L18).

```go
import "github.com/danielgtaylor/mexpr"

// Convenience for lexing/parsing/running in one step:
result, err := mexpr.Eval("a > b", map[string]interface{}{
	"a": 2,
	"b": 1,
})

// Manual method with type checking and fast AST re-use. Error handling is
// omitted for brevity.
l := mexpr.NewLexer("a > b")
p := mexpr.NewParser(l)
ast, err := mexpr.Parse()
typeExamples = map[string]interface{}{
	"a": 2,
	"b": 1,
}
err := mexpr.TypeCheck(ast, typeExamples)
interpreter := mexpr.NewInterpreter(ast)
result1, err := interpreter.Run(map[string]interface{}{
	"a": 1,
	"b": 2,
})
result2, err := interpreter.Run(map[string]interfae{}{
	"a": 150,
	"b": 30,
})
```

Pretty errors use the passed-in input along with the error's offset to display an arrow of where within the expression the error occurs.

```go
inputStr := "2 * foo"
_, err := mexpr.Eval(inputStr, nil)
if err != nil {
	fmt.Println(err.Pretty(inputStr))
}
```

### Options

When running the interpreter a set of options can be passed in to change behavior. Available options:

| Option            | Default | Description                                                                                        |
| ----------------- | ------- | -------------------------------------------------------------------------------------------------- |
| `StrictMode`      | `false` | Be more strict, for example return an error when an identifier is not found rather than `nil`      |
| `UnquotedStrings` | `false` | Enable the use of unquoted strings, i.e. return a string instead of `nil` for undefined parameters |

```go
// Using the top-level eval
mexpr.Eval(expression, inputObj, StrictMode)

// Using an interpreter instance
interpreter.Run(inputObj, StrictMode)
```

## Syntax

### Literals

- **strings** double quoted e.g. `"hello"`
- **numbers** e.g. `123`, `2.5`, `1_000_000`

Internally all numbers are treated as `float64`, which means fewer conversions/casts when taking arbitrary JSON/YAML inputs.

### Accessing properties

- Use `.` between property names
- Use `[` and `]` for indexes, which can be negative

```py
foo.bar[0].value
```

### Arithmetic operators

- `+` (addition)
- `-` (subtration)
- `*` (multiplication)
- `/` (division)
- `%` (modulus)
- `^` (power)

```py
(1 + 2) * 3^2
```

Math operations between constants are precomputed when possible, so it is efficient to write meaningful operations like `size <= 4 * 1024 * 1024`. The interpreter will see this as `size <= 4194304`.

### Comparison operators

- `==` (equal)
- `!=` (not equal)
- `<` (less than)
- `>` (greater than)
- `<=` (less than or equal to)
- `>=` (greater than or equal to)

```py
100 >= 42
```

### Logical operators

- `not` (negation)
- `and`
- `or`

```py
1 < 2 and 3 < 4
```

Non-boolean values are converted to booleans. The following result in `true`:

- numbers greater than zero
- non-empty string
- array with at least one item
- map with at least one key/value pair

### String operators

- Indexing, e.g. `foo[0]`
- Slicing, e.g. `foo[1:2]` or `foo[2:]`
- `.length` pseudo-property, e.g. `foo.length`
- `.lower` pseudo-property for lowercase, e.g. `foo.lower`
- `.upper` pseudo-property for uppercase, e.g. `foo.upper`
- `+` (concatenation)
- `in` e.g. `"f" in "foo"`
- `contains` e.g. `"foo" contains "f"`
- `startsWith` e.g. `"foo" startsWith "f"`
- `endsWith` e.g. `"foo" endsWith "o"`

Indexes are zero-based. Slice indexes are optional and are _inclusive_. `foo[1:2]` returns `el` if the `foo` is `hello`. Indexes can be negative, e.g. `foo[-1]` selects the last item in the array.

Any value concatenated with a string will result in a string. For example `"id" + 1` will result in `"id1"`.

There is no distinction between strings, bytes, or runes. Everything is treated as a string.

#### Date Comparisons

String dates & times can be compared if they follow RFC 3339 / ISO 8601 with or without timezones.

- `before`, e.g. `start before "2020-01-01"`
- `after`, e.g. `created after "2020-01-01T12:00:00Z"`

### Array/slice operators

- Indexing, e.g. `foo[1]`
- Slicing, e.g. `foo[1:2]` or `foo[2:]`
- `.length` pseudo-property, e.g. `foo.length`
- `+` (concatenation)
- `in` (has item), e.g. `1 in foo`
- `contains` e.g. `foo contains 1`

Indexes are zero-based. Slice indexes are optional and are _inclusive_. `foo[1:2]` returns `[2, 3]` if the `foo` is `[1, 2, 3, 4]`. Indexes can be negative, e.g. `foo[-1]` selects the last item in the array.

#### Array/slice filtering

A `where` clause can be used to filter the items in an array. The left side of the clause is the array to be filtered, while the right side is an expression to run on each item of the array. If the right side expression evaluates to true then the item is added to the result slice. For example:

```
// Get a list of items where the item.id is bigger than 3
items where id > 3

// More complex example
items where (id > 3 and labels contains "best")
```

This also makes it possible to implement one/any/all/none logic:

```
// One
(items where id > 3).length == 1

// Any
items where id > 3
(items where id > 3).length > 0

// All
(items where id > 3).length == items.length

// None
not (items where id > 3)
(items where id > 3).length == 0
```

### Map operators

- Accessing values, e.g. `foo.bar.baz`
- `in` (has key), e.g. `"key" in foo`
- `contains` e.g. `foo contains "key"`

#### Map wildcard filtering

A `where` clause can be used as a wildcard key to filter values for all keys in a map. The left side of the clause is the map to be filtered, while the right side is an expression to run on each value of the map. If the right side expression evaluates to true then the value is added to the result slice. For example, given:

```json
{
  "operations": {
    "id1": { "method": "GET", "path": "/op1" },
    "id2": { "method": "PUT", "path": "/op2" },
    "id3": { "method": "DELETE", "path": "/op3" }
  }
}
```

You can run:

```
// Get all operations where the HTTP method is GET
operations where method == "GET"
```

And the result would be a slice of matched values:

```json
[{ "method": "GET", "path": "/op1" }]
```

## Performance

Performance compares favorably to [antonmedv/expr](https://github.com/antonmedv/expr) for both `Eval(...)` and cached program performance, which is expected given the more limited feature set. The `slow` benchmarks include lexing/parsing/interpreting while the `cached` ones are just the interpreting step. The `complex` example expression used is non-trivial: `foo.bar / (1 * 1024 * 1024) >= 1.0 and "v" in baz and baz.length > 3 and arr[2:].length == 1`.

```
goos: darwin
goarch: amd64
pkg: github.com/danielgtaylor/mexpr
cpu: Intel(R) Core(TM) i7-9750H CPU @ 2.60GHz
Benchmark/mexpr-field-slow-12           3673572       286.5 ns/op    144 B/op      6 allocs/op
Benchmark/_expr-field-slow-12            956689      1276 ns/op     1096 B/op     23 allocs/op

Benchmark/mexpr-comparison-slow-12      1000000      1020 ns/op      656 B/op     16 allocs/op
Benchmark/_expr-comparison-slow-12       383491      3069 ns/op     2224 B/op     38 allocs/op

Benchmark/mexpr-logical-slow-12         1000000      1063 ns/op      464 B/op     17 allocs/op
Benchmark/_expr-logical-slow-12          292824      4148 ns/op     2336 B/op     38 allocs/op

Benchmark/mexpr-math-slow-12            1000000      1035 ns/op      656 B/op     16 allocs/op
Benchmark/_expr-math-slow-12             399708      3004 ns/op     2184 B/op     38 allocs/op

Benchmark/mexpr-string-slow-12          1822945       655.6 ns/op    258 B/op     10 allocs/op
Benchmark/_expr-string-slow-12           428604      2508 ns/op     1640 B/op     35 allocs/op

Benchmark/mexpr-index-slow-12           2015856       592.0 ns/op    280 B/op     10 allocs/op
Benchmark/_expr-index-slow-12            517360      2301 ns/op     1872 B/op     30 allocs/op

Benchmark/mexpr-complex-slow-12          244039      5078 ns/op     2232 B/op     64 allocs/op
Benchmark/_expr-complex-slow-12           69387     16825 ns/op    14378 B/op    107 allocs/op

Benchmark/mexpr-field-cached-12       100000000        11.37 ns/op     0 B/op      0 allocs/op
Benchmark/_expr-field-cached-12         7761153       146.5 ns/op     48 B/op      2 allocs/op

Benchmark/mexpr-comparison-cached-12   38098502        30.93 ns/op     0 B/op      0 allocs/op
Benchmark/_expr-comparison-cached-12    4563463       251.0 ns/op     64 B/op      3 allocs/op

Benchmark/mexpr-logical-cached-12      37563720        31.35 ns/op     0 B/op      0 allocs/op
Benchmark/_expr-logical-cached-12      11000991       105.9 ns/op     32 B/op      1 allocs/op

Benchmark/mexpr-math-cached-12         24463279        47.41 ns/op     8 B/op      1 allocs/op
Benchmark/_expr-math-cached-12          4531693       268.0 ns/op     72 B/op      4 allocs/op

Benchmark/mexpr-string-cached-12       43399368        26.83 ns/op     0 B/op      0 allocs/op
Benchmark/_expr-string-cached-12        7302940       162.0 ns/op     48 B/op      2 allocs/op

Benchmark/mexpr-index-cached-12        45289230        25.67 ns/op     0 B/op      0 allocs/op
Benchmark/_expr-index-cached-12         6057562       180.0 ns/op     48 B/op      2 allocs/op

Benchmark/mexpr-complex-cached-12       4271955       278.7 ns/op     40 B/op      3 allocs/op
Benchmark/_expr-complex-cached-12       1456266       818.7 ns/op    208 B/op      9 allocs/op

```

On average mexpr is around 3-10x faster for both full parsing and cached performance.

## References

These were a big help in understanding how Pratt parsers work:

- https://dev.to/jrop/pratt-parsing
- https://journal.stuffwithstuff.com/2011/03/19/pratt-parsers-expression-parsing-made-easy/
- https://matklad.github.io/2020/04/13/simple-but-powerful-pratt-parsing.html
- https://www.oilshell.org/blog/2017/03/31.html
