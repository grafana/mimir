# Casing

A small Go library to switch between CamelCase, lowerCamelCase, snake_case, and kebab-case, among others. Features:

- Unicode support 👩‍💻
- Intelligent `Split` function
- Camel, lowerCamel, snake, and kebab casing built-in
- Extensible via simple composition
- Optional handling of initialisms like `ID` and `HTTP`

Alternatives

I wrote this library because the below ones don't support unicode, or have strange edge cases, or used a different implementation approach that was less flexible / extensible. They are good projects worth linking to nonetheless, and inspired me to create this library.

- [iancoleman/strcase](https://github.com/iancoleman/strcase)
- [gobeam/stringy](https://github.com/gobeam/stringy)
- [segmentio/go-camelcase](https://github.com/segmentio/go-camelcase)

Due to the different approaches I decided to create a new library rather than submitting a PR to completely rewrite their projects, potentially introducing unexpected behavior or breaking their existing users.

## Usage

The library is easy to get started with:

```go
import "github.com/danielgtaylor/casing"

input := "AnyKind of_string"

// Convert to different cases!
fmt.Println(casing.Camel(input))
fmt.Println(casing.LowerCamel(input))
fmt.Println(casing.Snake(input))
fmt.Println(casing.Kebab(input))

// Unicode works too!
fmt.Println(casing.Snake("UnsinnÜberall🎉"))
```

If you would rather convert unicode characters to ASCII approximations, check out [rainycape/unidecode](https://github.com/rainycape/unidecode) as a potential preprocessor.

### Transforms

Each casing function can also take in `TransformFunc` parameters that apply a transformation to each part of a split string. Here is a simple identity transform showing how to write such a function:

```go
// Identity returns the same part that was passed in.
func Identity(part string) string {
	return part
}
```

You may notice the transform function signature matches some existing standard library signatures, for example `strings.ToLower` and `strings.ToUpper`. These can be used directly as transform functions without needing to wrap them.

This supports any number of transformations before the casing join operation. For one example use-case, imagine generating Go code and you want to generate a public variable name that might contain an initialism that should be capitalized:

```go
// Generates `UserID` which passes go-lint
name := casing.Camel("USER_ID", strings.ToLower, casing.Initialism)
```

Transformations are applied in-order and provide a powerful way to customize output for many different use-cases.

If no transformation function is passed in, then `strings.ToLower` is used by default. You can pass in the identity function to disable this behavior.

### Split, Join & Merge

The library also exposes intelligent split and join functions along with the ability to merge some parts together before casing, which are used by the high-level casing functions above.

```go
// Returns ["Any", "Kind", "of", "STRING"]
parts := casing.Split("AnyKind of_STRING")

// Returns "Any.Kind.Of.String"
casing.Join(parts, ".", strings.ToLower, strings.Title)
```

There is also a function to merge numbers in some cases for nicer output. This is useful when joining with a separator and you want to prevent separators between some parts that are prefixed or suffixed with numbers. for example:

```go
// Splits into ["mp", "3", "player"]
parts := casing.Split("mp3 player")

// Merges into ["mp3", "player"]
merged := casing.MergeNumbers(parts)

// Returns "MP3.Player"
casing.Join(merged, ".", strings.Title, casing.Initialism)
```

Together these primitives make it easy to build your own custom casing if needed.
