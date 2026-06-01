# What is this?

A small code generator that produces interface methods for MQE planning
nodes from `//node:generate`-annotated structs. It exists to remove
boilerplate (and the silent drift between handwritten methods and
struct fields) for the methods every planning node has to implement on
top of `planning.Node`.

# Supported generators

| Method                         | Description                           |
| ------------------------------ | ------------------------------------- |
| `Child(idx int) planning.Node` | Returns the child at the given index. |
| `ChildCount() int`             | Returns the number of children.       |

# How to opt a struct in

1. Add `//node:generate` on the line directly above the struct.
2. Tag each child field:
   - `node:"child"` — single mandatory child (`planning.Node`).
   - `node:"child,nilable"` — single child that may be `nil` at
     runtime (rare; e.g. `AggregateExpression.Param`).
   - `node:"children"` — slice of children (`[]planning.Node`).

# How to run

```sh
# Regenerate node_gen.go in every annotated package.
make generate-node-methods

# Verify generated files are up to date (used by CI).
make check-node-methods
```

The generator writes a `node_gen.go` file into each package containing
annotated structs.
