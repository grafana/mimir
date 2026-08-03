# What is this?

A small code generator that produces interface methods for MQE planning
nodes from `//node:generate`-annotated structs. It exists to remove
boilerplate (and the silent drift between handwritten methods and
struct fields) for the methods every planning node has to implement on
top of `planning.Node`.

# Supported generators

| Method                                                           | Description                                                                       |
| ---------------------------------------------------------------- | --------------------------------------------------------------------------------- |
| `Child(idx int) planning.Node`                                   | Returns the child at the given index.                                             |
| `ChildCount() int`                                               | Returns the number of children.                                                   |
| `SetChildren(children []planning.Node) error`                    | Sets all children at once, validating the count and child types.                  |
| `ReplaceChild(idx int, child planning.Node) error`               | Replaces the child at the given index, validating the index and type.             |
| `ChildrenLabels() []string`                                      | Returns a human-readable label for each child, shown in plan output.              |
| `EquivalentToIgnoringHintsAndChildren(other planning.Node) bool` | Reports whether `other` is an equivalent node by comparing its fields one by one. |

# How to opt a struct in

1. Add `//node:generate` on the line directly above the struct.
2. Tag each child field:
   - `node:"child"` — single mandatory child (`planning.Node`).
   - `node:"child,nilable"` — single child that may be `nil` at
     runtime (rare; e.g. `AggregateExpression.Param`).
   - `node:"children"` — slice of children (`[]planning.Node`).
   - `node:"children,min=N"` — as `children`, but a node
     requires at least `N` children.
3. For `ChildrenLabels`, give the children labels (a single child is
   unlabeled `""` by default):
   - `node:"child,label=X"` — label this child `X` (used when the node
     has more than one child; e.g. `BinaryExpression` LHS/RHS).
   - `node:"children,labelfmt=X"` — label each child with
     `fmt.Sprintf(X, i)`, e.g. `labelfmt=node %d` yields `node 0`,
     `node 1`, ...
   - `node:"children,labelfmt=X,nocollapse"` — by default, when a node
     has exactly one child its label is collapsed to `""`.
     `nocollapse` disables that, so even a lone child is labeled via
     `labelfmt` (e.g. `RemoteExecutionGroup` always shows `node 0`).
4. `EquivalentToIgnoringHintsAndChildren` is generated for every annotated
   struct, comparing all of its fields. A struct-typed field of type `T` is
   compared using a helper named `genEqualsT` (e.g. `genEqualsVectorMatching`).
   This helper is generated once per struct type and shared by every node in
   the package that has a field of that type. To exclude a field, list it in a
   `node:"hints=A;B"` tag on the embedded `*Details` field — these are hints that
   should not affect equality (e.g. `BinaryExpression` excludes `Hints`,
   `MatrixSelector`/`VectorSelector` exclude `SkipHistogramBuckets`).

# How to run

```sh
# Regenerate node_gen.go in every annotated package.
make generate-node-methods

# Verify generated files are up to date (used by CI).
make check-node-methods
```

The generator writes a `node_gen.go` file into each package containing
annotated structs.
