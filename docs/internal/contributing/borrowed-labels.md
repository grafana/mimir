# Borrowed label strings

Incoming write requests can decode label strings directly into pooled request
buffers. This avoids copying every label name and value. A reference is valid
only while the request owns that buffer, and the buffer can later serve a
different tenant. Ordinary string assignment does not copy the underlying bytes.

`mimirpb.BorrowedString` exposes read operations without exposing an ordinary
string. `LabelAdapter.Name` and `LabelAdapter.Value` use this type. Construct a
view with `mimirpb.BorrowString(value)` and copy into independently owned storage
with `value.Clone()` when the value must survive the request.

```go
if label.Name.EqualString("__name__") {
    // This comparison does not allocate.
}

// errorDetails survives processing of the request, so it must own its storage.
errorDetails.MetricName = label.Value.Clone()
```

Use operations such as `Len`, `EqualString`, `Compare`, and `AppendTo` rather than
cloning just to inspect a value. `AppendTo` copies bytes into caller-controlled
storage; the resulting storage follows the caller's normal ownership rules.
`BorrowString` does not copy and does not extend the lifetime of its argument.
Borrowed substrings have the same lifetime as their source.

## Guarantees and limits

The private representation prevents direct assignment or conversion of a
`BorrowedString` to an ordinary string in safe Go. It deliberately has no
`String()` accessor. This makes ownership copying explicit at ordinary
string-taking APIs, including retained error details, persistent maps, and
asynchronous work.

This is not a borrow checker. A borrowed value, label, or containing slice can
still be shallow-copied, retained, captured in a closure, or used as a map key.
None of those operations extends the source request's lifetime. Do not retain
borrowed values in persistent structures or use them after request cleanup.
Formatting and reflection are not ownership barriers either: any operation that
may execute asynchronously must receive owned values, not borrowed wrappers.

Returning slice/struct storage to pools remains a separate lifecycle obligation.
Even an independently owned string does not make a reused label slice safe.

## Prometheus interoperability

Prometheus label APIs accept ordinary strings. The following conversions form an
explicitly unsafe interoperability boundary:

- `UnsafeFromLabelAdaptersToLabels`
- `UnsafeFromLabelAdaptersOverwriteLabels`
- `UnsafeFromLabelAdaptersToBuilder`
- `UnsafeFromExemplarProtosToExemplars`

Conservatively treat their output as borrowed across every label build variant.
Some variants may copy, but callers must not rely on that to extend lifetime.
Keep their use synchronous within the source request's ownership window. Prefer
`FromLabelAdaptersToLabelsWithCopy` for retained labels. Layout-dependent casts
are confined to `mimirpb` and tested; representation compatibility does not prove
lifetime safety.

## In-place label modification

`UnsafeHashLabelValueInto` is deliberately unsafe. Its destination must be a view
of writable, exclusively owned request-buffer storage. It must not be used on an
ordinary immutable string, even one wrapped with `BorrowString`. Source and
destination may overlap because hashing finishes before bytes are overwritten.
Do not expose writable byte slices or introduce generic callbacks that can retain
the underlying string. Keep operations with writable-buffer contracts narrow and
reviewed.

## Verification when changing this API

Check protobuf and JSON compatibility, Remote Write 1 and 2, exemplar labels,
retained error strings after buffer reuse, and both default and `slicelabels`
builds. Preserve the generator's expected patch when generated protobuf code
changes. Run the gRPC buffer-holder linter as well as package tests: it recognizes
borrowed strings as references to request buffers.

See [local verification results](borrowed-labels-verification.md) for the initial
migration's test coverage, measured costs, and remaining verification limits.

Check allocations and throughput for decode, hashing, validation and label
conversion before claiming a change is performance-neutral. Inlining and escape
analysis matter: a small read operation can be free after inlining, but an
interface or hidden clone on a hot path need not be.
