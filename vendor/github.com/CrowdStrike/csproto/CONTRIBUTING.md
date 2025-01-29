# Contributing

_Welcome!_ We're excited you want to take part in the CrowdStrike community!

Please review this document for details regarding getting started with your first contribution, tools
you'll need to install as a developer, and our development and Pull Request process. If you have any
questions, please let us know by posting your question in the [discussion board](https://github.com/CrowdStrike/csproto/discussions).

## Table of Contents

- [Code of Conduct](#code-of-conduct)
- [How you can contribute](#how-you-can-contribute)
- [Working with the Code](#working-with-the-code)
  - [Commit Messages](#commit-message-formatting-and-hygiene)
  - [Benchmarks](#benchmarks)
- [Pull Requests](#pull-requests)
  - [Code Coverage](#code-coverage)

## Code of Conduct

Please refer to CrowdStrike's general [Code of Conduct](https://opensource.crowdstrike.com/code-of-conduct/)
and [contribution guidelines](https://opensource.crowdstrike.com/contributing/).

## How you can contribute

- See something? Say something! Submit a [bug report](https://github.com/CrowdStrike/csproto/issues/new?assignees=&labels=bug%2Ctriage&template=bug.md&title=) to let the community know what you've experienced or found.
  - Please propose new features on the discussion board first.
- Join the [discussion board](https://github.com/CrowdStrike/csproto/discussions) where you can:
  - [Interact](https://github.com/CrowdStrike/csproto/discussions/categories/general) with other members of the community
  - [Start a discussion](https://github.com/CrowdStrike/csproto/discussions/categories/ideas) or submit a [feature request](https://github.com/CrowdStrike/csproto/issues/new?assignees=&labels=enhancement%2Ctriage&template=feature_request.md&title=)
  - Provide [feedback](https://github.com/CrowdStrike/csproto/discussions/categories/q-a)
  - [Show others](https://github.com/CrowdStrike/csproto/discussions/categories/show-and-tell) how you are using `csproto` today
- Submit a [Pull Request](#pull-requests)

## Working with the Code

To simplify and standardize things, we use GNU Make to execute local development tasks like building
and linting the code, running tests and benchmarks, etc.

The table below shows the most common targets:

| Target       | Description                                                                                                                                                         |
| ------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `test`       | Runs all tests for the root module and the nested `examples` module                                                                                                 |
| `bench`      | Runs the benchmarks in the nested `examples` module                                                                                                                 |
| `generate`   | Generates the Go code in the nested `examples` module by invoking the appropriate `protoc` plug-ins                                                                 |
| `regenerate` | Regenerates the Go code in the nested `examples` module by removing the existing generated code and build artifacts then invoking the appropriate `protoc` plug-ins |

### Prerequisites

We have tried to minimize the "extra" work you'll have to do, but there are a few prerequisites:

- GNU Make
- Go 1.17 or higher
- The `protoc` Protobuf compiler

As the exact details of how to install these tools varies from system to system, we have chosen to
leave that part to you.

### Other Build-Time Tools

All other build-time tooling is installed into a project-level `bin/` folder because we don't want to impact
any other projects by overwriting binaries in `$GOPATH/bin`. To accomodate that, `Makefile` prepends
the local `bin/` folder to `$PATH` so that any Make targets will find the build-time tools there. If
you choose to not use our Make targets, please ensure that you adjust your environment accordingly.

Another twist: we need to generate code using both the "old" (V1 Protobuf API) and "new" (V2 Protobuf API)
versions of `protoc-gen-go`, so we explicitly rename the resulting binaries in the project-local `bin/`
folder to `protoc-gen-go-v1` and `protoc-gen-go-v2`, respectively, and use the `--plugin` option to
`protoc` to point the `--go_out=...` parameter to the "correct" binary when generating code.

### Commit Message Formatting and Hygiene

We use [_Conventional Commits_](https://www.conventionalcommits.org/en/v1.0.0/) formatting for commit
messages, which we feel leads to a much more informative change history. Please familiarize yourself
with that specification and format your commit messages accordingly.

Another aspect of achieving a clean, informative commit history is to avoid "noise" in commits.
Ideally, condense your changes to a single commit with a well-written _Conventional Commits_ message
before submitting a PR. In the rare case that a single PR is introducing more than one change, each
change should be a single commit with its own well-written message.

### Benchmarks

The primary purpose of this library is to provide a runtime-independent Protobuf API, but allowing for
optimal performance is a close second. In fact, we were able to beat both Google's and Gogo's runtime
performance by as much as 40% by using our `protoc-gen-fastmarshal` code generator.

With that in mind, any changes that negatively impact these benchmarks will likely be rejected. At a
minimum, changes that decrease performance will receive pushback and will need to be justified explicitly.

## Pull Requests

All code changes should be submitted via a Pull Request targeting the `main` branch. We are not assuming
that every merged PR creates a release, so we will not be automatically creating new SemVer tags as
a side effect of merging your Pull Request. Instead, we will manually tag new releases when required.

### Code Coverage

While we feel like achieving and maintaining 100% code coverage is often an untenable goal with
diminishing returns, any changes that reduce code coverage will receive pushback. We don't want
people to spend days trying to bump coverage from 97% to 98%, often at the expense of code clarity,
but that doesn't mean that we're okay with making things worse.
