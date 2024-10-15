# Failsafe-go

[![Build Status](https://img.shields.io/github/actions/workflow/status/failsafe-go/failsafe-go/test.yml)](https://github.com/failsafe-go/failsafe-go/actions/workflows/test.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/failsafe-go/failsafe-go)](https://goreportcard.com/report/github.com/failsafe-go/failsafe-go)
[![codecov](https://codecov.io/gh/failsafe-go/failsafe-go/graph/badge.svg?token=UC2BU7NTJ7)](https://codecov.io/gh/failsafe-go/failsafe-go)
[![License](http://img.shields.io/:license-mit-brightgreen.svg)](https://opensource.org/licenses/MIT)
[![Slack](https://img.shields.io/badge/slack-failsafe-brightgreen.svg?logo=slack)](https://failsafe-go.slack.com)
[![Godoc](https://pkg.go.dev/badge/github.com/failsafe-go/failsafe-go)](https://pkg.go.dev/github.com/failsafe-go/failsafe-go)

Failsafe-go is a library for building resilient, fault tolerant Go applications. It works by wrapping functions with one or more resilience policies, which can be combined and composed as needed.

Policies include [Retry](https://failsafe-go.dev/retry), [CircuitBreaker](https://failsafe-go.dev/circuit-breaker), [RateLimiter](https://failsafe-go.dev/rate-limiter), [Timeout](https://failsafe-go.dev/timeout), [Fallback](https://failsafe-go.dev/fallback), [Hedge](https://failsafe-go.dev/hedge/), [Bulkhead](https://failsafe-go.dev/bulkhead), and [Cache](https://failsafe-go.dev/cache).

## Usage

Visit [failsafe-go.dev](https://failsafe-go.dev) for usage info, docs, and additional resources.

## Contributing

Check out the [contributing guidelines](https://github.com/failsafe-go/failsafe-go/blob/master/CONTRIBUTING.md).

## License

&copy; 2023-present Jonathan Halterman and contributors. Released under the [MIT license](https://github.com/failsafe-go/failsafe-go/blob/master/LICENSE).
