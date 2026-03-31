# Failsafe-go

[![Build Status](https://img.shields.io/github/actions/workflow/status/failsafe-go/failsafe-go/test.yml)](https://github.com/failsafe-go/failsafe-go/actions/workflows/test.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/failsafe-go/failsafe-go)](https://goreportcard.com/report/github.com/failsafe-go/failsafe-go)
[![codecov](https://codecov.io/gh/failsafe-go/failsafe-go/graph/badge.svg?token=UC2BU7NTJ7)](https://codecov.io/gh/failsafe-go/failsafe-go)
[![License](http://img.shields.io/:license-mit-brightgreen.svg)](https://opensource.org/licenses/MIT)
[![Godoc](https://pkg.go.dev/badge/github.com/failsafe-go/failsafe-go)](https://pkg.go.dev/github.com/failsafe-go/failsafe-go)

Failsafe-go is a library for building resilient, fault tolerant Go applications. It works by wrapping functions with one or more resilience policies, which can be combined and composed as needed. Policies include:

- Failure handling: [Retry](https://failsafe-go.dev/retry), [Fallback](https://failsafe-go.dev/fallback)
- Load limiting: [Circuit Breaker](https://failsafe-go.dev/circuit-breaker), [Adaptive Limiter](https://failsafe-go.dev/adaptive-limiter), [Adaptive Throttler](https://failsafe-go.dev/adaptive-throttler), [Budget](https://failsafe-go.dev/budget), [Bulkhead](https://failsafe-go.dev/bulkhead), [Rate Limiter](https://failsafe-go.dev/rate-limiter), [Cache](https://failsafe-go.dev/cache)
- Time limiting: [Timeout](https://failsafe-go.dev/timeout), [Hedge](https://failsafe-go.dev/hedge)

## Usage

Visit [failsafe-go.dev](https://failsafe-go.dev) for usage info, docs, and additional resources.

## Contributing

Check out the [contributing guidelines](https://github.com/failsafe-go/failsafe-go/blob/master/CONTRIBUTING.md).

## License

&copy; 2023-present Jonathan Halterman and contributors. Released under the [MIT license](https://github.com/failsafe-go/failsafe-go/blob/master/LICENSE).
