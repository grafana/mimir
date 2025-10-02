## Context

- This is a simple stateful webhook used for integration tests with Alertmanager.
- The point is having a webhook that can be easily deployed in a test environment and record notifications received
- On container stop, it will dump the state to a folder configurable via the `results-output-path` flag
- The file name can also be configured via the `container` flag, which is useful when running multiple instances of the webhook in parallel

## Usage

```bash
docker build -t webhook_listener .
docker run -p8080:8080 webhook_listener:latest -container=test
```
