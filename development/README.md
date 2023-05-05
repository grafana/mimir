# Mimir Development

Mimir offers a few docker-compose based development environments for different [deployment modes][deployment-modes], including microservices mode, monolothic mode, and read-write mode.

## Running

Choose one of the sub-folders for whichever Mimir deployment mode you want to run. Then, to build Mimir from source and run it via docker-compose, run:

```bash
./compose-up.sh
```

This should give you a running Mimir system with Grafana available at [htttp://localhost:3000](http://localhost:3000), and other Mimir service UIs available via different ports (check `docker ps` for exact ports).

The Minio console is available in most dev environments at [http://localhost:9001](http://localhost:9001), with the credentials defined in [mimir.yaml][minio-creds].

## Configuring Mimir

The Mimir configuration is available in each environment in the `/config` directory, along with configurations for other apps that run in the environment. Some deployment related configurations are also available at the top of each environment's `docker-compose.jsonnet` file.

## Debugging

The [mimir-microservices-mode](/mimir-microservices-mode) environment supports debugging running components. To enable debug, set `debug: true` at the top of the [docker-compose.jsonnet](/mimir-microservices-mode/docker-compose.jsonnet). When Mimir is running, debug ports are at the service's API port + 10000. Run configurations for Goland are available in the [mimir-microservices-mode/goland](/mimir-microservices-mode/golang) folder, which will connect to each serve on their debug port.


[deployment-modes]: https://grafana.com/docs/mimir/latest/operators-guide/architecture/deployment-modes/
[minio-creds]: https://github.com/grafana/mimir/blob/2917f0865603870d402b579593a1f122b9407f02/development/mimir-microservices-mode/config/mimir.yaml#L63-L64