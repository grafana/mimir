local envs = import 'envs.libsonnet';
local jaeger = import 'jaeger.libsonnet';

local all_caches = ['-blocks-storage.bucket-store.index-cache', '-blocks-storage.bucket-store.chunks-cache', '-blocks-storage.bucket-store.metadata-cache', '-query-frontend.results-cache', '-ruler-storage.cache'];
local all_rings = ['-ingester.ring', '-distributor.ring', '-compactor.ring', '-store-gateway.sharding-ring', '-ruler.ring', '-alertmanager.sharding-ring'];

{
  // This function builds docker-compose declaration for Mimir service.
  // Default grpcPort is (httpPort + 1000), and default debug port is (httpPort + 10000)
  mimirService(config, serviceOptions):: {
    local defaultOptions = {
      local s = self,
      name: error 'missing name',
      target: error 'missing target',
      jaegerApp: self.target,
      httpPort: error 'missing httpPort',
      grpcPort: self.httpPort + 1000,
      debugPort: self.httpPort + 10000,
      // Extra arguments passed to Mimir command line.
      extraArguments: '',
      dependsOn: ['minio'] + (
        if config.ring == 'consul' || config.ring == 'multi'
        then ['consul']
        else if s.target != 'distributor'
        then ['distributor-1']
        else []
      ),
      env: jaeger.jaegerEnv(s.jaegerApp),
      extraVolumes: [],
      memberlistNodeName: self.jaegerApp,
      memberlistBindPort: self.httpPort + 2000,
    },

    local options = defaultOptions + serviceOptions,

    build: {
      context: '.',
      dockerfile: 'dev.dockerfile',
    },
    image: 'mimir',
    command: [
      'sh',
      '-c',
      std.join(' ', [
        // some of the following expressions use "... else null", which std.join seem to ignore.
        (if config.sleep_seconds > 0 then 'sleep %d &&' % [config.sleep_seconds] else null),
        (if config.debug then 'exec ./dlv exec ./mimir --listen=:%(debugPort)d --headless=true --api-version=2 --accept-multiclient --continue -- ' % options else 'exec ./mimir'),
        ('-config.file=./config/mimir.yaml -target=%(target)s -server.http-listen-port=%(httpPort)d -server.grpc-listen-port=%(grpcPort)d -activity-tracker.filepath=/activity/%(target)s-%(httpPort)d %(extraArguments)s' % options),
        (if config.ring == 'memberlist' || config.ring == 'multi' then '-memberlist.nodename=%(memberlistNodeName)s -memberlist.bind-port=%(memberlistBindPort)d' % options else null),
        (if config.ring == 'memberlist' then std.join(' ', [x + '.store=memberlist' for x in all_rings]) else null),
        (if config.ring == 'multi' then std.join(' ', [x + '.store=multi' for x in all_rings] + [x + '.multi.primary=consul' for x in all_rings] + [x + '.multi.secondary=memberlist' for x in all_rings]) else null),
      ]),
    ],
    environment: envs.formatEnv(jaeger.jaegerEnv(options.jaegerApp)),
    hostname: options.name,
    // Only publish HTTP and debug port, but not gRPC one.
    ports: ['%d:%d' % [options.httpPort, options.httpPort]] +
           ['%d:%d' % [options.memberlistBindPort, options.memberlistBindPort]] +
           if config.debug then [
             '%d:%d' % [options.debugPort, options.debugPort],
           ] else [],
    depends_on: options.dependsOn,
    volumes: ['./generated:/mimir/config', './activity:/activity'] + options.extraVolumes,
  },

  newMimirServices(config):: {
    // We explicitly list all important services here, so that it's easy to disable them by commenting out.
    services:
      self.distributors(config.distributors.replicas, config.distributors.first_port) +
      self.ingesters(config.ingesters.replicas, config.ingesters.first_port) +
      self.queriers(config.queriers.replicas, config.queriers.first_port) +
      self.query_frontends(config.query_frontends.replicas, config.query_frontends.first_port) +
      self.query_schedulers(config.query_schedulers.replicas, config.query_schedulers.first_port) +
      self.store_gateways(config.store_gateways.replicas, config.store_gateways.first_port) +
      self.compactors(config.compactors.replicas, config.compactors.first_port) +
      self.rulers(config.rulers.replicas, config.rulers.first_port) +
      self.alertmanagers(config.alertmanagers.replicas, config.alertmanagers.first_port) +
      (if config.enable_continuous_test then self.continuous_test else {}) +
      (if config.enable_load_generator then self.load_generator else {}) +
      (if config.enable_query_tee then self.query_tee else {}) +
      {},


    distributors(replicas, first_port):: if replicas <= 0 then {} else {
      ['distributor-%d' % id]: $.mimirService(config, {
        name: 'distributor-' + id,
        target: 'distributor',
        httpPort: first_port + id - 1,
        jaegerApp: 'distributor-%d' % id,
      })
      for id in std.range(1, replicas)
    },

    ingesters(replicas, first_port):: if replicas <= 0 then {} else {
      ['ingester-%d' % id]: $.mimirService(config, {
        name: 'ingester-' + id,
        target: 'ingester',
        httpPort: first_port + id - 1,
        jaegerApp: 'ingester-%d' % id,
        extraVolumes: ['.data-ingester-%d:/tmp/mimir-tsdb-ingester:delegated' % id],
      })
      for id in std.range(1, replicas)
    },

    queriers(replicas, first_port):: if replicas <= 0 then {} else {
      ['querier-%d' % id]: $.mimirService(config, {
        name: 'querier-' + id,
        target: 'querier',
        httpPort: first_port + id - 1,
      })
      for id in std.range(1, replicas)
    },

    query_frontends(replicas, first_port):: if replicas <= 0 then {} else {
      ['query_frontend-%d' % id]: $.mimirService(config, {
        name: 'query_frontend-' + id,
        target: 'query-frontend',
        httpPort: first_port + id - 1,
      })
      for id in std.range(1, replicas)
    },

    query_schedulers(replicas, first_port):: if replicas <= 0 then {} else {
      ['query_scheduler-%d' % id]: $.mimirService(config, {
        name: 'query_scheduler-' + id,
        target: 'query-scheduler',
        httpPort: first_port + id - 1,
      })
      for id in std.range(1, replicas)
    },

    compactors(replicas, first_port):: if replicas <= 0 then {} else {
      ['compactor-%d' % id]: $.mimirService(config, {
        name: 'compactor-' + id,
        target: 'compactor',
        httpPort: first_port + id - 1,
      })
      for id in std.range(1, replicas)
    },

    rulers(count, first_port):: if count <= 0 then {} else {
      ['ruler-%d' % id]: $.mimirService(config, {
        name: 'ruler-' + id,
        target: 'ruler',
        httpPort: first_port + id - 1,
        jaegerApp: 'ruler-%d' % id,
        extraArguments: if config.ruler_use_remote_execution then '-ruler.query-frontend.address=dns:///query-frontend:9007' else '',
      })
      for id in std.range(1, count)
    },

    alertmanagers(count, first_port):: if count <= 0 then {} else {
      ['alertmanager-%d' % id]: $.mimirService(config, {
        name: 'alertmanager-' + id,
        target: 'alertmanager',
        httpPort: first_port + id - 1,
        extraArguments: '-alertmanager.web.external-url=http://localhost:%d/alertmanager' % (first_port + id - 1),
        jaegerApp: 'alertmanager-%d' % id,
      })
      for id in std.range(1, count)
    },

    store_gateways(count, first_port):: {
      ['store-gateway-%d' % id]: $.mimirService(config, {
        name: 'store-gateway-' + id,
        target: 'store-gateway',
        httpPort: first_port + id - 1,
        jaegerApp: 'store-gateway-%d' % id,
      })
      for id in std.range(1, count)
    },

    continuous_test:: {
      'continuous-test': $.mimirService(config, {
        name: 'continuous-test',
        target: 'continuous-test',
        httpPort: 8090,
        extraArguments:
          ' -tests.run-interval=2m' +
          ' -tests.read-endpoint=http://envoy:8080/prometheus' +
          ' -tests.tenant-id=mimir-continuous-test' +
          ' -tests.write-endpoint=http://envoy:8080' +
          ' -tests.write-read-series-test.max-query-age=1h' +
          ' -tests.write-read-series-test.num-series=100',
      }),
    },

    load_generator:: {
      'load-generator': {
        image: 'pracucci/cortex-load-generator:add-query-support-8633d4e',
        command: [
          '--remote-url=http://distributor-2:8001/api/v1/push',
          '--remote-write-concurrency=5',
          '--remote-write-interval=10s',
          '--series-count=1000',
          '--tenants-count=1',
          '--query-enabled=true',
          '--query-interval=1s',
          '--query-url=http://querier:8005/prometheus',
          '--server-metrics-port=9900',
        ],
        ports: ['9900:9900'],
      },
    },

    query_tee:: {
      'query-tee': {
        local env = jaeger.jaegerEnv('query-tee'),

        image: 'query-tee',
        build: {
          context: '../../cmd/query-tee',
        },
        command: '-backend.endpoints=http://envoy:8080 -backend.preferred=envoy -proxy.passthrough-non-registered-routes=true -server.path-prefix=/prometheus',
        environment: envs.formatEnv(env),
        hostname: 'query-tee',
        ports: ['9999:80'],
      },
    },
  }
}