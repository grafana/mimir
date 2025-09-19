local docker = import 'docker.libsonnet';

{
  newMimirEnvoyConfig(options):: {
    static_resources: {
      listeners: [
        {
          name: "mimir-microservices-mode",
          address: {
            socket_address: {
              address: "0.0.0.0",
              port_value: options.config.envoy.traffic_port
            }
          },
          filter_chains: [
            {
              filters: [
                {
                  name: "envoy.filters.network.http_connection_manager",
                  typed_config: {
                    "@type": "type.googleapis.com/envoy.extensions.filters.network.http_connection_manager.v3.HttpConnectionManager",
                    generate_request_id: true,
                    tracing: {
                      random_sampling: {
                        value: 50
                      },
                      provider: {
                        name: "envoy.tracers.zipkin",
                        typed_config: {
                          "@type": "type.googleapis.com/envoy.config.trace.v3.ZipkinConfig",
                          collector_cluster: "jaeger",
                          collector_endpoint: "/api/v2/spans",
                          shared_span_context: false,
                          collector_endpoint_version: "HTTP_JSON",
                        }
                      }
                    },
                    stat_prefix: "ingress_http",
                    route_config: {
                      name: "mimir-microservices-mode",
                      virtual_hosts: [
                        {
                          name: "mimir-microservices-mode",
                          domains: ["*"],
                          routes: [
                            // Distributer Routes
                            $._envoy_route("/distributor", "distributor"),
                            $._envoy_route("/api/v1/push", "distributor"),
                            $._envoy_route("/otlp/v1/metrics", "distributor"),

                            // Alertmanager Routes
                            $._envoy_route("/alertmanager", "alertmanager"),
                            $._envoy_route("/multitenant_alertmanager/status", "alertmanager"),
                            $._envoy_route("/multitenant_alertmanager/configs", "alertmanager"),
                            $._envoy_route("/api/v1/alerts", "alertmanager"),
                            $._envoy_route("/api/v1/grafana/config", "alertmanager"),
                            $._envoy_route("/api/v1/grafana/config/status", "alertmanager"),
                            $._envoy_route("/api/v1/grafana/full_state", "alertmanager"),
                            $._envoy_route("/api/v1/grafana/state", "alertmanager"),
                            $._envoy_route("/api/v1/grafana/receivers", "alertmanager"),
                            $._envoy_route("/api/v1/grafana/receivers/test", "alertmanager"),
                            $._envoy_route("/api/v1/grafana/templates/test", "alertmanager"),
                            
                            // Ruler Routes
                            $._envoy_route("/prometheus/config/v1/rules", "alertmanager"),
                            $._envoy_route("/prometheus/api/v1/rules", "alertmanager"),
                            $._envoy_route("/prometheus/api/v1/alerts", "alertmanager"),
                            $._envoy_route("/ruler/ring", "alertmanager"),
                            
                            // Query Frontend Routes
                            $._envoy_route("/prometheus", "query_frontend"),
                            $._envoy_route("/api/v1/status/buildinfo", "query_frontend"),
                            
                            // Compactor Routes
                            $._envoy_route("/api/v1/upload/block/", "compactor"),
                          ]
                        }
                      ]
                    },
                    http_filters: [
                      {
                        name: "envoy.filters.http.router",
                        typed_config: {
                          "@type": "type.googleapis.com/envoy.extensions.filters.http.router.v3.Router"
                        }
                      }
                    ]
                  }
                }
              ]
            }
          ]
        }
      ],

      clusters: 
        $._cluster_for_service({service: "distributor", config: options.config}) +
        $._cluster_for_service({service: "query_frontend", config: options.config}) + 
        $._cluster_for_service({service: "compactor", config: options.config}) + 
        $._cluster_for_service({service: "ruler", config: options.config}) + 
        $._cluster_for_service({service: "alertmanager", config: options.config}) + 
        [
          {
            name: "jaeger",
            connect_timeout: "10s",
            type: "STRICT_DNS",
            lb_policy: "ROUND_ROBIN",
            load_assignment: {
              cluster_name: "jaeger",
              endpoints: [
                {
                  lb_endpoints: [
                    $._envoy_endpoint("jaeger", options.config.jaeger.port)
                  ]
                }
              ]
            }
          }
        ]
    },
    
    admin: {
      address: {
        socket_address: {
          address: "0.0.0.0",
          port_value: options.config.envoy.metrics_port
        }
      }
    }
  },

  _cluster_for_service(options):: [{
    name: options.service,
    connect_timeout: "10s",
    type: "STRICT_DNS",
    lb_policy: "ROUND_ROBIN",
    load_assignment: {
      cluster_name: options.service,
      endpoints: [
        {
          lb_endpoints: $._endpoints_for_service(options.service, options.config["%s%s" % [options.service, "s"]])
        }
      ]
    }
  }],

  _endpoints_for_service(name, service):: if service.replicas <= 0 then {} else [
    $._envoy_endpoint("%(name)s-%(id)d" % {name: name, id: id}, service.first_port + id - 1) for id in std.range(1, service.replicas)
  ],

  _envoy_endpoint(host, port): {
    endpoint: {
      address: {
        socket_address: {
          address: host,
          port_value: port
        }
      }
    }
  },

  _envoy_route(path, host):: {
    match: {
      prefix: path
    },
    route: {
      cluster: host
    }
  },

  newEnvoyService(options):: {
    envoy: {
        image: "envoyproxy/envoy:v1.35-latest",
        ports: [
          "%(port)d:%(port)d" % { port: options.traffic_port },
          "%(port)d:%(port)d" % { port: options.metrics_port},
        ],
        volumes: [
          "./generated/envoy.yaml:/etc/envoy/envoy.yaml"
        ]
      }
    }
}