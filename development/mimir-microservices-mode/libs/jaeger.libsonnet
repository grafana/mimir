{
  newJaegerService:: {
    jaeger: {
      // Use 1.62 specifically since 1.63 removes the agent which we depend on for now.
      image: 'jaegertracing/all-in-one:1.62.0',
      ports: ['16686:16686', '14268'],
    },
  },

  jaegerEnv(appName):: {
    JAEGER_AGENT_HOST: 'jaeger',
    JAEGER_AGENT_PORT: 6831,
    JAEGER_SAMPLER_TYPE: 'const',
    JAEGER_SAMPLER_PARAM: 1,
    JAEGER_TAGS: 'app=%s' % appName,
  },
}