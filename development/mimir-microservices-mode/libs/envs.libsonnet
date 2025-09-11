{
  jaegerEnv(appName):: {
    JAEGER_AGENT_HOST: 'jaeger',
    JAEGER_AGENT_PORT: 6831,
    JAEGER_SAMPLER_TYPE: 'const',
    JAEGER_SAMPLER_PARAM: 1,
    JAEGER_TAGS: 'app=%s' % appName,
  },

  formatEnv(env):: [
    '%s=%s' % [key, env[key]]
    for key in std.objectFields(env)
    if env[key] != null
  ],
}
