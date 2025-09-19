{
  formatEnv(env):: [
    '%s=%s' % [key, env[key]]
    for key in std.objectFields(env)
    if env[key] != null
  ],
}
