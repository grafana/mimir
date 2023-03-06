local redis = import 'redis/redis.libsonnet';

// TODO
redis {
  _config+: {
    namespace: foo,
  },
}
