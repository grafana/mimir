{
  minio:: {
    minio: {
      image: 'minio/minio:RELEASE.2025-05-24T17-08-30Z',
      command: ['server', '--console-address', ':9001', '/data'],
      environment: ['MINIO_ROOT_USER=mimir', 'MINIO_ROOT_PASSWORD=supersecret'],
      ports: [
        '9000:9000',
        '9001:9001',
      ],
      volumes: ['.data-minio:/data:delegated'],
    },
  },

  memcached:: {
    memcached: {
      image: 'memcached:1.6.34-alpine',
      ports: [
        '11211:11211',
      ],
    },
  },

  memcached_exporter:: {
    'memcached-exporter': {
      image: 'prom/memcached-exporter:v0.15.3',
      command: ['--memcached.address=memcached:11211', '--web.listen-address=0.0.0.0:9150'],
    },
  },

  consul:: {
    consul: {
      image: 'consul:1.15',
      command: ['agent', '-dev', '-client=0.0.0.0', '-log-level=debug'],
      hostname: 'consul',
      ports: ['8500:8500'],
    },
  },
}