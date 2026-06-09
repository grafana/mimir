local mimir = import 'mimir/mimir.libsonnet';

mimir {
  _config+:: {
    namespace: 'default',
    external_url: 'http://test',

    // required by config, but not used in this test
    storage_backend: 'gcs',
    blocks_storage_bucket_name: 'blocks-bucket',

    ruler_enabled: true,
    ruler_storage_backend: 'local',
    ruler_local_directory: '/data/ruler-rules',
    ruler_local_rules: {
      'tenant-1': {
        'rules.yaml': |||
          groups:
            - name: example
              rules:
                - record: job:http_requests:rate5m
                  expr: sum by (job) (rate(http_requests_total[5m]))
        |||,
      },
      'tenant-2': {
        'alerts.yaml': |||
          groups:
            - name: alerts
              rules:
                - alert: HighErrorRate
                  expr: rate(errors_total[5m]) > 0.1
        |||,
      },
    },
  },
}
