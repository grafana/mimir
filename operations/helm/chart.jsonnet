local mimir = import 'mimir/mimir.libsonnet';

{
  apiVersion: 'tanka.dev/v1alpha1',
  kind: 'Environment',
  metadata: { name: '.' },
  data: mimir {
    _config+:: {
      namespace: '{{ .Release.Namespace }}',
      blocks_storage_backend: 'gcs',
      blocks_storage_bucket_name: 'example-bucket',
    },
  },
}
