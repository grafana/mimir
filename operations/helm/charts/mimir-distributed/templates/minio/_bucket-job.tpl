{{/* Pod template shared by the Job and its immutable-template identity. */}}
{{- define "mimir.minio.bucketJobPodTemplate" -}}
metadata:
  labels:
    app: {{ template "minio.name" . }}-job
    release: {{ .Release.Name }}
{{- if .Values.minio.podLabels }}
{{ toYaml .Values.minio.podLabels | indent 4 }}
{{- end }}
{{- if .Values.minio.makeBucketJob.podAnnotations }}
  annotations:
{{ toYaml .Values.minio.makeBucketJob.podAnnotations | indent 4 }}
{{- end }}
spec:
  restartPolicy: OnFailure
{{- with (include "minio.imagePullSecrets" . | trim) }}
{{ . | indent 2 }}
{{- end }}
{{- if .Values.minio.nodeSelector }}
  nodeSelector:
{{ toYaml .Values.minio.makeBucketJob.nodeSelector | indent 4 }}
{{- end }}
{{- with .Values.minio.makeBucketJob.affinity }}
  affinity:
{{ toYaml . | indent 4 }}
{{- end }}
{{- with .Values.minio.makeBucketJob.tolerations }}
  tolerations:
{{ toYaml . | indent 4 }}
{{- end }}
{{- if .Values.minio.makeBucketJob.securityContext.enabled }}
  securityContext:
    runAsUser: {{ .Values.minio.makeBucketJob.securityContext.runAsUser }}
    runAsGroup: {{ .Values.minio.makeBucketJob.securityContext.runAsGroup }}
    fsGroup: {{ .Values.minio.makeBucketJob.securityContext.fsGroup }}
{{- end }}
  volumes:
    - name: minio-configuration
      projected:
        sources:
        - configMap:
            name: {{ template "minio.fullname" .Subcharts.minio }}
        - secret:
            name: {{ template "minio.fullname" .Subcharts.minio }}
    {{- if .Values.minio.tls.enabled }}
    - name: cert-secret-volume-mc
      secret:
        secretName: {{ .Values.minio.tls.certSecret }}
        items:
        - key: {{ .Values.minio.tls.publicCrt }}
          path: CAs/public.crt
    {{ end }}
  containers:
  - name: minio-mc
    image: "{{ .Values.minio.mcImage.repository }}:{{ .Values.minio.mcImage.tag }}"
    imagePullPolicy: {{ .Values.minio.mcImage.pullPolicy }}
    command: ["/bin/sh", "/config/initialize"]
    env:
      - name: MINIO_ENDPOINT
        value: {{ template "minio.fullname" .Subcharts.minio }}
      - name: MINIO_PORT
        value: {{ .Values.minio.service.port | quote }}
    volumeMounts:
      - name: minio-configuration
        mountPath: /config
      {{- if .Values.minio.tls.enabled }}
      - name: cert-secret-volume-mc
        mountPath: {{ .Values.minio.configPathmc }}certs
      {{ end }}
    resources:
{{ toYaml .Values.minio.makeBucketJob.resources | indent 6 }}
{{- end -}}
