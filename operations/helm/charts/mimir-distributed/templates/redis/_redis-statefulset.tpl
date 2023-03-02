{{/*
redis StatefulSet
*/}}
{{- define "mimir.redis.statefulSet" -}}
{{ with (index $.ctx.Values $.component) }}
{{- if .enabled -}}
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: {{ include "mimir.resourceName" (dict "ctx" $.ctx "component" $.component) }}
  labels:
    {{- include "mimir.labels" (dict "ctx" $.ctx "component" "redis") | nindent 4 }}
  annotations:
    {{- toYaml .annotations | nindent 4 }}
  namespace: {{ $.ctx.Release.Namespace | quote }}
spec:
  podManagementPolicy: {{ .podManagementPolicy }}
  replicas: {{ .replicas }}
  selector:
    matchLabels:
      {{- include "mimir.selectorLabels" (dict "ctx" $.ctx "component" $.component) | nindent 6 }}
  updateStrategy:
    {{- toYaml .statefulStrategy | nindent 4 }}
  serviceName: {{ template "mimir.fullname" $.ctx }}-{{ $.component }}

  template:
    metadata:
      labels:
        {{- include "mimir.podLabels" $ | nindent 8 }}
      annotations:
        {{- with $.ctx.Values.global.podAnnotations }}
        {{- toYaml . | nindent 8 }}
        {{- end }}
        {{- with .podAnnotations }}
        {{- toYaml . | nindent 8 }}
        {{- end }}

    spec:
      serviceAccountName: {{ template "mimir.serviceAccountName" $.ctx }}
      {{- if .priorityClassName }}
      priorityClassName: {{ .priorityClassName }}
      {{- end }}
      securityContext:
        {{- toYaml $.ctx.Values.redis.podSecurityContext | nindent 8 }}
      initContainers:
        {{- toYaml .initContainers | nindent 8 }}
      nodeSelector:
        {{- toYaml .nodeSelector | nindent 8 }}
      affinity:
        {{- toYaml .affinity | nindent 8 }}
      topologySpreadConstraints:
        {{- include "mimir.lib.topologySpreadConstraints" $ | nindent 8 }}
      tolerations:
        {{- toYaml .tolerations | nindent 8 }}
      terminationGracePeriodSeconds: {{ .terminationGracePeriodSeconds }}
      {{- if $.ctx.Values.image.pullSecrets }}
      imagePullSecrets:
      {{- range $.ctx.Values.image.pullSecrets }}
        - name: {{ . }}
      {{- end }}
      {{- end }}
      containers:
        {{- if .extraContainers }}
        {{ toYaml .extraContainers | nindent 8 }}
        {{- end }}
        - name: redis
          {{- with $.ctx.Values.redis.image }}
          image: {{ .repository }}:{{ .tag }}
          imagePullPolicy: {{ .pullPolicy }}
          {{- end }}
          resources:
          {{- if .resources }}
            {{- toYaml .resources | nindent 12 }}
          {{- else }}
          {{- /* Calculate requested memory as round(allocatedMemory * 1.2). But with integer built-in operators. */}}
          {{- $requestMemory := div (add (mul .allocatedMemory 12) 5) 10 }}
            limits:
              memory: {{ $requestMemory }}Mi
            requests:
              cpu: 500m
              memory: {{ $requestMemory }}Mi
          {{- end }}
          ports:
            - containerPort: {{ .port }}
              name: client
          args:
            - -m {{ .allocatedMemory }}
            - -o
            - modern
            - -I {{ .maxItemMemory }}m
            - -c 16384
            - -v
            - -u {{ .port }}
            {{- range $key, $value := .extraArgs }}
            - "-{{ $key }} {{ $value }}"
            {{- end }}
          env:
            {{- with $.ctx.Values.global.extraEnv }}
              {{ toYaml . | nindent 12 }}
            {{- end }}
          envFrom:
            {{- with $.ctx.Values.global.extraEnvFrom }}
              {{- toYaml . | nindent 12 }}
            {{- end }}
          securityContext:
            {{- toYaml $.ctx.Values.redis.containerSecurityContext | nindent 12 }}

      {{- if $.ctx.Values.redisExporter.enabled }}
        - name: exporter
          {{- with $.ctx.Values.redisExporter.image }}
          image: {{ .repository}}:{{ .tag }}
          imagePullPolicy: {{ .pullPolicy }}
          {{- end }}
          ports:
            - containerPort: 9150
              name: http-metrics
          args:
            - "--redis.address=localhost:{{ .port }}"
            - "--web.listen-address=0.0.0.0:9150"
          resources:
            {{- toYaml $.ctx.Values.redisExporter.resources | nindent 12 }}
          securityContext:
            {{- toYaml $.ctx.Values.redisExporter.containerSecurityContext | nindent 12 }}
      {{- end }}
{{- end -}}
{{- end -}}
{{- end -}}
