{{/*
memcached StatefulSet
*/}}
{{- define "mimir.memcached.statefulSet" -}}
{{ with (index $.ctx.Values $.component) }}
{{- if .enabled -}}
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: {{ include "mimir.resourceName" (dict "ctx" $.ctx "component" $.component) }}
  labels:
    {{- include "mimir.labels" (dict "ctx" $.ctx "component" "memcached") | nindent 4 }}
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
        {{- include "mimir.podLabels" (dict "ctx" $.ctx "component" $.component) | nindent 8 }}
        {{- with .podLabels }}
        {{- toYaml . | nindent 8 }}
        {{- end }}
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
        {{- toYaml $.ctx.Values.memcached.podSecurityContext | nindent 8 }}
      initContainers:
        {{- toYaml .initContainers | nindent 8 }}
      nodeSelector:
        {{- toYaml .nodeSelector | nindent 8 }}
      affinity:
        {{- toYaml .affinity | nindent 8 }}
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
        - name: memcached
          {{- with $.ctx.Values.memcached.image }}
          image: {{ .repository }}:{{ .tag }}
          imagePullPolicy: {{ .pullPolicy }}
          {{- end }}
          resources:
          {{- if .resources }}
            {{- toYaml .resources | nindent 12 }}
          {{- else }}
            limits:
              memory: {{ round (mulf .allocatedMemory 1.2) 0 }}Mi
            requests:
              cpu: 500m
              memory: {{ round (mulf .allocatedMemory 1.2) 0 }}Mi
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
            {{- toYaml $.ctx.Values.memcached.containerSecurityContext | nindent 12 }}

      {{- if $.ctx.Values.memcachedExporter.enabled }}
        - name: exporter
          {{- with $.ctx.Values.memcachedExporter.image }}
          image: {{ .repository}}:{{ .tag }}
          imagePullPolicy: {{ .pullPolicy }}
          {{- end }}
          ports:
            - containerPort: 9150
              name: http-metrics
          args:
            - "--memcached.address=localhost:{{ .port }}"
            - "--web.listen-address=0.0.0.0:9150"
          resources:
            {{- toYaml $.ctx.Values.memcachedExporter.resources | nindent 12 }}
      {{- end }}
{{- end -}}
{{- end -}}
{{- end -}}
