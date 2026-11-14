{{- define "boomerang.fullname" -}}{{ .Release.Name }}{{- end -}}

{{- define "boomerang.labels" -}}
app.kubernetes.io/name: boomerang
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version }}
{{- end -}}

{{- define "boomerang.selector" -}}
app.kubernetes.io/name: boomerang
app.kubernetes.io/instance: {{ .root.Release.Name }}
app.kubernetes.io/component: {{ .component }}
{{- end -}}

{{- define "boomerang.secretName" -}}
{{- if .Values.existingSecret }}{{ .Values.existingSecret }}{{ else }}{{ include "boomerang.fullname" . }}-secrets{{ end -}}
{{- end -}}

{{/* Shared container env for backend roles. */}}
{{- define "boomerang.env" -}}
- name: DEMO_MODE
  value: {{ .Values.demoMode | quote }}
- name: SERVICE_ID
  valueFrom: { fieldRef: { fieldPath: metadata.name } }
- name: KAFKA_BROKERS
  value: {{ .Values.external.kafkaBrokers | quote }}
- name: REDIS_ADDR
  value: {{ .Values.external.redisAddr | quote }}
- name: POSTGRES_URL
  value: {{ .Values.external.postgresURL | quote }}
- name: POSTGRES_DIRECT_URL
  value: {{ .Values.external.postgresDirectURL | quote }}
- name: CLICKHOUSE_URL
  value: {{ .Values.external.clickhouseURL | quote }}
- name: CLICKHOUSE_USER
  value: {{ .Values.external.clickhouseUser | quote }}
- name: MINIO_ENDPOINT
  value: {{ .Values.external.s3Endpoint | quote }}
- name: MINIO_BUCKET
  value: {{ .Values.external.s3Bucket | quote }}
- name: MINIO_USE_SSL
  value: {{ .Values.external.s3UseSSL | quote }}
- name: RECEIVERS_URL
  value: http://{{ include "boomerang.fullname" . }}-receivers:9000
- name: SSRF_ALLOW_HOSTS
  value: {{ include "boomerang.fullname" . }}-receivers
- name: API_INTERNAL_URL
  value: http://{{ include "boomerang.fullname" . }}-api:8080
- name: GATEWAY_URL
  value: http://{{ include "boomerang.fullname" . }}-api:8080
- name: OTEL_EXPORTER_OTLP_ENDPOINT
  value: {{ .Values.external.otlpEndpoint | quote }}
- name: GOMEMLIMIT
  value: 192MiB
- name: SECRETS_ENCRYPTION_KEY
  valueFrom: { secretKeyRef: { name: {{ include "boomerang.secretName" . }}, key: secretsEncryptionKey } }
- name: INTERNAL_TOKEN
  valueFrom: { secretKeyRef: { name: {{ include "boomerang.secretName" . }}, key: internalToken } }
- name: SHOPSIM_API_KEY
  valueFrom: { secretKeyRef: { name: {{ include "boomerang.secretName" . }}, key: shopsimAPIKey } }
- name: CLICKHOUSE_PASSWORD
  valueFrom: { secretKeyRef: { name: {{ include "boomerang.secretName" . }}, key: clickhousePassword } }
- name: MINIO_ACCESS_KEY
  valueFrom: { secretKeyRef: { name: {{ include "boomerang.secretName" . }}, key: s3AccessKey } }
- name: MINIO_SECRET_KEY
  valueFrom: { secretKeyRef: { name: {{ include "boomerang.secretName" . }}, key: s3SecretKey } }
{{- end -}}

{{/* A backend Deployment. Args: root, component, values, port, extraEnv, serviceAccount */}}
{{- define "boomerang.backendDeployment" -}}
{{- $root := .root -}}
apiVersion: apps/v1
kind: Deployment
metadata:
  name: {{ include "boomerang.fullname" $root }}-{{ .component }}
  labels:
    {{- include "boomerang.labels" $root | nindent 4 }}
    app.kubernetes.io/component: {{ .component }}
spec:
  replicas: {{ .values.replicas }}
  selector:
    matchLabels:
      {{- include "boomerang.selector" (dict "root" $root "component" .component) | nindent 6 }}
  template:
    metadata:
      labels:
        {{- include "boomerang.selector" (dict "root" $root "component" .component) | nindent 8 }}
      annotations:
        prometheus.io/scrape: "true"
        prometheus.io/port: "8080"
    spec:
      {{- if .serviceAccount }}
      serviceAccountName: {{ .serviceAccount }}
      {{- end }}
      terminationGracePeriodSeconds: {{ .values.terminationGracePeriodSeconds | default 30 }}
      securityContext:
        runAsNonRoot: true
        runAsUser: 10001
      containers:
        - name: {{ .component }}
          image: {{ $root.Values.image.backend }}
          imagePullPolicy: {{ $root.Values.image.pullPolicy }}
          args: [{{ .role | default .component | quote }}]
          env:
            {{- include "boomerang.env" $root | nindent 12 }}
            {{- with .extraEnv }}
            {{- toYaml . | nindent 12 }}
            {{- end }}
          ports:
            - { name: http, containerPort: {{ .port | default 8080 }} }
          readinessProbe:
            httpGet: { path: /readyz, port: http }
            periodSeconds: 5
          livenessProbe:
            httpGet: { path: /healthz, port: http }
            periodSeconds: 10
          resources:
            {{- toYaml .values.resources | nindent 12 }}
          securityContext:
            allowPrivilegeEscalation: false
            readOnlyRootFilesystem: true
            capabilities: { drop: [ALL] }
{{- end -}}

{{- define "boomerang.service" -}}
apiVersion: v1
kind: Service
metadata:
  name: {{ include "boomerang.fullname" .root }}-{{ .component }}
  labels:
    {{- include "boomerang.labels" .root | nindent 4 }}
spec:
  selector:
    {{- include "boomerang.selector" (dict "root" .root "component" .component) | nindent 4 }}
  ports:
    - { name: http, port: {{ .port }}, targetPort: {{ .targetPort | default .port }} }
{{- end -}}
