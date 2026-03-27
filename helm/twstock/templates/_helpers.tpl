{{/*
Expand the name of the chart.
*/}}
{{- define "twstock.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "twstock.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "twstock.labels" -}}
helm.sh/chart: {{ include "twstock.chart" . }}
{{ include "twstock.selectorLabels" . }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}

{{- define "twstock.selectorLabels" -}}
app.kubernetes.io/name: {{ include "twstock.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{- define "twstock.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Per-service image reference helpers — support global registry override.
CI publishes to: ghcr.io/hijason-me/twstock-{api,collector,analyzer}
*/}}
{{- define "twstock.apiImage" -}}
{{- $registry := .Values.global.imageRegistry | default .Values.image.registry -}}
{{- printf "%s/%s:%s" $registry .Values.images.api.repository .Values.image.tag -}}
{{- end }}

{{- define "twstock.collectorImage" -}}
{{- $registry := .Values.global.imageRegistry | default .Values.image.registry -}}
{{- printf "%s/%s:%s" $registry .Values.images.collector.repository .Values.image.tag -}}
{{- end }}

{{- define "twstock.analyzerImage" -}}
{{- $registry := .Values.global.imageRegistry | default .Values.image.registry -}}
{{- printf "%s/%s:%s" $registry .Values.images.analyzer.repository .Values.image.tag -}}
{{- end }}

{{/*
Database URL (PostgreSQL)
*/}}
{{- define "twstock.databaseUrl" -}}
{{- $host := printf "%s-postgresql" (include "twstock.fullname" .) -}}
{{- printf "postgresql+asyncpg://%s:%s@%s:5432/%s"
      .Values.postgresql.auth.username
      .Values.postgresql.auth.password
      $host
      .Values.postgresql.auth.database -}}
{{- end }}

{{/*
Redis URL
*/}}
{{- define "twstock.redisUrl" -}}
{{- $host := printf "%s-redis" (include "twstock.fullname" .) -}}
{{- printf "redis://%s:6379/0" $host -}}
{{- end }}
