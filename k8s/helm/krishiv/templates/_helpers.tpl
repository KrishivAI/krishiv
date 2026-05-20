{{/*
Expand the name of the chart.
*/}}
{{- define "krishiv.name" -}}
{{- .Chart.Name | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a fully qualified app name.
*/}}
{{- define "krishiv.fullname" -}}
{{- printf "%s" .Release.Name | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels applied to all resources.
*/}}
{{- define "krishiv.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | quote }}
app.kubernetes.io/name: {{ include "krishiv.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels for coordinator pods.
*/}}
{{- define "krishiv.coordinatorSelectorLabels" -}}
app.kubernetes.io/name: {{ include "krishiv.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: coordinator
{{- end }}

{{/*
Selector labels for executor pods.
*/}}
{{- define "krishiv.executorSelectorLabels" -}}
app.kubernetes.io/name: {{ include "krishiv.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: executor
{{- end }}

{{/*
Selector labels for operator pods.
*/}}
{{- define "krishiv.operatorSelectorLabels" -}}
app.kubernetes.io/name: {{ include "krishiv.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: operator
{{- end }}

{{/*
Service account name.
*/}}
{{- define "krishiv.serviceAccountName" -}}
{{- if .Values.rbac.create }}
{{- .Values.rbac.serviceAccountName | default "krishiv" }}
{{- else }}
{{- "default" }}
{{- end }}
{{- end }}
