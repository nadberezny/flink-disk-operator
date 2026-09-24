{{- define "flink-disk-scaledjob.name" -}}
{{ .Release.Name }}
{{- end -}}

{{- define "flink-disk-scaledjob.serviceAccountName" -}}
{{ .Values.serviceAccount.name | default (include "flink-disk-scaledjob.name" .) }}
{{- end -}}

{{- define "flink-disk-scaledjob.labels" -}}
app.kubernetes.io/name: flink-disk-scaledjob
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
flink-disk-operator.nadberezny.com/flink-deployment: {{ .Values.flinkDeployment.name }}
{{- end -}}

{{/*
Regex on the persistentvolumeclaim label selecting this deployment's TaskManager volumes.
The kubelet names a generic ephemeral volume's PVC <pod>-<volume>, and the Flink operator names
TaskManager pods <deployment>-taskmanager-<attempt>-<index>. Mirrors PrometheusClient.pvcPattern.
*/}}
{{- define "flink-disk-scaledjob.pvcRegex" -}}
{{ .Values.flinkDeployment.name | replace "." "\\." }}-taskmanager-[0-9]+-[0-9]+-{{ .Values.flinkDeployment.volumeName | replace "." "\\." }}
{{- end -}}

{{- define "flink-disk-scaledjob.selector" -}}
{namespace="{{ .Release.Namespace }}",persistentvolumeclaim=~"{{ include "flink-disk-scaledjob.pvcRegex" . }}"}
{{- end -}}

{{/* Fullest TaskManager volume as a ratio; a single label-less sample, as KEDA requires. */}}
{{- define "flink-disk-scaledjob.query" -}}
max(kubelet_volume_stats_used_bytes{{ include "flink-disk-scaledjob.selector" . }} / kubelet_volume_stats_capacity_bytes{{ include "flink-disk-scaledjob.selector" . }})
{{- end -}}
