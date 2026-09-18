{{- define "traefik.ingress" -}}
{{- $root := .root -}}
{{- $ingress := .ingress -}}
{{- $tls := default (dict) $ingress.tls -}}
{{- if $ingress.enabled }}
apiVersion: traefik.io/v1alpha1
kind: IngressRoute
metadata:
  name: {{ $root.Release.Name }}
  namespace: {{ $root.Release.Namespace }}
spec:
  entryPoints:
    {{- range ($ingress.entryPoints | default (list "websecure")) }}
    - {{ . }}
    {{- end }}
  routes:
    {{- if $ingress.routes }}
    {{- range $ingress.routes }}
    - match: {{ .match | quote }}
      kind: Rule
      services:
        {{- toYaml .services | nindent 8 }}
      {{- with .middlewares }}
      middlewares:
        {{- toYaml . | nindent 8 }}
      {{- end }}
    {{- end }}
    {{- else }}
    {{- range $ingress.paths }}
    - match: {{ printf "PathPrefix(`%s`)" . | quote }}
      kind: Rule
      services:
        - name: {{ $ingress.serviceName | default $root.Release.Name }}
          port: {{ $ingress.servicePort | default "http" }}
    {{- end }}
    {{- end }}
  {{- if $ingress.tls }}
  tls:
    secretName: {{ $tls.secretName | default (printf "%s-tls" $root.Release.Name) }}
  {{- end }}
{{- end }}
{{- end }}
