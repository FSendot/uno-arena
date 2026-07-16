{{- define "unoArena.productionRenderFile" -}}
{{- $root := index . 0 -}}
{{- $file := index . 1 -}}
{{- $raw := $root.Files.Get $file -}}
{{- $raw = replace "https://gitlab.example.invalid/GROUP/PROJECT.git" (required "gitRepoURL is required" $root.Values.gitRepoURL) $raw -}}
{{- $raw = replace "https://gitlab.example.invalid/api/v4/projects/PROJECT_ID/packages/helm/stable" (required "helmRepoURL is required" $root.Values.helmRepoURL) $raw -}}
{{- $raw -}}
{{- end -}}
