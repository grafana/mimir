# GEM secrets generator is provided to assist in creation of the secrets required to install Grafana Enterprise Metrics

## DO NOT STORE GENERATED SECRETS IN GIT!!!
## Utilize the external Secrets Operator + a secrets manager or similar. Kubernetes secrets in cluster/etcd are not encrypted! Encrypt at rest.

## Requirements

Basic understanding of Go, Go properly installed.
license.jwt downloaded and available for the config. 

## Metrics secrets generator
This Go tool will output 3 secrets; one admin, one for S3 storage and one for the license.jwt that is provided. Command line arguments can be used or config.json. 
` go build gem-secrets-generator.go` or `go run  go build gem-secrets-generator.go`

### Sample run, no flags to display help
```bash
./gem-secrets-generator 
Usage: go run metrics-secrets-generator.go [--config=config.json] [flags]

Available flags to override config.json:
  --adminUser string
  --adminPassword string
  --AWS_ACCESS_KEY string
  --AWS_SECRET_ACCESS_KEY string
  --licenseFile string

Example config.json:
{
  "adminUser": "admin",
  "adminPassword": "s3cr3t",
  "AWS_ACCESS_KEY": "AKIA...",
  "AWS_SECRET_ACCESS_KEY": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
  "licenseFile": "license.jwt"
}
```

### Sample run with config.json and output
```bash
./gem-secrets-generator --config=config.json
Secret written to: admin-secret.yaml
Secret written to: metrics-bucket-secret.yaml
Secret written to: metrics-license-secret.yaml
```
