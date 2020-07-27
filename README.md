# gcs-exporter

A Prometheus exporter that reports on GCS file stats of one or multiple buckets

## Metrics

```
gcs_update_time_seconds{bucket}
gcs_update_errors_total{bucket,type}
gcs_files_total{bucket}
gcs_bytes_total{bucket}
```

## Usage

```
docker run -it --rm deviavir/gcs-exporter:v0.4 --prometheusx.listen-address=:9112 --source=<bucket-name> --time=60s
```
