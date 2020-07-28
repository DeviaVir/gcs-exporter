# gcs-exporter

A Prometheus exporter that reports on GCS file stats of one or multiple buckets, and their directories.

## Metrics

```
gcs_update_time_seconds{bucket}
gcs_update_errors_total{bucket,type}
gcs_bytes_total{bucket="bucket"} 1.090393457076e+12
gcs_files_total{bucket="bucket"} 254
gcs_folder_bytes_total{bucket="bucket",folder="example/path"} 1.007658421126e+12
gcs_folder_files_total{bucket="bucket",folder="example/path"} 45
gcs_folder_last_created_date_seconds{bucket="bucket",folder="example/path"} 1.595970607e+09
```

## Usage

```
docker run -it --rm deviavir/gcs-exporter:v0.4 --prometheusx.listen-address=:9112 --source=<bucket-name> --time=60s
```
