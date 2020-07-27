# gcs-exporter

A Prometheus exporter that reports on GCS file stats of one or multiple buckets

## Usage

```
docker run -it --rm deviavir/gcs-exporter:0.3 --prometheusx.listen-address=:9112 --source=<bucket-name> --time=60s
```
