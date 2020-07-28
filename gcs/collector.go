package gcs

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
)

var (
	promLastUpdateDuration = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "gcs_update_time_seconds",
			Help: "Most recent time to update metrics",
		},
		[]string{"bucket"},
	)
	promErrors = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "gcs_update_errors_total",
			Help: "Number of update errors",
		},
		[]string{"bucket", "type"},
	)
	promFiles = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "gcs_files_total",
			Help: "GCS file count",
		},
		[]string{"bucket"},
	)
	promBytes = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "gcs_bytes_total",
			Help: "GCS file bytes total",
		},
		[]string{"bucket"},
	)
)

// Update runs the collector query and atomically updates the cached metrics.
func Update(ctx context.Context, client *storage.Client, bucket string) error {
	start := time.Now()
	log.Println("Starting to walk:", start.Format("2006/01/02"))

	files, size, err := listFiles(ctx, client, bucket)
	promFiles.WithLabelValues(bucket).Set(float64(files))
	promBytes.WithLabelValues(bucket).Set(float64(size))

	log.Println("Total time to Update:", time.Since(start))
	promLastUpdateDuration.WithLabelValues(bucket).Set(time.Since(start).Seconds())
	return err
}

func listFiles(ctx context.Context, client *storage.Client, bucket string) (int, int64, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Second*60)
	defer cancel()

	it := client.Bucket(bucket).Objects(ctx, nil)
	files := 0
	var size int64
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			promErrors.WithLabelValues(bucket, "bucket-objects").Inc()
			return 0, 0, fmt.Errorf("Bucket(%q).Objects: %v", bucket, err)
		}
		files++
		//fmt.Println(fmt.Printf("Name: %v\n", attrs.Name))

		o := client.Bucket(bucket).Object(attrs.Name)
		objectAttrs, err := o.Attrs(ctx)
		if err != nil {
			promErrors.WithLabelValues(bucket, "bucket-object").Inc()
			return 0, 0, fmt.Errorf("Object(%q).Attrs: %v", attrs.Name, err)
		}
		//fmt.Println(fmt.Printf("Size: %v\n", objectAttrs.Size))
		size += objectAttrs.Size
	}
	return files, size, nil
}
