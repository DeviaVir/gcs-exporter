package gcs

import (
	"context"
	"fmt"
	"log"
	"path/filepath"
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
	promFolderFiles = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "gcs_folder_files_total",
			Help: "GCS folder file count",
		},
		[]string{"bucket", "folder"},
	)
	promFolderBytes = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "gcs_folder_bytes_total",
			Help: "GCS folder file bytes total",
		},
		[]string{"bucket", "folder"},
	)
	promFolderDate = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "gcs_folder_last_created_date_seconds",
			Help: "GCS folder last created file unix timestamp",
		},
		[]string{"bucket", "folder"},
	)
)

// Update runs the collector query and atomically updates the cached metrics.
func Update(ctx context.Context, client *storage.Client, bucket string) error {
	start := time.Now()
	log.Println("Starting to walk:", start.Format("2006/01/02"))

	files, size, folderCount, folderSize, folderDate, err := listFiles(ctx, client, bucket)
	if err != nil {
		// if we hit an error while collecting metrics, better to not update these
		// values to false positives.
		log.Println(err)
		return nil
	}
	promFiles.WithLabelValues(bucket).Set(float64(files))
	promBytes.WithLabelValues(bucket).Set(float64(size))
	for f, c := range folderCount {
		promFolderFiles.WithLabelValues(bucket, f).Set(float64(c))
	}
	for f, s := range folderSize {
		promFolderBytes.WithLabelValues(bucket, f).Set(float64(s))
	}
	for f, d := range folderDate {
		promFolderDate.WithLabelValues(bucket, f).Set(float64(d))
	}

	log.Println("Total time to Update:", time.Since(start))
	promLastUpdateDuration.WithLabelValues(bucket).Set(time.Since(start).Seconds())
	return nil
}

func listFiles(ctx context.Context, client *storage.Client, bucket string) (int, int64, map[string]int, map[string]int64, map[string]int64, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Second*60)
	defer cancel()

	folderCount := make(map[string]int)
	folderSize := make(map[string]int64)
	folderDate := make(map[string]int64)

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
			return 0, 0, folderCount, folderSize, folderDate, fmt.Errorf("Bucket(%q).Objects: %v", bucket, err)
		}

		folder := filepath.Dir(attrs.Name)
		if _, ok := folderCount[folder]; ok {
			folderCount[folder]++
		} else {
			folderCount[folder] = 1
		}
		files++

		if _, ok := folderDate[folder]; ok {
			if folderDate[folder] < attrs.Created.Unix() {
				folderDate[folder] = attrs.Created.Unix()
			}
		} else {
			folderDate[folder] = attrs.Created.Unix()
		}

		if _, ok := folderSize[folder]; ok {
			folderSize[folder] += attrs.Size
		} else {
			folderSize[folder] = attrs.Size
		}
		size += attrs.Size
	}
	return files, size, folderCount, folderSize, folderDate, nil
}
