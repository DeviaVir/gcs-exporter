// Copyright 2020 gcs-exporter Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//////////////////////////////////////////////////////////////////////////////

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
	lastUpdateDuration = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "gcs_update_time_seconds",
			Help: "Most recent time to update metrics",
		},
		[]string{"bucket"},
	)
	updateErrors = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "gcs_update_errors_total",
			Help: "Number of update errors",
		},
		[]string{"bucket", "type"},
	)
	archiveFiles = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "gcs_files_total",
			Help: "GCS file count",
		},
		[]string{"bucket"},
	)
	archiveBytes = promauto.NewCounterVec(
		prometheus.CounterOpts{
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
	archiveFiles.WithLabelValues(bucket).Add(float64(files))
	archiveBytes.WithLabelValues(bucket).Add(float64(size))

	log.Println("Total time to Update:", time.Since(start))
	lastUpdateDuration.WithLabelValues(bucket).Set(time.Since(start).Seconds())
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
			updateErrors.WithLabelValues(bucket, "bucket-objects").Inc()
			return 0, 0, fmt.Errorf("Bucket(%q).Objects: %v", bucket, err)
		}
		files++
		//fmt.Println(fmt.Printf("Name: %v\n", attrs.Name))

		o := client.Bucket(bucket).Object(attrs.Name)
		objectAttrs, err := o.Attrs(ctx)
		if err != nil {
			updateErrors.WithLabelValues(bucket, "bucket-object").Inc()
			return 0, 0, fmt.Errorf("Object(%q).Attrs: %v", attrs.Name, err)
		}
		//fmt.Println(fmt.Printf("Size: %v\n", objectAttrs.Size))
		size += objectAttrs.Size
	}
	return files, size, nil
}
