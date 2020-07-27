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

package main

import (
	"context"
	"flag"
	"log"
	"sync"
	"time"

	"github.com/DeviaVir/gcs-exporter/gcs"
	"github.com/m-lab/go/flagx"
	"github.com/m-lab/go/prometheusx"
	"github.com/m-lab/go/rtx"
	"google.golang.org/api/option"

	"cloud.google.com/go/storage"
)

var (
	sources      flagx.StringArray
	collectTimes flagx.DurationArray
)

func init() {
	flag.Var(&sources, "source", "gs://<bucket>")
	flag.Var(&collectTimes, "time", "Run collections at given interval <600s>.")
	log.SetFlags(log.LUTC | log.Lshortfile | log.Ltime | log.Ldate)
}

var (
	opts                []option.ClientOption
	mainCtx, mainCancel = context.WithCancel(context.Background())
)

// updateForever runs the gcs.Update on the given bucket at the given collect time every day.
func updateForever(ctx context.Context, wg *sync.WaitGroup, client *storage.Client, bucket string, collect time.Duration) {
	defer wg.Done()

	gcs.Update(mainCtx, client, bucket)

	for {
		select {
		case <-mainCtx.Done():
			return
		case <-time.After(collect):
			gcs.Update(mainCtx, client, bucket)
		}
	}
}

var logFatal = log.Fatal

func main() {
	flag.Parse()
	rtx.Must(flagx.ArgsFromEnv(flag.CommandLine), "Failed to parse args")

	if len(sources) != len(collectTimes) {
		logFatal("Must provide same number of sources as collection times.")
	}

	srv := prometheusx.MustServeMetrics()
	defer srv.Close()

	client, err := storage.NewClient(mainCtx, opts...)
	defer client.Close()
	rtx.Must(err, "Failed to create client")

	wg := sync.WaitGroup{}
	for i, t := range collectTimes {
		wg.Add(1)
		go updateForever(mainCtx, &wg, client, sources[i], t)
	}
	wg.Wait()
}
