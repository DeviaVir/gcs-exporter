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
