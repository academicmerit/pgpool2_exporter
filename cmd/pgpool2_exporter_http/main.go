package main

import (
    "fmt"
    "time"
    "context"

    "net/url"
    "regexp"
    "github.com/blang/semver"

	"encoding/json"
	"strings"
	"os"
	"net/http"
	"io/ioutil"

    "os/signal"
    "syscall"

    "go.opentelemetry.io/otel/attribute"
    "go.opentelemetry.io/otel/sdk/metric"

    httpexp "github.com/VincentHokie/pgpool2_exporter"
)

// Pgpool-II version
var pgpoolVersionRegex = regexp.MustCompile(`^((\d+)(\.\d+)(\.\d+)?)`)
var version42 = semver.MustParse("4.2.0")
var PgpoolSemver semver.Version

func main() {

    ctx := context.Background()
    var env = os.Getenv("ENVIRONMENT")
    env, isEnvSet := os.LookupEnv("ENVIRONMENT")

    ecsMetadataURI, isECSEnvSet := os.LookupEnv("ECS_CONTAINER_METADATA_URI_V4")

    var attributes []attribute.KeyValue

    if isECSEnvSet {
        response, err := http.Get(ecsMetadataURI)

        if err != nil {
            fmt.Print(err.Error())
        }

        responseData, err := ioutil.ReadAll(response.Body)
        if err != nil {
            fmt.Print(err)
        }

        var result map[string]any
        json.Unmarshal(responseData, &result)

        labels := result["Labels"].(map[string]any)
        arn := labels["com.amazonaws.ecs.task-arn"].(string)
        taskId := arn[strings.LastIndex(arn, "/")+1:]

        attributes = append(attributes, attribute.String("ECS_TASK_ID", taskId))
    }

    if isEnvSet {
        attributes = append(attributes, attribute.String("env", env))
    }

    var dsn = os.Getenv("DATA_SOURCE_NAME")

	if len(dsn) == 0 {
		var user = os.Getenv("DATA_SOURCE_USER")
		var pass = os.Getenv("DATA_SOURCE_PASS")
		var uri = os.Getenv("DATA_SOURCE_URI")
		ui := url.UserPassword(user, pass).String()

		dsn = "postgresql://" + ui + "@" + uri
	}
    
    candence, isCadenceSet := os.LookupEnv("PERIODIC_EXPORT_CADENCE")

    if !isCadenceSet {
        // by default if a cadence is not set, export metrics every 30 seconds
        candence = "30s"
    }

    cadenceDuration, err := time.ParseDuration(candence)

    if err != nil {
        fmt.Printf("Failed to cast PERIODIC_EXPORT_CADENCE into time.Duration type: %v\n", err)
        return
    }

    // Create the custom exporter
    exporter, err := httpexp.NewCustomExporter(ctx, dsn, attributes)
    if err != nil {
        fmt.Printf("Failed to create custom exporter: %v\n", err)
        return
    }

    // Create a PeriodicReader that exports every 30 seconds
    periodicReader := metric.NewPeriodicReader(exporter, metric.WithInterval(cadenceDuration))

    meterProvider := metric.NewMeterProvider(metric.WithReader(periodicReader),)

    var meter = meterProvider.Meter("pgpool2_exporter")

    exporter.SetMeter(meter)
    exporter.SetMetrics()


    // Retrieve Pgpool-II version
	v, err := httpexp.QueryVersion(exporter.DB)
	if err != nil {
        fmt.Printf("Failed to query pgpool version: %v\n", err)
	}
	httpexp.PgpoolSemver = v

    fmt.Println("Application is running...")

    // Create a channel to listen for OS signals
    quit := make(chan os.Signal, 1)
    signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

    // Block until a signal is received
    <-quit
    fmt.Println("Shutting down gracefully...")

}
