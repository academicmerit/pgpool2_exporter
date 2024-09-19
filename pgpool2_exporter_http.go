/*
Copyright (c) 2021 PgPool Global Development Group

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/

package pgpool2_exporter_http

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	// "net/url"
	_ "os"
	"regexp"
	"strconv"
	"sync"
	"time"
	"log"

	"github.com/blang/semver"
	_ "github.com/lib/pq"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
)

// columnUsage should be one of several enum values which describe how a
// queried row is to be converted to an OpenTelemetry metric.
type columnUsage int

// Convert a string to the corresponding columnUsage
func stringTocolumnUsage(s string) (u columnUsage, err error) {
	switch s {
	case "DISCARD":
		u = DISCARD

	case "LABEL":
		u = LABEL

	case "COUNTER":
		u = COUNTER

	case "GAUGE":
		u = GAUGE

	case "MAPPEDMETRIC":
		u = MAPPEDMETRIC

	case "DURATION":
		u = DURATION

	default:
		err = fmt.Errorf("wrong columnUsage given : %s", s)
	}

	return
}

// nolint: golint
const (
	DISCARD      columnUsage = iota // Ignore this column
	LABEL        columnUsage = iota // Use this column as a label
	COUNTER      columnUsage = iota // Use this column as a counter
	GAUGE        columnUsage = iota // Use this column as a gauge
	MAPPEDMETRIC columnUsage = iota // Use this column with the supplied mapping of text values
	DURATION     columnUsage = iota // This column should be interpreted as a text duration (and converted to milliseconds)
)

// Implement the yaml.Unmarshaller interface
func (cu *columnUsage) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var value string
	if err := unmarshal(&value); err != nil {
		return err
	}

	columnUsage, err := stringTocolumnUsage(value)
	if err != nil {
		return err
	}

	*cu = columnUsage
	return nil
}

// Groups metric maps under a shared set of labels
type MetricMapNamespace struct {
	labels         []string             // Label names for this namespace
	columnMappings map[string]MetricMap // Column mappings in this namespace
}

// Stores the OpenTelemetry metric description which a given column will be mapped
// to by the collector
type MetricMap struct {
	discard    bool                 // Should metric be discarded during mapping?
	counterMetric metric.Int64Counter
	gaugeMetric metric.Float64Gauge
	usage columnUsage
}

// User-friendly representation of a OpenTelemetry descriptor map
type ColumnMapping struct {
	usage       columnUsage `yaml:"usage"`
	description string      `yaml:"description"`
}

// Exporter collects Pgpool-II stats from the given server and exports
// them using the OpenTelemetry metrics package.
type Exporter struct {
	*otlpmetrichttp.Exporter
	duration     metric.Float64Gauge
	up           metric.Float64Gauge
	error        metric.Float64Gauge
	totalScrapes metric.Int64Counter
	mu          sync.Mutex // Mutex for thread-safe access
	metricMap    map[string]MetricMapNamespace
	DB           *sql.DB
	universalAttributes []attribute.KeyValue
	dsn string
	meter metric.Meter
}

var (
	metricMaps = map[string]map[string]ColumnMapping{
		"pool_nodes": {
			"hostname":          {LABEL, "Backend hostname"},
			"port":              {LABEL, "Backend port"},
			"role":              {LABEL, "Role (primary or standby)"},
			"status":            {GAUGE, "Backend node Status (1 for up or waiting, 0 for down or unused)"},
			"select_cnt":        {COUNTER, "SELECT statement counts issued to each backend"},
			"replication_delay": {GAUGE, "Replication delay"},
		},
		"pool_backend_stats": {
			"hostname":   {LABEL, "Backend hostname"},
			"port":       {LABEL, "Backend port"},
			"role":       {LABEL, "Role (primary or standby)"},
			"status":     {GAUGE, "Backend node Status (1 for up or waiting, 0 for down or unused)"},
			"select_cnt": {COUNTER, "SELECT statement counts issued to each backend"},
			"insert_cnt": {COUNTER, "INSERT statement counts issued to each backend"},
			"update_cnt": {COUNTER, "UPDATE statement counts issued to each backend"},
			"delete_cnt": {COUNTER, "DELETE statement counts issued to each backend"},
			"ddl_cnt":    {COUNTER, "DDL statement counts issued to each backend"},
			"other_cnt":  {COUNTER, "other statement counts issued to each backend"},
			"panic_cnt":  {COUNTER, "Panic message counts returned from backend"},
			"fatal_cnt":  {COUNTER, "Fatal message counts returned from backend)"},
			"error_cnt":  {COUNTER, "Error message counts returned from backend"},
		},
		"pool_health_check_stats": {
			"hostname":            {LABEL, "Backend hostname"},
			"port":                {LABEL, "Backend port"},
			"role":                {LABEL, "Role (primary or standby)"},
			"status":              {GAUGE, "Backend node Status (1 for up or waiting, 0 for down or unused)"},
			"total_count":         {GAUGE, "Number of health check count in total"},
			"success_count":       {GAUGE, "Number of successful health check count in total"},
			"fail_count":          {GAUGE, "Number of failed health check count in total"},
			"skip_count":          {GAUGE, "Number of skipped health check count in total"},
			"retry_count":         {GAUGE, "Number of retried health check count in total"},
			"average_retry_count": {GAUGE, "Number of average retried health check count in a health check session"},
			"max_retry_count":     {GAUGE, "Number of maximum retried health check count in a health check session"},
			"max_duration":        {GAUGE, "Maximum health check duration in Millie seconds"},
			"min_duration":        {GAUGE, "Minimum health check duration in Millie seconds"},
			"average_duration":    {GAUGE, "Average health check duration in Millie seconds"},
			// last_status_change
			// last_health_check
			// last_successful_health_check
			// last_skip_health_check
			// last_failed_health_check
		},
		// "pool_processes": {
		// 	"pool_pid": {DISCARD, "PID of Pgpool-II child processes"},
		// 	"database": {DISCARD, "Database name of the currently active backend connection"},
		// },
		// "pool_pools": {
		// 	"pool_pid": {DISCARD, "PID of Pgpool-II child processes"},
		// },
		"pool_cache": {
			"num_cache_hits":              {GAUGE, "The number of hits against the query cache"},
			"num_selects":                 {GAUGE, "The number of SELECT that did not hit against the query cache"},
			"cache_hit_ratio":             {GAUGE, "Query cache hit ratio"},
			"num_hash_entries":            {GAUGE, "Number of total hash entries"},
			"used_hash_entries":           {GAUGE, "Number of used hash entries"},
			"num_cache_entries":           {GAUGE, "Number of used cache entries"},
			"used_cache_entries_size":     {GAUGE, "Total size in bytes of used cache size"},
			"free_cache_entries_size":     {GAUGE, "Total size in bytes of free cache size"},
			"fragment_cache_entries_size": {GAUGE, "Total size in bytes of the fragmented cache"},
		},
	}
)

func makeDescMap(metricMaps map[string]map[string]ColumnMapping, meter metric.Meter) map[string]MetricMapNamespace {
	var metricMap = make(map[string]MetricMapNamespace)

	for metricNamespace, mappings := range metricMaps {
		thisMap := make(map[string]MetricMap)

		// Get the constant labels
		var variableLabels []string
		for columnName, columnMapping := range mappings {
			if columnMapping.usage == LABEL {
				variableLabels = append(variableLabels, columnName)
			}
		}

		for columnName, columnMapping := range mappings {
			// Determine how to convert the column based on its usage.
			switch columnMapping.usage {
			case DISCARD, LABEL:
				thisMap[columnName] = MetricMap{
					discard: true,
					usage: columnMapping.usage,
				}
			case COUNTER:
				counterMertic, err := meter.Int64Counter(
						fmt.Sprintf("%s_%s", metricNamespace, columnName),
						metric.WithDescription(columnMapping.description),
				)
				if err != nil {
					panic(err)
				}
				thisMap[columnName] = MetricMap{
					counterMetric: counterMertic,
					usage: columnMapping.usage,
				}
			case GAUGE:
				gaugeMetric, err := meter.Float64Gauge(
					fmt.Sprintf("%s_%s", metricNamespace, columnName),
					metric.WithDescription(columnMapping.description),
			)
				if err != nil {
					panic(err)
				}
				
				thisMap[columnName] = MetricMap{
					gaugeMetric: gaugeMetric,
					usage: columnMapping.usage,
				}
			}
		}

		metricMap[metricNamespace] = MetricMapNamespace{variableLabels, thisMap}
	}

	return metricMap
}

// Pgpool-II version
var pgpoolVersionRegex = regexp.MustCompile(`^((\d+)(\.\d+)(\.\d+)?)`)
var version42 = semver.MustParse("4.2.0")
var PgpoolSemver semver.Version

func NewCustomExporter(ctx context.Context, dsn string, attributes []attribute.KeyValue) (*Exporter, error) {
	exporter, err := otlpmetrichttp.New(ctx)
	if err != nil {
			return nil, err
	}

	db, err := getDBConn(dsn)

	// If pgpool is down on exporter startup, keep waiting for pgpool to be up
	for err != nil {
		log.Println("err: %s", err)
		log.Println("Sleeping for 5 seconds before trying to connect again")
					time.Sleep(5 * time.Second)

			db, err = getDBConn(dsn)
	}

	return &Exporter{
			Exporter: exporter,
			DB:        db,
			universalAttributes: attributes,
			dsn: dsn,
	}, nil
}

// Query within a namespace mapping and emit metrics. Returns fatal errors if
// the scrape fails, and a slice of errors if they were non-fatal.
func queryNamespaceMapping(ctx context.Context, db *sql.DB, namespace string, mapping MetricMapNamespace, universalAttributes []attribute.KeyValue) ([]error, error) {
	query := fmt.Sprintf("SHOW %s;", namespace)

	// Don't fail on a bad scrape of one metric
	rows, err := db.Query(query)
	if err != nil {
		return []error{}, errors.New(fmt.Sprintln("Error running query on database: ", namespace, err))
	}

	defer rows.Close()

	var columnNames []string
	columnNames, err = rows.Columns()
	if err != nil {
		return []error{}, errors.New(fmt.Sprintln("Error retrieving column list for: ", namespace, err))
	}

	// Make a lookup map for the column indices
	var columnIdx = make(map[string]int, len(columnNames))
	for i, n := range columnNames {
		columnIdx[n] = i
	}

	var columnData = make([]interface{}, len(columnNames))
	var scanArgs = make([]interface{}, len(columnNames))
	for i := range columnData {
		scanArgs[i] = &columnData[i]
	}

	nonfatalErrors := []error{}

	// Read from the result of "SHOW pool_pools"
	if namespace == "pool_pools" {

		// totalBackends := float64(0)
		// totalBackendsInUse := float64(0)

		// // pool_pid -> pool_id -> backend_id ->username -> database -> count
		// backendsInUse := make(map[string]map[string]map[string]map[string]map[string]float64)

		// totalBackendsByProcess := make(map[string]float64)

		// for rows.Next() {
		// 	err = rows.Scan(scanArgs...)
		// 	if err != nil {
		// 		return []error{}, errors.New(fmt.Sprintln("Error retrieving rows:", namespace, err))
		// 	}
		// 	var valueDatabase string
		// 	var valueUsername string
		// 	var valuePoolPid string
		// 	var valuePoolId string
		// 	var valueBackendId string
		// 	for idx, columnName := range columnNames {
		// 		switch columnName {
		// 		case "pool_pid":
		// 			valuePoolPid, _ = exp.dbToString(columnData[idx])
		// 		case "pool_id":
		// 			valuePoolId, _ = exp.dbToString(columnData[idx])
		// 		case "backend_id":
		// 			valueBackendId, _ = exp.dbToString(columnData[idx])
		// 		case "database":
		// 			valueDatabase, _ = exp.dbToString(columnData[idx])
		// 		case "username":
		// 			valueUsername, _ = exp.dbToString(columnData[idx])
		// 		}
		// 	}
		// 	if len(valuePoolPid) > 0 {
		// 		totalBackends++
		// 		totalBackendsByProcess[valuePoolPid]++
		// 	}
		// 	if len(valueUsername) > 0 {
		// 		totalBackendsInUse++
		// 		_, ok := backendsInUse[valuePoolPid]
		// 		if !ok {
		// 			backendsInUse[valuePoolPid] = make(map[string]map[string]map[string]map[string]float64)
		// 		}
		// 		_, ok = backendsInUse[valuePoolPid][valuePoolId]
		// 		if !ok {
		// 			backendsInUse[valuePoolPid][valuePoolId] = make(map[string]map[string]map[string]float64)
		// 		}
		// 		_, ok = backendsInUse[valuePoolPid][valuePoolId][valueBackendId]
		// 		if !ok {
		// 			backendsInUse[valuePoolPid][valuePoolId][valueBackendId] = make(map[string]map[string]float64)
		// 		}
		// 		_, ok = backendsInUse[valuePoolPid][valuePoolId][valueBackendId][valueUsername]
		// 		if !ok {
		// 			backendsInUse[valuePoolPid][valuePoolId][valueBackendId][valueUsername] = make(map[string]float64)
		// 		}
		// 		backendsInUse[valuePoolPid][valuePoolId][valueBackendId][valueUsername][valueDatabase]++
		// 	}
		// }

		// for poolPid, poolIds := range backendsInUse {
		// 	var usedProcessBackends float64

		// 	for poolId, backendIds := range poolIds {
		// 		for backendId, userNames := range backendIds {
		// 			for userName, dbNames := range userNames {
		// 				for dbName, count := range dbNames {

		// 					usedProcessBackends++
		// 					variableLabels := []string{"pool_pid", "pool_id", "backend_id", "username", "database"}
		// 					labels := []string{poolPid, poolId, backendId, userName, dbName}
		// 					ch <- prometheus.MustNewConstMetric(
		// 						prometheus.NewDesc(prometheus.BuildFQName("pgpool2", "", "backend_by_process_used"), "Number of backend connection slots in use", variableLabels, nil),
		// 						prometheus.GaugeValue,
		// 						count,
		// 						labels...,
		// 					)

		// 				}
		// 			}
		// 		}
		// 	}
		// 	variableLabels := []string{"pool_pid"}
		// 	labels := []string{poolPid}
		// 	ch <- prometheus.MustNewConstMetric(
		// 		prometheus.NewDesc(prometheus.BuildFQName("pgpool2", "", "backend_by_process_used_ratio"), "Number of backend connection slots in use", variableLabels, nil),
		// 		prometheus.GaugeValue,
		// 		usedProcessBackends/totalBackendsByProcess[poolPid],
		// 		labels...,
		// 	)
		// 	ch <- prometheus.MustNewConstMetric(
		// 		prometheus.NewDesc(prometheus.BuildFQName("pgpool2", "", "backend_by_process_total"), "Number of backend connection slots in use", variableLabels, nil),
		// 		prometheus.GaugeValue,
		// 		totalBackendsByProcess[poolPid],
		// 		labels...,
		// 	)
		// }

		// ch <- prometheus.MustNewConstMetric(
		// 	prometheus.NewDesc(prometheus.BuildFQName("pgpool2", "", "backend_total"), "Number of total possible backend connection slots", nil, nil),
		// 	prometheus.GaugeValue,
		// 	totalBackends,
		// )
		// ch <- prometheus.MustNewConstMetric(
		// 	prometheus.NewDesc(prometheus.BuildFQName("pgpool2", "", "backend_used"), "Number of backend connection slots in use", nil, nil),
		// 	prometheus.GaugeValue,
		// 	totalBackendsInUse,
		// )
		// ch <- prometheus.MustNewConstMetric(
		// 	prometheus.NewDesc(prometheus.BuildFQName("pgpool2", "", "backend_used_ratio"), "Ratio of backend connections in use to total backend connection slots", nil, nil),
		// 	prometheus.GaugeValue,
		// 	totalBackendsInUse/totalBackends,
		// )

		return nonfatalErrors, nil
	}

	// Read from the result of "SHOW pool_processes"
	if namespace == "pool_processes" {
		// frontendByUserDb := make(map[string]map[string]int)
		// var frontend_total float64
		// var frontend_used float64

		// for rows.Next() {
		// 	err = rows.Scan(scanArgs...)
		// 	if err != nil {
		// 		return []error{}, errors.New(fmt.Sprintln("Error retrieving rows:", namespace, err))
		// 	}
		// 	frontend_total++
		// 	// Loop over column names to find currently connected backend database
		// 	var valueDatabase string
		// 	var valueUsername string
		// 	for idx, columnName := range columnNames {
		// 		switch columnName {
		// 		case "database":
		// 			valueDatabase, _ = exp.dbToString(columnData[idx])
		// 		case "username":
		// 			valueUsername, _ = exp.dbToString(columnData[idx])
		// 		}
		// 	}
		// 	if len(valueDatabase) > 0 && len(valueUsername) > 0 {
		// 		frontend_used++
		// 		dbCount, ok := frontendByUserDb[valueUsername]
		// 		if !ok {
		// 			dbCount = map[string]int{valueDatabase: 0}
		// 		}
		// 		dbCount[valueDatabase]++
		// 		frontendByUserDb[valueUsername] = dbCount
		// 	}
		// }

		// variableLabels := []string{"username", "database"}
		// for userName, dbs := range frontendByUserDb {
		// 	for dbName, count := range dbs {
		// 		labels := []string{userName, dbName}
		// 		ch <- prometheus.MustNewConstMetric(
		// 			prometheus.NewDesc(prometheus.BuildFQName("pgpool2", "", "frontend_used"), "Number of used child processes", variableLabels, nil),
		// 			prometheus.GaugeValue,
		// 			float64(count),
		// 			labels...,
		// 		)
		// 	}
		// }

		// // Generate the metric for "pool_processes"
		// ch <- prometheus.MustNewConstMetric(
		// 	prometheus.NewDesc(prometheus.BuildFQName("pgpool2", "", "frontend_total"), "Number of total child processed", nil, nil),
		// 	prometheus.GaugeValue,
		// 	frontend_total,
		// )
		// ch <- prometheus.MustNewConstMetric(
		// 	prometheus.NewDesc(prometheus.BuildFQName("pgpool2", "", "frontend_used_ratio"), "Ratio of child processes to total processes", nil, nil),
		// 	prometheus.GaugeValue,
		// 	frontend_used/frontend_total,
		// )

		return nonfatalErrors, nil
	}

	for rows.Next() {
		err = rows.Scan(scanArgs...)
		if err != nil {
			return []error{}, errors.New(fmt.Sprintln("Error retrieving rows:", namespace, err))
		}

		// Get the label values for this row.
		var attributes []attribute.KeyValue
		for _, label := range mapping.labels {
			var labelValue, _ = dbToString(columnData[columnIdx[label]])
			attributes = append(attributes, attribute.String(label, labelValue))
		}

		attributes = append(attributes, universalAttributes...)

		// Loop over column names, and match to scan data.
		for idx, columnName := range columnNames {
			if metricMapping, ok := mapping.columnMappings[columnName]; ok {
				// Is this a metricy metric?
				if metricMapping.discard {
					continue
				}

				// If status column, convert string to int.
				if columnName == "status" {
					valueString, ok := dbToString(columnData[idx])
					if !ok {
						nonfatalErrors = append(nonfatalErrors, errors.New(fmt.Sprintln("Unexpected error parsing column: ", namespace, columnName, columnData[idx])))
						continue
					}
					value := parseStatusField(valueString)

					// Generate the metric
					metricMapping.gaugeMetric.Record(
						ctx,
						value,
						metric.WithAttributes(attributes...),
				)
					continue
				}

				value, ok := dbToFloat64(columnData[idx])
				if !ok {
					nonfatalErrors = append(nonfatalErrors, errors.New(fmt.Sprintln("Unexpected error parsing column: ", namespace, columnName, columnData[idx])))
					continue
				}

				if metricMapping.usage == COUNTER {
					// Generate the metric
					metricMapping.counterMetric.Add(
						ctx,
						int64(value),
						metric.WithAttributes(attributes...),
					)
				}

				if metricMapping.usage == GAUGE {
					// Generate the metric
					metricMapping.gaugeMetric.Record(
						ctx,
						value,
						metric.WithAttributes(attributes...),
				)
				}
			}
		}
	}
	return nonfatalErrors, nil
}

// Iterate through all the namespace mappings in the exporter and run their queries.
func queryNamespaceMappings(ctx context.Context, db *sql.DB, metricMap map[string]MetricMapNamespace, attributes []attribute.KeyValue) map[string]error {
	// Return a map of namespace -> errors
	namespaceErrors := make(map[string]error)

	for namespace, mapping := range metricMap {
		// pool_backend_stats and pool_health_check_stats can not be used before 4.1.
		if namespace == "pool_backend_stats" || namespace == "pool_health_check_stats" {
			if PgpoolSemver.LT(version42) {
				continue
			}
		}

		log.Println("Querying namespace: %s", namespace)
		nonFatalErrors, err := queryNamespaceMapping(ctx, db, namespace, mapping, attributes)
		// Serious error - a namespace disappeard
		if err != nil {
			namespaceErrors[namespace] = err
			log.Println("namespace disappeard: %s", err)
		}
		// Non-serious errors - likely version or parsing problems.
		if len(nonFatalErrors) > 0 {
			for _, err := range nonFatalErrors {
				log.Println("error parsing: %s", err.Error())
			}
		}
	}

	return namespaceErrors
}

// Export implements the export.MetricExporter interface.
func (e *Exporter) Export(ctx context.Context, res *metricdata.ResourceMetrics) error {
	var err error
	e.mu.Lock() // Lock to ensure safe access to totalScrapes
	defer e.mu.Unlock()

	startTime := time.Now() // Start the timer

	// Increment the export count
	e.totalScrapes.Add(ctx, 1)

	// Check connection availability and close the connection if it fails.
	if err = ping(e.DB); err != nil {
		log.Println("Error pinging Pgpool-IIg: %s", err)
		if cerr := e.DB.Close(); cerr != nil {
			log.Println("Error while closing non-pinging connection: %s", err)
		}
		log.Println("Reconnecting to Pgpool-II")
		e.DB, err = sql.Open("postgres", e.dsn)
		e.DB.SetMaxOpenConns(1)
		e.DB.SetMaxIdleConns(1)

		if err = ping(e.DB); err != nil {
			log.Println("Error pinging Pgpool-II: %s", err)
			if cerr := e.DB.Close(); cerr != nil {
				log.Println("Error while closing non-pinging connection: %s", err)
			}
			e.up.Record(
					ctx,
					0,
					metric.WithAttributes(e.universalAttributes...),
			)
			return nil
		}
	}

	e.up.Record(
		ctx,
		1,
		metric.WithAttributes(e.universalAttributes...),
)
	e.error.Record(
		ctx,
		0,
		metric.WithAttributes(e.universalAttributes...),
)

	errMap := queryNamespaceMappings(ctx, e.DB, e.metricMap, e.universalAttributes)
	if len(errMap) > 0 {
		log.Println("err: %s", errMap)
		e.error.Record(
			ctx,
			1,
			metric.WithAttributes(e.universalAttributes...),
	)
	}

	e.duration.Record(
		ctx,
		time.Since(startTime).Seconds(),
		metric.WithAttributes(e.universalAttributes...),
)

	// Log the metrics export count and duration
	log.Println("Exporting metrics...")

	// Call the original exporter
	return e.Exporter.Export(ctx, res)

}

// Shutdown performs any necessary cleanup.
func (e *Exporter) Shutdown(ctx context.Context) error {
	log.Println("Shutting down custom exporter.")
	return e.Exporter.Shutdown(ctx)
}

// SetMeter initializes the gauge metric using the provided meter.
func (e *Exporter) SetMeter(meter metric.Meter) {
	e.meter = meter
}

// SetMeter initializes the gauge metric using the provided meter.
func (e *Exporter) SetMetrics() {

	up, err := e.meter.Float64Gauge(
		"up",
		metric.WithDescription("Whether the Pgpool-II server is up (1 for yes, 0 for no)."),
	)

	if err != nil {
			panic(err)
	}

	duration, err := e.meter.Float64Gauge(
					"last_scrape_duration_seconds",
					metric.WithDescription("Duration of the last scrape of metrics from Pgpool-II."),
	)

	if err != nil {
			panic(err)
	}

	totalScrapes, err := e.meter.Int64Counter(
					"scrapes_total",
					metric.WithDescription("Total number of times Pgpool-II has been scraped for metrics."),
	)

	if err != nil {
			panic(err)
	}

	error, err := e.meter.Float64Gauge(
				"last_scrape_error",
				metric.WithDescription("Whether the last scrape of metrics from Pgpool-II resulted in an error (1 for error, 0 for success)."),
	)

	if err != nil {
			panic(err)
	}

	e.up = up
	e.duration = duration
	e.totalScrapes = totalScrapes
	e.error = error
	e.metricMap = makeDescMap(metricMaps, e.meter)
}

// Retrieve Pgpool-II version.
func QueryVersion(db *sql.DB) (semver.Version, error) {

	log.Println("Querying Pgpool-II version")

	versionRows, err := db.Query("SHOW POOL_VERSION;")
	if err != nil {
		return semver.Version{}, errors.New(fmt.Sprintln("Error querying SHOW POOL_VERSION:", err))
	}
	defer versionRows.Close()

	var columnNames []string
	columnNames, err = versionRows.Columns()
	if err != nil {
		return semver.Version{}, errors.New(fmt.Sprintln("Error retrieving column name for version:", err))
	}
	if len(columnNames) != 1 || columnNames[0] != "pool_version" {
		return semver.Version{}, errors.New(fmt.Sprintln("Error returning Pgpool-II version:", err))
	}

	var pgpoolVersion string
	for versionRows.Next() {
		err := versionRows.Scan(&pgpoolVersion)
		if err != nil {
			return semver.Version{}, errors.New(fmt.Sprintln("Error retrieving SHOW POOL_VERSION rows:", err))
		}
	}

	v := pgpoolVersionRegex.FindStringSubmatch(pgpoolVersion)
	if len(v) > 1 {
		log.Println("pgpool_version: %s", v[1])
		return semver.ParseTolerant(v[1])
	}

	return semver.Version{}, errors.New(fmt.Sprintln("Error retrieving Pgpool-II version:", err))
}

// Connect to Pgpool-II and run "SHOW POOL_VERSION;" to check connection availability.
func ping(db *sql.DB) error {

	rows, err := db.Query("SHOW POOL_VERSION;")
	if err != nil {
		return fmt.Errorf("error connecting to Pgpool-II: %s", err)
	}
	defer rows.Close()

	return nil
}


// Convert database.sql types to float64s for Prometheus consumption. Null types are mapped to NaN. string and []byte
// types are mapped as NaN and !ok
func dbToFloat64(t interface{}) (float64, bool) {
	switch v := t.(type) {
	case int64:
		return float64(v), true
	case float64:
		return v, true
	case time.Time:
		return float64(v.Unix()), true
	case []byte:
		// Try and convert to string and then parse to a float64
		strV := string(v)
		if strV == "-nan" || strV == "nan" {
			return math.NaN(), true
		}
		result, err := strconv.ParseFloat(strV, 64)
		if err != nil {
			return math.NaN(), false
		}
		return result, true
	case string:
		if v == "-nan" || v == "nan" {
			return math.NaN(), true
		}
		result, err := strconv.ParseFloat(v, 64)
		if err != nil {
			log.Println("Could not parse string: %s", err)
			return math.NaN(), false
		}
		return result, true
	case bool:
		if v {
			return 1.0, true
		}
		return 0.0, true
	case nil:
		return math.NaN(), true
	default:
		return math.NaN(), false
	}
}

// Convert database.sql to string for Prometheus labels. Null types are mapped to empty strings.
func dbToString(t interface{}) (string, bool) {
	switch v := t.(type) {
	case int64:
		return fmt.Sprintf("%v", v), true
	case float64:
		return fmt.Sprintf("%v", v), true
	case time.Time:
		return fmt.Sprintf("%v", v.Unix()), true
	case nil:
		return "", true
	case []byte:
		// Try and convert to string
		return string(v), true
	case string:
		return v, true
	case bool:
		if v {
			return "true", true
		}
		return "false", true
	default:
		return "", false
	}
}

// Convert bool to int.
func parseStatusField(value string) float64 {
	switch value {
	case "true", "up", "waiting":
		return 1.0
	case "false", "unused", "down":
		return 0.0
	}
	return 0.0
}


// Establish a new DB connection using dsn.
func getDBConn(dsn string) (*sql.DB, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	err = ping(db)
	if err != nil {
		return nil, err
	}

	return db, nil
}