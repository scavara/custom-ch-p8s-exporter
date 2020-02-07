package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"net/http"

	"github.com/ClickHouse/clickhouse-go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	addr = flag.String("listen-address", ":2277", "The address to listen on for HTTP requests.")
)

var (
	aChMetricDurationHistogram = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "a_ch_metric_duration_seconds",
		Help:    "A ClickHouse Metric duration.",
		Buckets: []float64{60, 300, 7200}, //needs adjusting 
	},
		[]string{"a_ch_metric_label"},
	)
)

func init() {
	// Register the summary and the histogram with Prometheus's default registry.
	prometheus.MustRegister(aChMetricDurationHistogram)
	// Add Go module build info.
	prometheus.MustRegister(prometheus.NewBuildInfoCollector())
}

func main() {
	flag.Parse()
	connect, err := sql.Open("clickhouse", "tcp://ch_server_name:9000?username=ifany&compress=true&debug=true") //adjust
	checkErr(err)
	if err := connect.Ping(); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			fmt.Printf("[%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
		} else {
			fmt.Println(err)
		}
		return
	}

	go func() {
		checkErr(err)
		tx, err := connect.Begin()
		checkErr(err)
		checkErr(tx.Commit())
		//adjust the following statement accordingly
		query := `SELECT 
		properties.value
		FROM db.table
		WHERE ('some conditions') 
		GROUP BY properties.value`
		rows, err := connect.Query(query)
		checkErr(err)
		for rows.Next() {
			var (
				a_ch_metric_label string
				duration     float64
			)
			checkErr(rows.Scan(&a_ch_metric_label, &duration))
			//uncomment when debugging
			//log.Printf("a_ch_metric_label: %s, duration: %v", a_ch_metric_label, duration)
			aChMetricDurationHistogram.WithLabelValues(a_ch_metric_label).Observe(duration)
		}
	}()

	// Expose the registered metrics via HTTP.
	http.Handle("/metrics", promhttp.HandlerFor(
		prometheus.DefaultGatherer,
		promhttp.HandlerOpts{
			// Opt into OpenMetrics to support exemplars.
			EnableOpenMetrics: true,
		},
	))
	log.Fatal(http.ListenAndServe(*addr, nil))
}

func checkErr(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
