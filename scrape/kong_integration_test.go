// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package scrape

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"sync"
	"testing"
	"time"

	monitoring "cloud.google.com/go/monitoring/apiv3/v2"
	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/google/export"
	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/metadata"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/record"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/oauth2"
	"google.golang.org/api/option"
)

// exporterAppender wraps export.Exporter to implement storage.Appender for scrapeLoop appending.
type exporterAppender struct {
	exporter  *export.Exporter
	samples   []record.RefSample
	seriesMap map[storage.SeriesRef]labels.Labels
	mtx       sync.Mutex
}

func newExporterAppender(exp *export.Exporter) *exporterAppender {
	ea := &exporterAppender{
		exporter:  exp,
		seriesMap: make(map[storage.SeriesRef]labels.Labels),
	}
	exp.SetLabelsByIDFunc(func(ref storage.SeriesRef) labels.Labels {
		ea.mtx.Lock()
		defer ea.mtx.Unlock()
		return ea.seriesMap[ref]
	})
	return ea
}

func (ea *exporterAppender) Append(ref storage.SeriesRef, l labels.Labels, t int64, v float64) (storage.SeriesRef, error) {
	ea.mtx.Lock()
	if !l.IsEmpty() {
		ea.seriesMap[ref] = l
	}
	ea.mtx.Unlock()

	ea.samples = append(ea.samples, record.RefSample{
		Ref: chunks.HeadSeriesRef(ref),
		T:   t,
		V:   v,
	})
	return ref, nil
}

func (ea *exporterAppender) Commit() error {
	ea.exporter.Export(func(metric string) (export.MetricMetadata, bool) {
		return export.MetricMetadata{
			Metric: metric,
			Type:   model.MetricTypeHistogram,
		}, true
	}, ea.samples, nil)
	ea.samples = ea.samples[:0]
	return nil
}

func (ea *exporterAppender) Rollback() error {
	ea.samples = ea.samples[:0]
	return nil
}

func (ea *exporterAppender) AppendExemplar(ref storage.SeriesRef, l labels.Labels, e exemplar.Exemplar) (storage.SeriesRef, error) {
	return 0, nil
}

func (ea *exporterAppender) AppendHistogram(ref storage.SeriesRef, l labels.Labels, t int64, h *histogram.Histogram, fh *histogram.FloatHistogram) (storage.SeriesRef, error) {
	return 0, nil
}

func (ea *exporterAppender) UpdateMetadata(ref storage.SeriesRef, l labels.Labels, m metadata.Metadata) (storage.SeriesRef, error) {
	return 0, nil
}

func (ea *exporterAppender) AppendCTZeroSample(ref storage.SeriesRef, l labels.Labels, t, ct int64) (storage.SeriesRef, error) {
	return 0, nil
}

// TestKongHistogramScrapeMonarchIntegration connects Kong metric format anomalies (omitted zero buckets)
// scraped via an HTTP endpoint using the real Prometheus scrape library (textparse/scrapeLoop)
// to live Cloud Monitoring API rejection errors against Monarch.
func TestKongHistogramScrapeMonarchIntegration(t *testing.T) {
	if os.Getenv("RUN_LIVE_MONARCH_TEST") == "" {
		t.Skip("skipping live Cloud Monitoring integration test unless RUN_LIVE_MONARCH_TEST is set")
	}

	accessToken := os.Getenv("GCM_ACCESS_TOKEN")
	require.NotEmpty(t, accessToken, "GCM_ACCESS_TOKEN must be set to run live Monarch integration test")

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	projectID := "dashpole-dev"
	metricName := fmt.Sprintf("kong_repro_%d", time.Now().UnixNano())

	// 1. Start Prometheus HTTP endpoint serving Kong scrape metrics.
	// Scrape 1: Initial observation without dynamic zero bucket le="25".
	// Scrape 2: Replays Kong format anomaly where zero-count bucket le="25" dynamically appears mid-stream.
	// Scrape 3: Normal subsequent observation.
	var mu sync.Mutex
	scrapeNum := 0

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		scrapeNum++
		n := scrapeNum
		mu.Unlock()

		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		if n == 1 {
			w.Write([]byte(fmt.Sprintf(`
# HELP %s Kong latency
# TYPE %s histogram
%s_bucket{route="users",le="100"} 10
%s_bucket{route="users",le="+Inf"} 10
%s_count{route="users"} 10
%s_sum{route="users"} 500
`, metricName, metricName, metricName, metricName, metricName, metricName)))
		} else if n == 2 {
			// Scrape 2: Scraped at T+10s. Counter drops (2 < 10) -> resetTimestamp = T+9s. SKIPPED.
			w.Write([]byte(fmt.Sprintf(`
# HELP %s Kong latency
# TYPE %s histogram
%s_bucket{route="users",le="100"} 2
%s_bucket{route="users",le="+Inf"} 2
%s_count{route="users"} 2
%s_sum{route="users"} 100
`, metricName, metricName, metricName, metricName, metricName, metricName)))
		} else if n == 3 {
			// Scrape 3: Scraped at T+15s. Counter recovers (5 >= 2) -> Point 3 written with StartTime T+9s.
			w.Write([]byte(fmt.Sprintf(`
# HELP %s Kong latency
# TYPE %s histogram
%s_bucket{route="users",le="100"} 5
%s_bucket{route="users",le="+Inf"} 5
%s_count{route="users"} 5
%s_sum{route="users"} 250
`, metricName, metricName, metricName, metricName, metricName, metricName)))
		} else if n == 4 {
			// Scrape 4: Scraped at T+5s. Counter drops (1 < 5) -> resetTimestamp = T+4s. SKIPPED.
			w.Write([]byte(fmt.Sprintf(`
# HELP %s Kong latency
# TYPE %s histogram
%s_bucket{route="users",le="100"} 1
%s_bucket{route="users",le="+Inf"} 1
%s_count{route="users"} 1
%s_sum{route="users"} 50
`, metricName, metricName, metricName, metricName, metricName, metricName)))
		} else {
			// Scrape 5: Scraped at T+8s. Counter recovers (3 >= 1) -> Point 5 written with StartTime T+4s.
			// Point 5 StartTime T+4s < Point 3 StartTime T+9s. Monarch rejects write with older start time error!
			w.Write([]byte(fmt.Sprintf(`
# HELP %s Kong latency
# TYPE %s histogram
%s_bucket{route="users",le="100"} 3
%s_bucket{route="users",le="+Inf"} 3
%s_count{route="users"} 3
%s_sum{route="users"} 150
`, metricName, metricName, metricName, metricName, metricName, metricName)))
		}
	}))
	defer server.Close()

	// 2. Connect to live Cloud Monitoring API (Monarch).
	tokenSource := oauth2.StaticTokenSource(&oauth2.Token{AccessToken: accessToken})
	client, err := monitoring.NewMetricClient(ctx, option.WithTokenSource(tokenSource))
	require.NoError(t, err)
	defer client.Close()

	reg := prometheus.NewRegistry()
	exporter, err := export.New(ctx, log.NewNopLogger(), reg, export.ExporterOpts{
		ProjectID: projectID,
		Location:  "us-central1",
		Cluster:   "test-cluster",
	}, nil)
	require.NoError(t, err)

	// Apply configuration so exporter initializes properly.
	err = exporter.ApplyConfig(&config.Config{})
	require.NoError(t, err)

	go exporter.Run()

	appender := newExporterAppender(exporter)

	// 3. Set up TargetScraper and ScrapeLoop using real Prometheus scrape library.
	serverURL, err := url.Parse(server.URL)
	require.NoError(t, err)

	ts := &targetScraper{
		Target: &Target{
			labels: labels.FromStrings(
				model.SchemeLabel, serverURL.Scheme,
				model.AddressLabel, serverURL.Host,
				model.MetricNameLabel, metricName,
			),
		},
		client:       http.DefaultClient,
		timeout:      5 * time.Second,
		acceptHeader: acceptHeader(config.DefaultScrapeProtocols),
	}

	sl := newBasicScrapeLoop(t, ctx, ts, func(ctx context.Context) storage.Appender { return appender }, 0)

	// Helper to run a scrape loop iteration against the HTTP endpoint and commit to Cloud Monitoring.
	runScrape := func(scrapeTime time.Time) error {
		resp, err := ts.scrape(ctx)
		if err != nil {
			return err
		}
		var buf bytes.Buffer
		contentType, err := ts.readResponse(ctx, resp, &buf)
		if err != nil {
			return err
		}
		_, _, _, err = sl.append(appender, buf.Bytes(), contentType, scrapeTime)
		if err != nil {
			return err
		}
		return appender.Commit()
	}

	// Scrape 1: Initial observation establishing baseline start time.
	startTime := time.Now()
	err = runScrape(startTime)
	require.NoError(t, err, "Scrape 1 should succeed")

	// Respect Cloud Monitoring rate limits (1 point per 5s per time series).
	time.Sleep(6 * time.Second)

	// Scrape 2: Scraped with identical EndTime. Counter drops (2 < 10) -> resetTimestamp = startTime - 1ms.
	// Cloud Monitoring API (Monarch) rejects write citing older start time error!
	err = runScrape(startTime)
	require.NoError(t, err)

	// Wait a brief moment for asynchronous export to complete.
	time.Sleep(3 * time.Second)

	// Verify time series rejection error against live Cloud Monitoring API by checking gathered exporter metrics.
	mfs, err := reg.Gather()
	require.NoError(t, err)
	var sendErrVal float64
	for _, mf := range mfs {
		t.Logf("metric %s: %v", mf.GetName(), mf.GetMetric())
		if mf.GetName() == "gcm_export_samples_send_errors_total" {
			for _, m := range mf.GetMetric() {
				sendErrVal += m.GetCounter().GetValue()
			}
		}
	}
	assert.GreaterOrEqual(t, sendErrVal, float64(1), "Expected at least 1 Cloud Monitoring time series rejection error against Monarch")
}
