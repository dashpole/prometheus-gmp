// Copyright 2026 Google LLC
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

package export

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	monitoring "cloud.google.com/go/monitoring/apiv3/v2"
	monitoring_pb "cloud.google.com/go/monitoring/apiv3/v2/monitoringpb"
	"github.com/go-kit/log"
	gax "github.com/googleapis/gax-go/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/textparse"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/oauth2"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

type mockMetricServer struct {
	monitoring_pb.UnimplementedMetricServiceServer
	mu           sync.Mutex
	lastStart    map[string]int64
	lastEnd      map[string]int64
	createErrors []error
	calls        int
}

func (s *mockMetricServer) CreateTimeSeries(ctx context.Context, req *monitoring_pb.CreateTimeSeriesRequest) (*emptypb.Empty, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	for _, ts := range req.GetTimeSeries() {
		for _, p := range ts.GetPoints() {
			key := fmt.Sprintf("%s/%v", ts.GetMetric().GetType(), ts.GetMetric().GetLabels())
			start := p.GetInterval().GetStartTime().GetSeconds()*1000 + int64(p.GetInterval().GetStartTime().GetNanos())/1000000
			end := p.GetInterval().GetEndTime().GetSeconds()*1000 + int64(p.GetInterval().GetEndTime().GetNanos())/1000000
			lastS, okS := s.lastStart[key]
			lastE, okE := s.lastEnd[key]
			if okS && okE && (start < lastS || (end <= lastE && start != lastS)) {
				err := status.Errorf(codes.InvalidArgument, "One or more of the points specified had an older start time than the most recent point")
				s.createErrors = append(s.createErrors, err)
				return nil, err
			}
			s.lastStart[key] = start
			s.lastEnd[key] = end
		}
	}
	return &emptypb.Empty{}, nil
}

type errorCapturingMetricClient struct {
	metricServiceClient
	calls int
	errs  []error
	mu    sync.Mutex
}

func (c *errorCapturingMetricClient) CreateTimeSeries(ctx context.Context, req *monitoring_pb.CreateTimeSeriesRequest, opts ...gax.CallOption) error {
	err := c.metricServiceClient.CreateTimeSeries(ctx, req, opts...)
	c.mu.Lock()
	c.calls++
	if err != nil {
		c.errs = append(c.errs, err)
	}
	c.mu.Unlock()
	return err
}

func histogramMetadataFunc(metric string) (MetricMetadata, bool) {
	if strings.HasSuffix(metric, "_bucket") || strings.HasSuffix(metric, "_sum") || strings.HasSuffix(metric, "_count") {
		return MetricMetadata{}, false
	}
	return MetricMetadata{
		Metric: metric,
		Type:   model.MetricTypeHistogram,
	}, true
}

func TestKongHistogramScrapeMonarchIntegration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	projectID := "dashpole-dev"
	metricName := fmt.Sprintf("kong_repro_%d", time.Now().UnixNano())

	var mu sync.Mutex
	var scrapeNum int

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		scrapeNum++
		n := scrapeNum
		mu.Unlock()

		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		if n == 1 {
			// Scrape 1 (scrapeTime1): Baseline observation without zero bucket le="50".
			w.Write([]byte(fmt.Sprintf(`
# HELP %s Kong latency
# TYPE %s histogram
%s_bucket{route="users",le="100"} 10
%s_bucket{route="users",le="+Inf"} 10
%s_count{route="users"} 10
%s_sum{route="users"} 500
`, metricName, metricName, metricName, metricName, metricName, metricName)))
		} else if n == 2 {
			// Scrape 2 (scrapeTime2): Simulates explicit Kong worker restart where cumulative counter resets.
			// GMP's getResetAdjusted sets reset timestamp to scrapeTime2 - 1ms.
			w.Write([]byte(fmt.Sprintf(`
# HELP %s Kong latency
# TYPE %s histogram
%s_bucket{route="users",le="100"} 2
%s_bucket{route="users",le="+Inf"} 2
%s_count{route="users"} 2
%s_sum{route="users"} 100
`, metricName, metricName, metricName, metricName, metricName, metricName)))
		} else if n == 3 {
			// Scrape 3 (scrapeTime3): Dynamic appearance of newly active zero bucket le="50"
			// and inconsistent count/sum due to mid-scrape coroutine yielding (count 20 uncoordinated with restart).
			// In unfixed exporter, le="50" arrives with !hasReset, skipping dist on Scrape 3.
			w.Write([]byte(fmt.Sprintf(`
# HELP %s Kong latency
# TYPE %s histogram
%s_bucket{route="users",le="50"} 1
%s_bucket{route="users",le="100"} 12
%s_bucket{route="users",le="+Inf"} 12
%s_count{route="users"} 20
%s_sum{route="users"} 600
`, metricName, metricName, metricName, metricName, metricName, metricName, metricName)))
		} else {
			// Scrape 4 (scrapeTime4): Subsequent normal observation.
			// Unfixed buildDistribution builds dist taking resetTimestamp from count (scrapeTime1 < scrapeTime2 - 1ms).
			// Monarch rejects write citing older start time error!
			w.Write([]byte(fmt.Sprintf(`
# HELP %s Kong latency
# TYPE %s histogram
%s_bucket{route="users",le="50"} 2
%s_bucket{route="users",le="100"} 14
%s_bucket{route="users",le="+Inf"} 14
%s_count{route="users"} 22
%s_sum{route="users"} 700
`, metricName, metricName, metricName, metricName, metricName, metricName, metricName)))
		}
	}))
	defer server.Close()

	useLive := os.Getenv("RUN_LIVE_MONARCH_TEST") == "1"
	var baseClient *monitoring.MetricClient
	var err error
	mockSrv := &mockMetricServer{
		lastStart: make(map[string]int64),
		lastEnd:   make(map[string]int64),
	}

	if useLive {
		accessToken := os.Getenv("GCM_ACCESS_TOKEN")
		require.NotEmpty(t, accessToken, "GCM_ACCESS_TOKEN must be set when RUN_LIVE_MONARCH_TEST=1")
		tokenSource := oauth2.StaticTokenSource(&oauth2.Token{AccessToken: accessToken})
		baseClient, err = monitoring.NewMetricClient(ctx, option.WithTokenSource(tokenSource))
		require.NoError(t, err)
	} else {
		lis, err := net.Listen("tcp", "localhost:0")
		require.NoError(t, err)
		grpcServer := grpc.NewServer()
		monitoring_pb.RegisterMetricServiceServer(grpcServer, mockSrv)
		go grpcServer.Serve(lis)
		defer grpcServer.Stop()

		baseClient, err = monitoring.NewMetricClient(ctx,
			option.WithEndpoint(lis.Addr().String()),
			option.WithoutAuthentication(),
			option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
		)
		require.NoError(t, err)
	}
	defer baseClient.Close()

	reg := prometheus.NewRegistry()
	opts := ExporterOpts{
		ProjectID: projectID,
		Location:  "us-central1",
	}
	opts.DefaultUnsetFields()
	exporter, err := New(ctx, log.NewNopLogger(), reg, opts, nil)
	require.NoError(t, err)
	exporter.ApplyConfig(&config.Config{})
	errClient := &errorCapturingMetricClient{metricServiceClient: baseClient}
	exporter.metricClient = errClient
	go exporter.Run()

	store := NewStorage(exporter)
	store.metadataFunc = histogramMetadataFunc

	runScrape := func(scrapeTime time.Time) error {
		resp, err := http.Get(server.URL)
		if err != nil {
			return err
		}
		defer resp.Body.Close()

		var buf bytes.Buffer
		buf.ReadFrom(resp.Body)

		p, err := textparse.New(buf.Bytes(), resp.Header.Get("Content-Type"), false, labels.NewSymbolTable())
		if err != nil {
			return err
		}

		appender := store.Appender(ctx)
		for {
			et, err := p.Next()
			if err != nil {
				break
			}
			switch et {
			case textparse.EntrySeries:
				_, timestamp, v := p.Series()
				var lset labels.Labels
				p.Metric(&lset)
				t := scrapeTime.UnixMilli()
				if timestamp != nil {
					t = *timestamp
				}
				appender.Append(0, lset, t, v)
			}
		}
		return appender.Commit()
	}

	startTime := time.Now()
	scrapeInterval := 100 * time.Millisecond
	if useLive {
		scrapeInterval = 6 * time.Second
	}

	// Scrape 1: Baseline observation.
	err = runScrape(startTime)
	require.NoError(t, err)

	time.Sleep(scrapeInterval)

	// Scrape 2: Simulates explicit Kong worker restart where cumulative counter resets.
	scrapeTime2 := startTime.Add(scrapeInterval)
	err = runScrape(scrapeTime2)
	require.NoError(t, err)

	time.Sleep(scrapeInterval)

	// Scrape 3: Dynamic appearance of zero bucket le="50" and mid-scrape yielding inconsistency.
	scrapeTime3 := scrapeTime2.Add(scrapeInterval)
	err = runScrape(scrapeTime3)
	require.NoError(t, err)

	time.Sleep(scrapeInterval)

	// Scrape 4: Subsequent normal observation.
	scrapeTime4 := scrapeTime3.Add(scrapeInterval)
	err = runScrape(scrapeTime4)
	require.NoError(t, err)

	time.Sleep(3 * time.Second)

	errClient.mu.Lock()
	calls := errClient.calls
	errs := append([]error(nil), errClient.errs...)
	errClient.mu.Unlock()

	t.Logf("CreateTimeSeries called %d times, errors: %v", calls, errs)
	assert.GreaterOrEqual(t, calls, 1, "CreateTimeSeries should be called at least once")
	assert.Empty(t, errs, "Expected Cloud Monitoring writes to succeed without start time rejection errors")
}
