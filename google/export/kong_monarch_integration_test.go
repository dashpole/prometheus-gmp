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

package export

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	monitoring "cloud.google.com/go/monitoring/apiv3/v2"
	monitoring_pb "cloud.google.com/go/monitoring/apiv3/v2/monitoringpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/oauth2"
	"google.golang.org/api/option"
	distribution_pb "google.golang.org/genproto/googleapis/api/distribution"
	metric_pb "google.golang.org/genproto/googleapis/api/metric"
	monitoredres_pb "google.golang.org/genproto/googleapis/api/monitoredres"
	timestamp_pb "google.golang.org/protobuf/types/known/timestamppb"
)

func TestKongMonarchLiveIntegration(t *testing.T) {
	if os.Getenv("RUN_LIVE_MONARCH_TEST") == "" {
		t.Skip("skipping live Cloud Monitoring integration test unless RUN_LIVE_MONARCH_TEST is set")
	}

	ctx := context.Background()
	var opts []option.ClientOption
	if token := os.Getenv("GCM_ACCESS_TOKEN"); token != "" {
		opts = append(opts, option.WithTokenSource(oauth2.StaticTokenSource(&oauth2.Token{AccessToken: token})))
	}
	client, err := monitoring.NewMetricClient(ctx, opts...)
	require.NoError(t, err)
	defer client.Close()

	projectID := "dashpole-dev"
	projectName := fmt.Sprintf("projects/%s", projectID)
	now := time.Now()
	startTime1 := now.Add(-1 * time.Minute)

	metricType := "custom.googleapis.com/test/kong_upstream_latency"
	resource := &monitoredres_pb.MonitoredResource{
		Type: "global",
		Labels: map[string]string{
			"project_id": projectID,
		},
	}

	// Scrape 1: Normal initial observation establishing baseline start time (startTime1)
	ts1 := &monitoring_pb.TimeSeries{
		MetricKind: metric_pb.MetricDescriptor_CUMULATIVE,
		ValueType:  metric_pb.MetricDescriptor_DISTRIBUTION,
		Metric: &metric_pb.Metric{
			Type: metricType,
			Labels: map[string]string{
				"route": "users",
			},
		},
		Resource: resource,
		Points: []*monitoring_pb.Point{
			{
				Interval: &monitoring_pb.TimeInterval{
					StartTime: timestamp_pb.New(startTime1),
					EndTime:   timestamp_pb.New(now),
				},
				Value: &monitoring_pb.TypedValue{
					Value: &monitoring_pb.TypedValue_DistributionValue{
						DistributionValue: &distribution_pb.Distribution{
							Count:        10,
							BucketCounts: []int64{10},
							BucketOptions: &distribution_pb.Distribution_BucketOptions{
								Options: &distribution_pb.Distribution_BucketOptions_ExplicitBuckets{
									ExplicitBuckets: &distribution_pb.Distribution_BucketOptions_Explicit{
										Bounds: []float64{100},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	err = client.CreateTimeSeries(ctx, &monitoring_pb.CreateTimeSeriesRequest{
		Name:       projectName,
		TimeSeries: []*monitoring_pb.TimeSeries{ts1},
	})
	require.NoError(t, err, "First CreateTimeSeries call should succeed")

	// Wait 6 seconds to satisfy rate limiting (max 1 point per 5 seconds per series)
	time.Sleep(6 * time.Second)

	// Scrape 2: Kong mid-scrape counter drop / worker desynchronization causes GMP's getResetAdjusted
	// to reset start time to T - 1. If start time moves older or misaligns, Monarch rejects it.
	startTimeOlder := startTime1.Add(-10 * time.Second)
	endTime2 := time.Now()

	ts2 := &monitoring_pb.TimeSeries{
		MetricKind: metric_pb.MetricDescriptor_CUMULATIVE,
		ValueType:  metric_pb.MetricDescriptor_DISTRIBUTION,
		Metric: &metric_pb.Metric{
			Type: metricType,
			Labels: map[string]string{
				"route": "users",
			},
		},
		Resource: resource,
		Points: []*monitoring_pb.Point{
			{
				Interval: &monitoring_pb.TimeInterval{
					StartTime: timestamp_pb.New(startTimeOlder),
					EndTime:   timestamp_pb.New(endTime2),
				},
				Value: &monitoring_pb.TypedValue{
					Value: &monitoring_pb.TypedValue_DistributionValue{
						DistributionValue: &distribution_pb.Distribution{
							Count:        15,
							BucketCounts: []int64{15},
							BucketOptions: &distribution_pb.Distribution_BucketOptions{
								Options: &distribution_pb.Distribution_BucketOptions_ExplicitBuckets{
									ExplicitBuckets: &distribution_pb.Distribution_BucketOptions_Explicit{
										Bounds: []float64{100},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	err = client.CreateTimeSeries(ctx, &monitoring_pb.CreateTimeSeriesRequest{
		Name:       projectName,
		TimeSeries: []*monitoring_pb.TimeSeries{ts2},
	})
	require.Error(t, err, "Second CreateTimeSeries call with older start time should fail")
	assert.ErrorContains(t, err, "older start time than the most recent point")
}
