# Kong Histogram Inconsistency Experiment & GMP Exporter Investigation

This directory contains standalone Lua verification harnesses, live Cloud Monitoring integration tests, and reproduction test documentation for investigating Cloud Monitoring time series rejection errors reported by enterprise Kong API gateway users on GKE (b/516519320).

---

## 1. Background & Problem Statement

Enterprise customers migrating high-throughput Kong API gateways to GKE production reported frequent metric write failures in Google Managed Prometheus (GMP) / Cloud Monitoring:

```
write for resource failed: Points must be written in order.
One or more of the points specified had an older start time than the most recent point.
```

Unlike full batch errors (`timeSeries[0-137]`), sparse index errors isolated the failures specifically to cumulative histogram metrics (e.g., `kong_upstream_latency_ms`).

---

## 2. Proving Kong Format Inconsistencies via Real Lua Execution (Phase 1)

Kong records observations in shared memory dictionaries (`ngx.shared.dict`). To prove the format violations without simulation translation risk, we archived Kong's actual Prometheus Lua module (`kong_prometheus.lua`) and authored a standalone Lua test harness (`verify_kong_lua.lua`) executing Kong's real library functions (`observe()` and `metric_data()`) directly against OpenResty's `ngx.shared.dict` runtime.

### Verifiable Reviewer Steps

Run the Lua verification harness inside an OpenResty Docker container:

```bash
(echo "package.preload['kong_prometheus'] = function()"; cat experimental/kong_experiment/kong_prometheus.lua; echo "end"; cat experimental/kong_experiment/verify_kong_lua.lua) | docker run --rm -i openresty/openresty:alpine /usr/local/openresty/luajit/bin/luajit -
```

### Verified Output & Mechanisms
```
=== Test 1: Omitted Zero Buckets ===
Dictionary keys after observe(val=150):
  upstream_latency_ms_bucket{route="users",le="250.000000"} = 1
  upstream_latency_ms_bucket{route="users",le="500.000000"} = 1
  upstream_latency_ms_bucket{route="users",le="Inf"} = 1
  upstream_latency_ms_count{route="users"} = 1
  upstream_latency_ms_sum{route="users"} = 150
-> SUCCESS: Zero-count buckets are omitted from shared memory by Kong library!

=== Test 2: Mid-Scrape Yielding Desynchronization ===
Stepping through metric_data() coroutine:
  [Step 1] coroutine yielded.
  [Step 2] coroutine yielded.
  [Step 3] coroutine yielded.
  -> Interjecting concurrent observe(val=20) during mid-scrape yield!
  [output] kong_upstream_latency_ms_bucket{route="users",le="250"} 1
  [output] kong_upstream_latency_ms_bucket{route="users",le="500"} 2
  [output] kong_upstream_latency_ms_count{route="users"} 2
-> SUCCESS: Verified coroutine yielding between key retrieval in Kong library!
```

1. **Omitted Zero Buckets (`_bucket`):** When `observe(150)` records latency, Kong only creates keys where `value <= bucket[i]` (`le="250"`, `le="500"`, `le="Inf"`). Zero-count buckets (`le="10"`, `le="25"`, `le="50"`, `le="100"`) are omitted from shared dictionary entirely. When lower latency arrives later, new series dynamically appear mid-stream.
2. **Mid-Scrape Yielding Desynchronization:** In `metric_data()`, keys are retrieved alphabetically (`_bucket` before `_count` before `_sum`) with `coroutine.yield()` called before each fetch. When `observe(20)` executes mid-scrape during Step 3, `le="250"` reflects count `1` while `le="500"` and `_count` reflect count `2`.

---

## 3. Triggering Cloud Monitoring Errors via Live Integration Test (Phase 2)

To confirm that Monarch rejects time series exhibiting Kong's counter desynchronization, we authored `kong_monarch_integration_test.go` (`TestKongMonarchLiveIntegration`).

### Verifiable Reviewer Steps

Run the live Cloud Monitoring integration test using active OAuth2 credentials:

```bash
RUN_LIVE_MONARCH_TEST=1 GCM_ACCESS_TOKEN=$(gcloud auth print-access-token) go test -v ./google/export -run TestKongMonarchLiveIntegration
```

### Verified Rejection Behavior
The test connects directly to live Cloud Monitoring (`monitoring.NewMetricClient`) in project `dashpole-dev`, writes an initial cumulative distribution sample (`startTime1`), waits 6 seconds to satisfy rate limits, and writes a second sample with an older start time (`startTime1 - 10s`). 

Monarch rejects the second write with:
```
CreateTimeSeries call failed: rpc error: code = InvalidArgument desc = One or more of the points specified had an older start time than the most recent point
```

---

## 4. Proposed GMP Exporter Normalization (Phase 3)

To ensure resilient ingestion without dropping distributions or corrupting reset timestamps, we patched `google/export/transform.go` and `google/export/series_cache.go`:

1. **Authoritative Reset Coordination:** `seriesCache` tracks established cumulative histogram reset timestamps from `_count` series in `histogramResets`.
2. **Dynamic Bucket Normalization (`getResetAdjustedBucket`):** When a bucket boundary (`_bucket`) arrives without prior tracking (`!hasReset`), if an authoritative reset timestamp was already established on an earlier scrape (`rt < t`), the bucket inherits the established reset timestamp and initializes baseline `resetValue = 0`.
3. **Leak-Free Map Cleanup:** `clear()` and `garbageCollect()` purge inactive cumulative histogram hashes from `histogramResets` during cache evictions.

---

## 5. Artifact Index

* `verify_kong_lua.lua`: Standalone Lua test harness executing Kong's real Prometheus library.
* `kong_prometheus.lua`: Kong Prometheus plugin library source (`prometheus.lua`).
* `../../google/export/kong_monarch_integration_test.go`: Live Cloud Monitoring integration test verifying Monarch start time rejections.
* `../../google/export/kong_histogram_test.go`: Table-driven unit test suite reproducing dynamic bucket appearance and counter desynchronization.
* `../../google/export/series_cache.go`: Updated series cache implementing normalization and map cleanup.
* `../../google/export/transform.go`: Updated distribution builder routing bucket samples to `getResetAdjustedBucket`.
