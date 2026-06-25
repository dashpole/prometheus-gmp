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

## 3. Triggering Cloud Monitoring Errors via Scrape Integration Test (Phase 2)

To confirm that Monarch rejects time series exhibiting Kong's counter desynchronization and dynamic bucket appearance when scraped via standard Prometheus libraries, we authored `kong_scrape_integration_test.go` (`TestKongHistogramScrapeMonarchIntegration`).

### Verifiable Reviewer Steps

By default (`RUN_LIVE_MONARCH_TEST=0`), the integration test runs against a local mock validation server with 100ms scrape intervals for rapid execution (~0.5s). To run against real live Cloud Monitoring API using active OAuth2 credentials (with 6-second rate limit spacing):

```bash
RUN_LIVE_MONARCH_TEST=1 GCM_ACCESS_TOKEN=$(gcloud auth print-access-token) go test -v ./google/export -run TestKongHistogramScrapeMonarchIntegration
```

### Verified Rejection Behavior & Text Format Inputs
The test sets up an HTTP server serving Prometheus text format metrics, parses them via the real Prometheus `textparse` library, and ingests them into GMP's custom `scrapeAppender` and `Exporter` pipeline connected to Cloud Monitoring in project `dashpole-dev`.

1. **Scrape 1 (`startTime`):** Establishes baseline cumulative histogram tracking.
```text
# HELP kong_repro_123 Kong latency
# TYPE kong_repro_123 histogram
kong_repro_123_bucket{route="users",le="100"} 10
kong_repro_123_bucket{route="users",le="+Inf"} 10
kong_repro_123_count{route="users"} 10
kong_repro_123_sum{route="users"} 500
```

2. **Scrape 2 (`startTime + scrapeInterval`):** Normal subsequent observation. Ingested successfully into Cloud Monitoring with StartTime = `startTime`, EndTime = `scrapeTime2`.
```text
# HELP kong_repro_123 Kong latency
# TYPE kong_repro_123 histogram
kong_repro_123_bucket{route="users",le="100"} 20
kong_repro_123_bucket{route="users",le="+Inf"} 20
kong_repro_123_count{route="users"} 20
kong_repro_123_sum{route="users"} 1000
```

3. **Scrape 3 (at identical EndTime `scrapeTime2`):** Replays Kong format anomaly where mid-scrape yielding causes `_count` to drop relative to the prior scrape (`2 < 20`).
```text
# HELP kong_repro_123 Kong latency
# TYPE kong_repro_123 histogram
kong_repro_123_bucket{route="users",le="100"} 2
kong_repro_123_bucket{route="users",le="+Inf"} 2
kong_repro_123_count{route="users"} 2
kong_repro_123_sum{route="users"} 100
```

When GMP's unfixed `getResetAdjusted` processes Scrape 3, because `v < lastValue` (`2 < 20`), it resets the start timestamp to `scrapeTime2 - 1ms`. When sent to Cloud Monitoring / Monarch, Monarch rejects the write with:
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
* `../../google/export/kong_scrape_integration_test.go`: Scrape integration test parsing Prometheus text format inputs and verifying Monarch start time rejections.
* `../../google/export/kong_monarch_integration_test.go`: Direct Monarch client integration test verifying interval start time regressions.
* `../../google/export/kong_histogram_test.go`: Table-driven unit test suite reproducing dynamic bucket appearance and counter desynchronization.
* `../../google/export/series_cache.go`: Updated series cache implementing normalization and map cleanup.
* `../../google/export/transform.go`: Updated distribution builder routing bucket samples to `getResetAdjustedBucket`.
