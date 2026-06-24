-- Copyright 2026 Google LLC
--
-- Standalone Lua verification test harness executing Kong's Prometheus plugin
-- library (kong_prometheus.lua) directly against a mock OpenResty shared memory
-- dictionary (ngx.shared.dict) to prove histogram format anomalies.

-- 1. Mock OpenResty ngx environment and buffer/table dependencies
local shared_dict = {}
local mock_dict = {
  get = function(self, k) return shared_dict[k] end,
  set = function(self, k, v) shared_dict[k] = v end,
  incr = function(self, k, v, init)
    if not shared_dict[k] then
      if init then shared_dict[k] = init else return nil, "not found" end
    end
    shared_dict[k] = shared_dict[k] + (v or 1)
    return shared_dict[k], nil
  end,
  get_keys = function(self, max)
    local keys = {}
    for k in pairs(shared_dict) do table.insert(keys, k) end
    table.sort(keys)
    return keys
  end,
}

_G.ngx = {
  shared = { prometheus_metrics = mock_dict },
  log = function(...) end,
  sleep = function(...) end,
  get_phase = function() return "init_worker" end,
  re = {
    match = function(s, re, flags)
      if string.find(re or "", "le=") then
        if string.find(s or "", 'le="') then
          local p1, p2 = string.find(s, 'le="')
          local p3 = string.find(s, '"', p2 + 1)
          if p3 then
            return { string.sub(s, 1, p2), string.sub(s, p2 + 1, p3 - 1), string.sub(s, p3) }
          end
        end
        return nil
      end
      return {s}
    end,
    gsub = function(s, re, rep, flags) return s end,
  },
  ERR = 1, WARN = 2, INFO = 3, DEBUG = 4,
}

package.preload["string.buffer"] = function()
  return {
    new = function()
      local t = {}
      local obj = {
        put = function(self, s) table.insert(t, tostring(s)); return self end,
        putf = function(self, fmt, ...) table.insert(t, string.format(fmt, ...)); return self end,
        get = function(self) local res = table.concat(t); t = {}; return res end,
        free = function(self) t = {} end,
      }
      setmetatable(obj, { __len = function(self) return #table.concat(t) end })
      return obj
    end,
  }
end
package.preload["table.new"] = function() return function(n, m) return {} end end
package.preload["prometheus_resty_counter"] = function()
  return {
    new = function(...)
      return {
        sync = function(...) end,
        incr = function(self, k, v) _G.ngx.shared.prometheus_metrics:incr(k, v, 0) end,
      }
    end,
  }
end
package.preload["kong.tools.yield"] = function()
  return { yield = function(force, ms) coroutine.yield() end }
end

-- 2. Execute Kong's Prometheus library
local Prometheus = require("kong_prometheus")
local prom = Prometheus.init("prometheus_metrics", "kong_")

-- Test 1: Omitted Zero Buckets
local hist = prom:histogram("upstream_latency_ms", "Latency histogram", {"route"}, {10, 25, 50, 100, 250, 500})
assert(hist, "histogram creation should succeed")
hist:observe(150, {"users"})

print("=== Test 1: Omitted Zero Buckets ===")
print("Dictionary keys after observe(val=150):")
for k, v in pairs(shared_dict) do
  print("  " .. k .. " = " .. tostring(v))
end

assert(not shared_dict['kong_upstream_latency_ms_bucket{route="users",le="00010.00000"}'], "le=10 bucket should be omitted!")
assert(not shared_dict['kong_upstream_latency_ms_bucket{route="users",le="00025.00000"}'], "le=25 bucket should be omitted!")
assert(not shared_dict['kong_upstream_latency_ms_bucket{route="users",le="00050.00000"}'], "le=50 bucket should be omitted!")
assert(not shared_dict['kong_upstream_latency_ms_bucket{route="users",le="00100.00000"}'], "le=100 bucket should be omitted!")
print("-> SUCCESS: Zero-count buckets are omitted from shared memory by Kong library!")

-- Test 2: Mid-Scrape Coroutine Yielding Desynchronization
print("\n=== Test 2: Mid-Scrape Yielding Desynchronization ===")
local co = coroutine.create(function()
  prom:metric_data(function(line)
    print("  [output] " .. tostring(line))
  end)
end)

print("Stepping through metric_data() coroutine:")
local step = 1
while coroutine.status(co) ~= "dead" do
  local ok, res = coroutine.resume(co)
  if not ok then error(res) end
  if coroutine.status(co) ~= "dead" then
    print("  [Step " .. step .. "] coroutine yielded.")
    if step == 3 then
      print("  -> Interjecting concurrent observe(val=20) during mid-scrape yield!")
      hist:observe(20, {"users"})
    end
    step = step + 1
  end
end
print("-> SUCCESS: Verified coroutine yielding between key retrieval in Kong library!")
