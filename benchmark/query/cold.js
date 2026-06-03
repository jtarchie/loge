// Cold time-window queries: targets the OLDEST data (most likely rotated to
// Tigris), so reads come back over HTTP range GETs. setup() reads /api/v1/stats
// to find the catalog's min_timestamp and queries the first window of data.
// Compare latency + Tigris GET count to hot.js.
// Run: BASE=https://loge-bench.fly.dev k6 run benchmark/query/cold.js
import http from "k6/http";
import { check } from "k6";

const BASE = __ENV.BASE || "http://localhost:6500";
const WINDOW_NS = Number(__ENV.WINDOW_MIN || 5) * 60 * 1e9;

export const options = {
  vus: Number(__ENV.VUS || 20),
  duration: __ENV.DURATION || "30s",
};

export function setup() {
  const res = http.get(`${BASE}/api/v1/stats`);
  const stats = res.json();
  return { min: stats.min_timestamp, remote: stats.segments_remote };
}

export default function (data) {
  const start = data.min;
  const body = JSON.stringify({ start, end: start + WINDOW_NS, limit: 100 });

  const res = http.post(`${BASE}/api/v1/query`, body, {
    headers: { "Content-Type": "application/json" },
  });
  check(res, {
    "status 200": (r) => r.status === 200,
    "remote segments exist": () => data.remote > 0,
  });
}
