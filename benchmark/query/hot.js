// Hot time-window queries: a recent window that should resolve from LOCAL
// segments only (no S3 reads). Compare its latency to cold.js.
// Run: BASE=https://loge-bench.fly.dev k6 run benchmark/query/hot.js
import http from "k6/http";
import { check } from "k6";

const BASE = __ENV.BASE || "http://localhost:6500";
const WINDOW_NS = Number(__ENV.WINDOW_MIN || 5) * 60 * 1e9;

export const options = {
  vus: Number(__ENV.VUS || 20),
  duration: __ENV.DURATION || "30s",
};

export default function () {
  const now = Date.now() * 1e6; // ms -> ns (approx; loses sub-ms precision, fine for a window)
  const body = JSON.stringify({ start: now - WINDOW_NS, end: now, limit: 100 });

  const res = http.post(`${BASE}/api/v1/query`, body, {
    headers: { "Content-Type": "application/json" },
  });
  check(res, { "status 200": (r) => r.status === 200 });
}
