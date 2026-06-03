// Keyword / FTS queries: substring matches that exercise the trigram
// line_search index on compacted/remote segments (and the LIKE fallback on fresh
// flush files). Mixes common phrases with a needle-in-haystack lookup.
// Run: BASE=https://loge-bench.fly.dev k6 run benchmark/query/keyword.js
import http from "k6/http";
import { check } from "k6";

const BASE = __ENV.BASE || "http://localhost:6500";

// Real words/paths the generator emits, plus a rare-ish one for selectivity.
const needles = [
  "connection refused",
  "timeout waiting for upstream",
  "payment authorized",
  "rate limit exceeded",
  "/checkout",
  "/api/v1/orders",
];

export const options = {
  vus: Number(__ENV.VUS || 20),
  duration: __ENV.DURATION || "30s",
};

export default function () {
  const line = needles[Math.floor(Math.random() * needles.length)];
  const body = JSON.stringify({ line, limit: 100 });

  const res = http.post(`${BASE}/api/v1/query`, body, {
    headers: { "Content-Type": "application/json" },
  });
  check(res, { "status 200": (r) => r.status === 200 });
}
