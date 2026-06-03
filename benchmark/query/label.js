// Label-filtered queries: json_extract matching + a regex matcher.
// Run: BASE=https://loge-bench.fly.dev k6 run benchmark/query/label.js
import http from "k6/http";
import { check } from "k6";

const BASE = __ENV.BASE || "http://localhost:6500";
const apps = ["checkout", "auth", "payments", "catalog", "search", "gateway"];

export const options = {
  vus: Number(__ENV.VUS || 20),
  duration: __ENV.DURATION || "30s",
};

export default function () {
  const body = JSON.stringify({
    matchers: [
      { name: "app", value: apps[Math.floor(Math.random() * apps.length)], type: "=" },
      { name: "level", value: "err.*", type: "=~" },
    ],
    limit: 100,
  });

  const res = http.post(`${BASE}/api/v1/query`, body, {
    headers: { "Content-Type": "application/json" },
  });
  check(res, { "status 200": (r) => r.status === 200 });
}
