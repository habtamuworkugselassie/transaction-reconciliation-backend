// k6 load test: API responsiveness while an import is running.
//
//   k6 run -e BASE_URL=http://localhost:8080 -e IMPORT_ID=<id of a running import> scripts/load-test.js
//
// Start a large import first (scripts/benchmark.sh or curl), then run this. The
// thresholds assert that latency-sensitive endpoints stay fast while the worker is
// saturating the CPU with risk scoring and the database with batch writes.
import http from 'k6/http';
import { check, sleep } from 'k6';

const BASE = __ENV.BASE_URL || 'http://localhost:8080';
const IMPORT_ID = __ENV.IMPORT_ID;

export const options = {
  scenarios: {
    status_polling: {
      executor: 'constant-vus',
      vus: 20,
      duration: '60s',
      exec: 'pollStatus',
    },
    readiness: {
      executor: 'constant-arrival-rate',
      rate: 50,
      timeUnit: '1s',
      duration: '60s',
      preAllocatedVUs: 10,
      exec: 'readiness',
    },
  },
  thresholds: {
    'http_req_duration{endpoint:status}': ['p(95)<200', 'p(99)<500'],
    'http_req_duration{endpoint:ready}': ['p(95)<100'],
    'http_req_failed': ['rate<0.01'],
  },
};

export function pollStatus() {
  if (!IMPORT_ID) {
    return;
  }
  const res = http.get(`${BASE}/v1/imports/${IMPORT_ID}`, { tags: { endpoint: 'status' } });
  check(res, { 'status 200': (r) => r.status === 200 });
  const summary = http.get(`${BASE}/v1/imports/${IMPORT_ID}/summary`, { tags: { endpoint: 'summary' } });
  check(summary, { 'summary 200': (r) => r.status === 200 });
  sleep(0.5);
}

export function readiness() {
  const res = http.get(`${BASE}/health/ready`, { tags: { endpoint: 'ready' } });
  check(res, { 'ready 200': (r) => r.status === 200 });
}
