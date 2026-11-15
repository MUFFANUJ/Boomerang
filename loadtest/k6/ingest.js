// k6 load test for the public ingestion API.
//
//   docker run --rm -i --network boomerang_default -e BASE_URL=http://gateway \
//     grafana/k6:0.55.0 run --summary-export=/dev/stdout - < loadtest/k6/ingest.js
//
// Ramps the arrival rate from START_RATE to PEAK_RATE events/s, holds, and checks latency SLOs.
import http from 'k6/http';
import { check } from 'k6';
import { Counter } from 'k6/metrics';

const BASE = __ENV.BASE_URL || 'http://localhost:8080';
const KEY = __ENV.API_KEY || 'bmr_live_shopsim_demo_key_0000000000000000';
const START = Number(__ENV.START_RATE || 200);
const PEAK = Number(__ENV.PEAK_RATE || 3000);
const accepted = new Counter('events_accepted');

export const options = {
  discardResponseBodies: false,
  scenarios: {
    ramp: {
      executor: 'ramping-arrival-rate',
      startRate: START,
      timeUnit: '1s',
      preAllocatedVUs: 200,
      maxVUs: 1000,
      stages: [
        { target: PEAK, duration: __ENV.RAMP || '60s' },
        { target: PEAK, duration: __ENV.HOLD || '60s' },
        { target: 0, duration: '10s' },
      ],
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.01'],
    'http_req_duration{expected_response:true}': ['p(95)<100', 'p(99)<250'],
  },
};

const types = ['order.created', 'payment.succeeded', 'order.shipped', 'payment.failed', 'order.refunded'];

export default function () {
  const order = `ord_k6_${__VU}_${__ITER}`;
  const res = http.post(
    `${BASE}/v1/events`,
    JSON.stringify({
      type: types[__ITER % types.length],
      ordering_key: order,
      data: { order_id: order, amount: 1000 + (__ITER % 9000), currency: 'USD', source: 'k6' },
    }),
    { headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${KEY}`, 'Idempotency-Key': `${order}_${Date.now()}` } },
  );
  if (check(res, { 'accepted (202)': (r) => r.status === 202 })) accepted.add(1);
}
