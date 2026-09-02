import http from 'k6/http';
import { check } from 'k6';

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8080';
const TENANT_COUNT = parseInt(__ENV.TENANT_COUNT || '5', 10);
const MEMBER_COUNT = parseInt(__ENV.MEMBER_COUNT || '1000', 10);
const EVENT_CODE = __ENV.EVENT_CODE || 'CART_ADD';

export const options = {
  scenarios: {
    ingest: {
      executor: 'constant-arrival-rate',
      rate: parseInt(__ENV.TARGET_RPS || '100', 10),
      timeUnit: '1s',
      duration: __ENV.DURATION || '30s',
      preAllocatedVUs: parseInt(__ENV.VUS || '50', 10),
      maxVUs: parseInt(__ENV.MAX_VUS || '200', 10),
    },
  },
};

export default function () {
  const tenantId = `loadtest-tenant-${Math.floor(Math.random() * TENANT_COUNT)}`;
  const externalMemberId = `member-${Math.floor(Math.random() * MEMBER_COUNT)}`;

  const payload = JSON.stringify({
    tenantId,
    eventCode: EVENT_CODE,
    member: { externalMemberId },
    attributes: { amount: Math.floor(Math.random() * 100000) },
  });

  const res = http.post(`${BASE_URL}/api/v1/events`, payload, {
    headers: { 'Content-Type': 'application/json' },
  });

  check(res, {
    'status is 202 or 429': (r) => r.status === 202 || r.status === 429,
  });
}
