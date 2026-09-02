#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${BASE_URL:-http://localhost:8080}"
TENANT_ID="demo-tenant-cli"
WORKFLOW_ID="demo-cli-workflow"

echo "1) 이벤트 정의 등록"
curl -s -X POST "$BASE_URL/api/v1/admin/event-definitions" -H "Content-Type: application/json" \
  -d "{\"tenantId\":\"$TENANT_ID\",\"code\":\"CART_ADD\",\"displayName\":\"장바구니 담기\"}" > /dev/null
curl -s -X POST "$BASE_URL/api/v1/admin/event-definitions" -H "Content-Type: application/json" \
  -d "{\"tenantId\":\"$TENANT_ID\",\"code\":\"PURCHASE\",\"displayName\":\"구매\"}" > /dev/null

echo "2) 워크플로 등록: CART_ADD -> WaitForEvent(PURCHASE, 1분) -> Matched:End / Timeout:쿠폰"
curl -s -X POST "$BASE_URL/api/v1/admin/workflows" -H "Content-Type: application/json" -d '{
  "id": "'"$WORKFLOW_ID"'",
  "tenantId": "'"$TENANT_ID"'",
  "name": "장바구니 리마인드 (CLI)",
  "triggerEventCode": "CART_ADD",
  "definitionJson": {
    "trigger": "CART_ADD",
    "nodes": [
      {"type":"TRIGGER","id":"n1","eventCode":"CART_ADD"},
      {"type":"WAIT_FOR_EVENT","id":"n2","event":{"eventCode":"PURCHASE","timeout":{"value":1,"unit":"MINUTES"}}},
      {"type":"END","id":"n3"},
      {"type":"ACTION","id":"n4","action":{"type":"ISSUE_COUPON","couponId":"CLI10"}},
      {"type":"END","id":"n5"}
    ],
    "edges": [
      {"from":"n1","to":"n2","route":{"type":"ALWAYS"}},
      {"from":"n2","to":"n3","route":{"type":"MATCHED"}},
      {"from":"n2","to":"n4","route":{"type":"TIMEOUT"}},
      {"from":"n4","to":"n5","route":{"type":"ALWAYS"}}
    ]
  }
}' > /dev/null

curl -s -X POST "$BASE_URL/api/v1/admin/workflows/$WORKFLOW_ID/enable?tenantId=$TENANT_ID" > /dev/null

echo "3) CART_ADD 이벤트 전송"
curl -s -X POST "$BASE_URL/api/v1/events" -H "Content-Type: application/json" \
  -d "{\"tenantId\":\"$TENANT_ID\",\"eventCode\":\"CART_ADD\",\"member\":{\"externalMemberId\":\"cli-member-1\"}}"
echo ""
echo "1분 안에 아래 명령으로 PURCHASE를 보내면 Matched 경로, 안 보내면 1분 뒤 쿠폰 액션(Timeout 경로)이 앱 로그에 찍힙니다:"
echo "  curl -X POST $BASE_URL/api/v1/events -H 'Content-Type: application/json' -d '{\"tenantId\":\"$TENANT_ID\",\"eventCode\":\"PURCHASE\",\"member\":{\"externalMemberId\":\"cli-member-1\"}}'"
