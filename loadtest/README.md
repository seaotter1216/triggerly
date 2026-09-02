# triggerly-claude 부하테스트

[k6](https://k6.io/) 설치 필요: `brew install k6`

## 사전 준비

1. `docker compose up -d`로 인프라 기동, 앱은 `local` 프로파일로 기동
2. 사용할 `tenantId`마다 `EventDefinition`을 먼저 등록해야 202를 받는다 (미등록이면 400):
   ```bash
   for i in 0 1 2 3 4; do
     curl -X POST http://localhost:8080/api/v1/admin/event-definitions \
       -H "Content-Type: application/json" \
       -d "{\"tenantId\":\"loadtest-tenant-$i\",\"code\":\"CART_ADD\",\"displayName\":\"장바구니\"}"
   done
   ```

## 단계적으로 rps 올려보기

```bash
TARGET_RPS=100 DURATION=30s k6 run loadtest/ingest.js
TARGET_RPS=1000 DURATION=1m VUS=200 MAX_VUS=500 k6 run loadtest/ingest.js
# 로컬 리소스 한계에 부딪히면 EC2 등 별도 환경에서 계속 올려보세요 (스펙 14절 — 실제 수만 rps 검증은 범위 밖)
TARGET_RPS=10000 DURATION=1m VUS=1000 MAX_VUS=2000 k6 run loadtest/ingest.js
```

## 튜닝 포인트 (`application.yml`)

- `triggerly.kafka.raw-events-topic.partitions` — 늘리면 컨슈머 병렬성 상한이 늘어남
- `triggerly.kafka.consumer.concurrency` — 컨슈머 스레드 수 (파티션 수 이하로)
- `triggerly.ratelimit.default-rps` — 429가 너무 많으면 올려서 재시도
- `triggerly.async.member-sync.*` — ES 동기화 풀 크기
- `docker compose -f docker-compose.yml -f docker-compose.observability.yml up -d` 후 Grafana(`localhost:3000`)에서 `/actuator/prometheus` 지표를 보며 조정
