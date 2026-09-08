# triggerly-claude

이벤트 트리거 워크플로 플랫폼(triggerly)의 데모 구현체입니다.
- 설계 배경: `docs/superpowers/specs/2026-09-01-triggerly-claude-demo-design.md`
- 구현 계획: `docs/superpowers/plans/2026-09-02-triggerly-claude-demo-implementation.md`

## 요구 사항

- Java 21 (`.java-version` 참고)
- Docker / Docker Compose
- (선택) [k6](https://k6.io/) — 부하테스트용

## 빠르게 실행하기 (자동 데모)

```bash
docker compose up -d          # MySQL, Elasticsearch, Redis, Kafka
sleep 20                      # ES/Kafka 초기화 대기

./gradlew :triggerly-bootstrap:bootRun --args='--spring.profiles.active=local,demo'
```

콘솔에서 `[DEMO ACTION]` 로그가 찍히는 걸 지켜보세요 (즉시~5분 소요, 시나리오별 상세는 스펙 8절).

## 수동으로 시나리오 재현하기

```bash
./gradlew :triggerly-bootstrap:bootRun --args='--spring.profiles.active=local'
./scripts/demo.sh
```

## API 문서

앱 기동 후 http://localhost:8080/swagger-ui.html

## 부하테스트

`loadtest/README.md` 참고.

## 관측 (선택)

```bash
docker compose -f docker-compose.yml -f docker-compose.observability.yml up -d
```
- Grafana: http://localhost:3000 (Prometheus/Loki 데이터소스 자동 프로비저닝됨)
- Prometheus: http://localhost:9090
- Kafka UI: http://localhost:8090 — 토픽/메시지/컨슈머 그룹 조회, 파티션 증설
- 로그: 앱을 `local` 프로필로 띄우면 `logs/triggerly.log`에도 기록되고, Promtail이 이를 Loki로
  전송한다. Grafana Explore 탭에서 `job="triggerly-claude"`로 조회

## 모듈 구조

구현 계획 문서의 "File Structure" 절 참고.
