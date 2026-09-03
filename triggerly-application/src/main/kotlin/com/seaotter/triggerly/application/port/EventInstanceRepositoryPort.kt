package com.seaotter.triggerly.application.port

import com.seaotter.triggerly.domain.EventInstance
import java.time.LocalDateTime

interface EventInstanceRepositoryPort {
  fun save(instance: EventInstance): EventInstance
  fun saveAll(instances: Collection<EventInstance>): List<EventInstance>
  fun deleteOlderThan(cutoff: LocalDateTime): Int

  // 카프카 재시도(재배달)로 같은 eventId가 다시 들어왔을 때 "이미 엔진 실행까지 끝난 이벤트"인지 판별하기
  // 위한 벌크 조회. MySQL(안정성 저장소)만 조회한다 — ES는 조회 성능용 사본일 뿐, 이 판별의 근거로 쓰면 안 됨.
  fun findExistingIds(ids: Collection<String>): Set<String>
}
