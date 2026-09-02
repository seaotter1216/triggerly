package com.seaotter.triggerly.bootstrap

import com.seaotter.triggerly.application.port.EventInstanceRepositoryPort
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import java.time.LocalDateTime

// 스펙 5.1절: event_instance는 MySQL에 30일만 유지 (ES event_log는 계속 보관)
@Component
class EventInstanceCleanupJob(
  private val eventInstanceRepositoryPort: EventInstanceRepositoryPort,
  @Value("\${triggerly.scheduler.event-instance-retention-days:30}") private val retentionDays: Long,
) {
  private val log = LoggerFactory.getLogger(EventInstanceCleanupJob::class.java)

  @Scheduled(cron = "0 0 3 * * *")
  fun cleanup() {
    val deleted = eventInstanceRepositoryPort.deleteOlderThan(LocalDateTime.now().minusDays(retentionDays))
    log.info("EventInstance 정리: ${deleted}건 삭제 (retention=${retentionDays}일)")
  }
}
