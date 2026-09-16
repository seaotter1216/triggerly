package com.seaotter.triggerly.application.usecase

import com.seaotter.triggerly.application.engine.ActionExecutor
import com.seaotter.triggerly.application.port.ActionDispatchMessage
import com.seaotter.triggerly.application.port.DistributedLockPort
import com.seaotter.triggerly.application.port.LockResult
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.time.Duration

// ActionDispatchConsumer(adapter-messaging)의 진입점. WorkflowEngine의 raw-events 처리 경로와 완전히
// 분리된 컨슈머 그룹에서 호출되므로, 여기서 프로바이더 API가 느려지거나 실패해도 이벤트 수집 쪽 스레드는
// 영향받지 않는다. fire-and-forget이라 결과를 어디에도 다시 쓰지 않는다 - 실패하면 예외를 그대로 던져
// ActionDispatchConsumer 쪽 재시도/DLT가 처리하게 둔다.
//
// dispatchId는 WorkflowEngine이 (workflowInstanceId, nodeId)로 결정론적으로 만들어 재시도 때도 항상
// 같은 값이다(WorkflowEngine.kt 참고) - 그런데도 지금까지 이 값을 실제로 검사하는 곳이 없어, 카프카
// 재배달이나 타임아웃 폴러의 락 TTL 만료 후 재획득 같은 엣지케이스에서 실제 액션(쿠폰 발급 등)이 중복
// 실행될 수 있었다. DistributedLockPort.tryLock을 24시간 TTL로 걸어 "이 dispatchId를 이미 처리했다"는
// 표시로 재사용한다 - 타임아웃 폴러의 행 단위 락(짧은 TTL)과 같은 메커니즘을 더 긴 TTL로 쓰는 것뿐이다.
@Service
class DispatchActionUseCase(
  private val actionExecutor: ActionExecutor,
  private val distributedLockPort: DistributedLockPort,
) {
  private val log = LoggerFactory.getLogger(DispatchActionUseCase::class.java)

  fun handle(message: ActionDispatchMessage) {
    val lockKey = "dispatch-dedup:${message.dispatchId}"
    when (distributedLockPort.tryLock(lockKey, DEDUP_TTL)) {
      is LockResult.AlreadyHeld -> {
        log.info("이미 처리된 디스패치 - 스킵: dispatchId={}", message.dispatchId)
        return
      }
      // Redis 장애로 이미 처리 여부를 판단할 수 없는 상태다 - "처리 완료"로 간주해 조용히 스킵하면
      // 장애 구간에 들어온 모든 액션(쿠폰 발급 등)이 예외/재시도/DLT 없이 유실된다. 그래서 "아직 처리
      // 안 됨"으로 간주하고 예외를 던져 ActionDispatchConsumer의 재시도/DLT가 이 메시지를 다시 다루게
      // 한다 - 장애가 길어지면 결국 DLT에 남아 최소한 눈에 보이게 된다.
      is LockResult.Unavailable ->
        throw IllegalStateException("락 서비스 장애로 디스패치 처리 여부를 판단할 수 없음: dispatchId=${message.dispatchId}")
      is LockResult.Acquired -> {}
    }
    try {
      actionExecutor.execute(message.action)
    } catch (e: Exception) {
      // 실행이 실패하면 "아직 처리 안 됨"이 맞는 상태다 - 락을 풀어 다음 재시도(카프카 재배달)가
      // 다시 tryLock에 성공하게 해준다. 그 후 원래 예외를 그대로 던져 ActionDispatchConsumer의
      // 재시도/DLT 메커니즘이 정상 작동하게 한다.
      distributedLockPort.release(lockKey)
      throw e
    }
  }

  companion object {
    private val DEDUP_TTL: Duration = Duration.ofHours(24)
  }
}
