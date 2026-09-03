package com.seaotter.triggerly.application.usecase

// IngestEventUseCase.handleBatch가 배치 중 특정 레코드에서 실패했을 때 "몇 번째(index) 레코드가
// 실패했는지"를 호출자(어댑터)에게 알리기 위한 신호용 예외.
// application 모듈은 메시징 기술(Kafka 등)에 의존하면 안 되므로 여기서는 기술 중립적인 정보만 담고,
// 실제 Kafka 전용 처리(BatchListenerFailedException으로 변환해 컨테이너에 알리는 것)는
// triggerly-adapter-messaging의 WorkflowTriggerConsumer가 이 예외를 잡아서 수행한다.
class BatchEventProcessingException(
  val failedIndex: Int,
  val eventId: String,
  cause: Throwable,
) : RuntimeException("이벤트 배치 처리 실패: index=$failedIndex eventId=$eventId", cause)
