package com.seaotter.triggerly.application.port

import java.time.Duration

// SET NX PX 기반 분산 락. tryLock이 true를 반환하면 호출자가 그 키의 유일한 소유자다 - ttl이 지나면
// 자동 해제되므로 대부분의 용도(타임아웃 폴러의 행 단위 락)에서는 명시적 release가 필요 없다. 다만
// 액션 디스패치 멱등성 가드(DispatchActionUseCase)처럼 "락을 잡은 뒤 실제 작업이 실패할 수 있는" 경우엔
// 실패 시 release로 명시적으로 풀어줘야 한다 - 그러지 않으면 다음 재시도가 "이미 처리됨"으로 오인돼
// 카프카 재시도/DLT 메커니즘이 무력화된다.
interface DistributedLockPort {
  fun tryLock(key: String, ttl: Duration): Boolean
  fun release(key: String)
}
