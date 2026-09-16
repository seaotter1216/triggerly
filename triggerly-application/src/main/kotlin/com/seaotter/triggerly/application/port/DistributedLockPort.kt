package com.seaotter.triggerly.application.port

import java.time.Duration

// tryLock의 결과를 3가지로 명시적으로 구분한다 - 예전에는 Boolean 하나로 "이미 처리됨"과 "Redis
// 장애로 판단 불가"를 동시에 표현했는데, 두 경우의 올바른 대응이 호출부마다 다르다(타임아웃 폴러는
// 둘 다 "이번 틱은 스킵, 다음 틱에 재시도"로 동일하게 처리해도 안전하지만, 액션 디스패치 멱등성
// 가드는 "이미 처리됨"이면 스킵해야 하고 "판단 불가"면 오히려 처리를 계속 시도해야 한다 - 그래야
// Redis 장애 구간에 들어온 액션이 조용히 유실되지 않고 카프카 재시도/DLT로 넘어간다).
sealed interface LockResult {
  data object Acquired : LockResult
  data object AlreadyHeld : LockResult
  data object Unavailable : LockResult
}

// SET NX PX 기반 분산 락. tryLock이 Acquired를 반환하면 호출자가 그 키의 유일한 소유자다 - ttl이 지나면
// 자동 해제되므로 대부분의 용도(타임아웃 폴러의 행 단위 락)에서는 명시적 release가 필요 없다. 다만
// 액션 디스패치 멱등성 가드(DispatchActionUseCase)처럼 "락을 잡은 뒤 실제 작업이 실패할 수 있는" 경우엔
// 실패 시 release로 명시적으로 풀어줘야 한다 - 그러지 않으면 다음 재시도가 "이미 처리됨"으로 오인돼
// 카프카 재시도/DLT 메커니즘이 무력화된다.
interface DistributedLockPort {
  fun tryLock(key: String, ttl: Duration): LockResult
  fun release(key: String)
}
