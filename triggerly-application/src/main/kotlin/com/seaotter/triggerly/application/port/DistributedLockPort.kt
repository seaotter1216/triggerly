package com.seaotter.triggerly.application.port

import java.time.Duration

// SET NX PX 기반 분산 락. tryLock이 true를 반환하면 호출자가 그 키의 유일한 소유자다 - ttl이 지나면
// 자동 해제되므로 명시적 unlock이 없다. 짧은 TTL로는 타임아웃 폴러의 행 단위 락(여러 인스턴스가 같은
// 만료 인스턴스를 동시에 집었을 때 한쪽만 처리)으로, 긴 TTL로는 액션 디스패치 멱등성 가드("이 dispatchId는
// 이미 처리했다"는 표시)로 재사용한다 - 두 용도 모두 "한 번 선점하면 그걸로 끝"이라 unlock이 필요 없다.
fun interface DistributedLockPort {
  fun tryLock(key: String, ttl: Duration): Boolean
}
