package com.seaotter.triggerly.adapter.messaging

import com.seaotter.triggerly.application.usecase.DispatchActionUseCase
import com.seaotter.triggerly.application.usecase.IngestEventUseCase
import io.mockk.mockk
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.context.annotation.Bean

@SpringBootApplication
class MessagingTestApplication {

  // Task 16이 추가한 WorkflowTriggerConsumer(@Component)는 이 앱을 공유하는 다른 모든 통합 테스트
  // (KafkaEventPublisherAdapterTest, RedisRateLimiterAdapterTest, RedisWaitingIndexAdapterTest 등,
  // com.seaotter.triggerly.adapter.messaging 하위를 스캔하는 컨텍스트라면 전부)에서도 컴포넌트 스캔되어
  // IngestEventUseCase 빈을 요구한다(실측 확인: 빈이 없어 NoSuchBeanDefinitionException으로 3개 테스트가
  // 깨짐). IngestEventUseCase는 이 모듈 밖(application 모듈)의 @Service라 이 모듈 테스트 컨텍스트에서는
  // 자동 스캔되지 않으므로, 여기에 이름이 다른 기본 mock 빈을 두어 안전망 역할을 하게 한다.
  // WorkflowTriggerConsumerTest는 자신만의 mock을 별도 이름 + @Import로 제공하며, 빈 이름이
  // 주입 지점 이름("ingestEventUseCase")과 일치해 그 쪽이 선택된다(다른 테스트에서는 이 기본 빈만 존재하므로
  // 모호성 없이 그대로 사용됨).
  @Bean
  fun defaultIngestEventUseCase(): IngestEventUseCase = mockk(relaxed = true)

  // ActionDispatchConsumer(@Component)도 같은 이유로 DispatchActionUseCase 빈이 필요하다 - 위와 동일한
  // 안전망 패턴.
  @Bean
  fun defaultDispatchActionUseCase(): DispatchActionUseCase = mockk(relaxed = true)
}
