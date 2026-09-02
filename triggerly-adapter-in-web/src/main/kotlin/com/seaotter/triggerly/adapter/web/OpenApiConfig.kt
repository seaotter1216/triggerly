package com.seaotter.triggerly.adapter.web

import io.swagger.v3.oas.models.OpenAPI
import io.swagger.v3.oas.models.info.Info
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class OpenApiConfig {
  @Bean
  fun triggerlyOpenApi(): OpenAPI = OpenAPI().info(Info().title("triggerly-claude API").version("v1"))
}
