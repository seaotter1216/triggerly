package com.seaotter.triggerly.bootstrap

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.scheduling.annotation.EnableScheduling

@SpringBootApplication(scanBasePackages = ["com.seaotter.triggerly"])
@EnableScheduling
class TriggerlyClaudeApplication

fun main(args: Array<String>) {
  runApplication<TriggerlyClaudeApplication>(*args)
}
