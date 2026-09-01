package com.seaotter.triggerly.domain

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import kotlin.test.Test
import kotlin.test.assertEquals

class BirthdayParsingTest {

  @Test
  fun `yyyyMMdd 포맷 생일 문자열을 LocalDate로 파싱할 수 있다`() {
    val birthday = "20250305"
    val date = LocalDate.parse(birthday, DateTimeFormatter.ofPattern("yyyyMMdd"))
    assertEquals(LocalDate.of(2025, 3, 5), date)
  }
}
