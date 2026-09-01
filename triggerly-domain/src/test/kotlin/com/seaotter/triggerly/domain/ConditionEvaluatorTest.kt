package com.seaotter.triggerly.domain

import kotlin.test.Test
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class ConditionEvaluatorTest {

  private val noStats = EventStatsResolver { _, _, _ -> error("stats should not be called") }

  @Test
  fun `EQ 연산자는 컨텍스트 값과 일치하면 true`() {
    val expr = ConditionExpression.Predicate("event.amount", ConditionOperator.EQ, 50000)
    assertTrue(ConditionEvaluator.evaluate(expr, mapOf("event.amount" to 50000), noStats))
  }

  @Test
  fun `GOE 연산자는 숫자를 Number로 통일해서 비교한다`() {
    val expr = ConditionExpression.Predicate("event.amount", ConditionOperator.GOE, 50000)
    assertTrue(ConditionEvaluator.evaluate(expr, mapOf("event.amount" to 50000.0), noStats))
    assertFalse(ConditionEvaluator.evaluate(expr, mapOf("event.amount" to 49999), noStats))
  }

  @Test
  fun `IN 연산자는 value가 List일 때 포함 여부를 확인한다`() {
    val expr = ConditionExpression.Predicate("member.city", ConditionOperator.IN, listOf("서울", "부산"))
    assertTrue(ConditionEvaluator.evaluate(expr, mapOf("member.city" to "서울"), noStats))
    assertFalse(ConditionEvaluator.evaluate(expr, mapOf("member.city" to "대전"), noStats))
  }

  @Test
  fun `EXISTS 연산자는 컨텍스트에 키가 있는지만 확인한다`() {
    val expr = ConditionExpression.Predicate("member.email", ConditionOperator.EXISTS, null)
    assertTrue(ConditionEvaluator.evaluate(expr, mapOf("member.email" to "a@b.com"), noStats))
    assertFalse(ConditionEvaluator.evaluate(expr, emptyMap(), noStats))
  }

  @Test
  fun `And Or Not 조합을 평가할 수 있다`() {
    val expr = ConditionExpression.And(
      listOf(
        ConditionExpression.Predicate("event.amount", ConditionOperator.GOE, 10000),
        ConditionExpression.Or(
          listOf(
            ConditionExpression.Predicate("member.city", ConditionOperator.EQ, "서울"),
            ConditionExpression.Not(ConditionExpression.Predicate("member.vip", ConditionOperator.EQ, false)),
          ),
        ),
      ),
    )
    val ctx = mapOf("event.amount" to 15000, "member.city" to "대전", "member.vip" to true)
    assertTrue(ConditionEvaluator.evaluate(expr, ctx, noStats))
  }

  @Test
  fun `stats eventCount 필드는 컨텍스트가 아니라 EventStatsResolver를 호출한다`() {
    val expr = ConditionExpression.Predicate("stats.eventCount:REVIEW_ADD:30d", ConditionOperator.GOE, 3)
    val resolver = EventStatsResolver { memberId, eventCode, withinDays ->
      assertTrue(memberId == "m1" && eventCode == "REVIEW_ADD" && withinDays == 30)
      3L
    }
    assertTrue(
      ConditionEvaluator.evaluate(expr, mapOf("member.id" to "m1"), resolver, memberId = "m1"),
    )
  }
}
