package com.seaotter.triggerly.domain

fun interface EventStatsResolver {
  fun countEvents(memberId: String, eventCode: String, withinDays: Int): Long
}

private val STATS_FIELD_REGEX = Regex("""^stats\.eventCount:([A-Z0-9_]+):(\d+)d$""")

object ConditionEvaluator {

  fun evaluate(
    expr: ConditionExpression,
    context: Map<String, Any?>,
    statsResolver: EventStatsResolver,
    memberId: String? = null,
  ): Boolean = when (expr) {
    is ConditionExpression.Predicate -> evaluatePredicate(expr, context, statsResolver, memberId)
    is ConditionExpression.And -> expr.conditions.all { evaluate(it, context, statsResolver, memberId) }
    is ConditionExpression.Or -> expr.conditions.any { evaluate(it, context, statsResolver, memberId) }
    is ConditionExpression.Not -> !evaluate(expr.condition, context, statsResolver, memberId)
  }

  private fun evaluatePredicate(
    predicate: ConditionExpression.Predicate,
    context: Map<String, Any?>,
    statsResolver: EventStatsResolver,
    memberId: String?,
  ): Boolean {
    if (predicate.operator == ConditionOperator.EXISTS) {
      return context.containsKey(predicate.field) && context[predicate.field] != null
    }

    val actual = resolveValue(predicate.field, context, statsResolver, memberId)
    return compare(actual, predicate.operator, predicate.value)
  }

  private fun resolveValue(
    field: String,
    context: Map<String, Any?>,
    statsResolver: EventStatsResolver,
    memberId: String?,
  ): Any? {
    val statsMatch = STATS_FIELD_REGEX.matchEntire(field)
    if (statsMatch != null) {
      requireNotNull(memberId) { "stats.eventCount 조건을 평가하려면 memberId가 필요합니다: $field" }
      val (eventCode, withinDays) = statsMatch.destructured
      return statsResolver.countEvents(memberId, eventCode, withinDays.toInt())
    }
    return context[field]
  }

  private fun compare(actual: Any?, operator: ConditionOperator, expected: Any?): Boolean = when (operator) {
    ConditionOperator.EQ -> actual == expected
    ConditionOperator.NE -> actual != expected
    ConditionOperator.GT -> compareNumbers(actual, expected) { a, b -> a > b }
    ConditionOperator.GOE -> compareNumbers(actual, expected) { a, b -> a >= b }
    ConditionOperator.LT -> compareNumbers(actual, expected) { a, b -> a < b }
    ConditionOperator.LOE -> compareNumbers(actual, expected) { a, b -> a <= b }
    ConditionOperator.IN -> (expected as? List<*>)?.contains(actual) ?: false
    ConditionOperator.NOT_IN -> (expected as? List<*>)?.contains(actual)?.not() ?: true
    ConditionOperator.EXISTS -> error("EXISTS는 evaluatePredicate에서 먼저 처리됨")
  }

  private fun compareNumbers(actual: Any?, expected: Any?, cmp: (Double, Double) -> Boolean): Boolean {
    val a = (actual as? Number)?.toDouble() ?: return false
    val b = (expected as? Number)?.toDouble() ?: return false
    return cmp(a, b)
  }
}
