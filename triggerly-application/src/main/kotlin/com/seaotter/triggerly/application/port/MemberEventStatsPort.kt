package com.seaotter.triggerly.application.port

interface MemberEventStatsPort {
  fun countEvents(tenantId: String, memberId: String, eventCode: String, withinDays: Int): Long
}
