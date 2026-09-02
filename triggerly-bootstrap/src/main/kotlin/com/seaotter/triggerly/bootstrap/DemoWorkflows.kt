package com.seaotter.triggerly.bootstrap

import com.seaotter.triggerly.domain.*
import kotlin.time.DurationUnit

object DemoWorkflows {

  // Trigger(CART_ADD) --Always--> WaitForEvent(PURCHASE, 1분) --Matched--> End
  //                                                            --Timeout--> Action(쿠폰) --Always--> End
  fun cartReminder() = WorkflowDefinition(
    trigger = "CART_ADD",
    nodes = listOf(
      Node.Trigger("n1", "CART_ADD"),
      Node.WaitForEvent("n2", WaitEventDefinition("PURCHASE", DurationDto(1, DurationUnit.MINUTES))),
      Node.End("n3"),
      Node.Action("n4", ActionDefinition.IssueCoupon("CART_REMIND10")),
      Node.End("n5"),
    ),
    edges = listOf(
      Edge("n1", "n2", EdgeRoute.Always),
      Edge("n2", "n3", EdgeRoute.Matched),
      Edge("n2", "n4", EdgeRoute.Timeout),
      Edge("n4", "n5", EdgeRoute.Always),
    ),
  )

  // Trigger(LOGIN) --Always--> Condition(생일=오늘) --True--> Action(쿠폰) --Always--> End
  //                                                  --False--> End
  fun birthdayCoupon() = WorkflowDefinition(
    trigger = "LOGIN",
    nodes = listOf(
      Node.Trigger("n1", "LOGIN"),
      Node.Condition("n2", ConditionExpression.Predicate("member.isBirthdayToday", ConditionOperator.EQ, true)),
      Node.Action("n3", ActionDefinition.IssueCoupon("BIRTHDAY10")),
      Node.End("n4"),
      Node.End("n5"),
    ),
    edges = listOf(
      Edge("n1", "n2", EdgeRoute.Always),
      Edge("n2", "n3", EdgeRoute.True),
      Edge("n3", "n4", EdgeRoute.Always),
      Edge("n2", "n5", EdgeRoute.False),
    ),
  )

  // Trigger(SIGN_UP) --Always--> Delay(30초) --Always--> Action(알림톡) --Always--> End
  fun welcomeDelay() = WorkflowDefinition(
    trigger = "SIGN_UP",
    nodes = listOf(
      Node.Trigger("n1", "SIGN_UP"),
      Node.Delay("n2", DurationDto(30, DurationUnit.SECONDS)),
      Node.Action("n3", ActionDefinition.SendAlimTalk("welcome")),
      Node.End("n4"),
    ),
    edges = listOf(
      Edge("n1", "n2", EdgeRoute.Always),
      Edge("n2", "n3", EdgeRoute.Always),
      Edge("n3", "n4", EdgeRoute.Always),
    ),
  )

  // Trigger(PURCHASE) --Always--> WaitForEvent(REVIEW_ADD, 5분) --Matched--> Action(쿠폰10%) --Always--> End
  //                                                             --Timeout--> Condition(30일 리뷰수>=3)
  //                                                                    --True--> Action(알림톡) --Always--> End
  //                                                                    --False--> End
  fun reviewNudge() = WorkflowDefinition(
    trigger = "PURCHASE",
    nodes = listOf(
      Node.Trigger("n1", "PURCHASE"),
      Node.WaitForEvent("n2", WaitEventDefinition("REVIEW_ADD", DurationDto(5, DurationUnit.MINUTES))),
      Node.Action("n3", ActionDefinition.IssueCoupon("REVIEW10")),
      Node.End("n6"),
      Node.Condition("n4", ConditionExpression.Predicate("stats.eventCount:REVIEW_ADD:30d", ConditionOperator.GOE, 3)),
      Node.Action("n5", ActionDefinition.SendAlimTalk("review-nudge")),
      Node.End("n8"),
      Node.End("n7"),
    ),
    edges = listOf(
      Edge("n1", "n2", EdgeRoute.Always),
      Edge("n2", "n3", EdgeRoute.Matched),
      Edge("n3", "n6", EdgeRoute.Always),
      Edge("n2", "n4", EdgeRoute.Timeout),
      Edge("n4", "n5", EdgeRoute.True),
      Edge("n4", "n7", EdgeRoute.False),
      Edge("n5", "n8", EdgeRoute.Always),
    ),
  )
}
