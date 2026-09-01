package com.seaotter.triggerly.application.engine

import com.seaotter.triggerly.application.port.MemberEventStatsPort
import com.seaotter.triggerly.application.port.WaitingIndexPort
import com.seaotter.triggerly.application.port.WorkflowExecutionRepositoryPort
import com.seaotter.triggerly.application.port.WorkflowInstanceRepositoryPort
import com.seaotter.triggerly.domain.*
import org.springframework.stereotype.Service
import java.time.LocalDateTime
import java.util.UUID

@Service
class WorkflowEngine(
  private val workflowInstanceRepositoryPort: WorkflowInstanceRepositoryPort,
  private val workflowExecutionRepositoryPort: WorkflowExecutionRepositoryPort,
  private val waitingIndexPort: WaitingIndexPort,
  private val memberEventStatsPort: MemberEventStatsPort,
  private val actionExecutor: ActionExecutor,
) {

  fun start(workflow: Workflow, tenantId: String, memberId: String?, context: Map<String, Any?>): WorkflowInstance {
    val triggerNode = workflow.definitionJson.nodes.first { it is Node.Trigger } as Node.Trigger
    val instance = workflowInstanceRepositoryPort.save(
      WorkflowInstance(
        id = UUID.randomUUID().toString(),
        workflowId = workflow.id,
        tenantId = tenantId,
        triggerEventCode = workflow.triggerEventCode,
        version = workflow.version,
        memberId = memberId,
        status = WorkflowInstanceStatus.RUNNING,
        startedAt = LocalDateTime.now(),
      ),
    )
    return runFrom(instance, workflow, triggerNode.id, context)
  }

  fun resumeOnMatch(instance: WorkflowInstance, workflow: Workflow, eventContext: Map<String, Any?>): WorkflowInstance {
    val node = currentNode(workflow, instance) as? Node.WaitForEvent
      ?: error("instance ${instance.id} is not waiting on a WaitForEvent node")
    val matched = node.event.matchConditions?.let {
      ConditionEvaluator.evaluate(it, eventContext, statsResolverFor(instance.tenantId), instance.memberId)
    } ?: true
    if (!matched) return instance

    completeRunningExecution(instance, node.id)
    waitingIndexPort.remove(instance.tenantId, node.event.eventCode, instance.memberId ?: "", instance.id)
    val nextNodeId = nextNodeId(workflow, node.id, EdgeRoute.Matched)
    return runFrom(instance, workflow, nextNodeId, eventContext)
  }

  fun resumeOnTimeout(instance: WorkflowInstance, workflow: Workflow): WorkflowInstance {
    return when (val node = currentNode(workflow, instance)) {
      is Node.WaitForEvent -> {
        completeRunningExecution(instance, node.id)
        waitingIndexPort.remove(instance.tenantId, node.event.eventCode, instance.memberId ?: "", instance.id)
        runFrom(instance, workflow, nextNodeId(workflow, node.id, EdgeRoute.Timeout), emptyMap())
      }
      is Node.Delay -> {
        completeRunningExecution(instance, node.id)
        runFrom(instance, workflow, nextNodeId(workflow, node.id, EdgeRoute.Always), emptyMap())
      }
      else -> error("instance ${instance.id} is not waiting (currentNodeId=${instance.currentNodeId})")
    }
  }

  private fun runFrom(instanceIn: WorkflowInstance, workflow: Workflow, startNodeId: String, context: Map<String, Any?>): WorkflowInstance {
    val instance = instanceIn
    var nodeId = startNodeId
    while (true) {
      val node = workflow.definitionJson.nodes.first { it.id == nodeId }
      val execution = workflowExecutionRepositoryPort.save(
        WorkflowExecution(
          id = UUID.randomUUID().toString(),
          workflowInstanceId = instance.id,
          nodeId = node.id,
          nodeType = nodeTypeOf(node),
          status = WorkflowExecutionStatus.RUNNING,
          startedAt = LocalDateTime.now(),
        ),
      )

      when (node) {
        is Node.Trigger -> {
          complete(execution)
          nodeId = nextNodeId(workflow, node.id, EdgeRoute.Always)
        }

        is Node.Condition -> {
          val passed = ConditionEvaluator.evaluate(node.condition, context, statsResolverFor(instance.tenantId), instance.memberId)
          complete(execution, result = passed.toString())
          nodeId = nextNodeId(workflow, node.id, if (passed) EdgeRoute.True else EdgeRoute.False)
        }

        is Node.Action -> {
          val result = actionExecutor.execute(node.action)
          complete(execution, result = result)
          nodeId = nextNodeId(workflow, node.id, EdgeRoute.Always)
        }

        is Node.WaitForEvent -> {
          instance.status = WorkflowInstanceStatus.WAITING
          instance.currentNodeId = node.id
          instance.waitingEventName = node.event.eventCode
          instance.waitingUntil = node.event.timeout?.let { LocalDateTime.now().plusNanos(it.toDuration().inWholeNanoseconds) }
          val saved = workflowInstanceRepositoryPort.save(instance)
          waitingIndexPort.register(saved.tenantId, node.event.eventCode, saved.memberId ?: "", saved.id)
          return saved
        }

        is Node.Delay -> {
          instance.status = WorkflowInstanceStatus.WAITING
          instance.currentNodeId = node.id
          instance.waitingUntil = LocalDateTime.now().plusNanos(node.duration.toDuration().inWholeNanoseconds)
          return workflowInstanceRepositoryPort.save(instance)
        }

        is Node.End -> {
          complete(execution)
          instance.status = WorkflowInstanceStatus.COMPLETED
          instance.currentNodeId = node.id
          instance.completedAt = LocalDateTime.now()
          return workflowInstanceRepositoryPort.save(instance)
        }
      }
    }
  }

  private fun complete(execution: WorkflowExecution, result: String? = null) {
    execution.status = WorkflowExecutionStatus.COMPLETED
    execution.completedAt = LocalDateTime.now()
    execution.result = result
    workflowExecutionRepositoryPort.save(execution)
  }

  private fun completeRunningExecution(instance: WorkflowInstance, nodeId: String) {
    val execution = workflowExecutionRepositoryPort.findRunning(instance.id, nodeId) ?: return
    complete(execution)
  }

  private fun currentNode(workflow: Workflow, instance: WorkflowInstance): Node? =
    instance.currentNodeId?.let { id -> workflow.definitionJson.nodes.firstOrNull { it.id == id } }

  private fun nextNodeId(workflow: Workflow, from: String, route: EdgeRoute): String =
    workflow.definitionJson.edges.firstOrNull { it.from == from && it.route == route }?.to
      ?: error("워크플로 ${workflow.id}에 노드 $from 에서 $route 로 가는 엣지가 없습니다")

  private fun nodeTypeOf(node: Node): NodeType = when (node) {
    is Node.Trigger -> NodeType.TRIGGER
    is Node.Condition -> NodeType.CONDITION
    is Node.Action -> NodeType.ACTION
    is Node.WaitForEvent -> NodeType.WAIT_FOR_EVENT
    is Node.Delay -> NodeType.DELAY
    is Node.End -> NodeType.END
  }

  private fun statsResolverFor(tenantId: String): EventStatsResolver =
    EventStatsResolver { memberId, eventCode, withinDays -> memberEventStatsPort.countEvents(tenantId, memberId, eventCode, withinDays) }
}
