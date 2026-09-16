package com.seaotter.triggerly.application.engine

import com.seaotter.triggerly.application.port.ActionDispatchMessage
import com.seaotter.triggerly.application.port.ActionDispatchPort
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
  private val actionDispatchPort: ActionDispatchPort,
) {

  // eventId+workflowId로 인스턴스 id를 고정한다(랜덤 UUID 대신). 카프카 배치 재시도로 같은 이벤트에 대해
  // start()가 다시 호출돼도 이미 존재하는 인스턴스를 그대로 반환해 워크플로가 중복 실행(쿠폰 중복 발급 등)되지
  // 않는다 - EventInstance에 eventId로 멱등성을 건 것과 동일한 패턴. workflow_instance.id 컬럼이
  // VARCHAR(36)(V1__init.sql)이라 "$eventId:${workflow.id}"를 그대로 못 쓰고, nameUUIDFromBytes로
  // 같은 입력이면 항상 같은 36자 UUID가 나오게 해시한다. 단, 프로세스가 runFrom 도중(WAITING/COMPLETED/
  // ERROR 어디에도 도달하기 전) 죽는 극단적인 경우엔 RUNNING 상태로 멈춘 인스턴스를 그대로 반환하고
  // 나머지 노드는 재실행하지 않는다 - 자주 없는 케이스라 지금은 감수한다.
  fun start(workflow: Workflow, tenantId: String, memberId: String?, context: Map<String, Any?>, eventId: String): WorkflowInstance {
    val instanceId = UUID.nameUUIDFromBytes("$eventId:${workflow.id}".toByteArray()).toString()
    workflowInstanceRepositoryPort.findById(instanceId)?.let { return it }

    val triggerNode = workflow.definitionJson.nodes.first { it is Node.Trigger } as Node.Trigger
    val instance = workflowInstanceRepositoryPort.save(
      WorkflowInstance(
        id = instanceId,
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
    val nextNodeId = workflow.definitionJson.nextNodeId(node.id, EdgeRoute.Matched)
    return runFrom(instance, workflow, nextNodeId, eventContext)
  }

  fun resumeOnTimeout(instance: WorkflowInstance, workflow: Workflow): WorkflowInstance {
    return when (val node = currentNode(workflow, instance)) {
      is Node.WaitForEvent -> {
        completeRunningExecution(instance, node.id)
        waitingIndexPort.remove(instance.tenantId, node.event.eventCode, instance.memberId ?: "", instance.id)
        runFrom(instance, workflow, workflow.definitionJson.nextNodeId(node.id, EdgeRoute.Timeout), emptyMap())
      }
      is Node.Delay -> {
        completeRunningExecution(instance, node.id)
        runFrom(instance, workflow, workflow.definitionJson.nextNodeId(node.id, EdgeRoute.Always), emptyMap())
      }
      else -> error("instance ${instance.id} is not waiting (currentNodeId=${instance.currentNodeId})")
    }
  }

  private fun runFrom(instanceIn: WorkflowInstance, workflow: Workflow, startNodeId: String, context: Map<String, Any?>): WorkflowInstance {
    val instance = instanceIn
    var nodeId = startNodeId
    val maxSteps = workflow.definitionJson.maxExecutionSteps
    var stepCount = 0
    while (true) {
      if (++stepCount > maxSteps) {
        instance.markError(nodeId)
        return workflowInstanceRepositoryPort.save(instance)
      }
      val node = workflow.definitionJson.nodeById(nodeId)
        ?: error("워크플로 ${workflow.id}에 노드 $nodeId 가 존재하지 않습니다")
      val execution = workflowExecutionRepositoryPort.save(
        WorkflowExecution(
          id = UUID.randomUUID().toString(),
          workflowInstanceId = instance.id,
          nodeId = node.id,
          nodeType = node.nodeType,
          status = WorkflowExecutionStatus.RUNNING,
          startedAt = LocalDateTime.now(),
        ),
      )

      when (node) {
        is Node.Trigger -> {
          complete(execution)
          nodeId = workflow.definitionJson.nextNodeId(node.id, EdgeRoute.Always)
        }

        is Node.Condition -> {
          val passed = ConditionEvaluator.evaluate(node.condition, context, statsResolverFor(instance.tenantId), instance.memberId)
          complete(execution, result = passed.toString())
          nodeId = workflow.definitionJson.nextNodeId(node.id, if (passed) EdgeRoute.True else EdgeRoute.False)
        }

        is Node.Action -> {
          // fire-and-forget: 실제 프로바이더 호출은 별도 컨슈머(ActionDispatchConsumer)가 완전히 독립된
          // 스레드 풀에서 처리한다 - 여기서 응답을 기다리면 프로바이더가 느려질 때 이 워크플로 엔진을 호출한
          // 카프카 컨슈머 스레드(raw-events 수집)까지 막히기 때문. 그래서 다음 노드가 이 결과를 참조하지
          // 않는다는 전제로, 발행만 하고 바로 다음 노드로 진행한다.
          val dispatchId = UUID.nameUUIDFromBytes("${instance.id}:${node.id}".toByteArray()).toString()
          actionDispatchPort.publish(
            ActionDispatchMessage(
              tenantId = instance.tenantId,
              memberId = instance.memberId,
              action = node.action,
              workflowInstanceId = instance.id,
              nodeId = node.id,
              dispatchId = dispatchId,
            ),
          )
          complete(execution, result = "dispatched(${node.action.describe()})")
          nodeId = workflow.definitionJson.nextNodeId(node.id, EdgeRoute.Always)
        }

        is Node.WaitForEvent -> {
          instance.markWaitingForEvent(node)
          val saved = workflowInstanceRepositoryPort.save(instance)
          waitingIndexPort.register(saved.tenantId, node.event.eventCode, saved.memberId ?: "", saved.id)
          return saved
        }

        is Node.Delay -> {
          instance.markWaitingForDelay(node)
          return workflowInstanceRepositoryPort.save(instance)
        }

        is Node.End -> {
          complete(execution)
          instance.markCompleted(node.id)
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
    instance.currentNodeId?.let { id -> workflow.definitionJson.nodeById(id) }

  private fun statsResolverFor(tenantId: String): EventStatsResolver =
    EventStatsResolver { memberId, eventCode, withinDays -> memberEventStatsPort.countEvents(tenantId, memberId, eventCode, withinDays) }
}
