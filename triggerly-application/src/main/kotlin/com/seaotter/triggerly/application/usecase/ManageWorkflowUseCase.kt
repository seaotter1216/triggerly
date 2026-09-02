package com.seaotter.triggerly.application.usecase

import com.seaotter.triggerly.application.port.WorkflowRepositoryPort
import com.seaotter.triggerly.domain.Node
import com.seaotter.triggerly.domain.Workflow
import com.seaotter.triggerly.domain.WorkflowDefinition
import com.seaotter.triggerly.domain.WorkflowStatus
import org.springframework.stereotype.Service
import java.time.LocalDateTime

@Service
class ManageWorkflowUseCase(private val port: WorkflowRepositoryPort) {
  fun create(workflow: Workflow): Workflow {
    validate(workflow.definitionJson)
    return port.save(workflow)
  }
  fun list(tenantId: String): List<Workflow> = port.findAll(tenantId)
  fun get(tenantId: String, id: String): Workflow? = port.findById(tenantId, id)

  fun enable(tenantId: String, id: String): Workflow {
    val workflow = port.findById(tenantId, id) ?: error("workflow not found: $id")
    workflow.status = WorkflowStatus.ENABLED
    workflow.lastUpdatedAt = LocalDateTime.now()
    return port.save(workflow)
  }

  private fun validate(definition: WorkflowDefinition) {
    val nodeIds = definition.nodes.map { it.id }.toSet()
    definition.edges.forEach { edge ->
      require(edge.from in nodeIds) { "워크플로 정의에 존재하지 않는 노드를 가리키는 엣지입니다 (from=${edge.from})" }
      require(edge.to in nodeIds) { "워크플로 정의에 존재하지 않는 노드를 가리키는 엣지입니다 (to=${edge.to})" }
    }

    val triggerNodes = definition.nodes.filterIsInstance<Node.Trigger>()
    require(triggerNodes.size == 1) {
      "워크플로 정의에는 정확히 하나의 Trigger 노드가 있어야 합니다 (실제 개수: ${triggerNodes.size})"
    }

    val adjacency = definition.edges.groupBy({ it.from }, { it.to })
    val visited = mutableSetOf<String>()
    val onStack = mutableSetOf<String>()

    fun detectCycle(nodeId: String) {
      if (nodeId in onStack) {
        throw IllegalArgumentException("워크플로 정의에 순환(cycle)이 존재합니다 (노드: $nodeId)")
      }
      if (nodeId in visited) return
      onStack += nodeId
      adjacency[nodeId]?.forEach { detectCycle(it) }
      onStack -= nodeId
      visited += nodeId
    }

    detectCycle(triggerNodes.first().id)
  }
}
