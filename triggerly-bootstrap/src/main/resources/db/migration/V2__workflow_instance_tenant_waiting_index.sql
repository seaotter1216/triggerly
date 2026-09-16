CREATE INDEX idx_workflow_instance_tenant_waiting
  ON workflow_instance (tenant_id, status, waiting_until);
