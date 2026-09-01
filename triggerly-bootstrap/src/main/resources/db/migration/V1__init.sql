CREATE TABLE event_definition (
  id VARCHAR(150) PRIMARY KEY,
  tenant_id VARCHAR(50) NOT NULL,
  code VARCHAR(100) NOT NULL,
  display_name VARCHAR(200) NOT NULL,
  created_at DATETIME NOT NULL,
  UNIQUE KEY uk_event_definition_tenant_code (tenant_id, code)
);

CREATE TABLE attribute_definition (
  id VARCHAR(36) PRIMARY KEY,
  tenant_id VARCHAR(50) NOT NULL,
  event_definition_id VARCHAR(150) NULL,
  attr_key VARCHAR(100) NOT NULL,
  display_name VARCHAR(200) NOT NULL,
  attr_type VARCHAR(20) NOT NULL,
  filterable BOOLEAN NOT NULL DEFAULT FALSE,
  created_at DATETIME NOT NULL,
  KEY idx_attribute_definition_tenant (tenant_id)
);

CREATE TABLE member (
  id VARCHAR(36) PRIMARY KEY,
  tenant_id VARCHAR(50) NOT NULL,
  external_member_id VARCHAR(100) NOT NULL,
  name VARCHAR(100) NULL,
  email VARCHAR(200) NULL,
  telephone VARCHAR(30) NULL,
  device_platform VARCHAR(20) NULL,
  gender VARCHAR(10) NULL,
  birthday DATE NULL,
  status VARCHAR(20) NULL,
  joined_at DATETIME NULL,
  last_login_at DATETIME NULL,
  withdrawn_at DATETIME NULL,
  created_at DATETIME NOT NULL,
  marketing_sms_agreed BOOLEAN NOT NULL DEFAULT FALSE,
  marketing_push_agreed BOOLEAN NOT NULL DEFAULT FALSE,
  marketing_email_agreed BOOLEAN NOT NULL DEFAULT FALSE,
  marketing_kakao_agreed BOOLEAN NOT NULL DEFAULT FALSE,
  marketing_agreed_at DATETIME NULL,
  attributes JSON NULL,
  UNIQUE KEY uk_member_tenant_external (tenant_id, external_member_id)
);

CREATE TABLE event_instance (
  id VARCHAR(36) PRIMARY KEY,
  tenant_id VARCHAR(50) NOT NULL,
  event_code VARCHAR(100) NOT NULL,
  occurred_at DATETIME NOT NULL,
  member_id VARCHAR(36) NULL,
  attributes JSON NULL,
  KEY idx_event_instance_occurred_at (occurred_at)
);

CREATE TABLE workflow (
  id VARCHAR(36) PRIMARY KEY,
  tenant_id VARCHAR(50) NOT NULL,
  name VARCHAR(200) NULL,
  trigger_event_code VARCHAR(100) NOT NULL,
  definition_json JSON NOT NULL,
  version BIGINT NOT NULL DEFAULT 1,
  status VARCHAR(20) NOT NULL,
  created_at DATETIME NOT NULL,
  last_updated_at DATETIME NOT NULL,
  KEY idx_workflow_tenant_trigger (tenant_id, trigger_event_code, status)
);

CREATE TABLE workflow_instance (
  id VARCHAR(36) PRIMARY KEY,
  workflow_id VARCHAR(36) NOT NULL,
  tenant_id VARCHAR(50) NOT NULL,
  trigger_event_code VARCHAR(100) NOT NULL,
  version BIGINT NOT NULL,
  member_id VARCHAR(36) NULL,
  status VARCHAR(20) NOT NULL,
  current_node_id VARCHAR(50) NULL,
  waiting_event_name VARCHAR(100) NULL,
  waiting_until DATETIME NULL,
  started_at DATETIME NULL,
  completed_at DATETIME NULL,
  KEY idx_workflow_instance_waiting (status, waiting_until)
);

CREATE TABLE workflow_execution (
  id VARCHAR(36) PRIMARY KEY,
  workflow_instance_id VARCHAR(36) NOT NULL,
  node_id VARCHAR(50) NOT NULL,
  node_type VARCHAR(20) NOT NULL,
  status VARCHAR(20) NOT NULL,
  started_at DATETIME NULL,
  completed_at DATETIME NULL,
  result VARCHAR(500) NULL,
  error_code VARCHAR(100) NULL,
  KEY idx_workflow_execution_instance_node (workflow_instance_id, node_id)
);
