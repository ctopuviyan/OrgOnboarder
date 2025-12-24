/**
 * Message types from Kafka topics
 */

export interface UpsertRow {
  email: string;
  statusInOrg: string;
}

export interface UpsertMessage {
  orgId: string;
  rows: UpsertRow[];
  eventId: string;
}

export interface DeltaMessage {
  orgId: string;
  email: string;
  deltaType: 'left' | 'inactive' | 'reactivated';
  eventId: string;
}

/**
 * HTTP response wrapper
 */
export interface HttpResponse {
  status: number;
  text: string;
}

/**
 * Batch payload for upserts endpoint
 * Note: Normalizer API expects 'messages' field, not 'rows'
 */
export interface UpsertBatchPayload {
  orgId: string;
  messages: UpsertRow[];
}

/**
 * Delta payload for deltas endpoint
 */
export interface DeltaPayload {
  orgId: string;
  email: string;
  deltaType: string;
}

/**
 * Health check response
 */
export interface HealthStatus {
  status: 'ok' | 'degraded';
  upserts: 'running' | 'stopped' | 'error';
  deltas: 'running' | 'stopped' | 'error';
  uptime: number;
}
