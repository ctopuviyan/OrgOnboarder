/**
 * Environment configuration with validation and defaults
 */

export interface Config {
  // Kafka
  kafkaBrokers: string[];
  kafkaClientId: string;
  kafkaGroupId: string;
  topicUpserts: string;
  topicDeltas: string;
  kafkaSaslUsername?: string;
  kafkaSaslPassword?: string;
  kafkaSaslMechanism?: string;
  kafkaSsl?: boolean;

  // Normalizer HTTP
  normalizerBaseUrl: string;
  ingestionToken: string;

  // Batch & retry tuning
  batchMaxRows: number;
  batchMaxMs: number;
  httpTimeoutMs: number;
  retryBaseMs: number;
  retryMaxMs: number;
  maxRetries: number;
  concurrency: number;

  // Health server
  port: number;
}

function parseEnv(key: string, defaultValue?: string): string {
  const value = process.env[key] || defaultValue;
  if (!value) {
    throw new Error(`Missing required environment variable: ${key}`);
  }
  return value;
}

function parseNumber(key: string, defaultValue: number): number {
  const value = process.env[key];
  if (!value) return defaultValue;
  const parsed = parseInt(value, 10);
  if (isNaN(parsed)) {
    throw new Error(`Invalid number for ${key}: ${value}`);
  }
  return parsed;
}

function parseBrokers(brokersStr: string): string[] {
  return brokersStr.split(',').map(b => b.trim()).filter(b => b.length > 0);
}

export function loadConfig(): Config {
  const brokersStr = parseEnv('KAFKA_BROKERS');
  const brokers = parseBrokers(brokersStr);
  
  if (brokers.length === 0) {
    throw new Error('KAFKA_BROKERS must contain at least one broker');
  }

  const normalizerBaseUrl = parseEnv('NORMALIZER_BASE_URL');
  if (!normalizerBaseUrl.startsWith('http://') && !normalizerBaseUrl.startsWith('https://')) {
    throw new Error('NORMALIZER_BASE_URL must start with http:// or https://');
  }

  const ingestionToken = parseEnv('INGESTION_TOKEN');
  if (ingestionToken.length < 8) {
    throw new Error('INGESTION_TOKEN must be at least 8 characters');
  }

  return {
    // Kafka
    kafkaBrokers: brokers,
    kafkaClientId: parseEnv('KAFKA_CLIENT_ID', 'kafka-bridge'),
    kafkaGroupId: parseEnv('KAFKA_GROUP_ID', 'kafka-bridge-1'),
    topicUpserts: parseEnv('TOPIC_UPSERTS', 'org-roster-upserts'),
    topicDeltas: parseEnv('TOPIC_DELTAS', 'org-roster-deltas'),
    kafkaSaslUsername: process.env.KAFKA_SASL_USERNAME,
    kafkaSaslPassword: process.env.KAFKA_SASL_PASSWORD,
    kafkaSaslMechanism: process.env.KAFKA_SASL_MECHANISM,
    kafkaSsl: process.env.KAFKA_SSL === 'true',

    // Normalizer HTTP
    normalizerBaseUrl: normalizerBaseUrl.replace(/\/$/, ''), // remove trailing slash
    ingestionToken,

    // Batch & retry tuning (green defaults)
    batchMaxRows: parseNumber('BATCH_MAX_ROWS', 1000),
    batchMaxMs: parseNumber('BATCH_MAX_MS', 1200),
    httpTimeoutMs: parseNumber('HTTP_TIMEOUT_MS', 15000),
    retryBaseMs: parseNumber('RETRY_BASE_MS', 500),
    retryMaxMs: parseNumber('RETRY_MAX_MS', 15000),
    maxRetries: parseNumber('MAX_RETRIES', 8),
    concurrency: parseNumber('CONCURRENCY', 1),

    // Health server
    port: parseNumber('PORT', 8080),
  };
}

export const config = loadConfig();
