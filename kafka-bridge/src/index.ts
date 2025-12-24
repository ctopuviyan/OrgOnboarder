/**
 * Kafka Bridge - Main entry point
 * Consumes Kafka topics and forwards to Normalizer HTTP endpoints
 */

import * as http from 'http';
import { Kafka, CompressionTypes, CompressionCodecs } from 'kafkajs';
import SnappyCodec from 'kafkajs-snappy';
import { config } from './env';
import { logger } from './logger';
import { UpsertsConsumer } from './consumerUpserts';
import { DeltasConsumer } from './consumerDeltas';
import type { HealthStatus } from './types';

// Register Snappy compression codec
CompressionCodecs[CompressionTypes.Snappy] = SnappyCodec;

const startTime = Date.now();

// Initialize Kafka client
const kafkaConfig: any = {
  clientId: config.kafkaClientId,
  brokers: config.kafkaBrokers,
  retry: {
    retries: 8,
    initialRetryTime: 300,
    maxRetryTime: 30000,
  },
};

// Add SSL/SASL for Upstash or other managed Kafka
if (config.kafkaSsl) {
  kafkaConfig.ssl = true;
}

if (config.kafkaSaslUsername && config.kafkaSaslPassword) {
  kafkaConfig.sasl = {
    mechanism: config.kafkaSaslMechanism || 'scram-sha-256',
    username: config.kafkaSaslUsername,
    password: config.kafkaSaslPassword,
  };
  logger.info('Kafka SASL authentication enabled', {
    mechanism: kafkaConfig.sasl.mechanism,
    username: config.kafkaSaslUsername.substring(0, 4) + '***',
  });
}

const kafka = new Kafka(kafkaConfig);

// Initialize consumers
const upsertsConsumer = new UpsertsConsumer(kafka);
const deltasConsumer = new DeltasConsumer(kafka);

/**
 * Health check HTTP server
 */
function createHealthServer(): http.Server {
  const server = http.createServer((req, res) => {
    if (req.url === '/health' && req.method === 'GET') {
      const uptime = Math.floor((Date.now() - startTime) / 1000);
      const health: HealthStatus = {
        status: 'ok',
        upserts: upsertsConsumer.getStatus(),
        deltas: deltasConsumer.getStatus(),
        uptime,
      };

      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify(health, null, 2));
    } else {
      res.writeHead(404, { 'Content-Type': 'text/plain' });
      res.end('Not Found');
    }
  });

  return server;
}

/**
 * Graceful shutdown handler
 */
async function gracefulShutdown(signal: string): Promise<void> {
  logger.info(`Received ${signal}, starting graceful shutdown`);

  try {
    // Stop accepting new health checks
    healthServer.close(() => {
      logger.info('Health server closed');
    });

    // Stop consumers (will flush pending batches and commit offsets)
    await Promise.all([
      upsertsConsumer.stop(),
      deltasConsumer.stop(),
    ]);

    logger.info('Graceful shutdown complete');
    process.exit(0);
  } catch (error) {
    logger.error('Error during graceful shutdown', {
      error: (error as Error).message,
    });
    process.exit(1);
  }
}

/**
 * Main bootstrap
 */
async function main(): Promise<void> {
  logger.info('Starting Kafka Bridge', {
    kafkaBrokers: config.kafkaBrokers,
    kafkaClientId: config.kafkaClientId,
    kafkaGroupId: config.kafkaGroupId,
    topicUpserts: config.topicUpserts,
    topicDeltas: config.topicDeltas,
    normalizerBaseUrl: config.normalizerBaseUrl,
    batchMaxRows: config.batchMaxRows,
    batchMaxMs: config.batchMaxMs,
    concurrency: config.concurrency,
  });

  try {
    // Start health server
    const healthServer = createHealthServer();
    healthServer.listen(config.port, () => {
      logger.info(`Health server listening on port ${config.port}`);
    });

    // Start consumers
    logger.info('Starting Kafka consumers');
    await Promise.all([
      upsertsConsumer.start(),
      deltasConsumer.start(),
    ]);

    logger.info('Kafka Bridge is running');
  } catch (error) {
    logger.error('Failed to start Kafka Bridge', {
      error: (error as Error).message,
      stack: (error as Error).stack,
    });
    process.exit(1);
  }
}

// Create health server (needs to be accessible for gracefulShutdown)
const healthServer = createHealthServer();

// Register shutdown handlers
process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
process.on('SIGINT', () => gracefulShutdown('SIGINT'));

// Handle uncaught errors
process.on('uncaughtException', (error) => {
  logger.error('Uncaught exception', {
    error: error.message,
    stack: error.stack,
  });
  gracefulShutdown('uncaughtException');
});

process.on('unhandledRejection', (reason) => {
  logger.error('Unhandled rejection', {
    reason: String(reason),
  });
  gracefulShutdown('unhandledRejection');
});

// Start the application
main().catch((error) => {
  logger.error('Fatal error in main', {
    error: error.message,
    stack: error.stack,
  });
  process.exit(1);
});
