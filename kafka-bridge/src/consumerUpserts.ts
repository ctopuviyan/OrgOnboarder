/**
 * Kafka consumer for upserts topic with batching
 */

import { Kafka, Consumer, EachMessagePayload } from 'kafkajs';
import { config } from './env';
import { logger } from './logger';
import { postJson } from './http';
import { Batcher } from './batching';
import type { UpsertMessage, UpsertRow } from './types';

const componentLogger = logger.withContext({ component: 'upserts-consumer' });

export class UpsertsConsumer {
  private consumer: Consumer;
  private batcher: Batcher;
  private flushTimer: NodeJS.Timeout | null = null;
  private isRunning = false;

  constructor(kafka: Kafka) {
    this.consumer = kafka.consumer({ 
      groupId: `${config.kafkaGroupId}-upserts`, // Separate group for upserts
      maxInFlightRequests: config.concurrency,
    });
    this.batcher = new Batcher();
  }

  async start(): Promise<void> {
    await this.consumer.connect();
    await this.consumer.subscribe({ 
      topic: config.topicUpserts,
      fromBeginning: false,
    });

    componentLogger.info('Subscribed to upserts topic', { 
      topic: config.topicUpserts,
      groupId: config.kafkaGroupId,
    });

    // Start periodic flush timer
    this.startFlushTimer();

    this.isRunning = true;

    await this.consumer.run({
      partitionsConsumedConcurrently: config.concurrency,
      eachMessage: async (payload: EachMessagePayload) => {
        await this.handleMessage(payload);
      },
    });
  }

  private async handleMessage(payload: EachMessagePayload): Promise<void> {
    const { message, partition, topic } = payload;
    
    if (!message.value) {
      componentLogger.warn('Received message with no value', { topic, partition });
      return;
    }

    let upsertMsg: UpsertMessage;
    
    try {
      const raw = message.value.toString();
      upsertMsg = JSON.parse(raw) as UpsertMessage;
    } catch (error) {
      componentLogger.warn('Malformed JSON, skipping message', {
        topic,
        partition,
        offset: message.offset,
        error: (error as Error).message,
      });
      // Commit to skip bad message (dead-letter via logs)
      return;
    }

    // Validate message structure
    if (!upsertMsg.orgId || !upsertMsg.eventId || !Array.isArray(upsertMsg.rows)) {
      componentLogger.warn('Invalid message structure, skipping', {
        topic,
        partition,
        offset: message.offset,
        hasOrgId: !!upsertMsg.orgId,
        hasEventId: !!upsertMsg.eventId,
        hasRows: Array.isArray(upsertMsg.rows),
      });
      return;
    }

    if (upsertMsg.rows.length === 0) {
      componentLogger.warn('Empty rows array, skipping', {
        orgId: upsertMsg.orgId,
        eventId: upsertMsg.eventId,
      });
      return;
    }

    // Normalize emails (lowercase, trim)
    const normalizedRows: UpsertRow[] = upsertMsg.rows.map(row => ({
      email: row.email?.toLowerCase().trim() || '',
      statusInOrg: row.statusInOrg || '',
    }));

    // Add rows to batch
    for (const row of normalizedRows) {
      const shouldFlush = this.batcher.add(upsertMsg.orgId, upsertMsg.eventId, row);
      
      // If batch is full, flush immediately
      if (shouldFlush) {
        await this.flushBatch(upsertMsg.orgId, upsertMsg.eventId);
      }
    }

    componentLogger.info('Batched upsert rows', {
      orgId: upsertMsg.orgId,
      eventId: upsertMsg.eventId,
      rowCount: normalizedRows.length,
      batchCount: this.batcher.size(),
      totalRows: this.batcher.totalRows(),
    });
  }

  private async flushBatch(orgId: string, eventId: string): Promise<void> {
    const payload = this.batcher.flush(orgId, eventId);
    
    if (!payload || payload.messages.length === 0) {
      return;
    }

    try {
      const response = await postJson(
        '/ingest/kafka/upserts',
        { orgId, eventId },
        payload
      );

      if (response.status >= 200 && response.status < 300) {
        componentLogger.info('Flushed upsert batch', {
          orgId,
          eventId,
          rowCount: payload.messages.length,
          status: response.status,
        });
      } else {
        componentLogger.error('Failed to flush upsert batch', {
          orgId,
          eventId,
          rowCount: payload.messages.length,
          status: response.status,
        });
        // Note: Retries are handled in postJson, if we get here it's final failure
        // In production, consider dead-letter queue or alerting
      }
    } catch (error) {
      componentLogger.error('Exception flushing upsert batch', {
        orgId,
        eventId,
        rowCount: payload.messages.length,
        error: (error as Error).message,
      });
      // Network failure after all retries - consider dead-letter queue
    }
  }

  private startFlushTimer(): void {
    this.flushTimer = setInterval(async () => {
      const dueBatches = this.batcher.getDueBatches();
      
      if (dueBatches.length > 0) {
        componentLogger.info('Flushing due batches', { count: dueBatches.length });
        
        for (const { key, payload } of dueBatches) {
          try {
            const response = await postJson(
              '/ingest/kafka/upserts',
              { orgId: key.orgId, eventId: key.eventId },
              payload
            );

            if (response.status >= 200 && response.status < 300) {
              componentLogger.info('Flushed due batch', {
                orgId: key.orgId,
                eventId: key.eventId,
                rowCount: payload.messages.length,
                status: response.status,
              });
            }
          } catch (error) {
            componentLogger.error('Exception flushing due batch', {
              orgId: key.orgId,
              eventId: key.eventId,
              error: (error as Error).message,
            });
          }
        }
      }
    }, config.batchMaxMs);
  }

  async stop(): Promise<void> {
    componentLogger.info('Stopping upserts consumer');
    this.isRunning = false;

    // Stop flush timer
    if (this.flushTimer) {
      clearInterval(this.flushTimer);
      this.flushTimer = null;
    }

    // Flush all pending batches
    const allBatches = this.batcher.flushAll();
    componentLogger.info('Flushing pending batches on shutdown', { count: allBatches.length });

    for (const { key, payload } of allBatches) {
      try {
        await postJson(
          '/ingest/kafka/upserts',
          { orgId: key.orgId, eventId: key.eventId },
          payload
        );
        componentLogger.info('Flushed shutdown batch', {
          orgId: key.orgId,
          eventId: key.eventId,
          rowCount: payload.messages.length,
        });
      } catch (error) {
        componentLogger.error('Failed to flush shutdown batch', {
          orgId: key.orgId,
          eventId: key.eventId,
          error: (error as Error).message,
        });
      }
    }

    // Disconnect consumer
    await this.consumer.disconnect();
    componentLogger.info('Upserts consumer stopped');
  }

  getStatus(): 'running' | 'stopped' {
    return this.isRunning ? 'running' : 'stopped';
  }
}
