/**
 * Kafka consumer for deltas topic (one-by-one processing)
 */

import { Kafka, Consumer, EachMessagePayload } from 'kafkajs';
import { config } from './env';
import { logger } from './logger';
import { postJson } from './http';
import type { DeltaMessage, DeltaPayload } from './types';

const componentLogger = logger.withContext({ component: 'deltas-consumer' });

export class DeltasConsumer {
  private consumer: Consumer;
  private isRunning = false;

  constructor(kafka: Kafka) {
    this.consumer = kafka.consumer({ 
      groupId: `${config.kafkaGroupId}-deltas`, // Separate group for deltas
      maxInFlightRequests: config.concurrency, // Keep at 1 to avoid reordering
    });
  }

  async start(): Promise<void> {
    await this.consumer.connect();
    await this.consumer.subscribe({ 
      topic: config.topicDeltas,
      fromBeginning: false,
    });

    componentLogger.info('Subscribed to deltas topic', { 
      topic: config.topicDeltas,
      groupId: config.kafkaGroupId,
    });

    this.isRunning = true;

    await this.consumer.run({
      partitionsConsumedConcurrently: config.concurrency,
      eachMessage: async (payload: EachMessagePayload) => {
        await this.handleMessage(payload);
      },
    });
  }

  private async handleMessage(msgPayload: EachMessagePayload): Promise<void> {
    const { message, partition, topic } = msgPayload;
    
    if (!message.value) {
      componentLogger.warn('Received message with no value', { topic, partition });
      return;
    }

    let deltaMsg: DeltaMessage;
    
    try {
      const raw = message.value.toString();
      deltaMsg = JSON.parse(raw) as DeltaMessage;
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
    if (!deltaMsg.orgId || !deltaMsg.email || !deltaMsg.deltaType || !deltaMsg.eventId) {
      componentLogger.warn('Invalid message structure, skipping', {
        topic,
        partition,
        offset: message.offset,
        hasOrgId: !!deltaMsg.orgId,
        hasEmail: !!deltaMsg.email,
        hasDeltaType: !!deltaMsg.deltaType,
        hasEventId: !!deltaMsg.eventId,
      });
      return;
    }

    // Validate deltaType
    const validTypes = ['left', 'inactive', 'reactivated'];
    if (!validTypes.includes(deltaMsg.deltaType)) {
      componentLogger.warn('Invalid deltaType, skipping', {
        orgId: deltaMsg.orgId,
        eventId: deltaMsg.eventId,
        deltaType: deltaMsg.deltaType,
        validTypes,
      });
      return;
    }

    // Normalize email
    const normalizedEmail = deltaMsg.email.toLowerCase().trim();

    // Build payload
    const payload: DeltaPayload = {
      orgId: deltaMsg.orgId,
      email: normalizedEmail,
      deltaType: deltaMsg.deltaType,
    };

    // POST to Normalizer
    try {
      const response = await postJson(
        '/ingest/kafka/deltas',
        { orgId: deltaMsg.orgId, eventId: deltaMsg.eventId },
        payload
      );

      if (response.status >= 200 && response.status < 300) {
        componentLogger.info('Processed delta event', {
          orgId: deltaMsg.orgId,
          eventId: deltaMsg.eventId,
          email: normalizedEmail,
          deltaType: deltaMsg.deltaType,
          status: response.status,
        });
      } else {
        componentLogger.error('Failed to process delta event', {
          orgId: deltaMsg.orgId,
          eventId: deltaMsg.eventId,
          email: normalizedEmail,
          deltaType: deltaMsg.deltaType,
          status: response.status,
        });
        // Note: Retries are handled in postJson, if we get here it's final failure
        // In production, consider dead-letter queue or alerting
      }
    } catch (error) {
      componentLogger.error('Exception processing delta event', {
        orgId: deltaMsg.orgId,
        eventId: deltaMsg.eventId,
        email: normalizedEmail,
        deltaType: deltaMsg.deltaType,
        error: (error as Error).message,
      });
      // Network failure after all retries - consider dead-letter queue
    }
  }

  async stop(): Promise<void> {
    componentLogger.info('Stopping deltas consumer');
    this.isRunning = false;

    // Disconnect consumer (will commit offsets for successfully processed messages)
    await this.consumer.disconnect();
    componentLogger.info('Deltas consumer stopped');
  }

  getStatus(): 'running' | 'stopped' {
    return this.isRunning ? 'running' : 'stopped';
  }
}
