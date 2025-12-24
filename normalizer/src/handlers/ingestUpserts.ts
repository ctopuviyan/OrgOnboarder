import { Request, Response } from 'express';
import { normalizeRow } from '../normalize';
import { beginRun, finalizeRun } from '../finalize';
import { getOptimizer } from '../utils/firestoreOptimizer';

interface KafkaUpsertMessage {
  email: string;
  statusInOrg?: string;
  ts?: string | number;
  eventId?: string | number;
  [key: string]: any;
}

interface KafkaUpsertPayload {
  orgId: string;
  messages: KafkaUpsertMessage[];
  closeAfter?: boolean; // If true, finalize the epoch after processing
}

/**
 * Handle employee upsert ingestion from Kafka Bridge - OPTIMIZED VERSION
 * POST /ingest/kafka/upserts
 * 
 * This is an HTTP endpoint handler that receives batched employee data from Kafka Bridge.
 * It does NOT consume from Kafka directly - that's handled by the kafka-bridge service.
 * 
 * Optimizations:
 * - Bulk queries with 'in' operator (10 emails per query)
 * - Firestore batch writes (500 operations per batch)
 * - Parallel batch commits (5 concurrent batches)
 * - Smart caching with TTL
 * - Adaptive batch sizing based on error rate
 * - Circuit breaker for error handling
 */
export async function handleIngestUpserts(req: Request, res: Response): Promise<void> {
  const startTime = Date.now();
  
  try {
    const payload = req.body as KafkaUpsertPayload;
    
    // Validate required fields
    if (!payload.orgId) {
      res.status(400).json({ error: 'orgId is required' });
      return;
    }
    
    if (!payload.messages || !Array.isArray(payload.messages)) {
      res.status(400).json({ error: 'messages array is required' });
      return;
    }
    
    const { orgId, messages, closeAfter = false } = payload;
    
    console.log(`[KafkaUpserts] Processing ${messages.length} messages for org ${orgId}`);
    
    // Begin a new epoch run if not already started
    const epochNumber = await beginRun(orgId);
    console.log(`[KafkaUpserts] Epoch number: ${epochNumber}`);
    
    // Normalize messages first
    const normalizedMessages: Array<{ email: string; [key: string]: any }> = [];
    let skipped = 0;
    
    for (const message of messages) {
      const normalized = normalizeRow(message);
      
      if (!normalized) {
        skipped++;
        continue;
      }
      
      normalizedMessages.push({
        email: normalized.email,
        statusInOrg: normalized.statusInOrg,
        eventId: message.eventId,
      });
    }
    
    console.log(`[KafkaUpserts] Normalized ${normalizedMessages.length} messages, skipped ${skipped}`);
    
    // Use optimized processor
    const optimizer = getOptimizer();
    const result = await optimizer.processUpserts(orgId, normalizedMessages, epochNumber);
    
    const duration = Date.now() - startTime;
    console.log(`[KafkaUpserts] Completed in ${duration}ms: ${result.processed} processed, ${result.errors} errors`);
    
    // If closeAfter flag is set, finalize the epoch
    if (closeAfter) {
      await finalizeRun(orgId, epochNumber);
      console.log(`[KafkaUpserts] Epoch ${epochNumber} finalized`);
    }
    
    res.json({
      success: true,
      processed: result.processed,
      skipped: skipped + result.skipped,
      errors: result.errors,
      epoch: epochNumber,
      finalized: closeAfter,
      durationMs: duration,
    });
    
  } catch (error) {
    const duration = Date.now() - startTime;
    console.error(`[KafkaUpserts] Error after ${duration}ms:`, error);
    
    res.status(500).json({ 
      error: 'Internal server error',
      message: error instanceof Error ? error.message : 'Unknown error',
      durationMs: duration,
    });
  }
}
