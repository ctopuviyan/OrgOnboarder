/**
 * In-memory batching system for upserts by orgId and eventId
 */

import { config } from './env';
import type { UpsertRow, UpsertBatchPayload } from './types';

interface BatchKey {
  orgId: string;
  eventId: string;
}

interface Batch {
  key: BatchKey;
  rows: UpsertRow[];
  createdAt: number;
}

export class Batcher {
  private batches: Map<string, Batch> = new Map();

  private getBatchId(orgId: string, eventId: string): string {
    return `${orgId}::${eventId}`;
  }

  /**
   * Add a row to the appropriate batch
   * @returns true if batch is ready to flush
   */
  add(orgId: string, eventId: string, row: UpsertRow): boolean {
    const batchId = this.getBatchId(orgId, eventId);
    let batch = this.batches.get(batchId);

    if (!batch) {
      batch = {
        key: { orgId, eventId },
        rows: [],
        createdAt: Date.now(),
      };
      this.batches.set(batchId, batch);
    }

    batch.rows.push(row);

    // Check if batch should be flushed
    return batch.rows.length >= config.batchMaxRows;
  }

  /**
   * Get batches that are due for flushing (by size or age)
   */
  getDueBatches(): Array<{ key: BatchKey; payload: UpsertBatchPayload }> {
    const now = Date.now();
    const due: Array<{ key: BatchKey; payload: UpsertBatchPayload }> = [];

    for (const [batchId, batch] of this.batches.entries()) {
      const age = now - batch.createdAt;
      const isFull = batch.rows.length >= config.batchMaxRows;
      const isOld = age >= config.batchMaxMs;

      if (isFull || isOld) {
        due.push({
          key: batch.key,
          payload: {
            orgId: batch.key.orgId,
            messages: batch.rows, // Changed from 'rows' to 'messages' for Normalizer API
          },
        });
        this.batches.delete(batchId);
      }
    }

    return due;
  }

  /**
   * Flush a specific batch by orgId and eventId
   */
  flush(orgId: string, eventId: string): UpsertBatchPayload | null {
    const batchId = this.getBatchId(orgId, eventId);
    const batch = this.batches.get(batchId);

    if (!batch) {
      return null;
    }

    this.batches.delete(batchId);
    return {
      orgId: batch.key.orgId,
      messages: batch.rows, // Changed from 'rows' to 'messages' for Normalizer API
    };
  }

  /**
   * Flush all batches (for graceful shutdown)
   */
  flushAll(): Array<{ key: BatchKey; payload: UpsertBatchPayload }> {
    const all: Array<{ key: BatchKey; payload: UpsertBatchPayload }> = [];

    for (const [batchId, batch] of this.batches.entries()) {
      all.push({
        key: batch.key,
        payload: {
          orgId: batch.key.orgId,
          messages: batch.rows, // Changed from 'rows' to 'messages' for Normalizer API
        },
      });
      this.batches.delete(batchId);
    }

    return all;
  }

  /**
   * Get current batch count (for monitoring)
   */
  size(): number {
    return this.batches.size;
  }

  /**
   * Get total rows across all batches (for monitoring)
   */
  totalRows(): number {
    let total = 0;
    for (const batch of this.batches.values()) {
      total += batch.rows.length;
    }
    return total;
  }
}
