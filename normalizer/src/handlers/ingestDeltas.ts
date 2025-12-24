import { Request, Response } from 'express';
import { Timestamp } from '@google-cloud/firestore';
import { employeesRef } from '../firestore';
import { isValidEmail } from '../status';

interface KafkaDeltaMessage {
  email: string;
  deltaType: 'left' | 'inactive' | 'reactivated';
  ts?: string | number;
  eventId?: string | number;
}

interface KafkaDeltaPayload {
  orgId: string;
  messages: KafkaDeltaMessage[];
}

/**
 * Handle employee delta ingestion from Kafka Bridge for immediate status changes
 * POST /ingest/kafka/deltas
 * 
 * This is an HTTP endpoint handler that receives delta messages from Kafka Bridge.
 * It does NOT consume from Kafka directly - that's handled by the kafka-bridge service.
 */
export async function handleIngestDeltas(req: Request, res: Response): Promise<void> {
  try {
    const payload = req.body as KafkaDeltaPayload;
    
    // Validate required fields
    if (!payload.orgId) {
      res.status(400).json({ error: 'orgId is required' });
      return;
    }
    
    if (!payload.messages || !Array.isArray(payload.messages)) {
      res.status(400).json({ error: 'messages array is required' });
      return;
    }
    
    const { orgId, messages } = payload;
    
    console.log(`Processing ${messages.length} delta messages for org ${orgId}`);
    
    const employeesCollection = employeesRef(orgId);
    
    let processed = 0;
    let skipped = 0;
    
    for (const message of messages) {
      // Validate email
      if (!message.email || !isValidEmail(message.email)) {
        console.warn(`Invalid email in delta message:`, message);
        skipped++;
        continue;
      }
      
      const email = message.email.toLowerCase().trim();
      
      // Find existing employee by email
      const existingQuery = await employeesCollection
        .where('email', '==', email)
        .limit(1)
        .get();
      
      if (existingQuery.empty) {
        console.warn(`Kafka: Employee not found for delta update: ${email}`);
        skipped++;
        continue;
      }
      
      // Determine the new status based on delta type
      let statusInOrg: 'active' | 'inactive' | 'left';
      
      switch (message.deltaType) {
        case 'left':
          statusInOrg = 'left';
          break;
        case 'inactive':
          statusInOrg = 'inactive';
          break;
        case 'reactivated':
          statusInOrg = 'active';
          break;
        default:
          console.warn(`Unknown delta type: ${(message as any).deltaType}`);
          skipped++;
          continue;
      }
      
      // Prepare update data
      const updateData: any = {
        statusInOrg,
        updatedAt: Timestamp.now(),
        source: 'kafka:delta',
      };
      
      // Update presentInLatest (don't override for reactivated)
      if (message.deltaType === 'reactivated') {
        updateData.presentInLatest = true;
      } else {
        updateData.presentInLatest = false;
      }
      
      // Add optional fields
      if (message.eventId) {
        updateData.lastEventId = String(message.eventId);
      }
      
      // Update existing employee
      const docRef = existingQuery.docs[0].ref;
      await docRef.update(updateData);
      
      console.log(`Kafka: Updated employee delta: ${email} -> ${message.deltaType}`);
      processed++;
    }
    
    console.log(`Processed ${processed} deltas, skipped ${skipped}`);
    
    res.json({
      success: true,
      processed,
      skipped
    });
    
  } catch (error) {
    console.error('Error processing Kafka deltas:', error);
    res.status(500).json({ 
      error: 'Internal server error',
      message: error instanceof Error ? error.message : 'Unknown error'
    });
  }
}
