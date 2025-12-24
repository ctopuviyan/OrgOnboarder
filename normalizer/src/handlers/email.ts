import { Request, Response } from 'express';
import { Timestamp } from '@google-cloud/firestore';
import { employeesRef } from '../firestore';
import { normalizeRow, RawEmployee } from '../normalize';
import { beginRun, finalizeRun } from '../finalize';
import { parse } from 'csv-parse';
import * as XLSX from 'xlsx';
import { Readable } from 'stream';

interface EmailPayload {
  orgId: string;
  orgName?: string; // Organization display name
  rows?: RawEmployee[]; // For JSON body
  kind?: 'upserts' | 'deltas'; // Default: upserts
}

/**
 * Parse CSV data from buffer or stream
 */
async function parseCSV(data: Buffer): Promise<RawEmployee[]> {
  return new Promise((resolve, reject) => {
    const records: RawEmployee[] = [];
    const parser = parse({
      columns: true,
      skip_empty_lines: true,
      trim: true,
      cast: true,
      cast_date: false
    });
    
    parser.on('readable', function() {
      let record;
      while ((record = parser.read()) !== null) {
        records.push(record);
      }
    });
    
    parser.on('error', (err) => reject(err));
    parser.on('end', () => resolve(records));
    
    // Convert buffer to stream and pipe
    const stream = Readable.from(data);
    stream.pipe(parser);
  });
}

/**
 * Parse XLSX data from buffer
 */
function parseXLSX(data: Buffer): RawEmployee[] {
  const workbook = XLSX.read(data, { type: 'buffer' });
  const firstSheetName = workbook.SheetNames[0];
  
  if (!firstSheetName) {
    throw new Error('No sheets found in Excel file');
  }
  
  const worksheet = workbook.Sheets[firstSheetName];
  const jsonData = XLSX.utils.sheet_to_json(worksheet, {
    raw: false, // Convert all values to strings
    defval: '', // Default value for empty cells
    header: 'A' // Use first row as headers
  });
  
  return jsonData as RawEmployee[];
}

/**
 * Parse NDJSON (newline-delimited JSON)
 */
function parseNDJSON(data: string): RawEmployee[] {
  const lines = data.split('\n').filter(line => line.trim());
  const records: RawEmployee[] = [];
  
  for (const line of lines) {
    try {
      const record = JSON.parse(line);
      if (typeof record === 'object' && record !== null) {
        records.push(record);
      }
    } catch (err) {
      console.warn('Skipping invalid JSON line:', line);
    }
  }
  
  return records;
}

/**
 * Handle email attachments (CSV/XLSX/JSON)
 * POST /ingest/email
 */
export async function handleEmail(req: Request, res: Response): Promise<void> {
  try {
    let orgId: string;
    let orgName: string | undefined;
    let kind: 'upserts' | 'deltas' = 'upserts';
    let rows: RawEmployee[] = [];
    
    // Handle different content types
    const contentType = req.headers['content-type'] || '';
    
    if (contentType.includes('multipart/form-data')) {
      // File upload
      const file = (req as any).file;
      if (!file) {
        res.status(400).json({ error: 'No file uploaded' });
        return;
      }
      
      orgId = req.body.orgId;
      orgName = req.body.orgName;
      kind = req.body.kind || 'upserts';
      
      // Parse file based on extension or mimetype
      const filename = file.originalname.toLowerCase();
      
      if (filename.endsWith('.csv') || file.mimetype === 'text/csv') {
        rows = await parseCSV(file.buffer);
      } else if (filename.endsWith('.xlsx') || filename.endsWith('.xls') || 
                 file.mimetype === 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet') {
        rows = parseXLSX(file.buffer);
      } else if (filename.endsWith('.json')) {
        const jsonData = JSON.parse(file.buffer.toString());
        rows = Array.isArray(jsonData) ? jsonData : jsonData.rows || [];
      } else if (filename.endsWith('.ndjson')) {
        rows = parseNDJSON(file.buffer.toString());
      } else {
        res.status(400).json({ error: 'Unsupported file type. Use CSV, XLSX, JSON, or NDJSON' });
        return;
      }
    } else {
      // JSON body
      const payload = req.body as EmailPayload;
      orgId = payload.orgId;
      orgName = payload.orgName;
      kind = payload.kind || 'upserts';
      rows = payload.rows || [];
    }
    
    // Validate required fields
    if (!orgId) {
      res.status(400).json({ error: 'orgId is required' });
      return;
    }
    
    if (!rows || rows.length === 0) {
      res.status(400).json({ error: 'No data rows found' });
      return;
    }
    
    // Extract orgName from first row if not provided in request body
    if (!orgName && rows.length > 0) {
      const firstRow = rows[0] as any;
      orgName = firstRow.orgName || firstRow.org_name || firstRow.organizationName || firstRow.organization_name;
    }
    
    console.log(`Processing ${rows.length} rows for org ${orgId}${orgName ? ` (${orgName})` : ''}, kind: ${kind}`);
    
    if (kind === 'upserts') {
      await handleUpserts(orgId, rows, orgName);
    } else {
      await handleDeltas(orgId, rows);
    }
    
    res.json({
      success: true,
      processed: rows.length,
      kind
    });
    
  } catch (error) {
    console.error('Error processing email attachment:', error);
    res.status(500).json({ 
      error: 'Internal server error',
      message: error instanceof Error ? error.message : 'Unknown error'
    });
  }
}

/**
 * Process upsert rows
 */
async function handleUpserts(orgId: string, rows: RawEmployee[], orgName?: string): Promise<void> {
  // Begin a new epoch run (pass orgName to store in organization document)
  const epochNumber = await beginRun(orgId, orgName);
  
  const employeesCollection = employeesRef(orgId);
  
  let processed = 0;
  let skipped = 0;
  
  for (const row of rows) {
    const normalized = normalizeRow(row);
    
    if (!normalized) {
      skipped++;
      continue;
    }
    
    const { email, statusInOrg } = normalized;
    
    // Check if employee already exists (query by email)
    const existingQuery = await employeesCollection
      .where('email', '==', email)
      .limit(1)
      .get();
    
    const employeeData = {
      email,
      statusInOrg,
      presentInLatest: true,
      lastSeenEpoch: epochNumber,
      updatedAt: Timestamp.now(),
      source: 'email:upsert',
    };
    
    if (!existingQuery.empty) {
      // Update existing employee
      const docRef = existingQuery.docs[0].ref;
      await docRef.set(employeeData, { merge: true });
      console.log(`Updated existing employee: ${email}`);
    } else {
      // Create new employee with auto-generated ID
      const docRef = employeesCollection.doc(); // Auto-ID
      await docRef.set(employeeData);
      console.log(`Created new employee with auto-ID: ${email} -> ${docRef.id}`);
    }
    
    processed++;
  }
  
  console.log(`Processed ${processed} employees, skipped ${skipped}`);
  
  // Finalize the epoch
  await finalizeRun(orgId, epochNumber);
}

/**
 * Process delta rows
 */
async function handleDeltas(orgId: string, rows: RawEmployee[]): Promise<void> {
  const employeesCollection = employeesRef(orgId);
  
  let processed = 0;
  let skipped = 0;
  
  for (const row of rows) {
    // Check for delta type in row
    const deltaType = (row as any).deltaType || (row as any).delta_type;
    if (!deltaType) {
      console.warn('No deltaType found in row:', row);
      skipped++;
      continue;
    }
    
    const normalized = normalizeRow(row);
    
    if (!normalized) {
      skipped++;
      continue;
    }
    
    const { email } = normalized;
    
    // Find existing employee by email
    const existingQuery = await employeesCollection
      .where('email', '==', email)
      .limit(1)
      .get();
    
    if (existingQuery.empty) {
      console.warn(`Employee not found for delta update: ${email}`);
      skipped++;
      continue;
    }
    
    // Determine status based on delta type
    let statusInOrg: 'active' | 'inactive' | 'left';
    let presentInLatest: boolean;
    
    switch (deltaType.toLowerCase()) {
      case 'left':
      case 'terminated':
        statusInOrg = 'left';
        presentInLatest = false;
        break;
      case 'inactive':
      case 'on_leave':
        statusInOrg = 'inactive';
        presentInLatest = false;
        break;
      case 'reactivated':
      case 'active':
        statusInOrg = 'active';
        presentInLatest = true;
        break;
      default:
        console.warn(`Unknown delta type: ${deltaType}`);
        skipped++;
        continue;
    }
    
    // Update existing employee
    const docRef = existingQuery.docs[0].ref;
    await docRef.update({
      statusInOrg,
      presentInLatest,
      updatedAt: Timestamp.now(),
      source: 'email:delta',
    });
    
    console.log(`Updated employee delta: ${email} -> ${deltaType}`);
    processed++;
  }
  
  console.log(`Processed ${processed} deltas, skipped ${skipped}`);
}
