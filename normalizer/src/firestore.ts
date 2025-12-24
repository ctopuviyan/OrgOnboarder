import { Firestore, BulkWriter, DocumentReference } from '@google-cloud/firestore';
import * as dotenv from 'dotenv';

dotenv.config();

let db: Firestore | null = null;

/**
 * Initialize and return Firestore Admin SDK instance
 */
export function getDb(): Firestore {
  if (!db) {
    const projectId = process.env.GCP_PROJECT_ID;
    const databaseId = process.env.FIRESTORE_DATABASE_ID || '(default)';
    
    if (!projectId) {
      throw new Error('GCP_PROJECT_ID environment variable is required');
    }

    db = new Firestore({
      projectId,
      databaseId: databaseId === '(default)' ? undefined : databaseId,
      timestampsInSnapshots: true,
    });
  }
  return db;
}

/**
 * Get reference to employees collection for an organization
 */
export function employeesRef(orgId: string): FirebaseFirestore.CollectionReference {
  return getDb()
    .collection('organizations')
    .doc(orgId)
    .collection('employees');
}

/**
 * Get reference to organization document
 */
export function orgRef(orgId: string): DocumentReference {
  return getDb().collection('organizations').doc(orgId);
}

/**
 * Create a BulkWriter instance with error handling
 * Note: Rate limiting is handled automatically by BulkWriter in newer Firestore SDK versions
 */
export function bulkWriter(): BulkWriter {
  const writer = getDb().bulkWriter();
  
  // Configure error handling with retry logic
  writer.onWriteError((error) => {
    if (error.failedAttempts < 3) {
      return true; // Retry
    } else {
      console.error('Write failed after 3 attempts:', error);
      return false; // Don't retry
    }
  });
  
  return writer;
}

export type EmployeeData = {
  email: string;
  statusInOrg: 'active' | 'inactive' | 'left' | 'removed';
  presentInLatest: boolean;
  lastSeenEpoch: number;
  updatedAt: FirebaseFirestore.Timestamp;
  source?: string;
  lastEventId?: string;
};

export type OrgData = {
  currentEpoch: number;
  updatedAt: FirebaseFirestore.Timestamp;
};
