import { getDb, orgRef, employeesRef } from './firestore';
import { Timestamp } from '@google-cloud/firestore';

/**
 * Begin a new epoch run for an organization
 * Reads the current epoch and increments it
 * @param orgId - Organization ID
 * @param orgName - Optional organization display name
 */
export async function beginRun(orgId: string, orgName?: string): Promise<number> {
  const orgDoc = orgRef(orgId);
  
  // Get current epoch, or start at 1
  const doc = await orgDoc.get();
  const currentEpoch = doc.exists ? (doc.data()?.currentEpoch || 0) : 0;
  const newEpoch = currentEpoch + 1;
  
  // Build update data
  const updateData: any = {
    currentEpoch: newEpoch,
    updatedAt: Timestamp.now()
  };
  
  // Add orgName if provided
  if (orgName) {
    updateData.name = orgName;
  }
  
  // Update organization with new epoch
  await orgDoc.set(updateData, { merge: true });
  
  console.log(`Started epoch ${newEpoch} for organization ${orgId}${orgName ? ` (${orgName})` : ''}`);
  return newEpoch;
}

/**
 * Finalize an epoch run by marking missing employees
 * Employees not seen in this epoch are marked with presentInLatest=false
 */
export async function finalizeRun(orgId: string, epochNumber: number): Promise<void> {
  console.log(`Finalizing epoch ${epochNumber} for organization ${orgId}`);
  
  const employeesCollection = employeesRef(orgId);
  const db = getDb();
  
  let totalUpdated = 0;
  let lastDocSnapshot = null;
  const pageSize = 1000;
  
  // Process in batches to handle large collections
  while (true) {
    // Build query for employees that were present but not seen in this epoch
    let query = employeesCollection
      .where('presentInLatest', '==', true)
      .where('lastSeenEpoch', '<', epochNumber)
      .orderBy('lastSeenEpoch')
      .limit(pageSize);
    
    // Add pagination if not first page
    if (lastDocSnapshot) {
      query = query.startAfter(lastDocSnapshot);
    }
    
    const snapshot = await query.get();
    
    if (snapshot.empty) {
      break; // No more documents to process
    }
    
    // Use batch for atomic updates
    const batch = db.batch();
    
    snapshot.docs.forEach(doc => {
      batch.update(doc.ref, {
        presentInLatest: false,
        updatedAt: Timestamp.now()
      });
      totalUpdated++;
    });
    
    // Commit the batch
    await batch.commit();
    
    // Update pagination cursor
    lastDocSnapshot = snapshot.docs[snapshot.docs.length - 1];
    
    console.log(`Marked ${totalUpdated} employees as missing so far...`);
    
    // If we got less than pageSize, we're done
    if (snapshot.docs.length < pageSize) {
      break;
    }
  }
  
  // Update organization to mark epoch as complete
  await orgRef(orgId).update({
    currentEpoch: epochNumber,
    lastFinalizedEpoch: epochNumber,
    updatedAt: Timestamp.now()
  });
  
  console.log(`Finalization complete. Marked ${totalUpdated} employees as missing.`);
}
