import { normalizeStatus, isValidEmail } from './status';

export interface RawEmployee {
  email?: string;
  statusInOrg?: string;
  ts?: string | number;
  eventId?: string | number;
  [key: string]: any; // Allow additional fields
}

export interface NormalizedEmployee {
  email: string;
  statusInOrg: 'active' | 'inactive' | 'left';
}

/**
 * Normalize a single employee row
 * Returns null if the row is invalid
 */
export function normalizeRow(row: RawEmployee): NormalizedEmployee | null {
  // Extract and validate email
  const email = row.email?.toLowerCase().trim();
  
  if (!email || !isValidEmail(email)) {
    console.warn(`Invalid or missing email in row:`, row);
    return null;
  }
  
  // Map status to normalized value
  const statusInOrg = normalizeStatus(row.statusInOrg);
  
  return {
    email,
    statusInOrg
  };
}
