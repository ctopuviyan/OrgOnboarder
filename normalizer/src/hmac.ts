import { createHmac } from 'crypto';

/**
 * Generate HMAC-SHA256 hash of email using per-organization secret
 * Privacy-safe way to store email identifiers
 */
export function emailKey(orgId: string, email: string): string {
  const secretKey = `HMAC_SECRET_${orgId}`;
  const secret = process.env[secretKey];
  
  if (!secret) {
    throw new Error(`Missing HMAC secret for organization: ${orgId}. Please set environment variable ${secretKey}`);
  }
  
  // Normalize email to lowercase before hashing
  const normalizedEmail = email.toLowerCase().trim();
  
  // Create HMAC-SHA256 hash
  const hmac = createHmac('sha256', secret);
  hmac.update(normalizedEmail);
  
  // Return hex-encoded hash
  return hmac.digest('hex');
}
