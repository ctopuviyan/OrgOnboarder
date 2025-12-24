/**
 * Convert messy organization status strings to normalized values
 */
export function normalizeStatus(status: string | undefined | null): 'active' | 'inactive' | 'left' {
  if (!status) {
    return 'active'; // Default to active if no status provided
  }
  
  const normalized = status.toLowerCase().trim();
  
  // Map various status strings to our three states
  const statusMap: Record<string, 'active' | 'inactive' | 'left'> = {
    // Active states
    'active': 'active',
    'employed': 'active',
    'current': 'active',
    'working': 'active',
    'full-time': 'active',
    'fulltime': 'active',
    'part-time': 'active',
    'parttime': 'active',
    'contractor': 'active',
    'consultant': 'active',
    'intern': 'active',
    
    // Inactive states (temporary absence)
    'inactive': 'inactive',
    'on leave': 'inactive',
    'onleave': 'inactive',
    'leave': 'inactive',
    'sabbatical': 'inactive',
    'maternity': 'inactive',
    'paternity': 'inactive',
    'medical': 'inactive',
    'suspended': 'inactive',
    
    // Left states (permanent departure)
    'left': 'left',
    'terminated': 'left',
    'former': 'left',
    'resigned': 'left',
    'retired': 'left',
    'departed': 'left',
    'exited': 'left',
    'quit': 'left',
    'fired': 'left',
    'removed': 'left',
  };
  
  // Check for exact match first
  if (statusMap[normalized]) {
    return statusMap[normalized];
  }
  
  // Check for partial matches
  for (const [key, value] of Object.entries(statusMap)) {
    if (normalized.includes(key)) {
      return value;
    }
  }
  
  // Default to inactive for unknown statuses
  console.warn(`Unknown status '${status}' defaulting to 'inactive'`);
  return 'inactive';
}

/**
 * Validate email format using simple RFC-ish regex
 */
export function isValidEmail(email: string | undefined | null): boolean {
  if (!email) {
    return false;
  }
  
  // Simple email validation regex
  const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
  return emailRegex.test(email.trim());
}
