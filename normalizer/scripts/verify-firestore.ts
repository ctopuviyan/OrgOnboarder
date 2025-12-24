#!/usr/bin/env ts-node
/**
 * Firestore Verification Script for Normalizer
 * 
 * Reads Firestore and prints a concise report for a given orgId:
 * - Organization document with currentEpoch
 * - Employee counts by statusInOrg and presentInLatest
 * - Most recent employees with key details
 * - Latest ingestion events
 * 
 * Usage:
 *   npm run verify:firestore -- --orgId=acme-corp --limit=5
 *   ts-node scripts/verify-firestore.ts --orgId=acme-corp
 * 
 * Exit codes:
 *   0: Success
 *   1: Invalid arguments
 *   2: Organization not found
 *   3: No employees found
 */

import { Firestore } from '@google-cloud/firestore';

// ANSI color codes for terminal output
const colors = {
  reset: '\x1b[0m',
  bright: '\x1b[1m',
  red: '\x1b[31m',
  green: '\x1b[32m',
  yellow: '\x1b[33m',
  blue: '\x1b[34m',
  cyan: '\x1b[36m',
};

interface CliArgs {
  orgId: string;
  limit: number;
}

interface OrgDoc {
  currentEpoch?: number;
  createdAt?: any;
  updatedAt?: any;
  [key: string]: any;
}

interface EmployeeDoc {
  emailKey?: string;
  emailNormalized?: string;
  statusInOrg?: string;
  presentInLatest?: boolean;
  lastSeenEpoch?: number;
  lastEventId?: string;
  updatedAt?: any;
  [key: string]: any;
}

interface IngestionEventDoc {
  eventId?: string;
  status?: string;
  kind?: string;
  rowCount?: number;
  createdAt?: any;
  [key: string]: any;
}

interface StatusCounts {
  active: number;
  inactive: number;
  left: number;
  terminated: number;
  current: number;
  other: number;
}

interface PresentCounts {
  present: number;
  notPresent: number;
}

/**
 * Parse command line arguments
 */
function parseArgs(): CliArgs {
  const args = process.argv.slice(2);
  let orgId = '';
  let limit = 5;

  for (const arg of args) {
    if (arg.startsWith('--orgId=')) {
      orgId = arg.split('=')[1];
    } else if (arg.startsWith('--limit=')) {
      limit = parseInt(arg.split('=')[1], 10);
    }
  }

  if (!orgId) {
    console.error(`${colors.red}Error: --orgId is required${colors.reset}`);
    console.error('Usage: npm run verify:firestore -- --orgId=<orgId> [--limit=<n>]');
    process.exit(1);
  }

  if (isNaN(limit) || limit < 1) {
    console.error(`${colors.red}Error: --limit must be a positive number${colors.reset}`);
    process.exit(1);
  }

  return { orgId, limit };
}

/**
 * Format timestamp for display
 */
function formatTimestamp(timestamp: any): string {
  if (!timestamp) return 'N/A';
  try {
    if (timestamp.toDate) {
      return timestamp.toDate().toISOString();
    }
    return new Date(timestamp).toISOString();
  } catch {
    return 'Invalid';
  }
}

/**
 * Print section header
 */
function printHeader(title: string) {
  console.log(`\n${colors.bright}${colors.cyan}=== ${title} ===${colors.reset}`);
}

/**
 * Print key-value pair
 */
function printKV(key: string, value: any, indent = 0) {
  const spaces = '  '.repeat(indent);
  console.log(`${spaces}${colors.yellow}${key}:${colors.reset} ${value}`);
}

/**
 * Get employee counts by status
 */
async function getStatusCounts(
  employeesCol: FirebaseFirestore.CollectionReference
): Promise<StatusCounts> {
  const counts: StatusCounts = {
    active: 0,
    inactive: 0,
    left: 0,
    terminated: 0,
    current: 0,
    other: 0,
  };

  // Query for each known status
  const statuses = ['active', 'inactive', 'left', 'terminated', 'current'];
  
  for (const status of statuses) {
    const snapshot = await employeesCol.where('statusInOrg', '==', status).count().get();
    counts[status as keyof StatusCounts] = snapshot.data().count;
  }

  // Get total count to calculate "other"
  const totalSnapshot = await employeesCol.count().get();
  const total = totalSnapshot.data().count;
  const knownTotal = Object.values(counts).reduce((sum, count) => sum + count, 0);
  counts.other = total - knownTotal;

  return counts;
}

/**
 * Get employee counts by presentInLatest
 */
async function getPresentCounts(
  employeesCol: FirebaseFirestore.CollectionReference
): Promise<PresentCounts> {
  const presentSnapshot = await employeesCol.where('presentInLatest', '==', true).count().get();
  const notPresentSnapshot = await employeesCol.where('presentInLatest', '==', false).count().get();

  return {
    present: presentSnapshot.data().count,
    notPresent: notPresentSnapshot.data().count,
  };
}

/**
 * Main verification function
 */
async function verifyFirestore() {
  const { orgId, limit } = parseArgs();

  console.log(`${colors.bright}Firestore Verification for Organization: ${orgId}${colors.reset}`);
  console.log(`Limit: ${limit} records\n`);

  // Initialize Firestore with Application Default Credentials
  const db = new Firestore();

  try {
    // 1. Verify organization exists
    printHeader('Organization Document');
    const orgRef = db.collection('organizations').doc(orgId);
    const orgSnap = await orgRef.get();

    if (!orgSnap.exists) {
      console.error(`${colors.red}✗ Organization '${orgId}' not found${colors.reset}`);
      process.exit(2);
    }

    const orgData = orgSnap.data() as OrgDoc;
    console.log(`${colors.green}✓ Organization found${colors.reset}`);
    printKV('Current Epoch', orgData.currentEpoch ?? 'N/A');
    printKV('Created At', formatTimestamp(orgData.createdAt));
    printKV('Updated At', formatTimestamp(orgData.updatedAt));

    // 2. Get employee collection reference
    const employeesCol = orgRef.collection('employees');

    // Check if employees exist
    const employeeCountSnap = await employeesCol.count().get();
    const totalEmployees = employeeCountSnap.data().count;

    if (totalEmployees === 0) {
      console.error(`\n${colors.red}✗ No employees found for organization '${orgId}'${colors.reset}`);
      process.exit(3);
    }

    // 3. Get status counts
    printHeader('Employee Counts by Status');
    console.log(`${colors.green}✓ Total Employees: ${totalEmployees}${colors.reset}`);
    
    const statusCounts = await getStatusCounts(employeesCol);
    printKV('Active', statusCounts.active, 1);
    printKV('Inactive', statusCounts.inactive, 1);
    printKV('Left', statusCounts.left, 1);
    printKV('Terminated', statusCounts.terminated, 1);
    printKV('Current', statusCounts.current, 1);
    if (statusCounts.other > 0) {
      printKV('Other/Unknown', statusCounts.other, 1);
    }

    // 4. Get presentInLatest counts
    printHeader('Employee Counts by Presence');
    const presentCounts = await getPresentCounts(employeesCol);
    printKV('Present in Latest', presentCounts.present, 1);
    printKV('Not Present', presentCounts.notPresent, 1);

    // 5. Get recent employees
    printHeader(`Most Recent ${limit} Employees`);
    const recentEmployeesSnap = await employeesCol
      .orderBy('updatedAt', 'desc')
      .limit(limit)
      .get();

    if (recentEmployeesSnap.empty) {
      console.log('  No employees found');
    } else {
      recentEmployeesSnap.forEach((doc, index) => {
        const emp = doc.data() as EmployeeDoc;
        console.log(`\n  ${colors.bright}[${index + 1}] ${doc.id}${colors.reset}`);
        printKV('Email Key', emp.emailKey ?? 'N/A', 2);
        printKV('Email Normalized', emp.emailNormalized ?? 'N/A', 2);
        printKV('Status', emp.statusInOrg ?? 'N/A', 2);
        printKV('Present in Latest', emp.presentInLatest ?? false, 2);
        printKV('Last Seen Epoch', emp.lastSeenEpoch ?? 'N/A', 2);
        printKV('Last Event ID', emp.lastEventId ?? 'N/A', 2);
        printKV('Updated At', formatTimestamp(emp.updatedAt), 2);
      });
    }

    // 6. Get recent ingestion events
    printHeader(`Latest ${limit} Ingestion Events`);
    const eventsCol = orgRef.collection('ingestionEvents');
    const recentEventsSnap = await eventsCol
      .orderBy('createdAt', 'desc')
      .limit(limit)
      .get();

    if (recentEventsSnap.empty) {
      console.log('  No ingestion events found');
    } else {
      recentEventsSnap.forEach((doc, index) => {
        const event = doc.data() as IngestionEventDoc;
        console.log(`\n  ${colors.bright}[${index + 1}] ${doc.id}${colors.reset}`);
        printKV('Event ID', event.eventId ?? 'N/A', 2);
        printKV('Status', event.status ?? 'N/A', 2);
        printKV('Kind', event.kind ?? 'N/A', 2);
        printKV('Row Count', event.rowCount ?? 'N/A', 2);
        printKV('Created At', formatTimestamp(event.createdAt), 2);
      });
    }

    // Success summary
    console.log(`\n${colors.green}${colors.bright}✓ Verification Complete${colors.reset}`);
    console.log(`Organization: ${orgId}`);
    console.log(`Total Employees: ${totalEmployees}`);
    console.log(`Active: ${statusCounts.active}, Inactive: ${statusCounts.inactive}`);
    console.log(`Present in Latest: ${presentCounts.present}`);
    console.log();

    process.exit(0);
  } catch (error) {
    console.error(`\n${colors.red}Error during verification:${colors.reset}`);
    console.error(error);
    process.exit(1);
  }
}

// Run the verification
verifyFirestore();
