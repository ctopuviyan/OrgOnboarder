#!/usr/bin/env node

/**
 * Check Employee Data in Firestore
 * 
 * This script helps you verify that employee data from CSV emails
 * is being properly stored in the Firestore subcollection.
 * 
 * Usage:
 *   node check_employees.js <orgId>
 * 
 * Example:
 *   node check_employees.js test-org
 *   node check_employees.js puviyan
 */

const admin = require('firebase-admin');
const path = require('path');

// Initialize Firebase Admin
const serviceAccountPath = process.env.GOOGLE_APPLICATION_CREDENTIALS || 
  path.join(__dirname, '../../../firebase-service-account.json');

try {
  admin.initializeApp({
    credential: admin.credential.cert(serviceAccountPath)
  });
  console.log('✅ Firebase Admin initialized\n');
} catch (error) {
  console.error('❌ Failed to initialize Firebase Admin:', error.message);
  console.log('\nMake sure you have:');
  console.log('1. Downloaded your service account JSON from Firebase Console');
  console.log('2. Set GOOGLE_APPLICATION_CREDENTIALS environment variable');
  console.log('   OR placed it at: firebase-service-account.json');
  process.exit(1);
}

const db = admin.firestore();

/**
 * Check employees for an organization
 */
async function checkEmployees(orgId) {
  console.log('═══════════════════════════════════════════════════════');
  console.log(`📊 Checking Employee Data for Organization: ${orgId}`);
  console.log('═══════════════════════════════════════════════════════\n');
  
  try {
    // Get organization document
    const orgDocRef = db.collection('organizations').doc(orgId);
    const orgDoc = await orgDocRef.get();
    
    if (!orgDoc.exists) {
      console.log('❌ Organization document not found!');
      console.log(`   Path: organizations/${orgId}`);
      console.log('\nPossible reasons:');
      console.log('1. No email has been sent yet for this organization');
      console.log('2. The orgId in email subject doesn\'t match');
      console.log('3. Email processing failed (check logs)');
      return;
    }
    
    const orgData = orgDoc.data();
    console.log('✅ Organization Document Found:');
    console.log('─────────────────────────────────────────────────────');
    console.log(`   Current Epoch: ${orgData.currentEpoch || 'N/A'}`);
    console.log(`   Last Finalized Epoch: ${orgData.lastFinalizedEpoch || 'N/A'}`);
    console.log(`   Updated At: ${orgData.updatedAt?.toDate() || 'N/A'}`);
    console.log('─────────────────────────────────────────────────────\n');
    
    // Get employees subcollection
    console.log('🔍 Checking Employees Subcollection...\n');
    const employeesSnapshot = await orgDocRef
      .collection('employees')
      .get();
    
    if (employeesSnapshot.empty) {
      console.log('⚠️  No employees found in subcollection!');
      console.log(`   Path: organizations/${orgId}/employees`);
      console.log('\nPossible reasons:');
      console.log('1. CSV file was empty or had no valid rows');
      console.log('2. All email addresses failed validation');
      console.log('3. CSV parsing failed (check format)');
      console.log('4. Processing error occurred (check normalizer logs)');
      console.log('\nExpected CSV format:');
      console.log('  email,firstName,lastName,role');
      console.log('  alice@test.com,Alice,Smith,Engineer');
      return;
    }
    
    console.log(`✅ Found ${employeesSnapshot.size} Employee(s):\n`);
    console.log('═══════════════════════════════════════════════════════');
    
    let activeCount = 0;
    let inactiveCount = 0;
    let leftCount = 0;
    let presentCount = 0;
    
    employeesSnapshot.forEach((doc, index) => {
      const data = doc.data();
      
      // Count statistics
      if (data.statusInOrg === 'active') activeCount++;
      else if (data.statusInOrg === 'inactive') inactiveCount++;
      else if (data.statusInOrg === 'left') leftCount++;
      if (data.presentInLatest) presentCount++;
      
      console.log(`\n${index + 1}. Employee Document:`);
      console.log('   ─────────────────────────────────────────────────');
      console.log(`   📧 Email: ${data.email || 'N/A'}`);
      console.log(`   📊 Status: ${data.statusInOrg || 'N/A'}`);
      console.log(`   ✓  Present in Latest: ${data.presentInLatest ? 'Yes' : 'No'}`);
      console.log(`   🔢 Last Seen Epoch: ${data.lastSeenEpoch || 'N/A'}`);
      console.log(`   📅 Updated At: ${data.updatedAt?.toDate() || 'N/A'}`);
      console.log(`   🔗 Source: ${data.source || 'N/A'}`);
      console.log(`   🆔 Document ID: ${doc.id}`);
    });
    
    console.log('\n═══════════════════════════════════════════════════════');
    console.log('📈 Summary Statistics:');
    console.log('─────────────────────────────────────────────────────');
    console.log(`   Total Employees: ${employeesSnapshot.size}`);
    console.log(`   Active: ${activeCount}`);
    console.log(`   Inactive: ${inactiveCount}`);
    console.log(`   Left: ${leftCount}`);
    console.log(`   Present in Latest: ${presentCount}`);
    console.log('═══════════════════════════════════════════════════════\n');
    
    // Additional checks
    if (presentCount === 0 && employeesSnapshot.size > 0) {
      console.log('⚠️  Warning: All employees marked as NOT present in latest!');
      console.log('   This might indicate an issue with epoch finalization.');
    }
    
    if (orgData.currentEpoch && orgData.currentEpoch > 1) {
      console.log(`ℹ️  Note: Organization has processed ${orgData.currentEpoch} epoch(s).`);
      console.log('   Multiple epochs indicate multiple CSV uploads.');
    }
    
  } catch (error) {
    console.error('❌ Error checking employees:', error);
    console.error('\nFull error:', error.message);
  }
}

/**
 * List all organizations
 */
async function listOrganizations() {
  console.log('═══════════════════════════════════════════════════════');
  console.log('📋 Available Organizations:');
  console.log('═══════════════════════════════════════════════════════\n');
  
  try {
    const orgsSnapshot = await db.collection('organizations').get();
    
    if (orgsSnapshot.empty) {
      console.log('⚠️  No organizations found!');
      console.log('\nThis means no emails have been processed yet.');
      console.log('Send a test email to get started.');
      return;
    }
    
    console.log(`Found ${orgsSnapshot.size} organization(s):\n`);
    
    for (const doc of orgsSnapshot.docs) {
      const data = doc.data();
      const employeesSnapshot = await doc.ref.collection('employees').get();
      
      console.log(`📁 ${doc.id}`);
      console.log(`   Epoch: ${data.currentEpoch || 0}`);
      console.log(`   Employees: ${employeesSnapshot.size}`);
      console.log(`   Updated: ${data.updatedAt?.toDate() || 'N/A'}`);
      console.log('');
    }
    
    console.log('═══════════════════════════════════════════════════════');
    console.log('\nTo check a specific organization, run:');
    console.log('  node check_employees.js <orgId>');
    console.log('\nExample:');
    orgsSnapshot.docs.slice(0, 1).forEach(doc => {
      console.log(`  node check_employees.js ${doc.id}`);
    });
    console.log('');
    
  } catch (error) {
    console.error('❌ Error listing organizations:', error.message);
  }
}

// Main execution
async function main() {
  const orgId = process.argv[2];
  
  if (!orgId) {
    console.log('ℹ️  No organization ID provided. Listing all organizations...\n');
    await listOrganizations();
  } else {
    await checkEmployees(orgId);
  }
  
  process.exit(0);
}

// Run the script
main().catch(error => {
  console.error('Fatal error:', error);
  process.exit(1);
});
