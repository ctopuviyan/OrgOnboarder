/**
 * Test script for Firestore Optimizer
 * 
 * Usage:
 *   node test-optimizer.js --employees 1000
 *   node test-optimizer.js --employees 10000 --org test-org
 */

const http = require('http');

// Parse command line arguments
const args = process.argv.slice(2);
const employeeCount = parseInt(args[args.indexOf('--employees') + 1] || '1000');
const orgId = args[args.indexOf('--org') + 1] || 'test-org';
const normalizerUrl = args[args.indexOf('--url') + 1] || 'http://localhost:8080';

console.log(`
╔════════════════════════════════════════════════════════════╗
║         Firestore Optimizer Load Test                      ║
╠════════════════════════════════════════════════════════════╣
║  Employees: ${employeeCount.toString().padEnd(45)}║
║  Org ID:    ${orgId.padEnd(45)}║
║  URL:       ${normalizerUrl.padEnd(45)}║
╚════════════════════════════════════════════════════════════╝
`);

// Generate test employees
function generateEmployees(count) {
  const employees = [];
  for (let i = 0; i < count; i++) {
    employees.push({
      email: `employee${i}@test.com`,
      statusInOrg: 'active',
      eventId: 'test-event-001'
    });
  }
  return employees;
}

// Send HTTP POST request
function sendRequest(payload) {
  return new Promise((resolve, reject) => {
    const url = new URL(`${normalizerUrl}/ingest/kafka/upserts`);
    const data = JSON.stringify(payload);
    
    const options = {
      hostname: url.hostname,
      port: url.port || 80,
      path: url.pathname,
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Content-Length': Buffer.byteLength(data)
      }
    };
    
    const req = http.request(options, (res) => {
      let responseData = '';
      
      res.on('data', (chunk) => {
        responseData += chunk;
      });
      
      res.on('end', () => {
        try {
          const result = JSON.parse(responseData);
          resolve(result);
        } catch (error) {
          reject(new Error(`Failed to parse response: ${responseData}`));
        }
      });
    });
    
    req.on('error', (error) => {
      reject(error);
    });
    
    req.write(data);
    req.end();
  });
}

// Run test
async function runTest() {
  console.log(`\n📊 Generating ${employeeCount} test employees...`);
  const employees = generateEmployees(employeeCount);
  
  const payload = {
    orgId: orgId,
    messages: employees,
    closeAfter: false
  };
  
  console.log(`\n🚀 Sending request to ${normalizerUrl}...`);
  const startTime = Date.now();
  
  try {
    const result = await sendRequest(payload);
    const duration = Date.now() - startTime;
    
    console.log(`\n✅ Request completed successfully!\n`);
    
    // Display results
    console.log(`╔════════════════════════════════════════════════════════════╗`);
    console.log(`║                      Test Results                          ║`);
    console.log(`╠════════════════════════════════════════════════════════════╣`);
    console.log(`║  Total Employees:     ${employeeCount.toString().padEnd(35)}║`);
    console.log(`║  Processed:           ${(result.processed || 0).toString().padEnd(35)}║`);
    console.log(`║  Skipped:             ${(result.skipped || 0).toString().padEnd(35)}║`);
    console.log(`║  Errors:              ${(result.errors || 0).toString().padEnd(35)}║`);
    console.log(`║  Duration (client):   ${duration.toString().padEnd(26)}ms     ║`);
    console.log(`║  Duration (server):   ${(result.durationMs || 0).toString().padEnd(26)}ms     ║`);
    console.log(`║  Throughput:          ${Math.round(employeeCount / (duration / 1000)).toString().padEnd(24)} emp/s  ║`);
    console.log(`║  Epoch:               ${(result.epoch || 'N/A').toString().padEnd(35)}║`);
    console.log(`╚════════════════════════════════════════════════════════════╝`);
    
    // Performance analysis
    const throughput = employeeCount / (duration / 1000);
    console.log(`\n📈 Performance Analysis:\n`);
    
    if (throughput > 150) {
      console.log(`   ✅ EXCELLENT: ${Math.round(throughput)} employees/second`);
      console.log(`   Your system is performing at industry-leading levels!`);
    } else if (throughput > 100) {
      console.log(`   ✅ GOOD: ${Math.round(throughput)} employees/second`);
      console.log(`   Your system is performing well.`);
    } else if (throughput > 50) {
      console.log(`   ⚠️  MODERATE: ${Math.round(throughput)} employees/second`);
      console.log(`   Consider tuning parallel batch commits.`);
    } else {
      console.log(`   ❌ SLOW: ${Math.round(throughput)} employees/second`);
      console.log(`   Check Firestore performance and network latency.`);
    }
    
    // Cost estimation
    const estimatedQueries = Math.ceil(employeeCount / 10); // 10 emails per query
    const estimatedWrites = Math.ceil(employeeCount / 500); // 500 per batch
    const readCost = (estimatedQueries / 100000) * 0.06;
    const writeCost = (estimatedWrites / 100000) * 0.18;
    const totalCost = readCost + writeCost;
    
    console.log(`\n💰 Estimated Firestore Cost:\n`);
    console.log(`   Queries:  ${estimatedQueries.toLocaleString()} reads  ($${readCost.toFixed(4)})`);
    console.log(`   Writes:   ${estimatedWrites.toLocaleString()} writes ($${writeCost.toFixed(4)})`);
    console.log(`   Total:    $${totalCost.toFixed(4)}`);
    
    // Comparison with unoptimized
    const unoptimizedReads = employeeCount;
    const unoptimizedWrites = employeeCount;
    const unoptimizedReadCost = (unoptimizedReads / 100000) * 0.06;
    const unoptimizedWriteCost = (unoptimizedWrites / 100000) * 0.18;
    const unoptimizedTotalCost = unoptimizedReadCost + unoptimizedWriteCost;
    const savings = ((unoptimizedTotalCost - totalCost) / unoptimizedTotalCost) * 100;
    
    console.log(`\n   Unoptimized cost: $${unoptimizedTotalCost.toFixed(4)}`);
    console.log(`   Savings:          $${(unoptimizedTotalCost - totalCost).toFixed(4)} (${savings.toFixed(1)}%)`);
    
    // Scaling projection
    console.log(`\n🌍 Scaling Projection (TCS: 600K employees):\n`);
    const scaleFactor = 600000 / employeeCount;
    const projectedDuration = (duration / 1000) * scaleFactor;
    const projectedCost = totalCost * scaleFactor;
    
    console.log(`   Estimated time:   ${Math.round(projectedDuration / 60)} minutes`);
    console.log(`   Estimated cost:   $${projectedCost.toFixed(2)}`);
    console.log(`   Annual cost:      $${(projectedCost * 365).toFixed(2)} (daily updates)`);
    
    if (projectedDuration < 3600) {
      console.log(`   ✅ Can process 600K employees in under 1 hour!`);
    } else {
      console.log(`   ⚠️  Would take ${Math.round(projectedDuration / 3600)} hours for 600K employees.`);
      console.log(`   Consider increasing parallel batches or Cloud Run resources.`);
    }
    
    console.log(`\n✨ Test completed successfully!\n`);
    
  } catch (error) {
    console.error(`\n❌ Test failed:`, error.message);
    console.error(`\nTroubleshooting:`);
    console.error(`  1. Ensure normalizer service is running: npm start`);
    console.error(`  2. Check the URL: ${normalizerUrl}`);
    console.error(`  3. Verify Firestore credentials are configured`);
    console.error(`  4. Check Cloud Run logs for errors\n`);
    process.exit(1);
  }
}

// Run the test
runTest().catch(console.error);
