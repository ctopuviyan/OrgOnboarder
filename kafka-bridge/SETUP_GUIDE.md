# 🚀 Kafka Bridge Setup Guide

Complete guide to configure and deploy the Kafka Bridge to connect your Kafka cluster to the Normalizer service.

## 📋 Prerequisites

- ✅ Normalizer service deployed at Cloud Run
- ✅ Normalizer configured with HMAC secrets and ingestion token
- ✅ Kafka cluster or Redpanda instance running
- ✅ Docker installed for containerized deployment
- ✅ Node.js 20+ installed for local development

## 🔐 Step 1: Configure Environment Variables

Create a `.env` file from the template:

```bash
cd cloud-pipeline/kafka-bridge
cp .env.template .env
```

Edit `.env` and fill in your configuration:

```bash
# Kafka Configuration
# Replace with your actual Kafka brokers (comma-separated)
KAFKA_BROKERS=your-kafka-broker-1:9092,your-kafka-broker-2:9092,your-kafka-broker-3:9092
KAFKA_CLIENT_ID=kafka-bridge
KAFKA_GROUP_ID=kafka-bridge-1

# Kafka Topics
TOPIC_UPSERTS=org-roster-upserts
TOPIC_DELTAS=org-roster-deltas

# Normalizer HTTP Configuration
# Get this from your Normalizer deployment
NORMALIZER_BASE_URL=https://normalizer-bshc2hcqya-uc.a.run.app

# Get this from normalizer/.env.production (created during Normalizer setup)
INGESTION_TOKEN=your-ingestion-token-here

# Batch & Retry Tuning (Green Defaults - adjust if needed)
BATCH_MAX_ROWS=1000
BATCH_MAX_MS=1200
HTTP_TIMEOUT_MS=15000
RETRY_BASE_MS=500
RETRY_MAX_MS=15000
MAX_RETRIES=8
CONCURRENCY=1

# Health Server
PORT=8080
```

### 🔍 Where to Find Configuration Values

1. **KAFKA_BROKERS**: Your Kafka cluster connection string
   - Production: Contact your Kafka admin
   - Local Redpanda: `localhost:9092`

2. **NORMALIZER_BASE_URL**: From Normalizer deployment
   - Check: `gcloud run services describe normalizer --region=us-central1 --format='value(status.url)'`
   - Or from: `cloud-pipeline/normalizer/DEPLOYMENT_SUCCESS.md`

3. **INGESTION_TOKEN**: From Normalizer configuration
   - Check: `cloud-pipeline/normalizer/.env.production`
   - Or regenerate: `cd ../normalizer && ./scripts/configure-runtime.sh`

## 📊 Step 2: Create Kafka Topics

Create the required topics **ONCE** before starting the bridge.

### For Production Kafka:

```bash
# Set your Kafka brokers
export BROKERS=your-kafka-broker-1:9092,your-kafka-broker-2:9092

# Create topics
./scripts/create-topics-kafka.sh
```

This creates:
- `org-roster-upserts`: 3 partitions, 3 replicas, 7 days retention
- `org-roster-deltas`: 3 partitions, 3 replicas, 3 days retention

### For Local Redpanda:

```bash
# Create topics (no BROKERS env needed)
./scripts/create-topics-redpanda.sh
```

This creates:
- `org-roster-upserts`: 3 partitions, 1 replica
- `org-roster-deltas`: 3 partitions, 1 replica

### Verify Topics Created:

```bash
# Kafka
kafka-topics --bootstrap-server $BROKERS --list | grep org-roster

# Redpanda
rpk topic list | grep org-roster
```

## 🏗️ Step 3: Build and Deploy

### Option A: Docker Deployment (Recommended for Production)

```bash
# Install dependencies and build
npm ci && npm run build

# Build Docker image
docker build -t kafka-bridge:latest .

# Run with restart policy
docker run -d \
  --name kafka-bridge \
  --env-file .env \
  --restart=always \
  kafka-bridge:latest

# Check logs
docker logs -f kafka-bridge

# Check health
curl http://localhost:8080/health
```

### Option B: Local Development

```bash
# Install dependencies
npm install

# Build TypeScript
npm run build

# Run in development mode (with auto-reload)
npm run dev

# Or run in production mode
npm start
```

### Docker Management Commands:

```bash
# Stop the bridge
docker stop kafka-bridge

# Start the bridge
docker start kafka-bridge

# Restart the bridge
docker restart kafka-bridge

# View logs
docker logs -f kafka-bridge

# Remove container
docker rm -f kafka-bridge

# Rebuild and redeploy
docker build -t kafka-bridge:latest . && \
docker rm -f kafka-bridge && \
docker run -d --name kafka-bridge --env-file .env --restart=always kafka-bridge:latest
```

## 🧪 Step 4: Smoke Test End-to-End

Test the complete pipeline from Kafka → Bridge → Normalizer → Firestore.

### For Production Kafka:

```bash
# Set your Kafka brokers
export BROKERS=your-kafka-broker-1:9092,your-kafka-broker-2:9092

# Run smoke test
./scripts/smoke-test.sh
```

### For Local Redpanda:

```bash
# Run smoke test (no BROKERS env needed)
./scripts/smoke-test-redpanda.sh
```

### What the Smoke Test Does:

1. **Produces a snapshot** to `org-roster-upserts`:
   - 3 employees: Alice (active), Bob (active), Charlie (terminated)

2. **Produces a delta** to `org-roster-deltas`:
   - Updates Charlie's status from "terminated" to "active"

3. **Waits for processing** and provides verification steps

### Verify Results:

```bash
# 1. Check Kafka Bridge logs
docker logs kafka-bridge | tail -50

# 2. Verify data in Firestore
cd ../normalizer
npm run verify:firestore -- --orgId=acme-corp --limit=5

# 3. Check Firestore Console
# https://console.firebase.google.com/project/gogreen-d6100/firestore
```

**Expected Results:**
- ✅ 3 employees in Firestore (Alice, Bob, Charlie)
- ✅ Charlie's status is "active" (updated by delta)
- ✅ 2 ingestion events recorded
- ✅ All employees have `presentInLatest: true`

## 📊 Step 5: Monitor and Verify

### Check Bridge Health:

```bash
curl http://localhost:8080/health
```

Expected response:
```json
{
  "status": "ok",
  "uptime": 12345,
  "consumers": {
    "upserts": "running",
    "deltas": "running"
  }
}
```

### Monitor Logs:

```bash
# Real-time logs
docker logs -f kafka-bridge

# Last 100 lines
docker logs kafka-bridge --tail 100

# Filter for errors
docker logs kafka-bridge 2>&1 | grep ERROR
```

### Key Log Messages to Look For:

✅ **Success indicators:**
```
✓ Kafka connected
✓ Consumer upserts started
✓ Consumer deltas started
✓ Batch sent: 3 rows → 200 OK
```

❌ **Error indicators:**
```
✗ Kafka connection failed
✗ HTTP 401 Unauthorized (check INGESTION_TOKEN)
✗ HTTP 500 Internal Server Error (check Normalizer logs)
```

## 🔧 Troubleshooting

### Bridge Won't Start

```bash
# Check environment variables
docker exec kafka-bridge env | grep -E 'KAFKA|NORMALIZER|TOKEN'

# Check if Kafka is reachable
docker exec kafka-bridge nc -zv your-kafka-broker 9092

# Check if Normalizer is reachable
docker exec kafka-bridge curl -I $NORMALIZER_BASE_URL/health
```

### Authentication Errors (401)

```bash
# Verify token matches Normalizer
cd ../normalizer
cat .env.production | grep INGESTION_TOKEN

# Update bridge .env with correct token
# Then restart: docker restart kafka-bridge
```

### No Messages Being Consumed

```bash
# Check if topics exist
kafka-topics --bootstrap-server $BROKERS --list | grep org-roster

# Check topic has messages
kafka-console-consumer --bootstrap-server $BROKERS \
  --topic org-roster-upserts --from-beginning --max-messages 1

# Check consumer group lag
kafka-consumer-groups --bootstrap-server $BROKERS \
  --group kafka-bridge-1 --describe
```

### Messages Not Reaching Firestore

```bash
# 1. Check bridge logs for HTTP errors
docker logs kafka-bridge | grep -E 'ERROR|failed'

# 2. Check Normalizer logs
gcloud run logs read normalizer --region=us-central1 --limit=50

# 3. Verify Normalizer is healthy
curl https://normalizer-bshc2hcqya-uc.a.run.app/health

# 4. Test Normalizer directly
curl -X POST https://normalizer-bshc2hcqya-uc.a.run.app/ingest/upserts \
  -H "Authorization: Bearer $INGESTION_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"eventId":"test-123","orgId":"acme-corp","kind":"snapshot","rows":[{"email":"test@acme.com","firstName":"Test","lastName":"User","statusInOrg":"active"}]}'
```

## 📈 Performance Tuning

Adjust these environment variables in `.env` based on your workload:

```bash
# Increase batch size for high throughput
BATCH_MAX_ROWS=5000
BATCH_MAX_MS=2000

# Increase timeout for slow networks
HTTP_TIMEOUT_MS=30000

# Increase concurrency for parallel processing
CONCURRENCY=3

# Adjust retry strategy
MAX_RETRIES=5
RETRY_MAX_MS=10000
```

After changes:
```bash
docker restart kafka-bridge
```

## 🔒 Production Checklist

Before going to production:

- [ ] `.env` file has production Kafka brokers
- [ ] `INGESTION_TOKEN` is secure and matches Normalizer
- [ ] Topics created with appropriate retention
- [ ] Docker container runs with `--restart=always`
- [ ] Health endpoint is monitored
- [ ] Logs are being collected (e.g., CloudWatch, Datadog)
- [ ] Consumer group lag is monitored
- [ ] Alerts configured for errors
- [ ] Backup bridge instance for high availability

## 📚 Additional Resources

- **README.md** - Full bridge documentation
- **docker-compose.yml** - Local development setup
- **../normalizer/FINALIZE_DEPLOYMENT.md** - Normalizer setup guide
- **../normalizer/COMMAND_REFERENCE.md** - Normalizer API reference

## 🆘 Support

If you encounter issues:

1. Check bridge logs: `docker logs kafka-bridge`
2. Check Normalizer logs: `gcloud run logs read normalizer --region=us-central1`
3. Verify configuration: Compare `.env` with `.env.template`
4. Test connectivity: Ensure bridge can reach both Kafka and Normalizer
5. Run smoke test: `./scripts/smoke-test.sh` or `./scripts/smoke-test-redpanda.sh`

---

**🎊 Success!** Your Kafka Bridge is now forwarding org roster data to the Normalizer!
