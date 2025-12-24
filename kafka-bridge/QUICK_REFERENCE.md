# ⚡ Kafka Bridge - Quick Reference

## 🚀 Quick Commands

### Start/Stop
```bash
# Start everything
docker-compose up -d

# Stop everything
docker-compose down

# Restart bridge only
docker-compose restart kafka-bridge

# View logs
docker-compose logs -f kafka-bridge
```

### Send Test Messages
```bash
# Upserts (bulk employee data)
echo '{"eventId":"test-'$(date +%s)'","orgId":"acme-corp","rows":[{"email":"alice@acme.com","firstName":"Alice","lastName":"Smith","statusInOrg":"active"}]}' | \
  docker-compose exec -T redpanda rpk topic produce org-roster-upserts

# Deltas (status change)
echo '{"eventId":"test-'$(date +%s)'","orgId":"acme-corp","email":"alice@acme.com","deltaType":"left"}' | \
  docker-compose exec -T redpanda rpk topic produce org-roster-deltas
```

### Monitor
```bash
# Health check
curl http://localhost:8080/health

# Check for successful processing (look for status:200)
docker logs kafka-bridge | grep "status\":200"

# Consumer lag
docker-compose exec redpanda rpk group describe kafka-bridge-1-upserts
```

## 📋 Message Schemas

### Upserts Topic
```json
{
  "eventId": "unique-event-id",
  "orgId": "acme-corp",
  "rows": [
    {
      "email": "user@acme.com",
      "firstName": "First",
      "lastName": "Last",
      "statusInOrg": "active"
    }
  ]
}
```

### Deltas Topic
```json
{
  "eventId": "unique-event-id",
  "orgId": "acme-corp",
  "email": "user@acme.com",
  "deltaType": "left"
}
```

**Valid deltaTypes:** `left`, `inactive`, `reactivated`

## 🔧 Configuration

### Environment Variables (.env)
```bash
KAFKA_BROKERS=localhost:9092
TOPIC_UPSERTS=org-roster-upserts
TOPIC_DELTAS=org-roster-deltas
NORMALIZER_BASE_URL=https://normalizer-298832040055.us-central1.run.app
INGESTION_TOKEN=af678b9c6f6602f2027bae756a2aa6b6e0a8ac664a67cfbb
BATCH_MAX_ROWS=1000
BATCH_MAX_MS=1200
```

### Consumer Groups
- **Upserts**: `kafka-bridge-1-upserts`
- **Deltas**: `kafka-bridge-1-deltas`

## 🐛 Troubleshooting

### Bridge won't start
```bash
docker-compose logs kafka-bridge
docker-compose restart kafka-bridge
```

### No messages processing
```bash
# Check topics exist
docker-compose exec redpanda rpk topic list

# Check consumer assignments
docker-compose exec redpanda rpk group describe kafka-bridge-1-upserts
```

### Normalizer errors
```bash
# Check Normalizer health
curl https://normalizer-298832040055.us-central1.run.app/health

# View Normalizer logs
gcloud run logs read normalizer --region=us-central1 --limit=20
```

## 📊 Success Indicators

✅ Health returns `{"status":"ok"}`  
✅ Logs show `"status":200`  
✅ Consumer lag is low  
✅ No errors in logs  

## 📚 Documentation

- **COMPLETE_SUCCESS.md** - Full setup summary
- **SCHEMA_FIX_COMPLETE.md** - Schema fix details
- **SETUP_SUCCESS.md** - Testing instructions
- **GET_STARTED.md** - Initial setup guide
