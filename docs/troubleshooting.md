# Troubleshooting

### Common Issues

#### 1. Redis Connection Failed

**Symptom**: `ConnectionRefusedError: [Errno 61] Connection refused`

**Solution**:

```bash
# Start Redis
redis-server

# Verify
redis-cli ping
```

#### 2. MongoDB Connection Timeout

**Symptom**: `ServerSelectionTimeoutError`

**Solution**:

- Check MongoDB Atlas connection string in `.env`
- Verify IP whitelist in Atlas dashboard
- Test connection:

```bash
python -c "from pymongo import MongoClient; client = MongoClient('YOUR_URI'); print(client.server_info())"
```

#### 3. No Historical Data

**Symptom**: `WARNING: Only found 0/3 trading days`

**Solution**:

```bash
# Clear existing data
python backend/src/utils/clear_mongodb.py

# Re-fetch historical data
python historical_data.py
```

#### 4. Model Not Found

**Symptom**: `FileNotFoundError: agent_ppo.zip`

**Solution**:

- Ensure model exists at `backend/finrl_integration/agent_ppo.zip`
- Train new model if needed (see FinRL documentation)

#### 5. OpenAI API Errors

**Symptom**: `RateLimitError` or `AuthenticationError`

**Solution**:

- Verify API key in `.env`
- Check rate limits (agents make multiple calls)
- Add retry logic if needed

#### 6. High Memory Usage

**Symptom**: Pipeline crashes with `MemoryError`

**Solution**:

- Reduce buffer sizes in agents:

```python
agent.max_buffer_size = 50  # Default is 100
```

- Clear Redis periodically:

```bash
redis-cli FLUSHALL
```

#### 7. FinRL Not Running

**Symptom**: No decisions in `finrl-decisions` stream

**Solution**:

- Check data availability:

```bash
redis-cli XLEN processed:price
```

- Verify model loaded:

```bash
ls -lh backend/finrl_integration/agent_ppo.zip
```

- Check logs:

```bash
tail -f agent_logs/enhanced_*/full_pipeline_enhanced.log
```

### Debug Commands

#### Check Redis Status

```bash
# Stream lengths
redis-cli XLEN raw:price-updates
redis-cli XLEN processed:price
redis-cli XLEN finrl-decisions

# Latest entry
redis-cli XREVRANGE processed:price + - COUNT 1

# Memory usage
redis-cli INFO memory
```

#### Check MongoDB Status

```bash
# Connect to MongoDB
mongosh "YOUR_MONGODB_URI"

# Count documents
db.market_data_1min.countDocuments()

# Find latest
db.market_data_1min.find().sort({date: -1}).limit(1)

# Check date range
db.market_data_1min.aggregate([
  {$group: {_id: null, min: {$min: "$date"}, max: {$max: "$date"}}}
])
```

#### Check Pipeline Status

```bash
# Process running
ps aux | grep full_pipeline_enhanced

# Log recent activity
tail -50 pipeline_output.log

# Check cycle count
ls -d agent_logs/enhanced_*/cycle_* | wc -l
```

### Performance Optimization

#### 1. Redis Persistence

Disable Redis persistence for faster writes:

```bash
redis-server --appendonly no --save ""
```

#### 2. MongoDB Indexes

Ensure indexes are created:

```python
collection.create_index([('date', -1)])
collection.create_index([('tic', 1)])
collection.create_index([('date', -1), ('tic', 1)], unique=True)
```

#### 3. Parallel Processing

Use parallel pipeline for faster agent processing:

```bash
python parallel_full_pipeline_clean.py
```

Expected speedup: 3-4x

#### 4. Quick Mode

Use quick mode for testing (faster intervals):

```bash
python full_pipeline_enhanced.py --quick --single
```

Intervals:

- MongoDB sync: 10 seconds (vs 60)
- Fine-tuning: 10 minutes (vs 2 hours)
- FinRL: 5 minutes (vs 2 hours)
