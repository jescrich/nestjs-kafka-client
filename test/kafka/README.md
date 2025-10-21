# Kafka Batch Processing Bug Reproduction Tests

This directory contains comprehensive tests to reproduce and validate the fix for a critical Kafka batch processing bug where messages could be committed without being processed.

## 🚨 The Bug

### Problem Description
The original implementation had a race condition in the `eachBatch` handler where:

1. Messages were added to the processing queue
2. `processNextMessage()` was called but didn't complete all messages
3. **`messageQueue.length = 0` cleared the queue prematurely** ⚠️
4. `eachBatch` returned, causing KafkaJS to commit offsets
5. **Unprocessed messages were lost** (committed but never processed)

### Root Cause
```typescript
// OLD BUGGY CODE (line ~1092 in kafka.client.ts)
await processNextMessage();
messageQueue.length = 0; // 🚨 BUG: Cleared queue before processing completed!
```

### Fix
```typescript
// NEW FIXED CODE 
await processNextMessage();

// Wait for all messages from this batch to be processed before returning
const batchOffsets = batch.messages.map(m => m.offset);
while (!allProcessed && !timeout) {
  // Check if all offsets resolved, continue processing
  await processNextMessage();
}
// Only return when all messages processed or timeout
```

## 🧪 Test Scenarios

### 1. **Single events with different messages and keys**
- Tests individual message processing
- Verifies no messages lost with different keys
- **File**: `kafka.client.integration.spec.ts` - Scenario 1

### 2. **Multiple events with same message key**
- Tests batch processing functionality
- Verifies messages with same key are batched correctly
- **File**: `kafka.client.integration.spec.ts` - Scenario 2

### 3. **Handler processes latest but commits all**
- **Critical test that reproduces the exact bug**
- Handler processes only latest event but all offsets should be committed
- **File**: `kafka.client.integration.spec.ts` - Scenario 3

### 4. **High volume mixed traffic**
- Stress test with multiple users and event types
- Tests system under realistic load
- **File**: `kafka.client.integration.spec.ts` - Scenario 4

### 5. **Slow processing with timeouts**
- Tests timeout scenarios where processing is slow
- Reproduces premature offset commits
- **File**: `kafka.client.integration.spec.ts` - Scenario 5

## 🛠️ How to Run Tests

### Prerequisites
```bash
# Start Kafka (Docker example)
docker run -p 9092:9092 apache/kafka:2.8.0

# Or use your existing Kafka setup
export TEST_KAFKA_BROKERS="localhost:9092"
```

### Option 1: Unit Tests (No Kafka Required)
```bash
# Run unit tests that simulate the bug without requiring Kafka
npm run test:kafka-unit-bug
```

### Option 2: Quick Bug Reproduction Test
```bash
# Run the focused bug reproduction test
npm run test:kafka-bug
```

### Option 3: Full Integration Test Suite
```bash
# Run comprehensive integration tests
npm run test:kafka-integration
```

### Option 4: Interactive Testing (Old vs New Implementation)
```bash
# Interactive script to test both implementations
npm run test:kafka-interactive

# Or run directly
./scripts/test-kafka-bug.sh
```

### Option 5: Manual Testing
```bash
# Run specific test files
npx ts-node test/kafka/bug-reproduction-test.ts
npm test test/kafka/kafka.client.integration.spec.ts
npm test test/kafka/kafka.client.unit-bug-test.spec.ts
```

## 🔍 How to Reproduce the Original Bug

To test the old buggy implementation:

### Method 1: Use the Interactive Script
```bash
npm run test:kafka-interactive
# Choose option 1: "Test OLD (buggy) implementation"
```

### Method 2: Manual Code Changes
1. **Comment out the fix** in `src/kafka/kafka.client.ts` (lines ~1080-1130):
```typescript
// Comment out this entire section:
// // Wait for all messages from this batch to be processed...
// const batchOffsets = batch.messages.map(m => m.offset);
// while (...) { ... }
```

2. **Add back the buggy line**:
```typescript
await processNextMessage();

// Add this buggy line back:
messageQueue.length = 0; // 🚨 This causes the bug!
```

3. **Run tests**:
```bash
npm run test:kafka-bug
```

4. **Look for these indicators**:
   - `🚨 BUG DETECTED!` messages
   - `Consumed: X` but `Processed: Y` where X > Y
   - Messages committed but not processed

## 📊 Expected Test Results

### With OLD (Buggy) Implementation
```bash
📊 RESULTS:
  📥 Messages Produced: 12
  📈 Kafka Consumed: 12          # ✅ All messages consumed
  ⚙️  Handler Processed: 3       # ❌ Only 3 actually processed!
  📦 Batches Processed: 1

🚨 BUG DETECTED!
  Kafka consumed 12 messages
  But only processed 3 events
  This means 9 messages were committed but not processed!
```

### With NEW (Fixed) Implementation
```bash
📊 RESULTS:
  📥 Messages Produced: 12
  📈 Kafka Consumed: 12          # ✅ All messages consumed  
  ⚙️  Handler Processed: 12      # ✅ All messages processed!
  📦 Batches Processed: 4

✅ NO BUG DETECTED
  All consumed messages were properly processed
```

## 🧩 Test File Overview

### `kafka.client.unit-bug-test.spec.ts` ⭐ **NEW**
- **Unit tests for bug reproduction (NO Kafka required)**
- Mock-based testing of batch processing scenarios
- Configuration validation and edge cases
- Perfect for CI/CD pipelines and quick testing

### `kafka.client.integration.spec.ts`
- **Comprehensive integration tests**
- Tests all scenarios with real Kafka
- Uses multiple topics and partitions
- Includes performance and stress testing

### `bug-reproduction-test.ts`
- **Focused bug reproduction**
- Simple, clear test scenarios
- Easy to understand output
- Perfect for demonstrating the issue

### `kafka.client.spec.ts` & `kafka.client.simple.spec.ts`
- **Unit tests**
- Mock-based testing
- Configuration and metrics testing

## 🎯 Key Metrics to Monitor

When testing, watch for these key indicators:

### ✅ Healthy Metrics (Fixed Implementation)
- `consumedMessages` = `processedEvents` 
- No timeout warnings
- All batch offsets resolved
- Queue properly managed

### 🚨 Bug Indicators (Old Implementation)
- `consumedMessages` > `processedEvents`
- `"BUG DETECTED"` messages
- Timeout warnings with unprocessed messages
- Queue cleared prematurely

## 🔧 Debugging Tips

### Enable Detailed Logging
```typescript
// In test files, enable more verbose logging
jest.spyOn(Logger.prototype, 'debug').mockImplementation(console.log);
```

### Check Kafka Topics
```bash
# List topics
kafka-topics --list --bootstrap-server localhost:9092

# Check consumer groups
kafka-consumer-groups --bootstrap-server localhost:9092 --list

# Check offsets
kafka-consumer-groups --bootstrap-server localhost:9092 --group test-group --describe
```

### Monitor Processing
- Watch for `[BATCH]` log messages
- Monitor `[COMMIT]` offset messages  
- Check processing time vs batch time
- Verify heartbeat messages

## 🚀 Performance Considerations

The fix includes several performance optimizations:

### Smart Queue Management
- Only cleans up stale messages from other batches
- Preserves current batch messages
- Uses timestamps for cleanup decisions

### Timeout Protection
- 30-second max wait per batch
- Prevents hanging on stuck messages
- Logs timeout warnings for debugging

### Efficient Offset Tracking
- Uses `Set<string>` instead of `string[]` for O(1) lookups
- Tracks resolved offsets to prevent duplicates
- Batch-aware offset resolution

## 📈 Monitoring in Production

After deploying the fix, monitor these metrics:

```typescript
// Key metrics to track
const metrics = kafkaClient.getMetrics();

// Should be equal in healthy system
console.log(`Consumed: ${metrics.consumedMessages}`);
console.log(`Processed: ${metrics.consumedMessages}`); // From handlers

// Performance metrics
console.log(`Avg Processing Time: ${metrics.avgProcessingTimeMs}ms`);
console.log(`Batch Efficiency: ${metrics.batchEfficiency}`);
console.log(`Memory Usage: ${metrics.memory.currentPercent}%`);
```

## 🤝 Contributing

When adding new test scenarios:

1. **Follow the existing patterns** in integration tests
2. **Include both individual and batch scenarios**
3. **Test with realistic data volumes**
4. **Add proper assertions** for bug detection
5. **Document expected behavior** for both old and new implementations

## 📚 Related Documentation

- **Kafka Configuration**: See workspace rules for `KafkaClientOptions`
- **Error Handling**: Check `kafka.event.handler.ts` for handler patterns
- **Metrics**: Review `KafkaMetricsCollector` for monitoring
- **Performance**: See resource management in `KafkaClient` 