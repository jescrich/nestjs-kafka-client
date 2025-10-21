# 📦 Batch Processing Integration Testing

This document explains how to run and understand the comprehensive batch processing tests that validate KafkaClient and Consumer Module batch processing behavior in different scenarios.

## 🎯 Test Coverage

### Batch Processing Scenarios Tested

1. **Single Message Processing** - Individual message handling without batching
2. **Same-Key Batch Processing** - Messages with identical keys batched using `handleBatch`
3. **Mixed-Key Batch Processing** - Multiple keys with different batching strategies
4. **High-Volume Batch Processing** - Performance testing with large message volumes
5. **Batch Processing with Failures** - Error handling in batch scenarios
6. **Batch Processing with Timeouts** - Slow processing and timeout handling
7. **Consumer Module Batch Processing** - Batch behavior through Consumer Module abstraction
8. **Batch Size Limits** - Testing batch size limits and overflow handling
9. **Consumer Module vs Direct KafkaClient** - Performance comparison
10. **Multiple Handlers** - Different batch strategies across handlers

### Test Files

- `test/kafka/kafka.batch.integration.spec.ts` - Core KafkaClient batch functionality tests (8 test scenarios)
- `test/consumer/consumer.batch.integration.spec.ts` - Consumer Module batch tests (7 test scenarios)

## 🚀 Running the Tests

### Prerequisites

1. **Kafka Instance Required**
   ```bash
   # Docker (recommended)
   docker run -d --name kafka -p 29092:9092 apache/kafka:2.8.0
   
   # Or custom Kafka
   export TEST_KAFKA_BROKERS="localhost:29092"
   ```

2. **Dependencies**
   ```bash
   npm install
   npm run build
   ```

### Running Tests

#### Option 1: Quick Batch Processing Test Run
```bash
./scripts/test-batch-processing.sh
```

#### Option 2: Manual Jest Run
```bash
# Set Kafka connection
export TEST_KAFKA_BROKERS="localhost:29092"

# Run batch processing tests
npx jest test/kafka/kafka.batch.integration.spec.ts --verbose --testTimeout=60000
npx jest test/consumer/consumer.batch.integration.spec.ts --verbose --testTimeout=60000
```

#### Option 3: Full Test Suite
```bash
# Run all Kafka tests including batch processing
npm run test:kafka
```

## 🔍 Test Scenarios Explained

### 1. Single Message Processing
```typescript
it('should process individual messages without batching')
```
**What it tests**: Messages processed individually when batch size is 1
**Expected behavior**: 
- Each message calls `handle()` method
- No `handleBatch()` calls
- One handler call per message

**Configuration**: `batchSizeMultiplier: 1, maxConcurrency: 1`

### 2. Same-Key Batch Processing
```typescript
it('should batch messages with the same key using handleBatch')
```
**What it tests**: Messages with identical keys are batched together
**Expected behavior**:
- Multiple messages with same key → single `handleBatch()` call
- Messages with different keys → individual `handle()` calls
- Batch contains all events for the key

**Configuration**: `batchSizeMultiplier: 10, minBatchSize: 2`

### 3. Mixed-Key Batch Processing
```typescript
it('should handle mixed keys with appropriate batching strategy')
```
**What it tests**: Complex scenarios with multiple keys and batch sizes
**Expected behavior**:
- Keys with multiple messages → batched
- Keys with single messages → individual processing
- Concurrent processing of different keys

**Configuration**: `maxConcurrency: 2, batchSizeMultiplier: 5`

### 4. High-Volume Batch Processing
```typescript
it('should handle high-volume message processing efficiently')
```
**What it tests**: Performance with large message volumes (50+ messages)
**Expected behavior**:
- Efficient batch processing
- Reasonable processing times
- High throughput (messages/second)
- Proper batch size optimization

**Metrics Tracked**:
- Total processing time
- Messages per second throughput
- Average batch size
- Batch efficiency percentage

### 5. Batch Processing with Failures
```typescript
it('should handle failures in batch processing correctly')
```
**What it tests**: Error handling in batch scenarios
**Expected behavior**:
- Failed messages trigger retries
- Non-retryable errors → DLQ
- Successful messages in batch still processed
- Proper error isolation

**Configuration**: `messageRetryLimit: 2, messageRetryDelayMs: 100`

### 6. Batch Processing with Timeouts
```typescript
it('should handle slow processing with appropriate timeouts')
```
**What it tests**: Slow processing scenarios and timeout handling
**Expected behavior**:
- Slow batches don't block other processing
- Proper heartbeat management
- Session timeout handling
- Concurrent processing continues

**Configuration**: `sessionTimeout: 30000, heartbeatInterval: 3000`

### 7. Consumer Module Batch Processing
```typescript
it('should batch messages with same key through Consumer Module')
```
**What it tests**: Batch processing through Consumer Module abstraction
**Expected behavior**:
- Same batch behavior as direct KafkaClient
- Proper handler registration
- Configuration passed through correctly
- Multiple handlers work independently

### 8. Batch Size Limits and Overflow
```typescript
it('should respect batch size limits and handle overflow correctly')
```
**What it tests**: Batch size limits and overflow scenarios
**Expected behavior**:
- Batches respect maximum size limits
- Overflow messages create new batches
- No message loss during overflow
- Reasonable batch size distribution

## 📊 Understanding Test Output

### Healthy Batch Processing
```bash
📊 Batch Processing Test:
  📥 Produced: 20
  🔄 Single Handler Calls: 2        # Individual messages
  📦 Batch Handler Calls: 3         # Batched messages
  👤 User1 Batched Events: 4        # Events in largest batch
  ⚡ Processing Time: 1250ms        # Total time
  🚀 Messages/Second: 16.00         # Throughput
  📈 Average Batch Size: 3.33       # Efficiency
```

### Performance Metrics
```bash
📊 High-Volume Batch Test:
  📥 Produced: 50
  ⚡ Processing Time: 2100ms
  🔄 Total Processed: 50
  📦 Batch Calls: 12
  📈 Average Batch Size: 4.17
  🚀 Messages/Second: 23.81
```

### Issue Indicators
```bash
🚨 Issues Detected:
  ❌ Total Processed: 25            # Less than produced
  ❌ Processing Time: 15000ms       # Too slow
  ❌ Average Batch Size: 1.0        # No batching happening
  ❌ Messages/Second: 1.67          # Poor throughput
```

## 🛠️ Test Configuration Options

### KafkaClient Batch Configuration
```typescript
const kafkaClient = new KafkaClient(clientId, brokers, {
  maxConcurrency: 2,              // Concurrent processors
  batchSizeMultiplier: 10,        // Batch size factor
  minBatchSize: 3,                // Minimum for batching
  sessionTimeout: 30000,          // Session timeout
  heartbeatInterval: 3000,        // Heartbeat frequency
  enableCpuMonitoring: false,     // Disable for tests
  enableMemoryMonitoring: false,  // Disable for tests
});
```

### Consumer Module Batch Configuration
```typescript
ConsumerModule.register({
  name: 'batch-test-consumer',
  brokers: TEST_BROKERS,
  consumers: [BatchHandler],
  options: {
    maxConcurrency: 2,
    batchSizeMultiplier: 8,
    messageRetryLimit: 3,
    messageRetryDelayMs: 100,
  }
})
```

## 🔧 Test Environment Setup

### Kafka Topic Management
Tests automatically create and cleanup topics:
- `batch-test-{uuid}` - Main test topic with 3 partitions
- `consumer-batch-test-1-{uuid}` - Consumer Module test topic
- `consumer-batch-test-2-{uuid}` - Multi-handler test topic

### Test Isolation
- Each test uses unique topics and consumer groups
- Proper cleanup in `afterEach` and `afterAll`
- Tests can run in parallel without interference
- Monitoring consumers track message flow

### Debugging Tests
```bash
# Enable verbose logging
export DEBUG=kafka*

# Run single test with detailed output
npx jest --testNamePattern="should handle high-volume" --verbose --no-cache

# Run with custom timeout
npx jest test/kafka/kafka.batch.integration.spec.ts --testTimeout=120000
```

## 📈 Performance Benchmarks

### Expected Performance (Local Kafka)
- **Single Message Processing**: 50-100 msg/sec
- **Batch Processing**: 200-500 msg/sec  
- **High-Volume Batching**: 300-800 msg/sec
- **Consumer Module**: 80-90% of direct KafkaClient performance

### Batch Efficiency Metrics
- **Average Batch Size**: 3-8 messages per batch
- **Batch Utilization**: 60-80% of messages batched
- **Processing Overhead**: <10% additional latency for batching

## 🎯 Test Success Criteria

### Functionality Tests
- ✅ All messages processed (produced = processed)
- ✅ Correct batching behavior per configuration
- ✅ Error handling doesn't lose messages
- ✅ Timeouts don't cause crashes

### Performance Tests
- ✅ Throughput > 100 messages/second
- ✅ Processing time < 10 seconds for 50 messages
- ✅ Average batch size > 2 when batching enabled
- ✅ Memory usage remains stable

### Consumer Module Tests
- ✅ Same behavior as direct KafkaClient
- ✅ Multiple handlers work independently
- ✅ Configuration properly passed through
- ✅ Error handling consistent

## 🔍 Troubleshooting Common Issues

### Issue: No Batching Occurring
**Symptoms**: Average batch size = 1.0, all single handler calls
**Causes**: 
- `batchSizeMultiplier` too low
- `minBatchSize` not met
- Messages have different keys
**Solution**: Increase batch configuration, use same keys

### Issue: Poor Performance
**Symptoms**: Low messages/second, high processing time
**Causes**:
- High `maxConcurrency` causing overhead
- Kafka rebalancing issues
- Network latency
**Solution**: Tune concurrency, check Kafka health

### Issue: Messages Not Processed
**Symptoms**: Produced > Processed
**Causes**:
- Handler errors not caught
- Kafka consumer crashes
- Topic/partition issues
**Solution**: Check error logs, verify Kafka setup

### Issue: Test Timeouts
**Symptoms**: Tests exceed time limits
**Causes**:
- Kafka not available
- Slow message processing
- Resource constraints
**Solution**: Check Kafka connectivity, increase timeouts

## 🤝 Adding New Batch Tests

When adding new batch processing scenarios:

```typescript
it('should handle your new batch scenario', async () => {
  if (!testKafka) return; // Skip if Kafka unavailable
  
  // Setup handler with both single and batch methods
  class YourBatchHandler implements IEventHandler<any> {
    async handle({ key, event }) {
      // Single message processing
      handlerCalls.push({ key, event, type: 'single' });
    }
    
    async handleBatch({ key, events }) {
      // Batch processing
      batchCalls.push({ key, eventCount: events.length, type: 'batch' });
    }
  }
  
  // Configure KafkaClient for your scenario
  kafkaClient = new KafkaClient(clientId, brokers, {
    // Your specific configuration
  });
  
  // Setup monitoring and produce messages
  await setupMonitoring();
  await produceMessages([...]);
  
  // Start consuming and wait
  await kafkaClient.consumeMany([...]);
  await setTimeout(3000);
  
  // Assert expected behavior
  expect(...);
  
  // Log results for analysis
  console.log(`📊 Your Test Results: ...`);
});
```

## 📝 Contributing

1. Follow existing test patterns and naming
2. Include both positive and negative test cases  
3. Test with realistic message volumes and keys
4. Add proper assertions for all scenarios
5. Document expected behavior and metrics
6. Consider performance implications
7. Test both KafkaClient and Consumer Module paths

The comprehensive batch processing test suite ensures that batching works correctly across all scenarios and provides confidence in the system's ability to handle high-volume message processing efficiently.


