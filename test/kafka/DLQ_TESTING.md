# 🔥 DLQ (Dead Letter Queue) Integration Testing

This document explains how to run and understand the DLQ integration tests that validate the current behavior and test the fixes for identified issues.

## 🎯 Test Coverage

### Issues Being Tested

1. **Point 1**: Consumer Module missing `dlqSuffix` configuration option
2. **Point 3.C**: DLQ send failure recovery mechanism to prevent message loss
3. **Race Conditions**: Batch processing with mixed success/failure scenarios
4. **Error Classification**: Retryable vs non-retryable error handling
5. **DLQ Metrics**: Proper tracking of DLQ operations

### Test Files

- `test/kafka/kafka.dlq.integration.spec.ts` - Core DLQ functionality tests
- `test/consumer/consumer.dlq.integration.spec.ts` - Consumer Module specific DLQ tests

## 🚀 Running the Tests

### Prerequisites

1. **Kafka Instance Required**
   ```bash
   # Option 1: Docker
   docker run -d --name kafka -p 29092:9092 apache/kafka:2.8.0
   
   # Option 2: Local Kafka
   # Start your local Kafka on port 29092
   
   # Option 3: Custom Kafka
   export TEST_KAFKA_BROKERS="your-kafka-host:29092"
   ```

2. **Dependencies**
   ```bash
   npm install
   npm run build
   ```

### Running Tests

#### Option 1: Quick DLQ Test Run
```bash
./scripts/test-dlq-integration.sh
```

#### Option 2: Manual Jest Run
```bash
# Set Kafka connection
export TEST_KAFKA_BROKERS="localhost:29092"

# Run DLQ tests specifically
npx jest --testMatch="**/test/kafka/kafka.dlq.integration.spec.ts" --verbose
npx jest --testMatch="**/test/consumer/consumer.dlq.integration.spec.ts" --verbose
```

#### Option 3: Full Test Suite
```bash
# Run all Kafka tests including DLQ
npm run test:kafka
```

## 🔍 Test Scenarios Explained

### 1. Basic DLQ Functionality
```typescript
it('should send messages to DLQ after retry limit exceeded')
```
**What it tests**: Messages that fail repeatedly are sent to DLQ after retry limit
**Expected behavior**: 
- Handler called multiple times (original + retries)
- Messages end up in DLQ topic with proper headers
- Offsets are committed to prevent redelivery

### 2. Error Classification
```typescript
it('should distinguish between retryable and non-retryable errors')
```
**What it tests**: Different error types are handled appropriately
**Expected behavior**:
- Network errors (ETIMEDOUT) → retried multiple times
- Validation errors → sent directly to DLQ
- Success cases → processed once

### 3. DLQ Failure Recovery (Point 3.C)
```typescript
it('should handle DLQ send failures without losing messages')
```
**What it tests**: When DLQ topic is unavailable, messages aren't lost
**Expected behavior**:
- DLQ send fails → messages stay in failed state
- Offsets NOT committed → messages will be redelivered
- No data loss occurs

### 4. Consumer Module Configuration Gap (Point 1)
```typescript
it('should demonstrate the missing dlqSuffix configuration in Consumer Module')
```
**What it tests**: Consumer Module doesn't expose DLQ configuration
**Current behavior**: Always uses default `-dlq` suffix
**After fix**: Should support custom DLQ suffix configuration

### 5. Batch Processing Race Conditions
```typescript
it('should handle batch processing with mixed success/failure/DLQ scenarios')
```
**What it tests**: Complex batch scenarios with mixed outcomes
**Expected behavior**:
- Successful messages → committed
- Failed retryable → retried
- Failed non-retryable → sent to DLQ
- No race conditions between batch processing and DLQ sends

## 📊 Understanding Test Output

### Healthy DLQ Behavior
```bash
📊 DLQ Test Results:
  📥 Produced: 2
  🔄 Handler Attempts: 6        # 2 original + 4 retries (2 per message)
  💀 DLQ Messages: 2           # Both messages in DLQ
  🏷️  DLQ Topic: test-topic-dlq  # Correct DLQ topic
```

### Issue Indicators
```bash
🚨 Issues Detected:
  ❌ DLQ Messages: 0           # Messages not reaching DLQ
  ❌ Handler Attempts: 2       # No retries happening
  ❌ DLQ Topic: wrong-suffix   # Incorrect DLQ configuration
```

## 🛠️ Test Environment Setup

### Kafka Topic Management
The tests automatically create and cleanup topics:
- `dlq-test-{uuid}` - Main test topic
- `dlq-test-{uuid}-dlq` - Default DLQ topic
- `dlq-test-{uuid}-custom-dlq` - Custom DLQ topic

### Test Isolation
- Each test uses unique topics and consumer groups
- Proper cleanup in `afterEach` and `afterAll`
- Tests can run in parallel without interference

### Debugging Tests
```bash
# Enable verbose logging
export DEBUG=kafka*

# Run single test with detailed output
npx jest --testNamePattern="should send messages to DLQ" --verbose --no-cache
```

## 🔧 Current Known Issues

### Issue 1: Consumer Module Configuration Gap
**Problem**: `dlqSuffix` not available in Consumer Module options
**Test**: `test/consumer/consumer.dlq.integration.spec.ts`
**Status**: ❌ Needs fix

### Issue 2: DLQ Failure Recovery
**Problem**: DLQ send failures may cause message loss
**Test**: `kafka.dlq.integration.spec.ts` - "should handle DLQ send failures"
**Status**: ❌ Needs fix

### Issue 3: Error Classification
**Problem**: Too many errors treated as non-retryable
**Test**: "should distinguish between retryable and non-retryable errors"
**Status**: ⚠️ May need tuning

## 🎯 Next Steps

1. **Run Tests**: Execute tests to see current behavior
2. **Fix Issues**: Implement fixes for identified problems
3. **Validate**: Re-run tests to ensure fixes work
4. **Monitor**: Use tests for regression testing

## 📝 Adding New DLQ Tests

When adding new DLQ test scenarios:

```typescript
it('should handle your new scenario', async () => {
  if (!testKafka) return; // Skip if Kafka unavailable
  
  // Setup handler
  class YourTestHandler implements IEventHandler<any> {
    async handle({ key, event }) {
      // Your test logic
    }
  }
  
  // Setup monitoring
  await startMonitoring();
  
  // Produce messages
  await produceMessages([...]);
  
  // Start consuming
  await kafkaClient.consumeMany([...]);
  
  // Wait and assert
  await setTimeout(2000);
  expect(...);
});
```

## 🤝 Contributing

1. Follow existing test patterns
2. Include both positive and negative test cases
3. Test with realistic data volumes
4. Add proper assertions for all scenarios
5. Document expected behavior clearly
