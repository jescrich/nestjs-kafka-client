#!/usr/bin/env ts-node

/**
 * Standalone Bug Reproduction Test
 * 
 * This script reproduces batch processing bugs without requiring Jest.
 * It can be run directly with ts-node for quick debugging.
 * 
 * Usage:
 *   ts-node test/kafka/bug-reproduction-test.ts
 *   
 * Or with npm:
 *   npm run test:kafka-bug
 */

import { KafkaClient } from '../../src/kafka/kafka.client';
import { IEventHandler } from '../../src/kafka/kafka.event.handler';
import { Kafka, Producer } from 'kafkajs';

const TEST_BROKERS = process.env.TEST_KAFKA_BROKERS || 'localhost:29092';
const TEST_TOPIC = `bug-repro-${Date.now()}`;

// Colors for console output
const colors = {
  reset: '\x1b[0m',
  red: '\x1b[31m',
  green: '\x1b[32m',
  yellow: '\x1b[33m',
  blue: '\x1b[34m',
  magenta: '\x1b[35m',
  cyan: '\x1b[36m',
};

function log(message: string, color: keyof typeof colors = 'reset') {
  console.log(`${colors[color]}${message}${colors.reset}`);
}

// Test handler that tracks processing
class TestHandler implements IEventHandler<any> {
  public processedMessages: Array<{ key: string; event: any; timestamp: number }> = [];
  public batchedMessages: Array<{ key: string; events: any[]; timestamp: number }> = [];

  async handle({ key, event }: { key: string; event: any }): Promise<void> {
    this.processedMessages.push({
      key,
      event,
      timestamp: Date.now(),
    });
    log(`  ✓ Processed single message: key=${key}, id=${event.id}`, 'green');
  }

  async handleBatch({ key, events }: { key: string; events: any[] }): Promise<void> {
    this.batchedMessages.push({
      key,
      events,
      timestamp: Date.now(),
    });
    log(`  ✓ Processed batch: key=${key}, count=${events.length}`, 'cyan');
  }

  reset() {
    this.processedMessages = [];
    this.batchedMessages = [];
  }

  getTotalProcessed(): number {
    return this.processedMessages.length + 
      this.batchedMessages.reduce((sum, batch) => sum + batch.events.length, 0);
  }
}

async function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function setupKafka(): Promise<{ kafka: Kafka; producer: Producer }> {
  log('\n🔧 Setting up Kafka connection...', 'blue');
  
  const kafka = new Kafka({
    clientId: 'bug-repro-client',
    brokers: [TEST_BROKERS],
    logLevel: 0, // NOTHING
  });

  const producer = kafka.producer();
  await producer.connect();
  
  // Create test topic
  const admin = kafka.admin();
  await admin.connect();
  
  try {
    await admin.createTopics({
      topics: [{ topic: TEST_TOPIC, numPartitions: 1, replicationFactor: 1 }],
    });
    log(`✓ Created topic: ${TEST_TOPIC}`, 'green');
  } catch (error) {
    log(`⚠ Topic may already exist: ${error.message}`, 'yellow');
  }
  
  await admin.disconnect();
  
  return { kafka, producer };
}

async function produceMessages(producer: Producer, messages: Array<{ key: string; value: any }>) {
  log(`\n📤 Producing ${messages.length} messages...`, 'blue');
  
  const kafkaMessages = messages.map(msg => ({
    key: msg.key,
    value: JSON.stringify(msg.value),
  }));

  await producer.send({
    topic: TEST_TOPIC,
    messages: kafkaMessages,
  });

  log(`✓ Produced ${messages.length} messages`, 'green');
}

async function testScenario1(producer: Producer): Promise<boolean> {
  log('\n' + '='.repeat(60), 'magenta');
  log('TEST 1: Single messages with different keys', 'magenta');
  log('='.repeat(60), 'magenta');

  const handler = new TestHandler();
  const kafkaClient = new KafkaClient(
    `bug-test-1-${Date.now()}`,
    TEST_BROKERS,
    {
      maxConcurrency: 1,
      fromBeginning: true,
      enableCpuMonitoring: false,
      enableMemoryMonitoring: false,
    }
  );

  await kafkaClient.onModuleInit();

  await produceMessages(producer, [
    { key: 'user1', value: { id: 1, action: 'login' } },
    { key: 'user2', value: { id: 2, action: 'logout' } },
    { key: 'user3', value: { id: 3, action: 'purchase' } },
  ]);

  await kafkaClient.consumeMany(
    [{ topic: TEST_TOPIC, handler }],
    `test-group-1-${Date.now()}`
  );

  await sleep(3000);

  const totalProcessed = handler.getTotalProcessed();
  const success = totalProcessed === 3;

  log(`\n📊 Results:`, 'blue');
  log(`  Messages processed: ${totalProcessed}/3`, success ? 'green' : 'red');
  log(`  Single calls: ${handler.processedMessages.length}`, 'cyan');
  log(`  Batch calls: ${handler.batchedMessages.length}`, 'cyan');

  await kafkaClient.shutdown();

  if (success) {
    log('\n✅ TEST 1 PASSED', 'green');
  } else {
    log('\n❌ TEST 1 FAILED', 'red');
  }

  return success;
}

async function testScenario2(producer: Producer): Promise<boolean> {
  log('\n' + '='.repeat(60), 'magenta');
  log('TEST 2: Multiple messages with same key (batching)', 'magenta');
  log('='.repeat(60), 'magenta');

  const handler = new TestHandler();
  const kafkaClient = new KafkaClient(
    `bug-test-2-${Date.now()}`,
    TEST_BROKERS,
    {
      maxConcurrency: 1,
      minBatchSize: 2,
      batchSizeMultiplier: 10,
      fromBeginning: true,
      enableCpuMonitoring: false,
      enableMemoryMonitoring: false,
    }
  );

  await kafkaClient.onModuleInit();

  await produceMessages(producer, [
    { key: 'user1', value: { id: 1, action: 'login' } },
    { key: 'user1', value: { id: 2, action: 'view' } },
    { key: 'user1', value: { id: 3, action: 'purchase' } },
  ]);

  await kafkaClient.consumeMany(
    [{ topic: TEST_TOPIC, handler }],
    `test-group-2-${Date.now()}`
  );

  await sleep(3000);

  const totalProcessed = handler.getTotalProcessed();
  const hasBatches = handler.batchedMessages.length > 0;
  const success = totalProcessed === 3 && hasBatches;

  log(`\n📊 Results:`, 'blue');
  log(`  Messages processed: ${totalProcessed}/3`, totalProcessed === 3 ? 'green' : 'red');
  log(`  Single calls: ${handler.processedMessages.length}`, 'cyan');
  log(`  Batch calls: ${handler.batchedMessages.length}`, hasBatches ? 'green' : 'red');

  await kafkaClient.shutdown();

  if (success) {
    log('\n✅ TEST 2 PASSED', 'green');
  } else {
    log('\n❌ TEST 2 FAILED', 'red');
  }

  return success;
}

async function testScenario3(producer: Producer): Promise<boolean> {
  log('\n' + '='.repeat(60), 'magenta');
  log('TEST 3: High volume mixed traffic', 'magenta');
  log('='.repeat(60), 'magenta');

  const handler = new TestHandler();
  const kafkaClient = new KafkaClient(
    `bug-test-3-${Date.now()}`,
    TEST_BROKERS,
    {
      maxConcurrency: 2,
      minBatchSize: 3,
      batchSizeMultiplier: 10,
      fromBeginning: true,
      enableCpuMonitoring: false,
      enableMemoryMonitoring: false,
    }
  );

  await kafkaClient.onModuleInit();

  const messages = [];
  const keys = ['user1', 'user2', 'user3'];
  for (let i = 0; i < 20; i++) {
    messages.push({
      key: keys[i % keys.length],
      value: { id: i, action: `action_${i}` },
    });
  }

  const startTime = Date.now();
  await produceMessages(producer, messages);

  await kafkaClient.consumeMany(
    [{ topic: TEST_TOPIC, handler }],
    `test-group-3-${Date.now()}`
  );

  await sleep(4000);
  const processingTime = Date.now() - startTime;

  const totalProcessed = handler.getTotalProcessed();
  const throughput = (totalProcessed / (processingTime / 1000)).toFixed(2);
  const success = totalProcessed >= 15; // At least 75% processed

  log(`\n📊 Results:`, 'blue');
  log(`  Messages processed: ${totalProcessed}/20`, success ? 'green' : 'red');
  log(`  Processing time: ${processingTime}ms`, 'cyan');
  log(`  Throughput: ${throughput} msg/sec`, 'cyan');
  log(`  Batch calls: ${handler.batchedMessages.length}`, 'cyan');

  await kafkaClient.shutdown();

  if (success) {
    log('\n✅ TEST 3 PASSED', 'green');
  } else {
    log('\n❌ TEST 3 FAILED', 'red');
  }

  return success;
}

async function cleanup(kafka: Kafka, producer: Producer) {
  log('\n🧹 Cleaning up...', 'blue');
  
  try {
    await producer.disconnect();
    
    const admin = kafka.admin();
    await admin.connect();
    await admin.deleteTopics({ topics: [TEST_TOPIC] });
    await admin.disconnect();
    
    log('✓ Cleanup complete', 'green');
  } catch (error) {
    log(`⚠ Cleanup error: ${error.message}`, 'yellow');
  }
}

async function main() {
  log('\n' + '='.repeat(60), 'cyan');
  log('🧪 Kafka Bug Reproduction Test Suite', 'cyan');
  log('='.repeat(60), 'cyan');

  let kafka: Kafka | undefined;
  let producer: Producer | undefined;

  try {
    ({ kafka, producer } = await setupKafka());

    const results = {
      test1: await testScenario1(producer),
      test2: await testScenario2(producer),
      test3: await testScenario3(producer),
    };

    // Summary
    log('\n' + '='.repeat(60), 'cyan');
    log('📊 SUMMARY', 'cyan');
    log('='.repeat(60), 'cyan');

    const passed = Object.values(results).filter(r => r).length;
    const total = Object.keys(results).length;

    log(`\nTest 1 (Single messages): ${results.test1 ? '✅ PASSED' : '❌ FAILED'}`, results.test1 ? 'green' : 'red');
    log(`Test 2 (Batching): ${results.test2 ? '✅ PASSED' : '❌ FAILED'}`, results.test2 ? 'green' : 'red');
    log(`Test 3 (High volume): ${results.test3 ? '✅ PASSED' : '❌ FAILED'}`, results.test3 ? 'green' : 'red');

    log(`\n${passed}/${total} tests passed`, passed === total ? 'green' : 'red');

    await cleanup(kafka, producer);

    process.exit(passed === total ? 0 : 1);

  } catch (error) {
    log(`\n❌ Fatal error: ${error.message}`, 'red');
    log(error.stack, 'red');
    
    if (kafka && producer) {
      await cleanup(kafka, producer);
    }
    
    process.exit(1);
  }
}

// Run if executed directly
if (require.main === module) {
  main();
}

export { main, testScenario1, testScenario2, testScenario3 };
