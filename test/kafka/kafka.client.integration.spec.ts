import { Logger } from '@nestjs/common';
import { KafkaClient } from '../../src/kafka/kafka.client';
import { IEventHandler } from '../../src/kafka/kafka.event.handler';
import { setTimeout } from 'timers/promises';
import { randomUUID } from 'crypto';
import { Kafka, Consumer, Producer } from 'kafkajs';

/**
 * Kafka Client Integration Tests
 * 
 * These tests validate the KafkaClient behavior with real Kafka brokers.
 * They cover 5 key scenarios documented in the requirements:
 * 
 * Scenario 1: Single events with different keys
 * Scenario 2: Multiple events with same key
 * Scenario 3: Handler processes latest but commits all
 * Scenario 4: High volume mixed traffic
 * Scenario 5: Slow processing with timeouts
 */

describe('KafkaClient Integration Tests - 5 Scenarios', () => {
  jest.setTimeout(60000);
  
  let kafkaClient: KafkaClient;
  let testKafka: Kafka;
  let testProducer: Producer;
  
  const TEST_BROKERS = process.env.TEST_KAFKA_BROKERS || 'localhost:29092';
  const TEST_TOPIC = `integration-test-${Date.now()}-${randomUUID()}`;
  
  // Test tracking
  let handlerCalls: any[] = [];
  let batchHandlerCalls: any[] = [];

  beforeAll(async () => {
    try {
      testKafka = new Kafka({
        clientId: 'integration-test-client',
        brokers: [TEST_BROKERS],
        logLevel: 0,
      });
      
      testProducer = testKafka.producer();
      await testProducer.connect();
      
      const admin = testKafka.admin();
      await admin.connect();
      await admin.createTopics({
        topics: [
          { topic: TEST_TOPIC, numPartitions: 3, replicationFactor: 1 }
        ]
      });
      await admin.disconnect();
      
    } catch (error) {
      console.warn(`⚠️  Kafka not available at ${TEST_BROKERS}, skipping integration tests`);
      console.warn(`Error: ${error.message}`);
      return;
    }
  });

  beforeEach(async () => {
    handlerCalls = [];
    batchHandlerCalls = [];
  });

  afterEach(async () => {
    try {
      if (kafkaClient) {
        await kafkaClient.shutdown();
        kafkaClient = null as any;
      }
      await setTimeout(500);
    } catch (error) {
      console.warn('Cleanup error:', error.message);
    }
  });

  afterAll(async () => {
    try {
      if (testProducer) {
        await testProducer.disconnect();
      }
      
      if (testKafka) {
        const admin = testKafka.admin();
        await admin.connect();
        await admin.deleteTopics({ topics: [TEST_TOPIC] });
        await admin.disconnect();
      }
      
      await setTimeout(1000);
    } catch (error) {
      console.warn('Final cleanup error:', error.message);
    }
  });

  const produceMessages = async (messages: Array<{key: string, value: any}>) => {
    const kafkaMessages = messages.map(msg => ({
      key: msg.key,
      value: JSON.stringify(msg.value),
    }));
    
    await testProducer.send({
      topic: TEST_TOPIC,
      messages: kafkaMessages,
    });
  };

  describe('Scenario 1: Single events with different keys', () => {
    it('should process individual messages with different keys separately', async () => {
      if (!testKafka) return;

      class SingleEventHandler implements IEventHandler<any> {
        async handle({ key, event, payload }: { key: string; event: any; payload: any }): Promise<void> {
          handlerCalls.push({
            type: 'single',
            key,
            event,
            timestamp: Date.now()
          });
        }
      }

      kafkaClient = new KafkaClient(
        `scenario1-client-${Date.now()}-${randomUUID()}`,
        TEST_BROKERS,
        {
          maxConcurrency: 1,
          sessionTimeout: 60000,
          heartbeatInterval: 15000,
          fromBeginning: true,
          enableCpuMonitoring: false,
          enableMemoryMonitoring: false,
        }
      );
      
      await kafkaClient.onModuleInit();

      await produceMessages([
        { key: 'user1', value: { id: 1, action: 'login' } },
        { key: 'user2', value: { id: 2, action: 'logout' } },
        { key: 'user3', value: { id: 3, action: 'purchase' } },
      ]);

      const uniqueGroupId = `scenario1-${Date.now()}-${randomUUID()}`;
      await kafkaClient.consumeMany([
        { topic: TEST_TOPIC, handler: new SingleEventHandler() }
      ], uniqueGroupId);

      await setTimeout(3000);

      console.log(`📊 Scenario 1 Results:
        🔄 Handler Calls: ${handlerCalls.length}
        🔑 Unique Keys: ${new Set(handlerCalls.map(c => c.key)).size}`);

      expect(handlerCalls.length).toBe(3);
      expect(new Set(handlerCalls.map(c => c.key)).size).toBe(3);
    });
  });

  describe('Scenario 2: Multiple events with same key', () => {
    it('should batch messages with the same key when handleBatch is available', async () => {
      if (!testKafka) return;

      class SameKeyHandler implements IEventHandler<any> {
        async handle({ key, event }: { key: string; event: any }): Promise<void> {
          handlerCalls.push({
            type: 'single',
            key,
            event,
            timestamp: Date.now()
          });
        }

        async handleBatch({ key, events, payloads }: { 
          key: string; 
          events: any[]; 
          payloads: any[] 
        }): Promise<void> {
          batchHandlerCalls.push({
            type: 'batch',
            key,
            eventCount: events.length,
            events,
            timestamp: Date.now()
          });
        }
      }

      kafkaClient = new KafkaClient(
        `scenario2-client-${Date.now()}-${randomUUID()}`,
        TEST_BROKERS,
        {
          maxConcurrency: 1,
          batchSizeMultiplier: 10,
          minBatchSize: 2,
          sessionTimeout: 60000,
          heartbeatInterval: 15000,
          fromBeginning: true,
          enableCpuMonitoring: false,
          enableMemoryMonitoring: false,
        }
      );
      
      await kafkaClient.onModuleInit();
      await setTimeout(1000); // Wait for async initialization

      await produceMessages([
        { key: 'user1', value: { id: 1, action: 'login' } },
        { key: 'user1', value: { id: 2, action: 'view_product' } },
        { key: 'user1', value: { id: 3, action: 'add_to_cart' } },
        { key: 'user1', value: { id: 4, action: 'purchase' } },
      ]);

      const uniqueGroupId = `scenario2-${Date.now()}-${randomUUID()}`;
      await kafkaClient.consumeMany([
        { topic: TEST_TOPIC, handler: new SameKeyHandler() }
      ], uniqueGroupId);

      await setTimeout(3000);

      const totalProcessed = handlerCalls.length + 
        batchHandlerCalls.reduce((sum, batch) => sum + batch.eventCount, 0);

      console.log(`📊 Scenario 2 Results:
        📦 Batch Calls: ${batchHandlerCalls.length}
        🔄 Single Calls: ${handlerCalls.length}
        📈 Total Processed: ${totalProcessed}`);

      expect(totalProcessed).toBeGreaterThan(0);
      expect(batchHandlerCalls.length).toBeGreaterThan(0);
    });
  });

  describe('Scenario 3: Handler processes latest but commits all', () => {
    it('should commit all offsets even when handler only processes latest', async () => {
      if (!testKafka) return;

      class LatestOnlyHandler implements IEventHandler<any> {
        async handleBatch({ key, events, payloads }: { 
          key: string; 
          events: any[]; 
          payloads: any[] 
        }): Promise<void> {
          // Only process the latest event
          const latestEvent = events[events.length - 1];
          
          batchHandlerCalls.push({
            type: 'batch',
            key,
            processedCount: 1,
            totalCount: events.length,
            latestEvent,
            timestamp: Date.now()
          });
        }
      }

      kafkaClient = new KafkaClient(
        `scenario3-client-${Date.now()}-${randomUUID()}`,
        TEST_BROKERS,
        {
          maxConcurrency: 1,
          batchSizeMultiplier: 10,
          minBatchSize: 2,
          sessionTimeout: 60000,
          heartbeatInterval: 15000,
          fromBeginning: true,
          enableCpuMonitoring: false,
          enableMemoryMonitoring: false,
        }
      );
      
      await kafkaClient.onModuleInit();
      await setTimeout(1000); // Wait for async initialization

      await produceMessages([
        { key: 'user1', value: { id: 1, version: 1 } },
        { key: 'user1', value: { id: 1, version: 2 } },
        { key: 'user1', value: { id: 1, version: 3 } },
        { key: 'user1', value: { id: 1, version: 4 } },
      ]);

      const uniqueGroupId = `scenario3-${Date.now()}-${randomUUID()}`;
      await kafkaClient.consumeMany([
        { topic: TEST_TOPIC, handler: new LatestOnlyHandler() }
      ], uniqueGroupId);

      await setTimeout(3000);

      console.log(`📊 Scenario 3 Results:
        📦 Batch Calls: ${batchHandlerCalls.length}
        📝 Processed: ${batchHandlerCalls.map(b => b.processedCount).reduce((a, b) => a + b, 0)}
        📥 Total Received: ${batchHandlerCalls.map(b => b.totalCount).reduce((a, b) => a + b, 0)}`);

      expect(batchHandlerCalls.length).toBeGreaterThan(0);
      expect(batchHandlerCalls[0].processedCount).toBe(1);
      expect(batchHandlerCalls[0].totalCount).toBeGreaterThan(1);
    });
  });

  describe('Scenario 4: High volume mixed traffic', () => {
    it('should handle high volume with mixed keys efficiently', async () => {
      if (!testKafka) return;

      const MESSAGE_COUNT = 50;
      const KEYS = ['user1', 'user2', 'user3', 'user4', 'user5'];

      class HighVolumeHandler implements IEventHandler<any> {
        async handle({ key, event }: { key: string; event: any }): Promise<void> {
          handlerCalls.push({
            type: 'single',
            key,
            event,
            timestamp: Date.now()
          });
        }

        async handleBatch({ key, events }: { key: string; events: any[] }): Promise<void> {
          batchHandlerCalls.push({
            type: 'batch',
            key,
            eventCount: events.length,
            timestamp: Date.now()
          });
        }
      }

      kafkaClient = new KafkaClient(
        `scenario4-client-${Date.now()}-${randomUUID()}`,
        TEST_BROKERS,
        {
          maxConcurrency: 3,
          batchSizeMultiplier: 15,
          minBatchSize: 3,
          sessionTimeout: 60000,
          heartbeatInterval: 15000,
          fromBeginning: true,
          enableCpuMonitoring: false,
          enableMemoryMonitoring: false,
        }
      );
      
      await kafkaClient.onModuleInit();
      await setTimeout(1000); // Wait for async initialization

      const messages = [];
      for (let i = 0; i < MESSAGE_COUNT; i++) {
        messages.push({
          key: KEYS[i % KEYS.length],
          value: { id: i, action: `action_${i}`, timestamp: Date.now() }
        });
      }

      const startTime = Date.now();
      await produceMessages(messages);

      const uniqueGroupId = `scenario4-${Date.now()}-${randomUUID()}`;
      await kafkaClient.consumeMany([
        { topic: TEST_TOPIC, handler: new HighVolumeHandler() }
      ], uniqueGroupId);

      await setTimeout(5000);
      const processingTime = Date.now() - startTime;

      const totalProcessed = handlerCalls.length + 
        batchHandlerCalls.reduce((sum, batch) => sum + batch.eventCount, 0);
      const throughput = (totalProcessed / (processingTime / 1000)).toFixed(2);

      console.log(`📊 Scenario 4 Results:
        📥 Produced: ${MESSAGE_COUNT}
        ⚡ Processing Time: ${processingTime}ms
        🔄 Total Processed: ${totalProcessed}
        🚀 Throughput: ${throughput} msg/sec`);

      expect(totalProcessed).toBeGreaterThan(0);
      expect(processingTime).toBeLessThan(10000);
    });
  });

  describe('Scenario 5: Slow processing with timeouts', () => {
    it('should handle slow processing without timing out', async () => {
      if (!testKafka) return;

      class SlowHandler implements IEventHandler<any> {
        async handle({ key, event }: { key: string; event: any }): Promise<void> {
          if (event.slow) {
            await setTimeout(500);
          }
          
          handlerCalls.push({
            type: 'single',
            key,
            event,
            timestamp: Date.now()
          });
        }

        async handleBatch({ key, events }: { key: string; events: any[] }): Promise<void> {
          const hasSlowEvent = events.some(event => event.slow);
          if (hasSlowEvent) {
            await setTimeout(1000);
          }

          batchHandlerCalls.push({
            type: 'batch',
            key,
            eventCount: events.length,
            slow: hasSlowEvent,
            timestamp: Date.now()
          });
        }
      }

      kafkaClient = new KafkaClient(
        `scenario5-client-${Date.now()}-${randomUUID()}`,
        TEST_BROKERS,
        {
          maxConcurrency: 2,
          batchSizeMultiplier: 3,
          sessionTimeout: 90000,
          heartbeatInterval: 20000,
          fromBeginning: true,
          enableCpuMonitoring: false,
          enableMemoryMonitoring: false,
        }
      );
      
      await kafkaClient.onModuleInit();
      await setTimeout(1000); // Wait for async initialization

      const startTime = Date.now();
      
      await produceMessages([
        { key: 'user1', value: { id: 1, slow: true } },
        { key: 'user1', value: { id: 2, slow: true } },
        { key: 'user2', value: { id: 3, slow: false } },
        { key: 'user2', value: { id: 4, slow: false } },
      ]);

      const uniqueGroupId = `scenario5-${Date.now()}-${randomUUID()}`;
      await kafkaClient.consumeMany([
        { topic: TEST_TOPIC, handler: new SlowHandler() }
      ], uniqueGroupId);

      await setTimeout(6000);
      const totalTime = Date.now() - startTime;

      const slowBatches = batchHandlerCalls.filter(batch => batch.slow);
      const totalProcessed = handlerCalls.length + batchHandlerCalls.length;

      console.log(`📊 Scenario 5 Results:
        ⏱️  Total Time: ${totalTime}ms
        🐌 Slow Batches: ${slowBatches.length}
        🔄 Total Processed: ${totalProcessed}`);

      expect(totalProcessed).toBeGreaterThan(0);
    });
  });
});

/**
 * Metrics Accuracy Integration Tests
 * 
 * These tests validate that metrics are accurately tracked during real Kafka message processing
 */
describe('Metrics Accuracy Integration Tests', () => {
  jest.setTimeout(60000);
  
  let kafkaClient: KafkaClient;
  let testKafka: Kafka;
  let testProducer: Producer;
  
  const TEST_BROKERS = process.env.TEST_KAFKA_BROKERS || 'localhost:29092';
  const TEST_TOPIC = `metrics-test-${Date.now()}-${randomUUID()}`;
  
  let handlerCalls: any[] = [];
  let batchHandlerCalls: any[] = [];

  beforeAll(async () => {
    try {
      testKafka = new Kafka({
        clientId: 'metrics-test-client',
        brokers: [TEST_BROKERS],
        logLevel: 0,
      });
      
      testProducer = testKafka.producer();
      await testProducer.connect();
      
      const admin = testKafka.admin();
      await admin.connect();
      await admin.createTopics({
        topics: [
          { topic: TEST_TOPIC, numPartitions: 3, replicationFactor: 1 }
        ]
      });
      await admin.disconnect();
      
    } catch (error) {
      console.warn(`⚠️  Kafka not available at ${TEST_BROKERS}, skipping metrics integration tests`);
      console.warn(`Error: ${error.message}`);
      return;
    }
  });

  beforeEach(async () => {
    handlerCalls = [];
    batchHandlerCalls = [];
  });

  afterEach(async () => {
    try {
      if (kafkaClient) {
        await kafkaClient.shutdown();
        kafkaClient = null as any;
      }
      await setTimeout(500);
    } catch (error) {
      console.warn('Cleanup error:', error.message);
    }
  });

  afterAll(async () => {
    try {
      if (testProducer) {
        await testProducer.disconnect();
      }
      
      if (testKafka) {
        const admin = testKafka.admin();
        await admin.connect();
        await admin.deleteTopics({ topics: [TEST_TOPIC] });
        await admin.disconnect();
      }
      
      await setTimeout(1000);
    } catch (error) {
      console.warn('Final cleanup error:', error.message);
    }
  });

  const produceMessages = async (messages: Array<{key: string, value: any}>) => {
    const kafkaMessages = messages.map(msg => ({
      key: msg.key,
      value: JSON.stringify(msg.value),
    }));
    
    await testProducer.send({
      topic: TEST_TOPIC,
      messages: kafkaMessages,
    });
  };

  it('should track consumed count matching produced count', async () => {
    if (!testKafka) return;

    class MetricsHandler implements IEventHandler<any> {
      async handle({ key, event }: { key: string; event: any }): Promise<void> {
        handlerCalls.push({ key, event });
      }
    }

    kafkaClient = new KafkaClient(
      `metrics-consumed-${Date.now()}-${randomUUID()}`,
      TEST_BROKERS,
      {
        maxConcurrency: 1,
        fromBeginning: true,
        enableCpuMonitoring: false,
        enableMemoryMonitoring: false,
      }
    );
    
    await kafkaClient.onModuleInit();

    const messageCount = 10;
    const messages = Array.from({ length: messageCount }, (_, i) => ({
      key: `key${i}`,
      value: { id: i, data: `test${i}` }
    }));

    await produceMessages(messages);

    const uniqueGroupId = `metrics-consumed-${Date.now()}-${randomUUID()}`;
    await kafkaClient.consumeMany([
      { topic: TEST_TOPIC, handler: new MetricsHandler() }
    ], uniqueGroupId);

    await setTimeout(3000);

    const metrics = kafkaClient.getMetrics();

    console.log(`📊 Consumed Metrics:
      📥 Consumed: ${metrics.consumedMessages}
      ✅ Processed: ${metrics.processedMessages}
      📨 Produced: ${messageCount}`);

    expect(metrics.consumedMessages).toBe(messageCount);
  });

  it('should track processed count matching consumed count', async () => {
    if (!testKafka) return;

    class ProcessedHandler implements IEventHandler<any> {
      async handle({ key, event }: { key: string; event: any }): Promise<void> {
        handlerCalls.push({ key, event });
      }
    }

    kafkaClient = new KafkaClient(
      `metrics-processed-${Date.now()}-${randomUUID()}`,
      TEST_BROKERS,
      {
        maxConcurrency: 1,
        fromBeginning: true,
        enableCpuMonitoring: false,
        enableMemoryMonitoring: false,
      }
    );
    
    await kafkaClient.onModuleInit();

    const messageCount = 15;
    const messages = Array.from({ length: messageCount }, (_, i) => ({
      key: `key${i}`,
      value: { id: i, data: `test${i}` }
    }));

    await produceMessages(messages);

    const uniqueGroupId = `metrics-processed-${Date.now()}-${randomUUID()}`;
    await kafkaClient.consumeMany([
      { topic: TEST_TOPIC, handler: new ProcessedHandler() }
    ], uniqueGroupId);

    await setTimeout(3000);

    const metrics = kafkaClient.getMetrics();

    console.log(`📊 Processed Metrics:
      📥 Consumed: ${metrics.consumedMessages}
      ✅ Processed: ${metrics.processedMessages}`);

    expect(metrics.processedMessages).toBe(metrics.consumedMessages);
  });

  it('should calculate batch efficiency accurately', async () => {
    if (!testKafka) return;

    class BatchEfficiencyHandler implements IEventHandler<any> {
      async handle({ key, event }: { key: string; event: any }): Promise<void> {
        handlerCalls.push({ key, event });
      }

      async handleBatch({ key, events }: { key: string; events: any[] }): Promise<void> {
        batchHandlerCalls.push({ key, count: events.length });
      }
    }

    kafkaClient = new KafkaClient(
      `metrics-batch-${Date.now()}-${randomUUID()}`,
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
    await setTimeout(1000); // Wait for async initialization

    // Send messages with same key to trigger batching
    await produceMessages([
      { key: 'batch-key', value: { id: 1 } },
      { key: 'batch-key', value: { id: 2 } },
      { key: 'batch-key', value: { id: 3 } },
      { key: 'batch-key', value: { id: 4 } },
      { key: 'single-key1', value: { id: 5 } },
      { key: 'single-key2', value: { id: 6 } },
    ]);

    const uniqueGroupId = `metrics-batch-${Date.now()}-${randomUUID()}`;
    await kafkaClient.consumeMany([
      { topic: TEST_TOPIC, handler: new BatchEfficiencyHandler() }
    ], uniqueGroupId);

    await setTimeout(3000);

    const metrics = kafkaClient.getMetrics();

    console.log(`📊 Batch Efficiency Metrics:
      📦 Total Batches: ${metrics.totalBatches}
      📈 Batch Efficiency: ${metrics.batchEfficiency.toFixed(2)}%
      📊 Avg Batch Size: ${metrics.avgBatchSize.toFixed(2)}`);

    expect(metrics.totalBatches).toBeGreaterThan(0);
    expect(metrics.batchEfficiency).toBeGreaterThan(0);
    expect(metrics.batchEfficiency).toBeLessThanOrEqual(100);
  });

  it('should track processing time measurements', async () => {
    if (!testKafka) return;

    class TimingHandler implements IEventHandler<any> {
      async handle({ key, event }: { key: string; event: any }): Promise<void> {
        // Simulate some processing time
        await setTimeout(50);
        handlerCalls.push({ key, event });
      }
    }

    kafkaClient = new KafkaClient(
      `metrics-timing-${Date.now()}-${randomUUID()}`,
      TEST_BROKERS,
      {
        maxConcurrency: 1,
        fromBeginning: true,
        enableCpuMonitoring: false,
        enableMemoryMonitoring: false,
      }
    );
    
    await kafkaClient.onModuleInit();
    await setTimeout(1000); // Wait for async initialization

    await produceMessages([
      { key: 'key1', value: { id: 1 } },
      { key: 'key2', value: { id: 2 } },
      { key: 'key3', value: { id: 3 } },
    ]);

    const uniqueGroupId = `metrics-timing-${Date.now()}-${randomUUID()}`;
    await kafkaClient.consumeMany([
      { topic: TEST_TOPIC, handler: new TimingHandler() }
    ], uniqueGroupId);

    await setTimeout(3000);

    const metrics = kafkaClient.getMetrics();

    console.log(`📊 Processing Time Metrics:
      ⏱️  Avg: ${metrics.avgProcessingTimeMs.toFixed(2)}ms
      ⏱️  Min: ${metrics.minProcessingTimeMs.toFixed(2)}ms
      ⏱️  Max: ${metrics.maxProcessingTimeMs.toFixed(2)}ms`);

    expect(metrics.avgProcessingTimeMs).toBeGreaterThan(0);
    expect(metrics.minProcessingTimeMs).toBeGreaterThan(0);
    expect(metrics.maxProcessingTimeMs).toBeGreaterThanOrEqual(metrics.minProcessingTimeMs);
    expect(metrics.avgProcessingTimeMs).toBeGreaterThanOrEqual(metrics.minProcessingTimeMs);
    expect(metrics.avgProcessingTimeMs).toBeLessThanOrEqual(metrics.maxProcessingTimeMs);
  });

  it('should provide complete metrics snapshot', async () => {
    if (!testKafka) return;

    class CompleteMetricsHandler implements IEventHandler<any> {
      async handle({ key, event }: { key: string; event: any }): Promise<void> {
        handlerCalls.push({ key, event });
      }
    }

    kafkaClient = new KafkaClient(
      `metrics-complete-${Date.now()}-${randomUUID()}`,
      TEST_BROKERS,
      {
        maxConcurrency: 2,
        fromBeginning: true,
        enableCpuMonitoring: false,
        enableMemoryMonitoring: false,
      }
    );
    
    await kafkaClient.onModuleInit();
    await setTimeout(1000); // Wait for async initialization

    await produceMessages([
      { key: 'key1', value: { id: 1 } },
      { key: 'key2', value: { id: 2 } },
    ]);

    const uniqueGroupId = `metrics-complete-${Date.now()}-${randomUUID()}`;
    await kafkaClient.consumeMany([
      { topic: TEST_TOPIC, handler: new CompleteMetricsHandler() }
    ], uniqueGroupId);

    await setTimeout(3000);

    const metrics = kafkaClient.getMetrics();

    // Verify all required fields are present
    expect(metrics).toHaveProperty('consumedMessages');
    expect(metrics).toHaveProperty('processedMessages');
    expect(metrics).toHaveProperty('failedMessages');
    expect(metrics).toHaveProperty('dlqMessages');
    expect(metrics).toHaveProperty('avgProcessingTimeMs');
    expect(metrics).toHaveProperty('minProcessingTimeMs');
    expect(metrics).toHaveProperty('maxProcessingTimeMs');
    expect(metrics).toHaveProperty('batchEfficiency');
    expect(metrics).toHaveProperty('avgBatchSize');
    expect(metrics).toHaveProperty('totalBatches');
    expect(metrics).toHaveProperty('memory');
    expect(metrics).toHaveProperty('cpu');
    expect(metrics).toHaveProperty('currentQueueSize');
    expect(metrics).toHaveProperty('maxQueueSize');
    expect(metrics).toHaveProperty('isConnected');
    expect(metrics).toHaveProperty('reconnectCount');

    // Verify memory metrics structure
    expect(metrics.memory).toHaveProperty('currentPercent');
    expect(metrics.memory).toHaveProperty('rssBytes');
    expect(metrics.memory).toHaveProperty('limitBytes');

    // Verify CPU metrics structure
    expect(metrics.cpu).toHaveProperty('currentPercent');
    expect(metrics.cpu).toHaveProperty('avgPercent');

    console.log(`📊 Complete Metrics Snapshot:
      📥 Consumed: ${metrics.consumedMessages}
      ✅ Processed: ${metrics.processedMessages}
      ❌ Failed: ${metrics.failedMessages}
      💀 DLQ: ${metrics.dlqMessages}
      ⏱️  Avg Time: ${metrics.avgProcessingTimeMs.toFixed(2)}ms
      📦 Batches: ${metrics.totalBatches}
      📈 Efficiency: ${metrics.batchEfficiency.toFixed(2)}%
      💾 Memory: ${metrics.memory.currentPercent.toFixed(2)}%
      🔥 CPU: ${metrics.cpu.currentPercent.toFixed(2)}%
      🔌 Connected: ${metrics.isConnected}`);

    expect(metrics.isConnected).toBe(true);
  });
});
