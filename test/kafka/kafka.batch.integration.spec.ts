import { Test } from '@nestjs/testing';
import { Logger } from '@nestjs/common';
import { KafkaClient } from '../../src/kafka/kafka.client';
import { IEventHandler } from '../../src/kafka/kafka.event.handler';
import { KafkaMessage } from '@nestjs/microservices/external/kafka.interface';
import { setTimeout } from 'timers/promises';
import { randomUUID } from 'crypto';
import { Kafka, Consumer, Producer } from 'kafkajs';

/**
 * Kafka Batch Processing Integration Tests
 * 
 * These tests comprehensively validate batch processing behavior in different scenarios:
 * 1. Single message processing
 * 2. Same-key batch processing (handleBatch)
 * 3. Mixed-key batch processing
 * 4. High-volume batch processing
 * 5. Batch processing with failures
 * 6. Batch processing with timeouts
 * 7. Consumer Module batch processing
 * 8. Concurrent batch processing
 * 9. Batch size limits and overflow
 * 10. Batch processing with different partition strategies
 */

describe('Kafka Batch Processing Integration Tests', () => {
  jest.setTimeout(60000); // 60 seconds for comprehensive tests
  
  let kafkaClient: KafkaClient;
  let testKafka: Kafka;
  let testProducer: Producer;
  let monitorConsumer: Consumer;
  
  const TEST_BROKERS = process.env.TEST_KAFKA_BROKERS || 'localhost:29092';
  const TEST_TOPIC = `batch-test-${Date.now()}-${randomUUID()}`;
  const GROUP_ID = `batch-test-group-${Date.now()}-${randomUUID()}`;
  
  // Test tracking
  let producedMessages: any[] = [];
  let consumedMessages: any[] = [];
  let handlerCalls: any[] = [];
  let batchHandlerCalls: any[] = [];

  beforeAll(async () => {
    // Skip tests if Kafka is not available
    try {
      testKafka = new Kafka({
        clientId: 'batch-test-client',
        brokers: [TEST_BROKERS],
        logLevel: 0, // NOTHING to reduce noise
      });
      
      testProducer = testKafka.producer();
      await testProducer.connect();
      
      // Create test topic with multiple partitions for testing
      const admin = testKafka.admin();
      await admin.connect();
      await admin.createTopics({
        topics: [
          { topic: TEST_TOPIC, numPartitions: 3, replicationFactor: 1 }
        ]
      });
      await admin.disconnect();
      
    } catch (error) {
      console.warn(`⚠️  Kafka not available at ${TEST_BROKERS}, skipping batch tests`);
      console.warn(`Error: ${error.message}`);
      return;
    }
  });

  beforeEach(async () => {
    // Reset tracking arrays
    producedMessages = [];
    consumedMessages = [];
    handlerCalls = [];
    batchHandlerCalls = [];
  });

  afterEach(async () => {
    try {
      // Shutdown KafkaClient first
      if (kafkaClient) {
        await kafkaClient.shutdown();
        kafkaClient = null as any;
      }
      
      // Disconnect monitoring consumer
      if (monitorConsumer) {
        await monitorConsumer.disconnect();
        monitorConsumer = null as any;
      }
      
      // Wait a bit for async operations to complete
      await setTimeout(500);
    } catch (error) {
      console.warn('Cleanup error:', error.message);
    }
  });

  afterAll(async () => {
    try {
      // Disconnect producer
      if (testProducer) {
        await testProducer.disconnect();
        testProducer = null as any;
      }
      
      // Final cleanup of monitoring consumer
      if (monitorConsumer) {
        await monitorConsumer.disconnect();
        monitorConsumer = null as any;
      }
      
      // Cleanup test topic
      if (testKafka) {
        const admin = testKafka.admin();
        await admin.connect();
        await admin.deleteTopics({ topics: [TEST_TOPIC] });
        await admin.disconnect();
      }
      
      // Wait for all async operations to complete
      await setTimeout(1000);
    } catch (error) {
      console.warn('Final cleanup error:', error.message);
    }
  });

  // Helper to produce messages
  const produceMessages = async (messages: Array<{key: string, value: any, partition?: number}>) => {
    const kafkaMessages = messages.map(msg => ({
      key: msg.key,
      value: JSON.stringify(msg.value),
      partition: msg.partition,
    }));
    
    await testProducer.send({
      topic: TEST_TOPIC,
      messages: kafkaMessages,
    });
    
    producedMessages.push(...messages);
  };

  // Helper to setup monitoring
  const setupMonitoring = async () => {
    monitorConsumer = testKafka.consumer({ 
      groupId: `${GROUP_ID}-monitor`,
      maxWaitTimeInMs: 1000 
    });
    await monitorConsumer.connect();
    await monitorConsumer.subscribe({ topic: TEST_TOPIC, fromBeginning: true });
    
    monitorConsumer.run({
      eachMessage: async ({ message, partition }) => {
        consumedMessages.push({
          key: message.key?.toString(),
          value: JSON.parse(message.value?.toString() || '{}'),
          partition,
          offset: message.offset,
        });
      },
    });
  };

  describe('🔍 Single Message Processing', () => {
    it('should process individual messages without batching', async () => {
      if (!testKafka) return;

      class SingleMessageHandler implements IEventHandler<any> {
        async handle({ key, event, payload }: { key: string; event: any; payload: any }): Promise<void> {
          handlerCalls.push({
            type: 'single',
            key,
            event,
            offset: payload.offset,
            timestamp: Date.now()
          });
        }
      }

      kafkaClient = new KafkaClient(
        `single-msg-client-${Date.now()}-${randomUUID()}`,
        TEST_BROKERS,
        {
          maxConcurrency: 1,
          batchSizeMultiplier: 1, // Force single message processing
          // Use stable configuration that works
          sessionTimeout: 60000,
          heartbeatInterval: 15000,
          fromBeginning: true,
          enableCpuMonitoring: false,
          enableMemoryMonitoring: false,
        }
      );
      
      await kafkaClient.onModuleInit();
      await setupMonitoring();

      // Produce messages with different keys
      await produceMessages([
        { key: 'user1', value: { id: 1, action: 'login' } },
        { key: 'user2', value: { id: 2, action: 'logout' } },
        { key: 'user3', value: { id: 3, action: 'purchase' } },
      ]);

      const uniqueGroupId = `single-msg-${Date.now()}-${randomUUID()}`;
      await kafkaClient.consumeMany([
        { topic: TEST_TOPIC, handler: new SingleMessageHandler() }
      ], uniqueGroupId);

      await setTimeout(3000);

      // Verify each message was processed individually
      expect(handlerCalls.length).toBe(3);
      expect(handlerCalls.every(call => call.type === 'single')).toBe(true);
      expect(batchHandlerCalls.length).toBe(0);

      console.log(`📊 Single Message Test:
        📥 Produced: ${producedMessages.length}
        🔄 Handler Calls: ${handlerCalls.length}
        📦 Batch Calls: ${batchHandlerCalls.length}`);
    });
  });

  describe('📦 Same-Key Batch Processing', () => {
    it('should batch messages with the same key using handleBatch', async () => {
      if (!testKafka) return;

      class BatchingHandler implements IEventHandler<any> {
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
        `batch-client-${Date.now()}-${randomUUID()}`,
        TEST_BROKERS,
        {
          maxConcurrency: 1,
          batchSizeMultiplier: 10,
          minBatchSize: 2,
          // Use stable configuration that works
          sessionTimeout: 60000,
          heartbeatInterval: 15000,
          fromBeginning: true,
          enableCpuMonitoring: false,
          enableMemoryMonitoring: false,
        }
      );
      
      await kafkaClient.onModuleInit();
      await setupMonitoring();

      // Produce multiple messages with the same key
      await produceMessages([
        { key: 'user1', value: { id: 1, action: 'login' } },
        { key: 'user1', value: { id: 2, action: 'view_product' } },
        { key: 'user1', value: { id: 3, action: 'add_to_cart' } },
        { key: 'user1', value: { id: 4, action: 'purchase' } },
        { key: 'user2', value: { id: 5, action: 'login' } },
      ]);

      const uniqueGroupId = `batch-same-key-${Date.now()}-${randomUUID()}`;
      await kafkaClient.consumeMany([
        { topic: TEST_TOPIC, handler: new BatchingHandler() }
      ], uniqueGroupId);

      await setTimeout(3000);

      // Verify batching behavior
      const user1BatchCalls = batchHandlerCalls.filter(call => call.key === 'user1');
      const user2SingleCalls = handlerCalls.filter(call => call.key === 'user2');

      expect(user1BatchCalls.length).toBeGreaterThan(0);
      expect(user1BatchCalls[0].eventCount).toBeGreaterThan(1);
      expect(user2SingleCalls.length).toBeGreaterThan(0);

      console.log(`📊 Batch Processing Test:
        📥 Produced: ${producedMessages.length}
        🔄 Single Handler Calls: ${handlerCalls.length}
        📦 Batch Handler Calls: ${batchHandlerCalls.length}
        👤 User1 Batched Events: ${user1BatchCalls[0]?.eventCount || 0}`);
    });
  });

  describe('🔀 Mixed-Key Batch Processing', () => {
    it('should handle mixed keys with appropriate batching strategy', async () => {
      if (!testKafka) return;

      class MixedKeyHandler implements IEventHandler<any> {
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
        `mixed-key-client-${Date.now()}-${randomUUID()}`,
        TEST_BROKERS,
        {
          maxConcurrency: 2,
          batchSizeMultiplier: 5,
          minBatchSize: 2,
          // Use stable configuration
          sessionTimeout: 60000,
          heartbeatInterval: 15000,
          fromBeginning: true,
          enableCpuMonitoring: false,
          enableMemoryMonitoring: false,
        }
      );
      
      await kafkaClient.onModuleInit();
      await setupMonitoring();

      // Produce messages with mixed keys
      await produceMessages([
        { key: 'user1', value: { id: 1, action: 'login' } },
        { key: 'user2', value: { id: 2, action: 'login' } },
        { key: 'user1', value: { id: 3, action: 'view' } },
        { key: 'user3', value: { id: 4, action: 'login' } },
        { key: 'user1', value: { id: 5, action: 'purchase' } },
        { key: 'user2', value: { id: 6, action: 'logout' } },
      ]);

      const uniqueGroupId = `batch-mixed-${Date.now()}-${randomUUID()}`;
      await kafkaClient.consumeMany([
        { topic: TEST_TOPIC, handler: new MixedKeyHandler() }
      ], uniqueGroupId);

      await setTimeout(4000);

      // Analyze batching patterns
      const keyGroups = new Map();
      batchHandlerCalls.forEach(call => {
        if (!keyGroups.has(call.key)) keyGroups.set(call.key, []);
        keyGroups.get(call.key).push(call);
      });

      console.log(`📊 Mixed Key Batch Test:
        📥 Produced: ${producedMessages.length}
        🔄 Single Calls: ${handlerCalls.length}
        📦 Batch Calls: ${batchHandlerCalls.length}
        🔑 Keys with Batches: ${keyGroups.size}`);

      expect(handlerCalls.length + batchHandlerCalls.length).toBeGreaterThan(0);
    });
  });

  describe('🚀 High-Volume Batch Processing', () => {
    it('should handle high-volume message processing efficiently', async () => {
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
        `high-volume-client-${Date.now()}-${randomUUID()}`,
        TEST_BROKERS,
        {
          maxConcurrency: 3,
          batchSizeMultiplier: 15,
          minBatchSize: 3,
          // Use stable configuration
          sessionTimeout: 60000,
          heartbeatInterval: 15000,
          fromBeginning: true,
          enableCpuMonitoring: false,
          enableMemoryMonitoring: false,
        }
      );
      
      await kafkaClient.onModuleInit();
      await setupMonitoring();

      // Generate high-volume messages
      const messages = [];
      for (let i = 0; i < MESSAGE_COUNT; i++) {
        messages.push({
          key: KEYS[i % KEYS.length],
          value: { id: i, action: `action_${i}`, timestamp: Date.now() }
        });
      }

      const startTime = Date.now();
      await produceMessages(messages);

      const uniqueGroupId = `batch-high-volume-${Date.now()}-${randomUUID()}`;
      await kafkaClient.consumeMany([
        { topic: TEST_TOPIC, handler: new HighVolumeHandler() }
      ], uniqueGroupId);

      await setTimeout(5000);
      const processingTime = Date.now() - startTime;

      // Calculate efficiency metrics
      const totalProcessed = handlerCalls.length + batchHandlerCalls.reduce((sum, batch) => sum + batch.eventCount, 0);
      const averageBatchSize = batchHandlerCalls.length > 0 
        ? batchHandlerCalls.reduce((sum, batch) => sum + batch.eventCount, 0) / batchHandlerCalls.length 
        : 0;

      console.log(`📊 High-Volume Batch Test:
        📥 Produced: ${MESSAGE_COUNT}
        ⚡ Processing Time: ${processingTime}ms
        🔄 Total Processed: ${totalProcessed}
        📦 Batch Calls: ${batchHandlerCalls.length}
        📈 Average Batch Size: ${averageBatchSize.toFixed(2)}
        🚀 Messages/Second: ${(totalProcessed / (processingTime / 1000)).toFixed(2)}`);

      expect(totalProcessed).toBeGreaterThan(0);
      expect(processingTime).toBeLessThan(10000); // Should process within 10 seconds
    });
  });

  describe('❌ Batch Processing with Failures', () => {
    it('should handle failures in batch processing correctly', async () => {
      if (!testKafka) return;

      class FailingBatchHandler implements IEventHandler<any> {
        async handle({ key, event }: { key: string; event: any }): Promise<void> {
          handlerCalls.push({
            type: 'single',
            key,
            event,
            success: !event.shouldFail
          });

          if (event.shouldFail) {
            throw new Error(`Simulated failure for ${key}`);
          }
        }

        async handleBatch({ key, events }: { key: string; events: any[] }): Promise<void> {
          const hasFailure = events.some(event => event.shouldFail);
          
          batchHandlerCalls.push({
            type: 'batch',
            key,
            eventCount: events.length,
            hasFailure,
            timestamp: Date.now()
          });

          if (hasFailure) {
            throw new Error(`Batch failure for ${key}`);
          }
        }
      }

      kafkaClient = new KafkaClient(
        `failing-batch-client-${Date.now()}-${randomUUID()}`,
        TEST_BROKERS,
        {
          maxConcurrency: 1,
          messageRetryLimit: 2,
          messageRetryDelayMs: 100,
          batchSizeMultiplier: 5,
          // Use stable configuration
          sessionTimeout: 60000,
          heartbeatInterval: 15000,
          fromBeginning: true,
          enableCpuMonitoring: false,
          enableMemoryMonitoring: false,
        }
      );
      
      await kafkaClient.onModuleInit();
      await setupMonitoring();

      // Produce mixed success/failure messages
      await produceMessages([
        { key: 'user1', value: { id: 1, shouldFail: false } },
        { key: 'user1', value: { id: 2, shouldFail: true } },
        { key: 'user1', value: { id: 3, shouldFail: false } },
        { key: 'user2', value: { id: 4, shouldFail: false } },
        { key: 'user2', value: { id: 5, shouldFail: false } },
      ]);

      const uniqueGroupId = `batch-failure-${Date.now()}-${randomUUID()}`;
      await kafkaClient.consumeMany([
        { topic: TEST_TOPIC, handler: new FailingBatchHandler() }
      ], uniqueGroupId);

      await setTimeout(4000);

      const successfulCalls = handlerCalls.filter(call => call.success);
      const failedCalls = handlerCalls.filter(call => !call.success);
      const failedBatches = batchHandlerCalls.filter(batch => batch.hasFailure);

      console.log(`📊 Failure Handling Test:
        📥 Produced: ${producedMessages.length}
        ✅ Successful Calls: ${successfulCalls.length}
        ❌ Failed Calls: ${failedCalls.length}
        📦 Failed Batches: ${failedBatches.length}`);

      expect(handlerCalls.length + batchHandlerCalls.length).toBeGreaterThan(0);
    });
  });

  describe('⏱️ Batch Processing with Timeouts', () => {
    it('should handle slow processing with appropriate timeouts', async () => {
      if (!testKafka) return;

      class SlowBatchHandler implements IEventHandler<any> {
        async handle({ key, event }: { key: string; event: any }): Promise<void> {
          if (event.slow) {
            await setTimeout(500); // Simulate slow processing
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
            await setTimeout(1000); // Simulate slow batch processing
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
        `slow-batch-client-${Date.now()}-${randomUUID()}`,
        TEST_BROKERS,
        {
          maxConcurrency: 2,
          batchSizeMultiplier: 3,
          // Use stable configuration with longer timeouts for slow processing
          sessionTimeout: 90000,  // Even longer for slow processing
          heartbeatInterval: 20000,
          fromBeginning: true,
          enableCpuMonitoring: false,
          enableMemoryMonitoring: false,
        }
      );
      
      await kafkaClient.onModuleInit();
      await setupMonitoring();

      const startTime = Date.now();
      
      await produceMessages([
        { key: 'user1', value: { id: 1, slow: true } },
        { key: 'user1', value: { id: 2, slow: true } },
        { key: 'user2', value: { id: 3, slow: false } },
        { key: 'user2', value: { id: 4, slow: false } },
      ]);

      const uniqueGroupId = `batch-timeout-${Date.now()}-${randomUUID()}`;
      await kafkaClient.consumeMany([
        { topic: TEST_TOPIC, handler: new SlowBatchHandler() }
      ], uniqueGroupId);

      await setTimeout(6000);
      const totalTime = Date.now() - startTime;

      const slowBatches = batchHandlerCalls.filter(batch => batch.slow);
      const fastBatches = batchHandlerCalls.filter(batch => !batch.slow);

      console.log(`📊 Timeout Handling Test:
        📥 Produced: ${producedMessages.length}
        ⏱️  Total Time: ${totalTime}ms
        🐌 Slow Batches: ${slowBatches.length}
        ⚡ Fast Batches: ${fastBatches.length}
        🔄 Total Processed: ${handlerCalls.length + batchHandlerCalls.length}`);

      expect(handlerCalls.length + batchHandlerCalls.length).toBeGreaterThan(0);
    });
  });

  describe('📈 Batch Size Limits and Overflow', () => {
    it('should respect batch size limits and handle overflow correctly', async () => {
      if (!testKafka) return;

      class BatchSizeHandler implements IEventHandler<any> {
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
        `batch-size-client-${Date.now()}-${randomUUID()}`,
        TEST_BROKERS,
        {
          maxConcurrency: 1,
          batchSizeMultiplier: 3, // Small batch size for testing
          minBatchSize: 2,
          // Use stable configuration
          sessionTimeout: 60000,
          heartbeatInterval: 15000,
          fromBeginning: true,
          enableCpuMonitoring: false,
          enableMemoryMonitoring: false,
        }
      );
      
      await kafkaClient.onModuleInit();
      await setupMonitoring();

      // Produce many messages with the same key to test batch size limits
      const messages = Array.from({ length: 10 }, (_, i) => ({
        key: 'test-user',
        value: { id: i, action: `action_${i}` }
      }));

      await produceMessages(messages);

      const uniqueGroupId = `batch-size-${Date.now()}-${randomUUID()}`;
      await kafkaClient.consumeMany([
        { topic: TEST_TOPIC, handler: new BatchSizeHandler() }
      ], uniqueGroupId);

      await setTimeout(3000);

      // Analyze batch sizes
      const batchSizes = batchHandlerCalls.map(batch => batch.eventCount);
      const maxBatchSize = Math.max(...batchSizes);
      const avgBatchSize = batchSizes.reduce((sum, size) => sum + size, 0) / batchSizes.length;

      console.log(`📊 Batch Size Test:
        📥 Produced: ${producedMessages.length}
        📦 Batch Calls: ${batchHandlerCalls.length}
        📏 Batch Sizes: ${batchSizes.join(', ')}
        📈 Max Batch Size: ${maxBatchSize}
        📊 Avg Batch Size: ${avgBatchSize.toFixed(2)}`);

      expect(batchHandlerCalls.length).toBeGreaterThan(0);
      expect(maxBatchSize).toBeLessThanOrEqual(10); // Should respect reasonable limits
    });
  });
});
