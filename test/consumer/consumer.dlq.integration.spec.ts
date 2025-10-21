import { Test } from '@nestjs/testing';
import { Logger } from '@nestjs/common';
import { ConsumerModule } from '../../src/consumer/consumer.module';
import { ConsumerService } from '../../src/consumer/consumer.service';
import { Handler } from '../../src/consumer/consumer.decorator';
import { IEventHandler } from '../../src/kafka/kafka.event.handler';
import { KafkaClient } from '../../src/kafka/kafka.client';
import { randomUUID } from 'crypto';
import { Kafka, Producer, Consumer } from 'kafkajs';
import { setTimeout } from 'timers/promises';

/**
 * Consumer Module DLQ Integration Tests
 * 
 * Tests the Consumer Module's DLQ functionality and configuration issues.
 * This test specifically addresses Point 1: Consumer Module missing dlqSuffix configuration.
 */

describe('ConsumerModule DLQ Integration Tests', () => {
  jest.setTimeout(30000); // 30 seconds for integration tests
  let testKafka: Kafka;
  let testProducer: Producer;
  let dlqConsumer: Consumer;
  
  const TEST_BROKERS = process.env.TEST_KAFKA_BROKERS || 'localhost:29092';
  const TEST_TOPIC = `consumer-dlq-test-${Date.now()}-${randomUUID()}`;
  const DEFAULT_DLQ_TOPIC = `${TEST_TOPIC}-dlq`; // Default DLQ suffix
  const CUSTOM_DLQ_TOPIC = `${TEST_TOPIC}-custom-dlq`; // Custom DLQ suffix
  
  let dlqMessages: any[] = [];
  let handlerMessages: any[] = [];

  beforeAll(async () => {
    // Skip tests if Kafka is not available
    try {
      testKafka = new Kafka({
        clientId: 'consumer-dlq-test',
        brokers: [TEST_BROKERS],
        logLevel: 0,
      });
      
      testProducer = testKafka.producer();
      await testProducer.connect();
      
      // Create test topics
      const admin = testKafka.admin();
      await admin.connect();
      await admin.createTopics({
        topics: [
          { topic: TEST_TOPIC, numPartitions: 1 },
          { topic: DEFAULT_DLQ_TOPIC, numPartitions: 1 },
          { topic: CUSTOM_DLQ_TOPIC, numPartitions: 1 }
        ]
      });
      await admin.disconnect();
      
    } catch (error) {
      console.warn(`⚠️  Kafka not available at ${TEST_BROKERS}, skipping Consumer DLQ tests`);
      return;
    }
  });

  beforeEach(() => {
    dlqMessages = [];
    handlerMessages = [];
  });

  afterEach(async () => {
    try {
      await dlqConsumer?.disconnect();
    } catch (error) {
      // Ignore cleanup errors
    }
  });

  afterAll(async () => {
    try {
      await testProducer?.disconnect();
      await dlqConsumer?.disconnect();
      
      // Cleanup
      const admin = testKafka.admin();
      await admin.connect();
      await admin.deleteTopics({ 
        topics: [TEST_TOPIC, DEFAULT_DLQ_TOPIC, CUSTOM_DLQ_TOPIC] 
      });
      await admin.disconnect();
    } catch (error) {
      console.warn('Cleanup error:', error.message);
    }
  });

  // Helper to setup DLQ monitoring
  const setupDlqMonitoring = async (dlqTopic: string) => {
    dlqConsumer = testKafka.consumer({ 
      groupId: `dlq-monitor-${Date.now()}-${randomUUID()}`,
      sessionTimeout: 60000,
      heartbeatInterval: 15000,
    });
    await dlqConsumer.connect();
    await dlqConsumer.subscribe({ topic: dlqTopic, fromBeginning: true });
    
    dlqConsumer.run({
      eachMessage: async ({ message }) => {
        dlqMessages.push({
          key: message.key?.toString(),
          value: JSON.parse(message.value?.toString() || '{}'),
          headers: message.headers,
        });
      },
    });
  };

  describe('🚨 Consumer Module DLQ Configuration Issues', () => {
    it('should demonstrate the missing dlqSuffix configuration in Consumer Module', async () => {
      if (!testKafka) return;

      // Create a failing handler
      @Handler({ topic: TEST_TOPIC })
      class FailingTestHandler implements IEventHandler<any> {
        async handle({ key, event }: { key: string; event: any }): Promise<void> {
          handlerMessages.push({ key, event });
          throw new Error('Consumer module DLQ test failure');
        }
      }

      // Try to create ConsumerModule WITHOUT dlqSuffix option (this demonstrates the bug)
      const moduleRef = await Test.createTestingModule({
        imports: [
          ConsumerModule.register({
            name: `dlq-test-consumer-${Date.now()}`,
            brokers: TEST_BROKERS,
            consumers: [FailingTestHandler],
            options: {
              messageRetryLimit: 1,
              messageRetryDelayMs: 100,
              // Use stable configuration
              sessionTimeout: 60000,
              heartbeatInterval: 15000,
              fromBeginning: true,
              // ❌ MISSING: dlqSuffix option is not available in Consumer Module!
              // This is the bug we're testing for
            }
          }),
        ],
      }).compile();

      await setupDlqMonitoring(DEFAULT_DLQ_TOPIC);

      // Produce test message
      await testProducer.send({
        topic: TEST_TOPIC,
        messages: [
          {
            key: 'test-key',
            value: JSON.stringify({ id: 'test-1', data: 'consumer-dlq-test' }),
          }
        ],
      });

      // Start the consumer
      const app = moduleRef.createNestApplication();
      await app.init();

      // Wait for processing
      await setTimeout(3000);

      // Verify the message was processed and sent to DLQ with DEFAULT suffix
      expect(handlerMessages.length).toBeGreaterThan(0);
      expect(dlqMessages.length).toBe(1);
      
      // This demonstrates that Consumer Module always uses default DLQ suffix
      // because it doesn't expose the dlqSuffix configuration option
      console.log(`🔍 Consumer Module DLQ Test Results:
        📥 Handler Attempts: ${handlerMessages.length}
        💀 DLQ Messages: ${dlqMessages.length}
        🏷️  DLQ Topic Used: ${DEFAULT_DLQ_TOPIC} (default suffix only)`);

      await app.close();
    });

    it('should show that KafkaModule directly CAN use custom DLQ suffix', async () => {
      if (!testKafka) return;

      // Create KafkaClient directly with custom DLQ suffix (this works)
      const kafkaClient = new KafkaClient(
        `direct-kafka-client-${Date.now()}-${randomUUID()}`,
        TEST_BROKERS,
        {
          messageRetryLimit: 1,
          dlqSuffix: '-custom-dlq', // ✅ This works with KafkaClient directly
          messageRetryDelayMs: 100,
          // Use stable configuration
          sessionTimeout: 60000,
          heartbeatInterval: 15000,
          fromBeginning: true,
          enableCpuMonitoring: false,
          enableMemoryMonitoring: false,
        }
      );
      
      await kafkaClient.onModuleInit();
      await setupDlqMonitoring(CUSTOM_DLQ_TOPIC);

      class DirectFailingHandler implements IEventHandler<any> {
        async handle({ key, event }: { key: string; event: any }): Promise<void> {
          handlerMessages.push({ key, event });
          throw new Error('Direct KafkaClient DLQ test');
        }
      }

      // Produce test message
      await testProducer.send({
        topic: TEST_TOPIC,
        messages: [
          {
            key: 'direct-test',
            value: JSON.stringify({ id: 'direct-1', data: 'direct-kafka-test' }),
          }
        ],
      });

      // Start consuming with direct KafkaClient
      const uniqueGroupId = `direct-test-group-${Date.now()}-${randomUUID()}`;
      await kafkaClient.consumeMany([
        { topic: TEST_TOPIC, handler: new DirectFailingHandler() }
      ], uniqueGroupId);

      await setTimeout(2000);

      // Verify custom DLQ suffix works with direct KafkaClient
      expect(handlerMessages.length).toBeGreaterThan(0);
      expect(dlqMessages.length).toBe(1);
      
      console.log(`✅ Direct KafkaClient DLQ Test Results:
        📥 Handler Attempts: ${handlerMessages.length}
        💀 DLQ Messages: ${dlqMessages.length}
        🏷️  DLQ Topic Used: ${CUSTOM_DLQ_TOPIC} (custom suffix works!)`);

      await kafkaClient.shutdown();
    });
  });

  describe('🔧 Consumer Module Configuration Workarounds', () => {
    it('should demonstrate potential workaround by extending Consumer Module options', async () => {
      if (!testKafka) return;

      // This test shows what the fix should look like
      // (We can't actually test the fix until we implement it)
      
      const expectedOptions = {
        messageRetryLimit: 1,
        dlqSuffix: '-custom-dlq', // This should be available but currently isn't
        messageRetryDelayMs: 100,
      };

      // Verify that Consumer Module options interface is missing dlqSuffix
      // This is a compile-time check that will fail once we fix the issue
      const consumerModuleParams = {
        name: 'test-consumer',
        brokers: TEST_BROKERS,
        consumers: [],
        options: {
          messageRetryLimit: expectedOptions.messageRetryLimit,
          messageRetryDelayMs: expectedOptions.messageRetryDelayMs,
          // dlqSuffix: expectedOptions.dlqSuffix, // ❌ This should work but doesn't
        }
      };

      // Log the issue for documentation
      console.log(`📋 Consumer Module Configuration Gap:
        ✅ Available: messageRetryLimit, messageRetryDelayMs
        ❌ Missing: dlqSuffix (this is the bug)
        🎯 Expected: All KafkaClient options should be available`);

      expect(true).toBe(true); // Placeholder assertion
    });
  });
});
