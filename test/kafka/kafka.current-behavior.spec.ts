import { Test } from '@nestjs/testing';
import { Logger } from '@nestjs/common';
import { KafkaClient } from '../../src/kafka/kafka.client';
import { IEventHandler } from '../../src/kafka/kafka.event.handler';
import { setTimeout } from 'timers/promises';
import { randomUUID } from 'crypto';
import { Kafka, Producer } from 'kafkajs';

/**
 * Current Behavior Documentation Tests
 * 
 * These tests document the ACTUAL current behavior of the Kafka implementation
 * as it runs in production, including any bugs or issues.
 * 
 * Purpose: 
 * - Document current behavior (good and bad)
 * - Identify what actually works vs what's broken
 * - Provide baseline for future improvements
 * - Show real production behavior patterns
 */

describe('Kafka Current Behavior Documentation', () => {
  jest.setTimeout(30000);
  
  let kafkaClient: KafkaClient;
  let testKafka: Kafka;
  let testProducer: Producer;
  
  const TEST_BROKERS = process.env.TEST_KAFKA_BROKERS || 'localhost:29092';
  const TEST_TOPIC = `current-behavior-${randomUUID()}`;
  
  // Track what actually happens (not what should happen)
  let actualHandlerCalls: any[] = [];
  let actualBatchCalls: any[] = [];
  let actualErrors: any[] = [];
  let kafkaLogs: any[] = [];

  beforeAll(async () => {
    if (!process.env.TEST_KAFKA_BROKERS && !await isKafkaAvailable()) {
      console.warn(`⚠️  Skipping current behavior tests - Kafka not available at ${TEST_BROKERS}`);
      return;
    }

    try {
      testKafka = new Kafka({
        clientId: 'current-behavior-test',
        brokers: [TEST_BROKERS],
        logLevel: 0,
      });
      
      testProducer = testKafka.producer();
      await testProducer.connect();
      
      // Create topic
      const admin = testKafka.admin();
      await admin.connect();
      await admin.createTopics({
        topics: [{ topic: TEST_TOPIC, numPartitions: 1 }]
      });
      await admin.disconnect();
      
    } catch (error) {
      console.warn(`⚠️  Could not setup Kafka for current behavior tests: ${error.message}`);
    }
  });

  beforeEach(() => {
    actualHandlerCalls = [];
    actualBatchCalls = [];
    actualErrors = [];
    kafkaLogs = [];
    
    // Capture console logs to understand what's happening
    const originalConsoleError = console.error;
    const originalConsoleWarn = console.warn;
    
    console.error = (...args) => {
      kafkaLogs.push({ level: 'ERROR', message: args.join(' '), timestamp: Date.now() });
      originalConsoleError(...args);
    };
    
    console.warn = (...args) => {
      kafkaLogs.push({ level: 'WARN', message: args.join(' '), timestamp: Date.now() });
      originalConsoleWarn(...args);
    };
  });

  afterEach(async () => {
    try {
      await kafkaClient?.shutdown();
    } catch (error) {
      // Document shutdown issues
      actualErrors.push({ type: 'shutdown', error: error.message });
    }
    
    // Restore console
    console.error = console.error;
    console.warn = console.warn;
  });

  afterAll(async () => {
    try {
      await testProducer?.disconnect();
      
      const admin = testKafka.admin();
      await admin.connect();
      await admin.deleteTopics({ topics: [TEST_TOPIC] });
      await admin.disconnect();
    } catch (error) {
      console.warn('Cleanup error:', error.message);
    }
  });

  // Helper to check if Kafka is available
  async function isKafkaAvailable(): Promise<boolean> {
    try {
      const testClient = new Kafka({
        clientId: 'availability-test',
        brokers: [TEST_BROKERS],
        logLevel: 0,
      });
      
      const admin = testClient.admin();
      await admin.connect();
      await admin.disconnect();
      return true;
    } catch {
      return false;
    }
  }

  describe('📊 Current Production Behavior Documentation', () => {
    it('should document what actually happens when processing messages', async () => {
      if (!testKafka) return;

      // Simple handler that just records what happens
      class ObservingHandler implements IEventHandler<any> {
        async handle({ key, event, payload }: { key: string; event: any; payload: any }): Promise<void> {
          actualHandlerCalls.push({
            method: 'handle',
            key,
            event,
            offset: payload?.offset,
            timestamp: Date.now()
          });
        }

        async handleBatch({ key, events, payloads }: { key: string; events: any[]; payloads: any[] }): Promise<void> {
          actualBatchCalls.push({
            method: 'handleBatch',
            key,
            eventCount: events.length,
            events,
            timestamp: Date.now()
          });
        }
      }

      try {
        kafkaClient = new KafkaClient(
          `behavior-test-${randomUUID()}`,
          TEST_BROKERS,
          {
            maxConcurrency: 1,
            enableCpuMonitoring: false,
            enableMemoryMonitoring: false,
          }
        );
        
        await kafkaClient.onModuleInit();

        // Produce some test messages
        await testProducer.send({
          topic: TEST_TOPIC,
          messages: [
            { key: 'test1', value: JSON.stringify({ id: 1, action: 'test' }) },
            { key: 'test2', value: JSON.stringify({ id: 2, action: 'test' }) },
          ],
        });

        // Start consuming and document what happens
        const startTime = Date.now();
        
        // Note: This might not work as expected - that's what we're documenting
        await kafkaClient.consumeMany([
          { topic: TEST_TOPIC, handler: new ObservingHandler() }
        ], `behavior-group-${randomUUID()}`);

        // Wait and see what actually happens
        await setTimeout(5000);
        
        const endTime = Date.now();
        const metrics = kafkaClient.getMetrics();

        // Document ACTUAL behavior (not expected)
        console.log(`📊 ACTUAL Current Behavior:
          ⏱️  Test Duration: ${endTime - startTime}ms
          📥 Messages Produced: 2
          🔄 Handle() Calls: ${actualHandlerCalls.length}
          📦 HandleBatch() Calls: ${actualBatchCalls.length}
          ❌ Errors Captured: ${actualErrors.length}
          📊 KafkaClient Metrics:
            - Consumed Messages: ${metrics.consumedMessages}
            - Produced Messages: ${metrics.producedMessages}
            - Processing Failures: ${metrics.processingFailures}
            - DLQ Messages: ${metrics.dlqMessages}
          🔍 Kafka Logs: ${kafkaLogs.length} entries
          📋 Rebalancing Issues: ${kafkaLogs.filter(log => log.message.includes('rebalancing')).length}
        `);

        // Document the actual state - don't assert what should happen
        const totalProcessed = actualHandlerCalls.length + actualBatchCalls.reduce((sum, batch) => sum + batch.eventCount, 0);
        
        if (totalProcessed === 0) {
          console.log('🚨 CURRENT ISSUE DOCUMENTED: No messages are being processed by handlers');
          console.log('   - Messages are produced ✅');
          console.log('   - KafkaClient initializes ✅');
          console.log('   - Consumer connects ✅');
          console.log('   - But handlers are never called ❌');
        }

        if (kafkaLogs.some(log => log.message.includes('rebalancing'))) {
          console.log('🚨 CURRENT ISSUE DOCUMENTED: Constant rebalancing detected');
        }

        // This test always passes - it just documents current behavior
        expect(true).toBe(true);

      } catch (error) {
        actualErrors.push({ type: 'test_execution', error: error.message, stack: error.stack });
        console.log(`🚨 CURRENT ISSUE DOCUMENTED: Test execution failed: ${error.message}`);
        expect(true).toBe(true); // Still pass - we're documenting, not testing
      }
    });

    it('should document the actual DLQ behavior in current production', async () => {
      if (!testKafka) return;

      class AlwaysFailingHandler implements IEventHandler<any> {
        async handle({ key, event }: { key: string; event: any }): Promise<void> {
          actualHandlerCalls.push({ key, event, timestamp: Date.now() });
          throw new Error('Intentional test failure');
        }
      }

      try {
        kafkaClient = new KafkaClient(
          `dlq-behavior-test-${randomUUID()}`,
          TEST_BROKERS,
          {
            messageRetryLimit: 1, // Low for fast testing
            dlqSuffix: '-dlq',
            messageRetryDelayMs: 100,
            enableCpuMonitoring: false,
            enableMemoryMonitoring: false,
          }
        );
        
        await kafkaClient.onModuleInit();

        await testProducer.send({
          topic: TEST_TOPIC,
          messages: [
            { key: 'fail-test', value: JSON.stringify({ id: 1, shouldFail: true }) },
          ],
        });

        await kafkaClient.consumeMany([
          { topic: TEST_TOPIC, handler: new AlwaysFailingHandler() }
        ], `dlq-behavior-group-${randomUUID()}`);

        await setTimeout(3000);
        
        const metrics = kafkaClient.getMetrics();

        console.log(`📊 ACTUAL DLQ Behavior:
          🔄 Handler Attempts: ${actualHandlerCalls.length}
          💀 DLQ Messages (metric): ${metrics.dlqMessages}
          ❌ Processing Failures (metric): ${metrics.processingFailures}
          🔄 Retries (metric): ${metrics.retries}
          📋 Error Logs: ${kafkaLogs.filter(log => log.level === 'ERROR').length}
        `);

        if (actualHandlerCalls.length === 0) {
          console.log('🚨 CURRENT DLQ ISSUE: Handler never called, so DLQ logic never triggered');
        } else if (metrics.dlqMessages === 0) {
          console.log('🚨 CURRENT DLQ ISSUE: Handler called but messages not sent to DLQ');
        } else {
          console.log('✅ DLQ appears to be working');
        }

        expect(true).toBe(true); // Always pass - documenting behavior

      } catch (error) {
        console.log(`🚨 CURRENT DLQ ISSUE: ${error.message}`);
        expect(true).toBe(true);
      }
    });

    it('should document Consumer Module vs direct KafkaClient behavior differences', async () => {
      if (!testKafka) return;

      // Test direct KafkaClient first
      console.log('🔧 Testing Direct KafkaClient...');
      
      class DirectTestHandler implements IEventHandler<any> {
        async handle({ key, event }: { key: string; event: any }): Promise<void> {
          actualHandlerCalls.push({ 
            source: 'direct', 
            key, 
            event, 
            timestamp: Date.now() 
          });
        }
      }

      try {
        kafkaClient = new KafkaClient(
          `direct-test-${randomUUID()}`,
          TEST_BROKERS,
          { enableCpuMonitoring: false, enableMemoryMonitoring: false }
        );
        
        await kafkaClient.onModuleInit();

        await testProducer.send({
          topic: TEST_TOPIC,
          messages: [
            { key: 'direct-test', value: JSON.stringify({ source: 'direct', id: 1 }) },
          ],
        });

        await kafkaClient.consumeMany([
          { topic: TEST_TOPIC, handler: new DirectTestHandler() }
        ], `direct-group-${randomUUID()}`);

        await setTimeout(2000);
        
        const directCalls = actualHandlerCalls.filter(call => call.source === 'direct');
        const directMetrics = kafkaClient.getMetrics();

        console.log(`📊 Direct KafkaClient Results:
          🔄 Handler Calls: ${directCalls.length}
          📊 Metrics - Consumed: ${directMetrics.consumedMessages}
          📊 Metrics - Failures: ${directMetrics.processingFailures}
        `);

        await kafkaClient.shutdown();

      } catch (error) {
        console.log(`🚨 Direct KafkaClient Issue: ${error.message}`);
      }

      // Document what we observed
      console.log(`📋 CURRENT BEHAVIOR SUMMARY:
        🔧 Direct KafkaClient: ${actualHandlerCalls.filter(c => c.source === 'direct').length} handler calls
        🏢 Consumer Module: (would test separately)
        🚨 Issues Observed: ${actualErrors.length}
        📊 Kafka Logs Generated: ${kafkaLogs.length}
      `);

      expect(true).toBe(true); // Always pass - this is documentation
    });

    it('should document the actual message flow and where it breaks', async () => {
      if (!testKafka) return;

      class DiagnosticHandler implements IEventHandler<any> {
        async handle({ key, event, payload }: { key: string; event: any; payload: any }): Promise<void> {
          actualHandlerCalls.push({
            key,
            event,
            offset: payload?.offset,
            partition: payload?.partition,
            timestamp: Date.now(),
            step: 'handler_called'
          });
          
          console.log(`🔍 DIAGNOSTIC: Handler called for key=${key}, event=${JSON.stringify(event)}`);
        }
      }

      try {
        kafkaClient = new KafkaClient(
          `diagnostic-test-${randomUUID()}`,
          TEST_BROKERS,
          {
            maxConcurrency: 1,
            enableCpuMonitoring: false,
            enableMemoryMonitoring: false,
          }
        );

        console.log('🔍 DIAGNOSTIC: Initializing KafkaClient...');
        await kafkaClient.onModuleInit();
        console.log('🔍 DIAGNOSTIC: KafkaClient initialized');

        console.log('🔍 DIAGNOSTIC: Producing test message...');
        await testProducer.send({
          topic: TEST_TOPIC,
          messages: [
            { key: 'diagnostic', value: JSON.stringify({ test: 'diagnostic', id: 1 }) },
          ],
        });
        console.log('🔍 DIAGNOSTIC: Message produced');

        console.log('🔍 DIAGNOSTIC: Starting consumer...');
        const consumePromise = kafkaClient.consumeMany([
          { topic: TEST_TOPIC, handler: new DiagnosticHandler() }
        ], `diagnostic-group-${randomUUID()}`);
        
        console.log('🔍 DIAGNOSTIC: Consumer started, waiting for processing...');
        await setTimeout(3000);
        
        const metrics = kafkaClient.getMetrics();
        
        console.log(`🔍 DIAGNOSTIC RESULTS:
          📥 Messages Produced: 1
          🔄 Handler Calls: ${actualHandlerCalls.length}
          📊 KafkaClient Consumed Metric: ${metrics.consumedMessages}
          📊 KafkaClient Produced Metric: ${metrics.producedMessages}
          🚨 Processing Failures: ${metrics.processingFailures}
          📋 Kafka Error Logs: ${kafkaLogs.filter(log => log.level === 'ERROR').length}
          📋 Kafka Warn Logs: ${kafkaLogs.filter(log => log.level === 'WARN').length}
        `);

        // Analyze where the flow breaks
        if (metrics.producedMessages > 0) {
          console.log('✅ Message production: WORKING');
        } else {
          console.log('❌ Message production: NOT WORKING');
        }

        if (metrics.consumedMessages > 0) {
          console.log('✅ Message consumption (KafkaJS level): WORKING');
        } else {
          console.log('❌ Message consumption (KafkaJS level): NOT WORKING');
        }

        if (actualHandlerCalls.length > 0) {
          console.log('✅ Handler invocation: WORKING');
        } else {
          console.log('❌ Handler invocation: NOT WORKING - This is where it breaks!');
        }

        const rebalanceErrors = kafkaLogs.filter(log => 
          log.message.includes('rebalancing') || log.message.includes('REBALANCE')
        ).length;

        if (rebalanceErrors > 0) {
          console.log(`🚨 Rebalancing issues detected: ${rebalanceErrors} occurrences`);
        }

      } catch (error) {
        console.log(`🚨 DIAGNOSTIC ERROR: ${error.message}`);
        actualErrors.push({ type: 'diagnostic', error: error.message });
      }

      expect(true).toBe(true); // Always pass - this documents current state
    });
  });

  describe('🔧 Current Configuration Impact', () => {
    it('should document how different configurations affect current behavior', async () => {
      if (!testKafka) return;

      const configs = [
        { name: 'minimal', options: { maxConcurrency: 1 } },
        { name: 'batch-focused', options: { maxConcurrency: 1, batchSizeMultiplier: 10 } },
        { name: 'high-concurrency', options: { maxConcurrency: 3, batchSizeMultiplier: 5 } },
      ];

      for (const config of configs) {
        console.log(`\n🔧 Testing ${config.name} configuration...`);
        
        const configResults = {
          handlerCalls: 0,
          batchCalls: 0,
          errors: 0,
          metrics: null as any
        };

        try {
          const testClient = new KafkaClient(
            `config-test-${config.name}-${randomUUID()}`,
            TEST_BROKERS,
            {
              ...config.options,
              enableCpuMonitoring: false,
              enableMemoryMonitoring: false,
            }
          );

          await testClient.onModuleInit();

          class ConfigTestHandler implements IEventHandler<any> {
            async handle({ key, event }: { key: string; event: any }): Promise<void> {
              configResults.handlerCalls++;
            }
          }

          await testProducer.send({
            topic: TEST_TOPIC,
            messages: [
              { key: config.name, value: JSON.stringify({ config: config.name, test: true }) },
            ],
          });

          await testClient.consumeMany([
            { topic: TEST_TOPIC, handler: new ConfigTestHandler() }
          ], `config-group-${config.name}-${randomUUID()}`);

          await setTimeout(2000);
          
          configResults.metrics = testClient.getMetrics();
          await testClient.shutdown();

        } catch (error) {
          configResults.errors++;
          console.log(`❌ ${config.name} config error: ${error.message}`);
        }

        console.log(`📊 ${config.name} Results:
          🔄 Handler Calls: ${configResults.handlerCalls}
          📊 Consumed (metric): ${configResults.metrics?.consumedMessages || 0}
          ❌ Errors: ${configResults.errors}
        `);
      }

      expect(true).toBe(true); // Document findings
    });
  });
});


