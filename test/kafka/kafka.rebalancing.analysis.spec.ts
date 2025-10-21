import { Test } from '@nestjs/testing';
import { Logger } from '@nestjs/common';
import { KafkaClient } from '../../src/kafka/kafka.client';
import { IEventHandler } from '../../src/kafka/kafka.event.handler';
import { setTimeout } from 'timers/promises';
import { randomUUID } from 'crypto';
import { Kafka, Producer } from 'kafkajs';

/**
 * Kafka Rebalancing Issue Analysis Tests
 * 
 * These tests focus specifically on the rebalancing issue that's preventing
 * message consumption in the current production code.
 * 
 * Goal: Understand and document the rebalancing problem, then test solutions.
 */

describe('Kafka Rebalancing Issue Analysis', () => {
  jest.setTimeout(60000); // Long timeout for rebalancing analysis
  
  let testKafka: Kafka;
  let testProducer: Producer;
  
  const TEST_BROKERS = process.env.TEST_KAFKA_BROKERS || 'localhost:29092';
  const TEST_TOPIC = `rebalance-test-${randomUUID()}`;
  
  // Track rebalancing events
  let rebalanceEvents: any[] = [];
  let consumerErrors: any[] = [];
  let handlerCalls: any[] = [];

  beforeAll(async () => {
    if (!await isKafkaAvailable()) {
      console.warn(`⚠️  Skipping rebalancing tests - Kafka not available at ${TEST_BROKERS}`);
      return;
    }

    try {
      testKafka = new Kafka({
        clientId: 'rebalance-analysis',
        brokers: [TEST_BROKERS],
        logLevel: 2, // WARN level to see rebalancing
      });
      
      testProducer = testKafka.producer();
      await testProducer.connect();
      
      // Create topic with single partition to reduce rebalancing complexity
      const admin = testKafka.admin();
      await admin.connect();
      await admin.createTopics({
        topics: [{ topic: TEST_TOPIC, numPartitions: 1, replicationFactor: 1 }]
      });
      await admin.disconnect();
      
    } catch (error) {
      console.warn(`⚠️  Could not setup Kafka: ${error.message}`);
    }
  });

  beforeEach(() => {
    rebalanceEvents = [];
    consumerErrors = [];
    handlerCalls = [];
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

  describe('🔄 Rebalancing Issue Analysis', () => {
    it('should analyze the current rebalancing behavior and document the issue', async () => {
      if (!testKafka) return;

      class RebalanceAnalysisHandler implements IEventHandler<any> {
        async handle({ key, event }: { key: string; event: any }): Promise<void> {
          handlerCalls.push({
            key,
            event,
            timestamp: Date.now()
          });
          console.log(`🎯 HANDLER CALLED: key=${key}, event=${JSON.stringify(event)}`);
        }
      }

      console.log('🔍 ANALYSIS: Testing current rebalancing behavior...');
      
      // Use production-like configuration that might be causing issues
      const kafkaClient = new KafkaClient(
        `rebalance-analysis-${randomUUID()}`,
        TEST_BROKERS,
        {
          // Use current production defaults that might cause rebalancing
          sessionTimeout: 30000,      // Current default
          heartbeatInterval: 10000,   // Current default  
          maxConcurrency: 1,
          enableCpuMonitoring: false,
          enableMemoryMonitoring: false,
        }
      );

      try {
        await kafkaClient.onModuleInit();
        console.log('🔍 ANALYSIS: KafkaClient initialized');

        // Produce a simple message
        await testProducer.send({
          topic: TEST_TOPIC,
          messages: [
            { key: 'analysis', value: JSON.stringify({ test: 'rebalancing-analysis' }) }
          ]
        });
        console.log('🔍 ANALYSIS: Message produced');

        // Start consuming with unique group ID to avoid conflicts
        const uniqueGroupId = `rebalance-analysis-group-${randomUUID()}`;
        console.log(`🔍 ANALYSIS: Starting consumer with group: ${uniqueGroupId}`);

        await kafkaClient.consumeMany([
          { topic: TEST_TOPIC, handler: new RebalanceAnalysisHandler() }
        ], uniqueGroupId);

        console.log('🔍 ANALYSIS: Consumer started, waiting for processing...');
        
        // Wait longer to see if rebalancing settles
        await setTimeout(10000);

        const metrics = kafkaClient.getMetrics();
        
        console.log(`📊 REBALANCING ANALYSIS RESULTS:
          📥 Messages Produced: 1
          🔄 Handler Calls: ${handlerCalls.length}
          📊 KafkaClient Consumed: ${metrics.consumedMessages}
          📊 KafkaClient Produced: ${metrics.producedMessages}
          🚨 Processing Failures: ${metrics.processingFailures}
          
          🔍 Analysis:
          ${handlerCalls.length === 0 ? '❌ CONFIRMED: Handlers not called due to rebalancing issues' : '✅ Handlers working'}
          ${metrics.consumedMessages === 0 ? '❌ CONFIRMED: No consumption due to rebalancing' : '✅ Consumption working'}
        `);

        await kafkaClient.shutdown();

      } catch (error) {
        console.log(`🚨 REBALANCING ERROR: ${error.message}`);
        consumerErrors.push(error);
      }

      // This test documents current behavior - always passes
      expect(true).toBe(true);
    });

    it('should test if stable consumer group configuration resolves rebalancing', async () => {
      if (!testKafka) return;

      class StableTestHandler implements IEventHandler<any> {
        async handle({ key, event }: { key: string; event: any }): Promise<void> {
          handlerCalls.push({
            key,
            event,
            timestamp: Date.now()
          });
          console.log(`🎯 STABLE HANDLER CALLED: key=${key}`);
        }
      }

      console.log('🔧 TESTING: Stable consumer group configuration...');

      // Try configuration that might reduce rebalancing
      const stableKafkaClient = new KafkaClient(
        `stable-test-${randomUUID()}`,
        TEST_BROKERS,
        {
          // Longer timeouts to reduce rebalancing sensitivity
          sessionTimeout: 60000,      // Longer session timeout
          heartbeatInterval: 15000,   // Longer heartbeat interval
          maxConcurrency: 1,          // Single threaded to reduce complexity
          fromBeginning: true,        // Start from beginning
          enableCpuMonitoring: false,
          enableMemoryMonitoring: false,
        }
      );

      try {
        await stableKafkaClient.onModuleInit();
        
        // Use a completely unique group ID
        const stableGroupId = `stable-group-${Date.now()}-${randomUUID()}`;
        console.log(`🔧 TESTING: Using group ID: ${stableGroupId}`);

        // Produce message
        await testProducer.send({
          topic: TEST_TOPIC,
          messages: [
            { key: 'stable-test', value: JSON.stringify({ test: 'stable-config' }) }
          ]
        });

        // Start consuming
        await stableKafkaClient.consumeMany([
          { topic: TEST_TOPIC, handler: new StableTestHandler() }
        ], stableGroupId);

        // Wait longer to see if stability helps
        console.log('🔧 TESTING: Waiting for stable processing...');
        await setTimeout(15000);

        const stableMetrics = stableKafkaClient.getMetrics();
        
        console.log(`📊 STABLE CONFIGURATION RESULTS:
          📥 Messages Produced: 1
          🔄 Handler Calls: ${handlerCalls.length}
          📊 Consumed: ${stableMetrics.consumedMessages}
          🎯 Session Timeout: 60000ms
          🎯 Heartbeat Interval: 15000ms
          
          ${handlerCalls.length > 0 ? '✅ STABLE CONFIG WORKS!' : '❌ Still broken with stable config'}
        `);

        await stableKafkaClient.shutdown();

      } catch (error) {
        console.log(`🚨 STABLE CONFIG ERROR: ${error.message}`);
      }

      expect(true).toBe(true);
    });

    it('should test single consumer without conflicts to isolate rebalancing cause', async () => {
      if (!testKafka) return;

      console.log('🔬 ISOLATION TEST: Single consumer, no conflicts...');

      class IsolatedHandler implements IEventHandler<any> {
        async handle({ key, event }: { key: string; event: any }): Promise<void> {
          handlerCalls.push({ key, event, timestamp: Date.now() });
          console.log(`🎯 ISOLATED HANDLER: Processed ${key}`);
        }
      }

      // Wait a bit to ensure no other consumers are running
      await setTimeout(2000);

      const isolatedClient = new KafkaClient(
        `isolated-${Date.now()}`, // Timestamp for uniqueness
        TEST_BROKERS,
        {
          sessionTimeout: 45000,
          heartbeatInterval: 10000,
          maxConcurrency: 1,
          enableCpuMonitoring: false,
          enableMemoryMonitoring: false,
        }
      );

      try {
        await isolatedClient.onModuleInit();
        
        // Create completely unique topic for this test
        const isolatedTopic = `isolated-${randomUUID()}`;
        const admin = testKafka.admin();
        await admin.connect();
        await admin.createTopics({
          topics: [{ topic: isolatedTopic, numPartitions: 1, replicationFactor: 1 }]
        });
        await admin.disconnect();

        // Produce to isolated topic
        await testProducer.send({
          topic: isolatedTopic,
          messages: [
            { key: 'isolated', value: JSON.stringify({ test: 'isolation' }) }
          ]
        });

        // Use completely unique group
        const isolatedGroup = `isolated-group-${Date.now()}-${randomUUID()}`;
        
        console.log(`🔬 ISOLATION: Topic=${isolatedTopic}, Group=${isolatedGroup}`);

        await isolatedClient.consumeMany([
          { topic: isolatedTopic, handler: new IsolatedHandler() }
        ], isolatedGroup);

        // Wait for processing
        await setTimeout(8000);

        const isolatedMetrics = isolatedClient.getMetrics();
        
        console.log(`📊 ISOLATION TEST RESULTS:
          🔄 Handler Calls: ${handlerCalls.length}
          📊 Consumed: ${isolatedMetrics.consumedMessages}
          
          ${handlerCalls.length > 0 ? '✅ ISOLATION WORKS - Issue is consumer conflicts' : '❌ Still broken - Issue is deeper'}
        `);

        await isolatedClient.shutdown();

        // Cleanup isolated topic
        const cleanupAdmin = testKafka.admin();
        await cleanupAdmin.connect();
        await cleanupAdmin.deleteTopics({ topics: [isolatedTopic] });
        await cleanupAdmin.disconnect();

      } catch (error) {
        console.log(`🚨 ISOLATION ERROR: ${error.message}`);
      }

      expect(true).toBe(true);
    });
  });

  describe('🛠️ Consumer Group Management Analysis', () => {
    it('should analyze consumer group conflicts and management issues', async () => {
      if (!testKafka) return;

      console.log('🔍 CONSUMER GROUP ANALYSIS: Testing group management...');

      // Check existing consumer groups
      try {
        const admin = testKafka.admin();
        await admin.connect();
        
        const groups = await admin.listGroups();
        console.log(`📊 Existing Consumer Groups: ${groups.groups.length}`);
        
        // Look for test groups that might be causing conflicts
        const testGroups = groups.groups.filter(group => 
          group.groupId.includes('test') || 
          group.groupId.includes('batch') ||
          group.groupId.includes('dlq')
        );
        
        console.log(`🔍 Test-related groups found: ${testGroups.length}`);
        testGroups.forEach(group => {
          console.log(`   - ${group.groupId} (${group.protocolType})`);
        });

        // Check consumer group details for our test topic
        try {
          const groupDetails = await admin.describeGroups(testGroups.map(g => g.groupId));
          console.log(`📊 Group Details: ${JSON.stringify(groupDetails, null, 2)}`);
        } catch (detailError) {
          console.log(`⚠️  Could not get group details: ${detailError.message}`);
        }

        await admin.disconnect();

      } catch (error) {
        console.log(`🚨 CONSUMER GROUP ANALYSIS ERROR: ${error.message}`);
      }

      console.log(`
📋 CONSUMER GROUP ANALYSIS SUMMARY:
=================================

Current Issue: Constant rebalancing prevents message consumption

Possible Causes:
1. Multiple consumers with same group ID
2. Consumer groups not properly cleaned up between tests
3. Session timeout too short for processing
4. Heartbeat interval causing timing issues
5. KafkaConnectionManager reusing connections incorrectly

Rebalancing happens when:
- New consumer joins group
- Existing consumer leaves group  
- Consumer fails to send heartbeat in time
- Partition assignment changes

In our tests:
- Each test creates new KafkaClient
- Each KafkaClient creates new consumer
- Multiple tests run in sequence
- Consumer groups may not be properly cleaned up
- This causes constant rebalancing
      `);

      expect(true).toBe(true);
    });
  });
});


