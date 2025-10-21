import { Test, TestingModule } from '@nestjs/testing';
import { KafkaClient } from '../../src/kafka/kafka.client';

describe('KafkaClient Non-Blocking Initialization', () => {
  let kafkaClient: KafkaClient;

  beforeEach(async () => {
    // Create KafkaClient with invalid brokers to simulate connection failure
    kafkaClient = new KafkaClient(
      'test-client',
      'invalid-broker:9092', // This will fail to connect
      {
        connectionRetryDelay: 1000,
        connectionMaxRetries: 3,
        enableCpuMonitoring: false,
        enableMemoryMonitoring: false,
      }
    );
  });

  afterEach(async () => {
    if (kafkaClient) {
      await kafkaClient.shutdown();
    }
  });

  it('should not block during onModuleInit when Kafka is unavailable', async () => {
    const startTime = Date.now();
    
    // This should complete quickly without blocking
    await kafkaClient.onModuleInit();
    
    const duration = Date.now() - startTime;
    
    // Should complete in less than 1 second (not block for minutes)
    expect(duration).toBeLessThan(1000);
  });

  it('should report uninitialized status immediately after onModuleInit', async () => {
    await kafkaClient.onModuleInit();
    
    const status = kafkaClient.getInitializationStatus();
    
    expect(status.isInitialized).toBe(false);
    expect(status.error).toBeNull(); // Error might not be set immediately
  });

  it('should report unhealthy when not initialized', async () => {
    await kafkaClient.onModuleInit();
    
    const healthy = await kafkaClient.isHealthy();
    
    expect(healthy).toBe(false);
  });

  it('should throw error when trying to produce before initialization', async () => {
    await kafkaClient.onModuleInit();
    
    await expect(kafkaClient.produce('test-topic', 'test-key', { message: 'test' }))
      .rejects.toThrow('KafkaClient not initialized');
  });

  it('should throw error when trying to consume before initialization', async () => {
    await kafkaClient.onModuleInit();
    
    const mockHandler = {
      handle: jest.fn(),
    };
    
    await expect(kafkaClient.consumeMany([{ topic: 'test-topic', handler: mockHandler }], 'test-group'))
      .rejects.toThrow('KafkaClient not initialized');
  });

  it('should eventually report initialization error after failed attempts', async () => {
    await kafkaClient.onModuleInit();
    
    // Wait a bit for initialization to attempt and fail
    await new Promise(resolve => setTimeout(resolve, 2000));
    
    const status = kafkaClient.getInitializationStatus();
    
    expect(status.isInitialized).toBe(false);
    expect(status.error).toBeTruthy();
    expect(status.error).toContain('Failed to connect producer');
  });

  it('should handle successful initialization with valid brokers', async () => {
    // Skip this test if no real Kafka is available
    if (!process.env.KAFKA_BROKERS) {
      return;
    }

    const validKafkaClient = new KafkaClient(
      'test-client-valid',
      process.env.KAFKA_BROKERS,
      {
        connectionRetryDelay: 1000,
        connectionMaxRetries: 3,
        enableCpuMonitoring: false,
        enableMemoryMonitoring: false,
      }
    );

    try {
      const startTime = Date.now();
      await validKafkaClient.onModuleInit();
      const duration = Date.now() - startTime;
      
      // Should still complete quickly
      expect(duration).toBeLessThan(1000);
      
      // Wait for initialization to complete
      await new Promise(resolve => setTimeout(resolve, 3000));
      
      const status = validKafkaClient.getInitializationStatus();
      expect(status.isInitialized).toBe(true);
      expect(status.error).toBeNull();
      
      const healthy = await validKafkaClient.isHealthy();
      expect(healthy).toBe(true);
      
    } finally {
      await validKafkaClient.shutdown();
    }
  });
});


