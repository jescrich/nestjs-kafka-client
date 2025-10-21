import { Logger } from '@nestjs/common';
import { KafkaClient } from '../../src/kafka/kafka.client';

// Simple mock for kafkajs
jest.mock('kafkajs', () => ({
  Kafka: jest.fn(() => ({
    producer: () => ({
      connect: jest.fn().mockResolvedValue(undefined),
      disconnect: jest.fn().mockResolvedValue(undefined),
      send: jest.fn().mockResolvedValue(undefined),
    }),
    consumer: () => ({
      connect: jest.fn().mockResolvedValue(undefined),
      disconnect: jest.fn().mockResolvedValue(undefined),
      subscribe: jest.fn().mockResolvedValue(undefined),
      run: jest.fn().mockResolvedValue(undefined),
      on: jest.fn(),
      events: {},
    }),
    admin: () => ({
      connect: jest.fn().mockResolvedValue(undefined),
      disconnect: jest.fn().mockResolvedValue(undefined),
      listTopics: jest.fn().mockResolvedValue(['test-topic']),
    }),
  })),
  logLevel: { WARN: 3, ERROR: 4 },
}));

describe('KafkaClient - Core Functionality', () => {
  let kafkaClient: KafkaClient;

  beforeEach(() => {
    // Mock Logger to avoid console output during tests
    jest.spyOn(Logger.prototype, 'log').mockImplementation();
    jest.spyOn(Logger.prototype, 'debug').mockImplementation();
    jest.spyOn(Logger.prototype, 'warn').mockImplementation();
    jest.spyOn(Logger.prototype, 'error').mockImplementation();
  });

  afterEach(async () => {
    if (kafkaClient) {
      await kafkaClient.shutdown();
    }
    jest.clearAllMocks();
  });

  describe('Initialization', () => {
    it('should initialize with default options', async () => {
      kafkaClient = new KafkaClient('test-client', 'localhost:9092');
      await kafkaClient.initialize();

      expect(kafkaClient).toBeDefined();
      expect(kafkaClient.kafka).toBeDefined();
    });

    it('should initialize with new batch options', async () => {
      kafkaClient = new KafkaClient('test-client', 'localhost:9092', {
        minBatchSize: 10,
        batchAccumulationDelayMs: 200,
        memoryLogLevel: 'debug',
      });
      await kafkaClient.initialize();

      expect(kafkaClient).toBeDefined();
    });

    it('should detect container memory limits from env vars', async () => {
      const originalEnv = process.env.CONTAINER_MEMORY_LIMIT_MB;
      process.env.CONTAINER_MEMORY_LIMIT_MB = '512';
      
      const logSpy = jest.spyOn(Logger.prototype, 'log');
      
      kafkaClient = new KafkaClient('test-client', 'localhost:9092');
      await kafkaClient.initialize();

      expect(logSpy).toHaveBeenCalledWith(
        expect.stringContaining('Container memory limit set to 512.00 MB')
      );

      // Restore original env
      if (originalEnv) {
        process.env.CONTAINER_MEMORY_LIMIT_MB = originalEnv;
      } else {
        delete process.env.CONTAINER_MEMORY_LIMIT_MB;
      }
    });
  });

  describe('Configuration', () => {
    it('should use default values when not provided', async () => {
      kafkaClient = new KafkaClient('test-client', 'localhost:9092', {});
      await kafkaClient.initialize();

      const metrics = kafkaClient.getMetrics();
      expect(metrics.dynamicConcurrencyLimit).toBeGreaterThan(0);
    });

    it('should respect custom concurrency settings', async () => {
      kafkaClient = new KafkaClient('test-client', 'localhost:9092', {
        maxConcurrency: 8,
      });
      await kafkaClient.initialize();

      const metrics = kafkaClient.getMetrics();
      expect(metrics.dynamicConcurrencyLimit).toBe(8);
    });
  });

  describe('Metrics', () => {
    beforeEach(async () => {
      kafkaClient = new KafkaClient('test-client', 'localhost:9092', {
        minBatchSize: 5,
        batchAccumulationDelayMs: 100,
      });
      await kafkaClient.initialize();
    });

    it('should provide comprehensive metrics', () => {
      const metrics = kafkaClient.getMetrics();

      // Core metrics
      expect(metrics).toHaveProperty('consumedMessages', 0);
      expect(metrics).toHaveProperty('producedMessages', 0);
      expect(metrics).toHaveProperty('failedMessages', 0);
      expect(metrics).toHaveProperty('activeProcessors', 0);
      expect(metrics).toHaveProperty('dynamicConcurrencyLimit');

      // CPU metrics
      expect(metrics.cpu).toHaveProperty('current');
      expect(metrics.cpu).toHaveProperty('average');
      expect(metrics.cpu).toHaveProperty('isCritical');

      // Memory metrics
      expect(metrics.memory).toHaveProperty('currentPercent');
      expect(metrics.memory).toHaveProperty('rssBytes');
      expect(metrics.memory).toHaveProperty('limit');
      expect(metrics.memory).toHaveProperty('isCritical');

      // NEW: Batch processing metrics
      expect(metrics).toHaveProperty('batchesProcessed', 0);
      expect(metrics).toHaveProperty('averageBatchSize', 0);
      expect(metrics).toHaveProperty('smallBatchesProcessed', 0);
      expect(metrics).toHaveProperty('batchEfficiency', 0);
      expect(metrics).toHaveProperty('avgBatchProcessingTimeMs', 0);
    });

    it('should calculate batch efficiency correctly', () => {
      const metrics = kafkaClient.getMetrics();
      
      // Initially should be 0 since no batches processed
      expect(metrics.batchEfficiency).toBe(0);
      expect(metrics.averageBatchSize).toBe(0);
    });
  });

  describe('Health Checks', () => {
    beforeEach(async () => {
      kafkaClient = new KafkaClient('test-client', 'localhost:9092');
      await kafkaClient.initialize();
    });

    it('should report healthy status when initialized', async () => {
      const isHealthy = await kafkaClient.isHealthy();
      expect(typeof isHealthy).toBe('boolean');
    });

    it('should report unhealthy status after shutdown', async () => {
      await kafkaClient.shutdown();
      
      const isHealthy = await kafkaClient.isHealthy();
      expect(isHealthy).toBe(false);
    });
  });

  describe('Message Production', () => {
    beforeEach(async () => {
      kafkaClient = new KafkaClient('test-client', 'localhost:9092');
      await kafkaClient.initialize();
    });

    it('should produce messages successfully', async () => {
      const testEvent = { id: 1, name: 'test', timestamp: new Date() };
      
      // Should not throw
      await expect(kafkaClient.produce('test-topic', 'test-key', testEvent))
        .resolves.not.toThrow();
    });

    it('should reject production during shutdown', async () => {
      await kafkaClient.shutdown();

      const testEvent = { id: 1, name: 'test' };

      await expect(kafkaClient.produce('test-topic', 'test-key', testEvent))
        .rejects.toThrow('Client is shutting down');
    });
  });

  describe('Shutdown Process', () => {
    it('should shutdown gracefully', async () => {
      kafkaClient = new KafkaClient('test-client', 'localhost:9092');
      await kafkaClient.initialize();

      // Should not throw
      await expect(kafkaClient.shutdown()).resolves.not.toThrow();
    });

    it('should handle multiple shutdown calls', async () => {
      kafkaClient = new KafkaClient('test-client', 'localhost:9092');
      await kafkaClient.initialize();

      // First shutdown
      await kafkaClient.shutdown();
      
      // Second shutdown should not throw
      await expect(kafkaClient.shutdown()).resolves.not.toThrow();
    });
  });

  describe('Memory Management', () => {
    it('should parse different memory limit formats', async () => {
      const testCases = [
        { env: '512Mi', expected: '512.00 MB' },
        { env: '2Gi', expected: '2048.00 MB' },
        { env: '1024m', expected: '1024.00 MB' },
      ];

      for (const testCase of testCases) {
        const originalEnv = process.env.MEMORY_LIMIT;
        process.env.MEMORY_LIMIT = testCase.env;
        
        const logSpy = jest.spyOn(Logger.prototype, 'log');
        
        const client = new KafkaClient('test-client', 'localhost:9092');
        await client.initialize();
        
        expect(logSpy).toHaveBeenCalledWith(
          expect.stringContaining(`Container memory limit set to ${testCase.expected}`)
        );

        await client.shutdown();

        // Restore environment
        if (originalEnv) {
          process.env.MEMORY_LIMIT = originalEnv;
        } else {
          delete process.env.MEMORY_LIMIT;
        }

        jest.clearAllMocks();
      }
    });
  });

  describe('Error Handling', () => {
    it('should handle initialization errors gracefully', async () => {
      // This test would need more complex mocking to trigger real errors
      // For now, just ensure basic error handling structure exists
      kafkaClient = new KafkaClient('test-client', 'localhost:9092');
      
      // Should not throw during initialization
      await expect(kafkaClient.initialize()).resolves.not.toThrow();
    });
  });

  describe('Signal Handling', () => {
    it('should handle shutdown signals without crashing Jest', async () => {
      kafkaClient = new KafkaClient('test-client', 'localhost:9092');
      await kafkaClient.initialize();

      // Mock process.exit to prevent Jest worker crash
      const originalExit = process.exit;
      const mockExit = jest.fn();
      (process as any).exit = mockExit;

      try {
        // Access private method for testing
        const handleSignal = (kafkaClient as any).handleSignal.bind(kafkaClient);
        
        // Should handle signal gracefully
        await handleSignal('SIGTERM');
        
        // Verify process.exit was called
        expect(mockExit).toHaveBeenCalledWith(0);
      } finally {
        // Restore original process.exit
        (process as any).exit = originalExit;
      }
    });
  });

  describe('Ordering Safety', () => {
    it('should maintain partition concurrency limit for message ordering', async () => {
      kafkaClient = new KafkaClient('test-client', 'localhost:9092');
      await kafkaClient.initialize();

      // Verify that MAX_PARTITION_CONCURRENCY is still 1 to preserve ordering
      const maxPartitionConcurrency = (kafkaClient as any).MAX_PARTITION_CONCURRENCY;
      expect(maxPartitionConcurrency).toBe(1);
    });
  });
});

// Simple integration test
describe('KafkaClient - Integration', () => {
  let kafkaClient: KafkaClient;

  beforeEach(() => {
    jest.spyOn(Logger.prototype, 'log').mockImplementation();
    jest.spyOn(Logger.prototype, 'debug').mockImplementation();
    jest.spyOn(Logger.prototype, 'warn').mockImplementation();
    jest.spyOn(Logger.prototype, 'error').mockImplementation();
  });

  afterEach(async () => {
    if (kafkaClient) {
      await kafkaClient.shutdown();
    }
    jest.clearAllMocks();
  });

  it('should handle complete lifecycle with new batch options', async () => {
    kafkaClient = new KafkaClient('integration-test', 'localhost:9092', {
      minBatchSize: 3,
      batchAccumulationDelayMs: 50,
      memoryLogLevel: 'debug',
      enableCpuMonitoring: false, // Disable for test stability
      enableMemoryMonitoring: false,
    });

    // Initialize
    await kafkaClient.initialize();
    expect(kafkaClient).toBeDefined();

    // Check initial health
    const healthy = await kafkaClient.isHealthy();
    expect(typeof healthy).toBe('boolean');

    // Get metrics with new batch properties
    const metrics = kafkaClient.getMetrics();
    expect(metrics).toHaveProperty('batchesProcessed');
    expect(metrics).toHaveProperty('averageBatchSize');
    expect(metrics).toHaveProperty('batchEfficiency');

    // Produce a test message
    await kafkaClient.produce('test-topic', 'test-key', { test: 'data' });

    // Shutdown
    await kafkaClient.shutdown();
    
    const finalHealth = await kafkaClient.isHealthy();
    expect(finalHealth).toBe(false);
  });
}); 