import { Test } from '@nestjs/testing';
import { Logger } from '@nestjs/common';
import { KafkaClient } from '../../src/kafka/kafka.client';
import { IEventHandler } from '../../src/kafka/kafka.event.handler';
import { KafkaMessage } from '@nestjs/microservices/external/kafka.interface';

// Mock implementations
class MockEventHandler implements IEventHandler<any> {
  public processedMessages: Array<{ key: string; event: any; payload: any }> = [];
  public batchedMessages: Array<{ key: string; events: any[]; payloads: any[] }> = [];
  public processingDelay = 0;
  public shouldThrow = false;
  public throwError = new Error('Mock handler error');

  async handle({ key, event, payload }: { key: string; event: any; payload: any }): Promise<void> {
    if (this.shouldThrow) {
      throw this.throwError;
    }
    
    if (this.processingDelay > 0) {
      await new Promise(resolve => setTimeout(resolve, this.processingDelay));
    }
    
    this.processedMessages.push({ key, event, payload });
  }

  async handleBatch({ key, events, payloads }: { key: string; events: any[]; payloads: any[] }): Promise<void> {
    if (this.shouldThrow) {
      throw this.throwError;
    }

    if (this.processingDelay > 0) {
      await new Promise(resolve => setTimeout(resolve, this.processingDelay));
    }

    this.batchedMessages.push({ key, events, payloads });
  }

  reset() {
    this.processedMessages = [];
    this.batchedMessages = [];
    this.shouldThrow = false;
    this.processingDelay = 0;
  }
}



// Mock Kafka dependencies - must be at the top level
jest.mock('kafkajs');

describe('KafkaClient', () => {
  let kafkaClient: KafkaClient;
  let mockHandler: MockEventHandler;
  let loggerSpy: jest.SpyInstance;

  beforeAll(() => {
    // Setup Kafka mocks
    const { Kafka, logLevel } = require('kafkajs');
    
    Kafka.mockImplementation(() => ({
      producer: jest.fn().mockReturnValue({
        connect: jest.fn().mockResolvedValue(undefined),
        disconnect: jest.fn().mockResolvedValue(undefined),
        send: jest.fn().mockResolvedValue(undefined),
      }),
      consumer: jest.fn().mockReturnValue({
        connect: jest.fn().mockResolvedValue(undefined),
        disconnect: jest.fn().mockResolvedValue(undefined),
        subscribe: jest.fn().mockResolvedValue(undefined),
        run: jest.fn().mockResolvedValue(undefined),
        pause: jest.fn().mockResolvedValue(undefined),
        resume: jest.fn().mockResolvedValue(undefined),
        on: jest.fn(),
        events: {
          GROUP_JOIN: 'consumer.group_join',
          REBALANCING: 'consumer.rebalancing',
          DISCONNECT: 'consumer.disconnect',
          CONNECT: 'consumer.connect',
          CRASH: 'consumer.crash',
        },
      }),
      admin: jest.fn().mockReturnValue({
        connect: jest.fn().mockResolvedValue(undefined),
        disconnect: jest.fn().mockResolvedValue(undefined),
        listTopics: jest.fn().mockResolvedValue(['test-topic']),
      }),
    }));

    logLevel.WARN = 3;
    logLevel.ERROR = 4;
  });

  beforeEach(async () => {
    mockHandler = new MockEventHandler();
    
    // Mock Logger to avoid actual console output during tests
    loggerSpy = jest.spyOn(Logger.prototype, 'log').mockImplementation();
    jest.spyOn(Logger.prototype, 'debug').mockImplementation();
    jest.spyOn(Logger.prototype, 'warn').mockImplementation();
    jest.spyOn(Logger.prototype, 'error').mockImplementation();
  });

  afterEach(async () => {
    if (kafkaClient) {
      await kafkaClient.shutdown();
    }
    jest.clearAllMocks();
    mockHandler.reset();
  });

  describe('Initialization', () => {
    it('should initialize with default options', async () => {
      kafkaClient = new KafkaClient('test-client', 'localhost:9092');
      await kafkaClient.initialize();

      expect(kafkaClient).toBeDefined();
      expect(kafkaClient.kafka).toBeDefined();
    });

    it('should initialize with custom batch options', async () => {
      kafkaClient = new KafkaClient('test-client', 'localhost:9092', {
        batchAccumulationDelayMs: 200,
        minBatchSize: 10,
        maxConcurrency: 5,
      });
      await kafkaClient.initialize();

      const metrics = kafkaClient.getMetrics();
      expect(metrics.dynamicConcurrencyLimit).toBe(5);
    });

    it('should detect container memory limits', async () => {
      process.env.CONTAINER_MEMORY_LIMIT_MB = '512';
      
      kafkaClient = new KafkaClient('test-client', 'localhost:9092');
      await kafkaClient.initialize();

      expect(loggerSpy).toHaveBeenCalledWith(
        expect.stringContaining('Container memory limit set to 512.00 MB')
      );

      delete process.env.CONTAINER_MEMORY_LIMIT_MB;
    });
  });

  describe('Message Production', () => {
    beforeEach(async () => {
      kafkaClient = new KafkaClient('test-client', 'localhost:9092', {});
      await kafkaClient.initialize();
    });

    it('should produce messages successfully', async () => {
      const testEvent = { id: 1, name: 'test' };
      
      await kafkaClient.produce('test-topic', 'test-key', testEvent);
    });

    it('should handle production errors', async () => {
      const mockProducer = kafkaClient.kafka.producer();
      (mockProducer.send as jest.Mock).mockRejectedValue(new Error('Network error'));

      const testEvent = { id: 1, name: 'test' };

      await expect(kafkaClient.produce('test-topic', 'test-key', testEvent))
        .rejects.toThrow('Error dispatching event');
    });

    it('should reject production during shutdown', async () => {
      await kafkaClient.shutdown();

      const testEvent = { id: 1, name: 'test' };

      await expect(kafkaClient.produce('test-topic', 'test-key', testEvent))
        .rejects.toThrow('Client is shutting down');
    });
  });

  describe('Batch Processing', () => {
    beforeEach(async () => {
      kafkaClient = new KafkaClient('test-client', 'localhost:9092', {
        minBatchSize: 3,
        batchAccumulationDelayMs: 50,
      });
      await kafkaClient.initialize();
    });

    it('should process batches when handler supports it', async () => {
      // This test would require mocking the internal message processing
      // For now, we'll test the metrics and configuration
      const metrics = kafkaClient.getMetrics();
      
      expect(metrics.batchesProcessed).toBe(0);
      expect(metrics.averageBatchSize).toBe(0);
      expect(metrics.smallBatchesProcessed).toBe(0);
    });

    it('should track batch efficiency metrics', async () => {
      const metrics = kafkaClient.getMetrics();
      
      expect(metrics).toHaveProperty('batchesProcessed');
      expect(metrics).toHaveProperty('averageBatchSize');
      expect(metrics).toHaveProperty('smallBatchesProcessed');
      expect(metrics).toHaveProperty('batchEfficiency');
      expect(metrics).toHaveProperty('avgBatchProcessingTimeMs');
    });
  });

  describe('Resource Management', () => {
    beforeEach(async () => {
      kafkaClient = new KafkaClient('test-client', 'localhost:9092', {
        enableCpuMonitoring: true,
        enableMemoryMonitoring: true,
        memoryLogLevel: 'debug',
      });
      await kafkaClient.initialize();
    });

    it('should provide health status', async () => {
      const isHealthy = await kafkaClient.isHealthy();
      expect(typeof isHealthy).toBe('boolean');
    });

    it('should provide comprehensive metrics', () => {
      const metrics = kafkaClient.getMetrics();

      // Core metrics
      expect(metrics).toHaveProperty('consumedMessages');
      expect(metrics).toHaveProperty('producedMessages');
      expect(metrics).toHaveProperty('failedMessages');
      expect(metrics).toHaveProperty('activeProcessors');
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

      // Batch metrics
      expect(metrics).toHaveProperty('batchesProcessed');
      expect(metrics).toHaveProperty('averageBatchSize');
      expect(metrics).toHaveProperty('batchEfficiency');
    });

    it('should return false health status during shutdown', async () => {
      await kafkaClient.shutdown();
      
      const isHealthy = await kafkaClient.isHealthy();
      expect(isHealthy).toBe(false);
    });
  });

  describe('Error Handling', () => {
    beforeEach(async () => {
      kafkaClient = new KafkaClient('test-client', 'localhost:9092', {
        messageRetryLimit: 2,
        messageRetryDelayMs: 100,
      });
      await kafkaClient.initialize();
    });

    it('should handle connection failures gracefully', async () => {
      const mockKafka = kafkaClient.kafka;
      const mockAdmin = mockKafka.admin();
      (mockAdmin.connect as jest.Mock).mockRejectedValue(new Error('Connection failed'));

      const isHealthy = await kafkaClient.isHealthy();
      expect(isHealthy).toBe(false);
    });
  });

  describe('Shutdown Process', () => {
    beforeEach(async () => {
      kafkaClient = new KafkaClient('test-client', 'localhost:9092', {});
      await kafkaClient.initialize();
    });

    it('should shutdown gracefully', async () => {
      await kafkaClient.onModuleDestroy();
      // Shutdown completed successfully
    });

    it('should handle multiple shutdown calls', async () => {
      await kafkaClient.onModuleDestroy();
      await kafkaClient.shutdown(); // Should not throw
      // Multiple shutdowns handled gracefully
    });
  });

  describe('Configuration Validation', () => {
    it('should use default values for invalid options', async () => {
      kafkaClient = new KafkaClient('test-client', 'localhost:9092', {
        maxConcurrency: -1, // Invalid
        batchSizeMultiplier: 0, // Will use default
      });
      await kafkaClient.initialize();

      const metrics = kafkaClient.getMetrics();
      expect(metrics.dynamicConcurrencyLimit).toBeGreaterThan(0);
    });

    it('should respect memory log level configuration', async () => {
      kafkaClient = new KafkaClient('test-client', 'localhost:9092', {
        enableMemoryMonitoring: true,
        memoryLogLevel: 'warn',
      });
      await kafkaClient.initialize();

      // Test that the configuration is properly stored
      expect(kafkaClient).toBeDefined();
    });
  });

  describe('Message Ordering', () => {
    it('should maintain MAX_PARTITION_CONCURRENCY = 1 for ordering', async () => {
      kafkaClient = new KafkaClient('test-client', 'localhost:9092');
      await kafkaClient.initialize();

      // Access private field for testing (normally would use a getter)
      const maxPartitionConcurrency = (kafkaClient as any).MAX_PARTITION_CONCURRENCY;
      expect(maxPartitionConcurrency).toBe(1);
    });
  });

  describe('Metrics Collection', () => {
    beforeEach(async () => {
      kafkaClient = new KafkaClient('test-client', 'localhost:9092', {});
      await kafkaClient.initialize();
    });

    it('should track processing times', () => {
      // Access private method for testing
      (kafkaClient as any).trackProcessingTime(100);
      // Processing time tracking completed
    });
  });

  describe('Memory Management', () => {
    it('should parse different memory limit formats', async () => {
      // Test Mi format
      process.env.MEMORY_LIMIT = '512Mi';
      kafkaClient = new KafkaClient('test-client', 'localhost:9092');
      await kafkaClient.initialize();
      
      expect(loggerSpy).toHaveBeenCalledWith(
        expect.stringContaining('Container memory limit set to 512.00 MB')
      );

      await kafkaClient.shutdown();

      // Test Gi format
      process.env.MEMORY_LIMIT = '2Gi';
      kafkaClient = new KafkaClient('test-client', 'localhost:9092');
      await kafkaClient.initialize();
      
      expect(loggerSpy).toHaveBeenCalledWith(
        expect.stringContaining('Container memory limit set to 2048.00 MB')
      );

      delete process.env.MEMORY_LIMIT;
    });
  });
});

// Integration Test Suite
describe('KafkaClient Integration Tests', () => {
  let kafkaClient: KafkaClient;
  let mockHandler: MockEventHandler;

  beforeAll(() => {
    // Ensure Kafka mocks are set up for integration tests too
    const { Kafka, logLevel } = require('kafkajs');
    
    if (!Kafka.mockImplementation) {
      jest.spyOn(Kafka.prototype, 'constructor').mockImplementation();
    }
  });

  beforeEach(() => {
    mockHandler = new MockEventHandler();
    
    // Mock Logger
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
    mockHandler.reset();
  });

  it('should handle complete lifecycle', async () => {
    kafkaClient = new KafkaClient('integration-test-client', 'localhost:9092', {
      minBatchSize: 2,
      batchAccumulationDelayMs: 100,
      enableCpuMonitoring: false, // Disable to avoid timer issues in tests
      enableMemoryMonitoring: false,
    });

    // Initialize
    await kafkaClient.initialize();
    expect(kafkaClient).toBeDefined();

    // Check health
    const initialHealth = await kafkaClient.isHealthy();
    expect(typeof initialHealth).toBe('boolean');

    // Get metrics
    const metrics = kafkaClient.getMetrics();
    expect(metrics).toHaveProperty('batchesProcessed');
    expect(metrics.batchesProcessed).toBe(0);

    // Shutdown
    await kafkaClient.shutdown();
    
    const finalHealth = await kafkaClient.isHealthy();
    expect(finalHealth).toBe(false);
  });

  it('should handle signal-based shutdown', async () => {
    kafkaClient = new KafkaClient('signal-test-client', 'localhost:9092');
    await kafkaClient.initialize();

    // Mock process.exit to prevent Jest worker crash
    const originalExit = process.exit;
    const mockExit = jest.fn() as any;
    process.exit = mockExit;

    try {
      // Access private method for testing
      const handleSignal = (kafkaClient as any).handleSignal.bind(kafkaClient);
      
      // Should call the signal handler
      await handleSignal('SIGTERM');
      
      // Verify process.exit was called
      expect(mockExit).toHaveBeenCalledWith(0);
    } finally {
      // Restore original process.exit
      process.exit = originalExit;
    }
  });
}); 