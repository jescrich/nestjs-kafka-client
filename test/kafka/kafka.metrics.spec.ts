import { MetricsCollector, KafkaMetrics } from '../../src/kafka/kafka.metrics';

describe('MetricsCollector', () => {
  let metricsCollector: MetricsCollector;
  let mockResourceProvider: any;

  beforeEach(() => {
    metricsCollector = new MetricsCollector();
    
    // Mock resource provider
    mockResourceProvider = {
      getMemoryUsage: jest.fn().mockReturnValue({
        currentPercent: 50,
        rssBytes: 512 * 1024 * 1024,
        limitBytes: 1024 * 1024 * 1024,
      }),
      getCpuUsage: jest.fn().mockReturnValue({
        currentPercent: 30,
        avgPercent: 25,
      }),
      getQueueSize: jest.fn().mockReturnValue(10),
      getMaxQueueSize: jest.fn().mockReturnValue(100),
      isConnected: jest.fn().mockReturnValue(true),
    };
  });

  describe('Counter Increments', () => {
    it('should increment consumed messages count', () => {
      metricsCollector.incrementConsumed(5);
      metricsCollector.incrementConsumed(3);

      const metrics = metricsCollector.getMetrics(mockResourceProvider);
      expect(metrics.consumedMessages).toBe(8);
    });

    it('should increment processed messages count', () => {
      metricsCollector.incrementProcessed(10, false);
      metricsCollector.incrementProcessed(5, true);

      const metrics = metricsCollector.getMetrics(mockResourceProvider);
      expect(metrics.processedMessages).toBe(15);
    });

    it('should increment failed messages count', () => {
      metricsCollector.incrementFailed(2);
      metricsCollector.incrementFailed(3);

      const metrics = metricsCollector.getMetrics(mockResourceProvider);
      expect(metrics.failedMessages).toBe(5);
    });

    it('should increment DLQ messages count', () => {
      metricsCollector.incrementDlq(1);
      metricsCollector.incrementDlq(2);

      const metrics = metricsCollector.getMetrics(mockResourceProvider);
      expect(metrics.dlqMessages).toBe(3);
    });

    it('should default to incrementing by 1 when count not provided', () => {
      metricsCollector.incrementConsumed();
      metricsCollector.incrementProcessed();
      metricsCollector.incrementFailed();
      metricsCollector.incrementDlq();

      const metrics = metricsCollector.getMetrics(mockResourceProvider);
      expect(metrics.consumedMessages).toBe(1);
      expect(metrics.processedMessages).toBe(1);
      expect(metrics.failedMessages).toBe(1);
      expect(metrics.dlqMessages).toBe(1);
    });
  });

  describe('Processing Time Tracking', () => {
    it('should track average processing time', () => {
      metricsCollector.recordProcessingTime(100);
      metricsCollector.recordProcessingTime(200);
      metricsCollector.recordProcessingTime(300);

      const metrics = metricsCollector.getMetrics(mockResourceProvider);
      expect(metrics.avgProcessingTimeMs).toBe(200);
    });

    it('should track minimum processing time', () => {
      metricsCollector.recordProcessingTime(100);
      metricsCollector.recordProcessingTime(50);
      metricsCollector.recordProcessingTime(200);

      const metrics = metricsCollector.getMetrics(mockResourceProvider);
      expect(metrics.minProcessingTimeMs).toBe(50);
    });

    it('should track maximum processing time', () => {
      metricsCollector.recordProcessingTime(100);
      metricsCollector.recordProcessingTime(300);
      metricsCollector.recordProcessingTime(200);

      const metrics = metricsCollector.getMetrics(mockResourceProvider);
      expect(metrics.maxProcessingTimeMs).toBe(300);
    });

    it('should return 0 for processing times when no data recorded', () => {
      const metrics = metricsCollector.getMetrics(mockResourceProvider);
      expect(metrics.avgProcessingTimeMs).toBe(0);
      expect(metrics.minProcessingTimeMs).toBe(0);
      expect(metrics.maxProcessingTimeMs).toBe(0);
    });
  });

  describe('Batch Efficiency Calculations', () => {
    it('should calculate batch efficiency correctly', () => {
      // Process 10 messages in batches and 5 individually
      metricsCollector.incrementProcessed(10, true);
      metricsCollector.incrementProcessed(5, false);

      const metrics = metricsCollector.getMetrics(mockResourceProvider);
      expect(metrics.batchEfficiency).toBeCloseTo(66.67, 1); // 10/15 * 100
    });

    it('should return 0 batch efficiency when no messages processed', () => {
      const metrics = metricsCollector.getMetrics(mockResourceProvider);
      expect(metrics.batchEfficiency).toBe(0);
    });

    it('should return 100% efficiency when all messages batched', () => {
      metricsCollector.incrementProcessed(20, true);

      const metrics = metricsCollector.getMetrics(mockResourceProvider);
      expect(metrics.batchEfficiency).toBe(100);
    });

    it('should return 0% efficiency when no messages batched', () => {
      metricsCollector.incrementProcessed(20, false);

      const metrics = metricsCollector.getMetrics(mockResourceProvider);
      expect(metrics.batchEfficiency).toBe(0);
    });

    it('should track average batch size', () => {
      metricsCollector.recordBatch(5);
      metricsCollector.recordBatch(10);
      metricsCollector.recordBatch(15);

      const metrics = metricsCollector.getMetrics(mockResourceProvider);
      expect(metrics.avgBatchSize).toBe(10);
    });

    it('should track total batches', () => {
      metricsCollector.recordBatch(5);
      metricsCollector.recordBatch(10);
      metricsCollector.recordBatch(3);

      const metrics = metricsCollector.getMetrics(mockResourceProvider);
      expect(metrics.totalBatches).toBe(3);
    });
  });

  describe('Circular Buffer Overflow Handling', () => {
    it('should limit processing time samples to MAX_SAMPLES', () => {
      // Record more than 1000 samples
      for (let i = 0; i < 1500; i++) {
        metricsCollector.recordProcessingTime(i);
      }

      const metrics = metricsCollector.getMetrics(mockResourceProvider);
      // Should only keep the last 1000 samples (500-1499)
      // Average should be around 999 (500+1499)/2
      expect(metrics.avgProcessingTimeMs).toBeGreaterThan(900);
      expect(metrics.avgProcessingTimeMs).toBeLessThan(1100);
      expect(metrics.minProcessingTimeMs).toBe(500);
      expect(metrics.maxProcessingTimeMs).toBe(1499);
    });

    it('should limit batch size samples to MAX_SAMPLES', () => {
      // Record more than 1000 batch sizes
      for (let i = 1; i <= 1500; i++) {
        metricsCollector.recordBatch(i);
      }

      const metrics = metricsCollector.getMetrics(mockResourceProvider);
      expect(metrics.totalBatches).toBe(1500);
      // Average should be around 1000 (501+1500)/2
      expect(metrics.avgBatchSize).toBeGreaterThan(900);
      expect(metrics.avgBatchSize).toBeLessThan(1100);
    });
  });

  describe('Reset Functionality', () => {
    it('should reset all counters', () => {
      metricsCollector.incrementConsumed(10);
      metricsCollector.incrementProcessed(8, true);
      metricsCollector.incrementFailed(2);
      metricsCollector.incrementDlq(1);
      metricsCollector.recordProcessingTime(100);
      metricsCollector.recordBatch(5);

      metricsCollector.reset();

      const metrics = metricsCollector.getMetrics(mockResourceProvider);
      expect(metrics.consumedMessages).toBe(0);
      expect(metrics.processedMessages).toBe(0);
      expect(metrics.failedMessages).toBe(0);
      expect(metrics.dlqMessages).toBe(0);
      expect(metrics.avgProcessingTimeMs).toBe(0);
      expect(metrics.totalBatches).toBe(0);
      expect(metrics.batchEfficiency).toBe(0);
    });
  });

  describe('Resource Provider Integration', () => {
    it('should include memory metrics from resource provider', () => {
      const metrics = metricsCollector.getMetrics(mockResourceProvider);
      
      expect(metrics.memory.currentPercent).toBe(50);
      expect(metrics.memory.rssBytes).toBe(512 * 1024 * 1024);
      expect(metrics.memory.limitBytes).toBe(1024 * 1024 * 1024);
    });

    it('should include CPU metrics from resource provider', () => {
      const metrics = metricsCollector.getMetrics(mockResourceProvider);
      
      expect(metrics.cpu.currentPercent).toBe(30);
      expect(metrics.cpu.avgPercent).toBe(25);
    });

    it('should include queue metrics from resource provider', () => {
      const metrics = metricsCollector.getMetrics(mockResourceProvider);
      
      expect(metrics.currentQueueSize).toBe(10);
      expect(metrics.maxQueueSize).toBe(100);
    });

    it('should include connection status from resource provider', () => {
      const metrics = metricsCollector.getMetrics(mockResourceProvider);
      
      expect(metrics.isConnected).toBe(true);
    });

    it('should handle disconnected state', () => {
      mockResourceProvider.isConnected.mockReturnValue(false);
      
      const metrics = metricsCollector.getMetrics(mockResourceProvider);
      expect(metrics.isConnected).toBe(false);
    });
  });

  describe('Complete Metrics Snapshot', () => {
    it('should return complete metrics object with all fields', () => {
      metricsCollector.incrementConsumed(100);
      metricsCollector.incrementProcessed(90, true);
      metricsCollector.incrementProcessed(5, false);
      metricsCollector.incrementFailed(3);
      metricsCollector.incrementDlq(2);
      metricsCollector.recordProcessingTime(150);
      metricsCollector.recordProcessingTime(200);
      metricsCollector.recordBatch(10);

      const metrics = metricsCollector.getMetrics(mockResourceProvider);

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

      // Verify values
      expect(metrics.consumedMessages).toBe(100);
      expect(metrics.processedMessages).toBe(95);
      expect(metrics.failedMessages).toBe(3);
      expect(metrics.dlqMessages).toBe(2);
    });
  });
});
