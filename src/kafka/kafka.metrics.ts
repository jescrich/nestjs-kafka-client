/**
 * Metrics collection for Kafka client operations
 */

export interface KafkaMetrics {
  // Message counters
  consumedMessages: number;
  processedMessages: number;
  producedMessages: number;
  failedMessages: number;
  processingFailures: number; // alias of failedMessages for reporting
  retries: number;
  dlqMessages: number;

  // Performance metrics
  avgProcessingTimeMs: number;
  minProcessingTimeMs: number;
  maxProcessingTimeMs: number;

  // Batch metrics
  batchEfficiency: number; // % of messages processed in batches
  avgBatchSize: number;
  averageBatchSize: number; // alias of avgBatchSize
  totalBatches: number;
  batchesProcessed: number; // alias of totalBatches
  smallBatchesProcessed: number;
  avgBatchProcessingTimeMs: number;

  // Concurrency / processor metrics
  activeProcessors: number;
  dynamicConcurrencyLimit: number;

  // Resource metrics
  memory: {
    currentPercent: number;
    rssBytes: number;
    limitBytes: number;
    limit: number; // alias of limitBytes
    isCritical: boolean;
  };
  cpu: {
    currentPercent: number;
    avgPercent: number;
    current: number; // alias of currentPercent
    average: number; // alias of avgPercent
    isCritical: boolean;
  };

  // Queue metrics
  currentQueueSize: number;
  maxQueueSize: number;

  // Connection metrics
  isConnected: boolean;
  reconnectCount: number;
}

/**
 * MetricsCollector class for tracking Kafka client metrics
 */
export class MetricsCollector {
  private consumedCount = 0;
  private processedCount = 0;
  private producedCount = 0;
  private failedCount = 0;
  private retriesCount = 0;
  private dlqCount = 0;

  private processingTimes: number[] = [];
  private readonly MAX_SAMPLES = 1000;

  private batchProcessedCount = 0;
  private singleProcessedCount = 0;
  private batchCount = 0;
  private smallBatchCount = 0;
  private batchSizes: number[] = [];
  private batchProcessingTimes: number[] = [];

  /**
   * Increment the count of consumed messages
   */
  incrementConsumed(count: number = 1): void {
    this.consumedCount += count;
  }

  /**
   * Increment the count of processed messages
   */
  incrementProcessed(count: number = 1, isBatch: boolean = false): void {
    this.processedCount += count;
    if (isBatch) {
      this.batchProcessedCount += count;
    } else {
      this.singleProcessedCount += count;
    }
  }

  /**
   * Increment the count of produced messages
   */
  incrementProduced(count: number = 1): void {
    this.producedCount += count;
  }

  /**
   * Increment the count of failed messages
   */
  incrementFailed(count: number = 1): void {
    this.failedCount += count;
  }

  /**
   * Increment the count of retry attempts
   */
  incrementRetries(count: number = 1): void {
    this.retriesCount += count;
  }

  /**
   * Increment the count of messages sent to DLQ
   */
  incrementDlq(count: number = 1): void {
    this.dlqCount += count;
  }

  /**
   * Record processing time for a message or batch
   */
  recordProcessingTime(timeMs: number): void {
    this.processingTimes.push(timeMs);
    // Keep only the most recent samples using circular buffer
    if (this.processingTimes.length > this.MAX_SAMPLES) {
      this.processingTimes.shift();
    }
  }

  /**
   * Record a batch processing event
   * @param size Number of messages in the batch
   * @param isSmall Whether the batch was below the configured minimum batch size
   */
  recordBatch(size: number, isSmall: boolean = false): void {
    this.batchCount++;
    this.batchSizes.push(size);
    if (isSmall) {
      this.smallBatchCount++;
    }
    // Keep only the most recent samples using circular buffer
    if (this.batchSizes.length > this.MAX_SAMPLES) {
      this.batchSizes.shift();
    }
  }

  /**
   * Record the wall-clock processing time of a batch
   */
  recordBatchProcessingTime(timeMs: number): void {
    this.batchProcessingTimes.push(timeMs);
    if (this.batchProcessingTimes.length > this.MAX_SAMPLES) {
      this.batchProcessingTimes.shift();
    }
  }

  /**
   * Get current metrics snapshot
   */
  getMetrics(resourceProvider: {
    getMemoryUsage: () => { currentPercent: number; rssBytes: number; limitBytes: number; isCritical?: boolean };
    getCpuUsage: () => { currentPercent: number; avgPercent: number; isCritical?: boolean };
    getQueueSize: () => number;
    getMaxQueueSize: () => number;
    isConnected: () => boolean;
    getActiveProcessors?: () => number;
    getConcurrencyLimit?: () => number;
  }): KafkaMetrics {
    const avgProcessingTime =
      this.processingTimes.length > 0
        ? this.processingTimes.reduce((a, b) => a + b, 0) / this.processingTimes.length
        : 0;

    const minProcessingTime =
      this.processingTimes.length > 0 ? Math.min(...this.processingTimes) : 0;

    const maxProcessingTime =
      this.processingTimes.length > 0 ? Math.max(...this.processingTimes) : 0;

    const batchEfficiency =
      this.processedCount > 0 ? (this.batchProcessedCount / this.processedCount) * 100 : 0;

    const avgBatchSize =
      this.batchSizes.length > 0
        ? this.batchSizes.reduce((a, b) => a + b, 0) / this.batchSizes.length
        : 0;

    const avgBatchProcessingTime =
      this.batchProcessingTimes.length > 0
        ? this.batchProcessingTimes.reduce((a, b) => a + b, 0) / this.batchProcessingTimes.length
        : 0;

    const mem = resourceProvider.getMemoryUsage();
    const cpu = resourceProvider.getCpuUsage();

    return {
      consumedMessages: this.consumedCount,
      processedMessages: this.processedCount,
      producedMessages: this.producedCount,
      failedMessages: this.failedCount,
      processingFailures: this.failedCount,
      retries: this.retriesCount,
      dlqMessages: this.dlqCount,

      avgProcessingTimeMs: avgProcessingTime,
      minProcessingTimeMs: minProcessingTime,
      maxProcessingTimeMs: maxProcessingTime,

      batchEfficiency,
      avgBatchSize,
      averageBatchSize: avgBatchSize,
      totalBatches: this.batchCount,
      batchesProcessed: this.batchCount,
      smallBatchesProcessed: this.smallBatchCount,
      avgBatchProcessingTimeMs: avgBatchProcessingTime,

      activeProcessors: resourceProvider.getActiveProcessors?.() ?? 0,
      dynamicConcurrencyLimit: resourceProvider.getConcurrencyLimit?.() ?? 0,

      memory: {
        currentPercent: mem.currentPercent,
        rssBytes: mem.rssBytes,
        limitBytes: mem.limitBytes,
        limit: mem.limitBytes,
        isCritical: mem.isCritical ?? false,
      },
      cpu: {
        currentPercent: cpu.currentPercent,
        avgPercent: cpu.avgPercent,
        current: cpu.currentPercent,
        average: cpu.avgPercent,
        isCritical: cpu.isCritical ?? false,
      },

      currentQueueSize: resourceProvider.getQueueSize(),
      maxQueueSize: resourceProvider.getMaxQueueSize(),

      isConnected: resourceProvider.isConnected(),
      reconnectCount: 0, // TODO: Track reconnections in future enhancement
    };
  }

  /**
   * Reset all metrics (useful for testing)
   */
  reset(): void {
    this.consumedCount = 0;
    this.processedCount = 0;
    this.producedCount = 0;
    this.failedCount = 0;
    this.retriesCount = 0;
    this.dlqCount = 0;
    this.processingTimes = [];
    this.batchProcessedCount = 0;
    this.singleProcessedCount = 0;
    this.batchCount = 0;
    this.smallBatchCount = 0;
    this.batchSizes = [];
    this.batchProcessingTimes = [];
  }
}
