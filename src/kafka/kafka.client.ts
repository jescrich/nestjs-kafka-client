import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { KafkaMessage } from '@nestjs/microservices/external/kafka.interface';
import { Kafka, logLevel, ConsumerConfig, Consumer, Producer, EachBatchPayload } from 'kafkajs';
import { IEventHandler } from './kafka.event.handler';
import { cpus } from 'os';
import { setTimeout } from 'timers/promises';
import * as os from 'os';
import { randomUUID } from 'crypto';

// Assume IEventHandler is defined elsewhere, e.g.:
// export interface IEventHandler<T> {
//   handle(data: { key: string | null; event: T; payload: KafkaMessage }): Promise<void>;
// }

/**
 * KafkaClient with advanced resource management and monitoring capabilities.
 * 
 * Memory Monitoring Configuration:
 * - enableMemoryMonitoring: Enable/disable memory monitoring (default: true)
 * - memoryLogLevel: Log level for normal memory usage ('debug' | 'info' | 'warn', default: 'debug')
 * 
 * Memory monitoring behavior:
 * - Logs every hour when memory usage is stable and normal
 * - Always logs when memory usage exceeds warning threshold (75%)
 * - Logs when memory usage changes significantly (>10%)
 * - Uses DEBUG level by default for normal usage to reduce log verbosity
 * - Uses WARN level when memory usage is above threshold
 */

// Define QueueItem at the class level or in a shared types file
interface QueueItem<T> {
  message: KafkaMessage;
  topic: string;
  partition: number;
  resolveOffset: (offset: string) => void;
  heartbeat: () => Promise<void>;
}

@Injectable()
export class KafkaClient implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(KafkaClient.name);
  kafka: Kafka;
  private producer: Producer;
  private messageQueueSize = 0;

  private readonly DEFAULT_MAX_CONCURRENCY = Math.max(1, Math.floor(cpus().length / 2));
  private readonly DEFAULT_BATCH_SIZE_MULTIPLIER = 10;
  private readonly MAX_PARTITION_CONCURRENCY = 1; // Keep at 1 to preserve message ordering within partitions
  private readonly MAX_CONCURRENCY_MULTIPLIER = 2;

  private readonly DEFAULT_CONSUMER_CONFIG: Partial<ConsumerConfig> = {
    maxInFlightRequests: 3, // Increased from 1 for better throughput with manual commit
    maxWaitTimeInMs: 1000, // Increased from 500 for better batching
    maxBytesPerPartition: 1048576, // 1MB
    sessionTimeout: 60000, // 60s (increased for manual commit processing)
    heartbeatInterval: 15000, // 15s intervals (4 beats per session)
    rebalanceTimeout: 120000, // 2 minutes for rebalancing
  };

  private activeProcessors = 0;
  private partitionProcessors = new Map<string, number>();
  private activeConsumers: Consumer[] = [];
  private consumerRegistry = new Map<string, Consumer>(); // Topic -> Consumer (if multiple consumers are ever used)
  private enableCpuMonitoring = false;
  private enableMemoryMonitoring = true;
  private memoryLogLevel: 'debug' | 'info' | 'warn' = 'debug';
  private connectionMaxRetries = 10;
  private connectionRetryDelay = 1000;

  private isShuttingDown = false;
  private shutdownPromise: Promise<void> | null = null;
  private isInitialized = false;
  private initializationError: Error | null = null;

  // Resource monitoring configuration
  private readonly CPU_CHECK_INTERVAL_MS = 10000;
  private readonly CPU_HIGH_THRESHOLD = 70; // Lowered for container context
  private readonly CPU_LOW_THRESHOLD = 30;
  private readonly CPU_CRITICAL_THRESHOLD = 85; // Lowered for container context
  private readonly CPU_SAMPLES_TO_KEEP = 6;

  // For process-based CPU tracking
  private lastCpuUsage = process.cpuUsage();
  private lastCpuTime = Date.now();

  private cpuUsageSamples: number[] = [];
  private cpuMonitoringTimer: NodeJS.Timeout | null = null;
  private isCpuCritical = false;
  private lastConcurrencyAdjustment = 0;
  private readonly CONCURRENCY_ADJUSTMENT_COOLDOWN_MS = 30000;

  private dynamicConcurrencyLimit: number;

  // Memory monitoring
  private readonly GC_INTERVAL_MS = 300000; // 5 minutes
  private readonly MEMORY_THRESHOLD_PERCENT = 75; // Lowered for container context
  private readonly MEMORY_CRITICAL_THRESHOLD = 85; // New threshold for critical memory usage
  private gcTimer: NodeJS.Timeout | null = null;
  private isMemoryCritical = false;
  private sessionTimeout = 60000; // Increased for manual commit
  private heartbeatInterval = 15000;

  // Container memory limit - initialized in constructor
  private containerMemoryLimitBytes: number = 0;



  private topicPartitions: Map<string, number[]> = new Map(); // topic -> [partitionIds]

  // Configurable options
  private readonly effectiveMaxConcurrency: number;
  private readonly effectiveBatchSizeMultiplier: number;
  private readonly effectiveDlqSuffix: string;
  private readonly effectiveMessageRetryLimit: number;
  private readonly effectiveMessageRetryDelayMs: number;
  private readonly effectiveFromBeginning: boolean;
  private readonly effectiveBatchAccumulationDelayMs: number;
  private readonly effectiveMinBatchSize: number;

  private readonly consumers = new Map<string, Consumer>();
  
  // DLQ failure recovery queue - tracks messages that failed to send to DLQ
  private readonly dlqFailureQueue = new Map<string, {
    message: KafkaMessage;
    topic: string;
    partition: number;
    failureCount: number;
    lastAttempt: number;
  }[]>();

  constructor(
    private readonly clientId: string = `kafka-client-${process.pid}`,
    private readonly brokers: string = 'localhost:9092',
    options: {
      maxConcurrency?: number;
      batchSizeMultiplier?: number;
      connectionRetryDelay?: number;
      connectionMaxRetries?: number;
      enableCpuMonitoring?: boolean;
      enableMemoryMonitoring?: boolean;
      memoryLogLevel?: 'debug' | 'info' | 'warn';
      dlqSuffix?: string;
      messageRetryLimit?: number;
      messageRetryDelayMs?: number;
      containerMemoryLimitMB?: number;
      fromBeginning?: boolean;
      sessionTimeout?: number;
      heartbeatInterval?: number;
      batchAccumulationDelayMs?: number;
      minBatchSize?: number;
    } = {
      maxConcurrency: 0,
      batchSizeMultiplier: 0,
      connectionRetryDelay: 5000,
      connectionMaxRetries: 10,
      enableCpuMonitoring: true,
      enableMemoryMonitoring: true,
      memoryLogLevel: 'debug',
      dlqSuffix: '-dlq',
      messageRetryLimit: 3,
      messageRetryDelayMs: 30000,
      fromBeginning: false,
      sessionTimeout: 30000,
      heartbeatInterval: 10000,
      batchAccumulationDelayMs: 100,
      minBatchSize: 5,
    },
  ) {
    const {
      maxConcurrency = 0,
      batchSizeMultiplier = 0,
      connectionRetryDelay = 5000,
      connectionMaxRetries = 10,
      enableCpuMonitoring = true,
      enableMemoryMonitoring = true,
      memoryLogLevel = 'debug',
      dlqSuffix = '-dlq',
      messageRetryLimit = 3,
      messageRetryDelayMs = 30000,
      containerMemoryLimitMB = 0,
      fromBeginning = false,
      sessionTimeout = 30000,
      heartbeatInterval = 10000,
      batchAccumulationDelayMs = 100,
      minBatchSize = 5,
    } = options;

    this.kafka = new Kafka({
      clientId: clientId,
      brokers: brokers.split(','),
      logLevel: logLevel.WARN, // Adjust as needed, e.g., logLevel.ERROR for prod
    });
    this.sessionTimeout = sessionTimeout;
    this.heartbeatInterval = heartbeatInterval;
    this.connectionMaxRetries = connectionMaxRetries;
    this.connectionRetryDelay = connectionRetryDelay;
    this.enableCpuMonitoring = enableCpuMonitoring;
    this.enableMemoryMonitoring = enableMemoryMonitoring;
    this.memoryLogLevel = memoryLogLevel;
    this.effectiveMaxConcurrency = maxConcurrency > 0 ? maxConcurrency : this.DEFAULT_MAX_CONCURRENCY;
    this.effectiveBatchSizeMultiplier =
      batchSizeMultiplier > 0 ? batchSizeMultiplier : this.DEFAULT_BATCH_SIZE_MULTIPLIER;
    this.dynamicConcurrencyLimit = this.effectiveMaxConcurrency;
    this.effectiveDlqSuffix = dlqSuffix;
    this.effectiveMessageRetryLimit = messageRetryLimit;
    this.effectiveMessageRetryDelayMs = messageRetryDelayMs;
    this.effectiveFromBeginning = fromBeginning;
    this.effectiveBatchAccumulationDelayMs = batchAccumulationDelayMs;
    this.effectiveMinBatchSize = minBatchSize;

    this.logger.log(`KafkaClient initialized with options: ${JSON.stringify(options)}`);
    // Initialize container memory limit from env vars or passed parameter
    this.initializeContainerMemoryLimit(containerMemoryLimitMB);

    // Graceful shutdown handlers
    process.on('SIGTERM', this.handleSignal.bind(this, 'SIGTERM'));
    process.on('SIGINT', this.handleSignal.bind(this, 'SIGINT'));
  }

  /**
   * Initializes the container memory limit based on environment variables or provided values
   */
  private initializeContainerMemoryLimit(containerMemoryLimitMB?: number): void {
    // Try to get memory limit from various sources in order of preference
    if (containerMemoryLimitMB && containerMemoryLimitMB > 0) {
      this.containerMemoryLimitBytes = containerMemoryLimitMB * 1024 * 1024;
    } else if (process.env.CONTAINER_MEMORY_LIMIT_MB) {
      this.containerMemoryLimitBytes = parseInt(process.env.CONTAINER_MEMORY_LIMIT_MB) * 1024 * 1024;
    } else if (process.env.MEMORY_LIMIT) {
      // Some container runtimes provide this format
      const limitStr = process.env.MEMORY_LIMIT;
      if (limitStr.endsWith('m')) {
        // If in millicores format
        this.containerMemoryLimitBytes = parseInt(limitStr.slice(0, -1)) * 1024 * 1024;
      } else if (limitStr.endsWith('Mi')) {
        this.containerMemoryLimitBytes = parseInt(limitStr.slice(0, -2)) * 1024 * 1024;
      } else if (limitStr.endsWith('Gi')) {
        this.containerMemoryLimitBytes = parseInt(limitStr.slice(0, -2)) * 1024 * 1024 * 1024;
      } else {
        // Try to parse as raw bytes
        this.containerMemoryLimitBytes = parseInt(limitStr);
      }
    } else {
      // Fallback to total system memory (less accurate in containers)
      this.containerMemoryLimitBytes = os.totalmem();
      this.logger.warn(
        'Container memory limit not provided. Using system total memory as fallback, which may be inaccurate in containerized environments.',
      );
    }

    this.logger.log(`Container memory limit set to ${(this.containerMemoryLimitBytes / (1024 * 1024)).toFixed(2)} MB`);
  }

  async onModuleInit(): Promise<void> {
    // Start initialization asynchronously - don't block bootstrap
    this.initializeAsync();
  }

  async onModuleDestroy(): Promise<void> {

    await this.shutdown();
  }

  private async handleSignal(signal: string): Promise<void> {
    this.logger.log(`${signal} received, initiating shutdown...`);
    await this.shutdown();
    process.exit(0);
  }

  /**
   * Initialize KafkaClient asynchronously without blocking bootstrap
   */
  private async initializeAsync(): Promise<void> {
    try {
      this.logger.log('Starting async KafkaClient initialization...');
      
      this.producer = this.kafka.producer();
      const connected = await this.connectWithRetry(this.producer, 'shared producer', 60000); // 1 minute timeout
      
      if (!connected) {
        throw new Error('Failed to connect producer');
      }



      if (this.enableCpuMonitoring) {
        this.startCpuMonitoring();
      }
      if (this.enableMemoryMonitoring) {
        this.startMemoryMonitoring();
      }
      
      this.isInitialized = true;
      this.initializationError = null;
      this.logger.log('KafkaClient initialized successfully.');
      
    } catch (error) {
      this.initializationError = error as Error;
      this.logger.error(`KafkaClient initialization failed: ${error.message}`, error.stack);
      
      // Schedule retry after delay
      global.setTimeout(() => {
        this.logger.log('Retrying KafkaClient initialization...');
        this.initializeAsync();
      }, 30000); // Retry after 30 seconds
    }
  }

  /**
   * Legacy synchronous initialize method for backward compatibility
   * @deprecated Use initializeAsync() instead
   */
  public async initialize(): Promise<void> {
    return this.initializeAsync();
  }

  public async shutdown(): Promise<void> {
    if (this.isShuttingDown) {
      if (this.shutdownPromise) return this.shutdownPromise;
      return;
    }
    this.isShuttingDown = true;
    this.logger.log('Initiating graceful shutdown of Kafka client...');

    this.shutdownPromise = (async () => {
      if (this.cpuMonitoringTimer) {
        clearInterval(this.cpuMonitoringTimer);
        this.cpuMonitoringTimer = null;
      }
      if (this.gcTimer) {
        clearInterval(this.gcTimer);
        this.gcTimer = null;
      }

      const maxWaitMs = 30000;
      const startTime = Date.now();
      while (this.activeProcessors > 0 && Date.now() - startTime < maxWaitMs) {
        this.logger.log(`Waiting for ${this.activeProcessors} active processors to complete...`);
        await setTimeout(1000);
      }
      if (this.activeProcessors > 0) {
        this.logger.warn(`Shutdown timeout reached with ${this.activeProcessors} processors still active.`);
      }

      try {
        await Promise.all(
          this.activeConsumers.map((consumer) => 
            consumer.disconnect()
          ),
        );
        this.logger.log(`${this.activeConsumers.length} Kafka consumer(s) disconnected.`);
        this.activeConsumers = [];
      } catch (error) {
        this.logger.error('Error during consumer disconnection phase', error);
      }

      try {
        if (this.producer) {
          // Check if producer was initialized
          await this.producer.disconnect();
          this.logger.log('Kafka producer disconnected.');
        }
      } catch (error) {
        this.logger.error('Error disconnecting Kafka producer', error);
      }
      this.logger.log('Kafka client shutdown complete.');
    })();
    return this.shutdownPromise;
  }

  private async connectWithRetry(entity: Consumer | Producer, entityName: string, maxTimeoutMs: number = 300000): Promise<boolean> {
    let retries = 0;
    const maxRetries = Math.max(this.connectionMaxRetries, 15); // Minimum 15 retries
    const baseDelay = Math.max(this.connectionRetryDelay, 1000); // Minimum 1s delay
    const startTime = Date.now();
    
    while (retries < maxRetries && (Date.now() - startTime) < maxTimeoutMs) {
      if (this.isShuttingDown) {
        this.logger.warn(`Shutdown initiated, aborting connection for ${entityName}`);
        return false;
      }
      try {
        // Add connection timeout to prevent hanging
        const connectionPromise = entity.connect();
        const timeoutPromise = new Promise<never>((_, reject) => {
          const timer = global.setTimeout(() => reject(new Error('Connection timeout')), 30000);
          return timer;
        });
        
        await Promise.race([connectionPromise, timeoutPromise]);
        this.logger.log(`${entityName} connected successfully after ${retries} retries.`);
        return true;
      } catch (error) {
        retries++;
        
        // Check if we've exceeded the total timeout
        if ((Date.now() - startTime) >= maxTimeoutMs) {
          this.logger.error(`Total connection timeout (${maxTimeoutMs}ms) reached for ${entityName}.`);
          return false;
        }
        
        // Exponential backoff with jitter to prevent thundering herd
        const exponentialDelay = Math.min(baseDelay * Math.pow(2, retries - 1), 30000);
        const jitter = Math.random() * 1000;
        const totalDelay = exponentialDelay + jitter;
        
        this.logger.warn(
          `Failed to connect ${entityName} (attempt ${retries}/${maxRetries}): ${error.message}. Retrying in ${Math.round(totalDelay)}ms`,
        );
        
        if (retries >= maxRetries) {
          this.logger.error(`Max connection retries (${maxRetries}) reached for ${entityName}.`);
          return false;
        }
        
        await setTimeout(totalDelay);
      }
    }
    
    if ((Date.now() - startTime) >= maxTimeoutMs) {
      this.logger.error(`Total connection timeout (${maxTimeoutMs}ms) reached for ${entityName}.`);
    }
    
    return false;
  }

  private getMaxConcurrency(): number {
    return this.effectiveMaxConcurrency;
  }

  private getBatchSizeMultiplier(): number {
    return this.effectiveBatchSizeMultiplier;
  }

  private getMaxQueueSize(): number {
    return this.getMaxConcurrency() * this.getBatchSizeMultiplier();
  }

  private getCurrentConcurrencyLimit(): number {
    return this.dynamicConcurrencyLimit;
  }

  async produce<T>(topic: string, key: string, event: T): Promise<void> {
    if (this.isShuttingDown) {
      this.logger.warn(`Shutting down, skipping produce message to topic ${topic}`);
      throw new Error('Client is shutting down, cannot produce message.');
    }
    
    if (!this.isInitialized) {
      const error = this.initializationError 
        ? `KafkaClient not initialized: ${this.initializationError.message}`
        : 'KafkaClient not initialized yet';
      this.logger.error(`Cannot produce message to topic ${topic}: ${error}`);
      throw new Error(error);
    }
    
    try {
      await this.producer.send({
        topic,
        messages: [{ key, value: JSON.stringify(event) }],
      });



      this.logger.debug(`Event dispatched to topic ${topic}, key ${key}`);
    } catch (e) {
      this.logger.error(`Error dispatching event to topic ${topic}, key ${key}: ${e.message}`, e.stack);
      // Depending on the error, you might want to retry or ensure producer is reconnected.
      // For now, rethrowing.
      throw new Error(`Error dispatching event. Key: ${key}, Topic: ${topic}. Error: ${e.message}`);
    }
  }

  private async sendToDeadLetterQueue(topic: string, message: KafkaMessage, partition?: number): Promise<void> {
    const dlqTopic = topic + this.effectiveDlqSuffix;
    
    try {
      await this.producer.send({
        topic: dlqTopic,
        messages: [{ 
          key: message.key, 
          value: message.value, 
          headers: {
           ...message.headers,
           // Add metadata about the original message
           'x-original-topic': topic,
           'x-original-offset': message.offset,
           'x-dlq-timestamp': Date.now().toString(),
         }
        }],
      });
      
      this.logger.debug(`Message sent to DLQ topic ${dlqTopic} - original offset ${message.offset} from topic ${topic}`);
      
      // Remove from DLQ failure queue if it was there
      this.removeDlqFailureEntry(topic, message, partition);
      
    } catch (e) {
      this.logger.error(
        `CRITICAL: Failed to send message offset ${message.offset} to DLQ ${dlqTopic}: ${e.message}`,
        e.stack,
      );
      
      // Add to DLQ failure recovery queue instead of losing the message
      this.addToDlqFailureQueue(topic, message, partition);
      
      throw e; // Rethrow to indicate DLQ send failure
    }
  }

  /**
   * Add message to DLQ failure recovery queue
   */
  private addToDlqFailureQueue(topic: string, message: KafkaMessage, partition?: number): void {
    const partitionNum = partition ?? 0;
    const key = `${topic}-${partitionNum}`;
    
    if (!this.dlqFailureQueue.has(key)) {
      this.dlqFailureQueue.set(key, []);
    }
    
    const queue = this.dlqFailureQueue.get(key)!;
    const existingIndex = queue.findIndex(entry => entry.message.offset === message.offset);
    
    if (existingIndex >= 0) {
      // Update existing entry
      queue[existingIndex].failureCount++;
      queue[existingIndex].lastAttempt = Date.now();
    } else {
      // Add new entry
      queue.push({
        message,
        topic,
        partition: partitionNum,
        failureCount: 1,
        lastAttempt: Date.now()
      });
    }
    
    const attemptCount = existingIndex >= 0 ? queue[existingIndex].failureCount : queue[queue.length - 1].failureCount;
    this.logger.warn(`Message offset ${message.offset} added to DLQ failure recovery queue for topic ${topic} (attempt ${attemptCount})`);
  }

  /**
   * Remove message from DLQ failure recovery queue
   */
  private removeDlqFailureEntry(topic: string, message: KafkaMessage, partition?: number): void {
    const partitionNum = partition ?? 0;
    const key = `${topic}-${partitionNum}`;
    const queue = this.dlqFailureQueue.get(key);
    
    if (queue) {
      const index = queue.findIndex(entry => entry.message.offset === message.offset);
      if (index >= 0) {
        queue.splice(index, 1);
        if (queue.length === 0) {
          this.dlqFailureQueue.delete(key);
        }
      }
    }
  }

  /**
   * Check if there are messages in DLQ failure queue for a specific topic/partition
   */
  private hasDlqFailuresForTopicPartition(topic: string, partition: number): boolean {
    const key = `${topic}-${partition}`;
    const queue = this.dlqFailureQueue.get(key);
    return !!(queue && queue.length > 0);
  }

  /**
   * Get DLQ failure queue status for monitoring
   */
  public getDlqFailureQueueStatus(): { 
    totalQueued: number; 
    queuesByTopic: Record<string, number> 
  } {
    let totalQueued = 0;
    const queuesByTopic: Record<string, number> = {};
    
    for (const [key, queue] of this.dlqFailureQueue.entries()) {
      totalQueued += queue.length;
      const topic = key.substring(0, key.lastIndexOf('-'));
      queuesByTopic[topic] = (queuesByTopic[topic] || 0) + queue.length;
    }
    
    return { totalQueued, queuesByTopic };
  }

  /**
   * Retry sending messages from DLQ failure queue
   */
  private async retryDlqFailures(): Promise<void> {
    const MAX_DLQ_RETRY_ATTEMPTS = 5;
    const DLQ_RETRY_DELAY_MS = 5000; // 5 seconds between DLQ retry attempts
    
    for (const [key, queue] of this.dlqFailureQueue.entries()) {
      const now = Date.now();
      
      // Process messages that are ready for retry
      for (let i = queue.length - 1; i >= 0; i--) {
        const entry = queue[i];
        
        // Check if enough time has passed since last attempt
        if (now - entry.lastAttempt < DLQ_RETRY_DELAY_MS) {
          continue;
        }
        
        // Check if we've exceeded max retry attempts
        if (entry.failureCount >= MAX_DLQ_RETRY_ATTEMPTS) {
          this.logger.error(`CRITICAL: Message offset ${entry.message.offset} from topic ${entry.topic} has exceeded DLQ retry limit (${MAX_DLQ_RETRY_ATTEMPTS}). Message will be permanently lost.`);
          queue.splice(i, 1);
          continue;
        }
        
        // Attempt to send to DLQ again
        try {
          await this.sendToDeadLetterQueue(entry.topic, entry.message);
          this.logger.log(`Successfully sent message offset ${entry.message.offset} to DLQ on retry attempt ${entry.failureCount}`);
          // Message will be removed from queue by sendToDeadLetterQueue on success
        } catch (error) {
          // sendToDeadLetterQueue will update the failure count
          this.logger.warn(`DLQ retry attempt ${entry.failureCount} failed for message offset ${entry.message.offset}: ${error.message}`);
        }
      }
      
      // Clean up empty queues
      if (queue.length === 0) {
        this.dlqFailureQueue.delete(key);
      }
    }
  }

  public async consumeMany<T>(
    topicHandlers: { topic: string; handler: IEventHandler<T> }[],
    groupId: string,
  ): Promise<void> {
    if (this.isShuttingDown) {
      this.logger.warn(`Shutting down, cannot start consumer for group ${groupId}`);
      return;
    }

    if (!this.isInitialized) {
      const error = this.initializationError 
        ? `KafkaClient not initialized: ${this.initializationError.message}`
        : 'KafkaClient not initialized yet';
      this.logger.error(`Cannot start consumer for group ${groupId}: ${error}`);
      throw new Error(error);
    }

    const consumer = this.kafka.consumer({ groupId, ...this.DEFAULT_CONSUMER_CONFIG });
    this.consumers.set(groupId, consumer);

    const handlerMap = new Map<string, IEventHandler<any>>();
    const topicsToSubscribe: string[] = [];
    for (const th of topicHandlers) {
      handlerMap.set(th.topic, th.handler);
      topicsToSubscribe.push(th.topic);
    }
    
    // Track retry counts per message offset to prevent infinite retries
    const retryTracker = new Map<string, number>();
    
    // Use shorter timeout for consumer connections to prevent blocking bootstrap
    const connected = await this.connectWithRetry(consumer, `consumer for group ${groupId}`, 60000); // 1 minute timeout
    if (!connected) {
      this.logger.error(`Failed to connect consumer for group ${groupId}. Consumer will not be available.`);
      throw new Error(`Failed to connect consumer for group ${groupId}`);
    }
    
    try {
      await consumer.subscribe({ topics: topicsToSubscribe, fromBeginning: this.effectiveFromBeginning });
    } catch (error) {
      this.logger.error(`Failed to subscribe to topics for group ${groupId}: ${error.message}`);
      throw error;
    }

    await consumer.run({
      autoCommit: false,
      eachBatch: async ({ batch, resolveOffset, heartbeat, isRunning, isStale }) => {
        if (!isRunning() || isStale() || this.isShuttingDown) {
          return;
        }

        // Configuration for batch processing limits
        const MAX_BATCH_SIZE = 100; // Maximum messages per batch
        const MAX_PROCESSING_TIME = 30000; // 30 seconds max processing time
        const HEARTBEAT_INTERVAL = 8000; // 8 seconds between heartbeats
        
        // Start heartbeat timer to prevent coordinator timeouts
        const heartbeatTimer = setInterval(async () => {
          try {
            if (isRunning() && !isStale() && !this.isShuttingDown) {
              await heartbeat();
              this.logger.debug(`Heartbeat sent during batch processing for ${batch.topic}-${batch.partition}`);
            }
          } catch (error) {
            this.logger.warn(`Heartbeat failed during processing: ${error.message}`);
          }
        }, HEARTBEAT_INTERVAL);

        const processingStartTime = Date.now();
        let processedCount = 0;

        try {
          // Send initial heartbeat
          await heartbeat();

          // Group messages by key to process them in batches.
          const messagesByKey = new Map<string, KafkaMessage[]>();
          for (const message of batch.messages) {
            const key = message.key?.toString() ?? 'unknown';
            if (!messagesByKey.has(key)) {
              messagesByKey.set(key, []);
            }
            messagesByKey.get(key)!.push(message);
          }

          const processingPromises: Promise<void>[] = [];
          const successfulMessages: KafkaMessage[] = [];
          const failedMessages: KafkaMessage[] = [];

          for (const [key, messages] of messagesByKey.entries()) {
            // Check processing time and count limits
            if (Date.now() - processingStartTime > MAX_PROCESSING_TIME) {
              this.logger.warn(`Processing time limit (${MAX_PROCESSING_TIME}ms) reached for ${batch.topic}-${batch.partition}, committing partial batch`);
              break;
            }
            
            if (processedCount >= MAX_BATCH_SIZE) {
              this.logger.warn(`Batch size limit (${MAX_BATCH_SIZE}) reached for ${batch.topic}-${batch.partition}, committing partial batch`);
              break;
            }

            const handler = handlerMap.get(batch.topic);
            if (!handler) {
              this.logger.warn(`No handler for topic ${batch.topic}, skipping ${messages.length} messages.`);
              // Add to failed messages so offsets are not committed
              failedMessages.push(...messages);
              continue;
            }

            // Process messages in chunks to allow for heartbeats
            const chunks = this.chunkArray(messages, 10); // Process 10 messages at a time
            
            for (const chunk of chunks) {
              // Send heartbeat between chunks
              await heartbeat();
              
              const task = async () => {
                const taskStartTime = Date.now();
                
                try {
                  // Apply backpressure if needed
                  if (this.isCpuCritical || this.isMemoryCritical) {
                    const backoffDelay = 100;
                    await setTimeout(backoffDelay);
                  }

                  if (chunk.length > 1 && handler.handleBatch) {
                    // Process as a batch
                    const events = chunk.map(m => JSON.parse(m.value!.toString()));
                    await handler.handleBatch({ key, events, payloads: chunk as any[] });
                  } else {
                    // Process messages individually
                    for (const message of chunk) {
                      const event = JSON.parse(message.value!.toString());
                      await handler.handle?.({ key, event, payload: message as any });
                    }
                  }
                  

                  
                  // Track processing time
                  const processingTime = Date.now() - taskStartTime;
                  this.trackProcessingTime(processingTime / chunk.length);
                  
                  // Add to successful messages for offset commit
                  successfulMessages.push(...chunk);
                  
                  // Clear retry counts for successful messages
                  for (const message of chunk) {
                    const retryKey = `${batch.topic}-${batch.partition}-${message.offset}`;
                    retryTracker.delete(retryKey);
                  }
                  
                  processedCount += chunk.length;
              
                } catch (error) {
                  const processingTime = Date.now() - taskStartTime;
                  
                  // Check retry counts for each message in this chunk
                  let shouldRetry = false;
                  const messagesToDlq: KafkaMessage[] = [];
                  const messagesToRetry: KafkaMessage[] = [];
                  
                  for (const message of chunk) {
                    const retryKey = `${batch.topic}-${batch.partition}-${message.offset}`;
                    const currentRetries = retryTracker.get(retryKey) || 0;
                    
                    if (currentRetries < this.effectiveMessageRetryLimit && this.isRetryableError(error)) {
                      retryTracker.set(retryKey, currentRetries + 1);
                      messagesToRetry.push(message);
                      shouldRetry = true;
                    } else {
                      messagesToDlq.push(message);
                      retryTracker.delete(retryKey); // Clean up
                    }
                  }
                  
                  this.logger.error(`Error processing chunk for key ${key} on topic ${batch.topic}`, {
                    error: error.message,
                    stack: error.stack,
                    processingTimeMs: processingTime,
                    messageCount: chunk.length,
                    retryable: this.isRetryableError(error),
                    offsets: chunk.map(m => m.offset),
                    toRetry: messagesToRetry.length,
                    toDlq: messagesToDlq.length
                  });
                  

                  
                  if (shouldRetry && messagesToRetry.length > 0) {
                    // Some messages still have retries left
                    this.logger.warn(`Retrying ${messagesToRetry.length} messages for key ${key} (retries remaining)`);

                    failedMessages.push(...messagesToRetry);
                  }
                  
                  if (messagesToDlq.length > 0) {
                    // Some messages exceeded retry limit or are non-retryable
                    this.logger.warn(`Sending ${messagesToDlq.length} messages to DLQ for key ${key} (retry limit exceeded or non-retryable error)`);
                    
                    try {
                      // Send each message to DLQ
                      for (const message of messagesToDlq) {
                        await this.sendToDeadLetterQueue(batch.topic, message, batch.partition);
                      }
                      
                      // Add to successful messages so offsets are committed (preventing redelivery)
                      successfulMessages.push(...messagesToDlq);

                      
                    } catch (dlqError) {
                      this.logger.error(`Failed to send messages to DLQ for key ${key}`, {
                        error: dlqError.message,
                        stack: dlqError.stack,
                        offsets: messagesToDlq.map(m => m.offset)
                      });
                      
                      // Messages are now in DLQ failure recovery queue
                      // Don't commit their offsets - they'll be retried later
                      failedMessages.push(...messagesToDlq);
                      
                      // Log critical situation
                      this.logger.error(`CRITICAL: ${messagesToDlq.length} messages failed to send to DLQ and are queued for retry. Offsets will not be committed to prevent data loss.`);
                    }
                  }
                }
              };

              processingPromises.push(task());
            }
          }

          try {
            // Retry any failed DLQ sends before processing new messages
            await this.retryDlqFailures();
            
            // Wait for all key-based tasks to complete
            // Note: tasks handle their own errors and don't throw
            await Promise.all(processingPromises);

            // Send final heartbeat before committing
            await heartbeat();

            // Commit offsets only for successfully processed messages
            if (successfulMessages.length > 0) {
              // Sort all messages (successful + failed) by offset
              const allMessages = [...successfulMessages, ...failedMessages]
                .sort((a, b) => parseInt(a.offset, 10) - parseInt(b.offset, 10));
              
              // Find the highest contiguous successful offset
              let commitOffset = parseInt(allMessages[0].offset, 10);
              
              for (const message of allMessages) {
                const offset = parseInt(message.offset, 10);
                if (successfulMessages.includes(message) && offset === commitOffset) {
                  commitOffset = offset + 1;
                } else {
                  break; // Stop at first gap or failed message
                }
              }
              
              if (commitOffset > parseInt(allMessages[0].offset, 10)) {
                await consumer.commitOffsets([{ 
                  topic: batch.topic, 
                  partition: batch.partition, 
                  offset: commitOffset.toString()
                }]);
                
                this.logger.debug(`[MANUAL COMMIT] Committed offset ${commitOffset} for ${batch.topic}-${batch.partition} (${successfulMessages.length}/${batch.messages.length} messages successful, processed ${processedCount} total)`);
              }
            }
            
            if (failedMessages.length > 0) {
              this.logger.warn(`Batch processing completed with ${failedMessages.length}/${batch.messages.length} failed messages. Failed offsets will be redelivered.`);
            }
            
          } catch (error) {
            this.logger.error(`Critical error in batch processing for topic ${batch.topic}`, {
              error: error.message,
              stack: error.stack
            });
            // Don't commit any offsets if there's a critical error
          }
        } finally {
          // Always clear the heartbeat timer
          clearInterval(heartbeatTimer);
        }
      },
    });
  }

  /**
   * Determines if an error is retryable based on its type and message
   */
  private isRetryableError(error: any): boolean {
    // Network and connection errors are usually retryable
    if (error.code === 'ECONNRESET' || 
        error.code === 'ECONNREFUSED' || 
        error.code === 'ETIMEDOUT' ||
        error.message?.includes('timeout') ||
        error.message?.includes('connection') ||
        error.message?.includes('network')) {
      return true;
    }
    
    // Kafka-specific retryable errors
    if (error.type === 'REBALANCE_IN_PROGRESS' || 
        error.type === 'UNKNOWN_MEMBER_ID' ||
        error.type === 'NOT_COORDINATOR_FOR_GROUP') {
      return true;
    }
    
    // Temporary service unavailable errors
    if (error.message?.includes('Service Unavailable') ||
        error.message?.includes('temporarily unavailable') ||
        error.status === 503) {
      return true;
    }
    
    // Database connection errors (if using database)
    if (error.message?.includes('connection pool') ||
        error.message?.includes('database connection')) {
      return true;
    }
    
    // By default, treat as non-retryable to prevent infinite loops
    return false;
  }

  /**
   * Get the initialization status of the KafkaClient
   */
  getInitializationStatus(): {
    isInitialized: boolean;
    error: string | null;
  } {
    return {
      isInitialized: this.isInitialized,
      error: this.initializationError?.message || null,
    };
  }

  async isHealthy(): Promise<boolean> {
    if (this.isShuttingDown) return false;
    
    // If not initialized, return false but don't log errors (initialization might still be in progress)
    if (!this.isInitialized) {
      return false;
    }
    
    let admin;
    try {
      admin = this.kafka.admin();
      await admin.connect();
      const topics = await admin.listTopics(); // Basic check: can connect and list topics

      // Check both CPU and memory usage
      const cpuUsage = this.getAverageCpuUsage();
      const memUsage = this.getContainerMemoryUsage().usedPercent;

      const healthy =
        topics.length >= 0 && cpuUsage < this.CPU_CRITICAL_THRESHOLD && memUsage < this.MEMORY_CRITICAL_THRESHOLD;

      if (cpuUsage >= this.CPU_HIGH_THRESHOLD) {
        this.logger.warn(`Health Check: High CPU usage: ${cpuUsage.toFixed(2)}%`);
      }

      if (memUsage >= this.MEMORY_THRESHOLD_PERCENT) {
        this.logger.warn(`Health Check: High memory usage: ${memUsage.toFixed(2)}%`);
      }

      if (!healthy) {
        this.logger.warn(
          `Health Check: Unhealthy. Topics: ${topics.length >= 0}, ` +
            `CPU: ${cpuUsage.toFixed(2)}% (Critical: ${this.CPU_CRITICAL_THRESHOLD}%), ` +
            `Memory: ${memUsage.toFixed(2)}% (Critical: ${this.MEMORY_CRITICAL_THRESHOLD}%)`,
        );
      }
      return healthy;
    } catch (error) {
      this.logger.error('Kafka health check failed:', error);
      return false;
    } finally {
      if (admin)
        await admin.disconnect().catch((e: Error) => this.logger.warn('Error disconnecting admin client in health check', e));
    }
  }

    private startCpuMonitoring(): void {
    this.logger.log(
      `Starting CPU monitoring. Check interval: ${this.CPU_CHECK_INTERVAL_MS}ms, High: ${this.CPU_HIGH_THRESHOLD}%, Critical: ${this.CPU_CRITICAL_THRESHOLD}%`,
    );

    // Take initial sample
    const initialCpuUsage = this.getContainerCpuUsage();
    this.cpuUsageSamples.push(initialCpuUsage);


    this.cpuMonitoringTimer = setInterval(() => {
      if (this.isShuttingDown) {
        clearInterval(this.cpuMonitoringTimer!);
        return;
      }

      try {
        const currentCpu = this.getContainerCpuUsage();
        this.cpuUsageSamples.push(currentCpu);
        if (this.cpuUsageSamples.length > this.CPU_SAMPLES_TO_KEEP) {
          this.cpuUsageSamples.shift();
        }
        const avgCpu = this.getAverageCpuUsage();


        // this.logger.debug(
        //   `CPU Usage - Current: ${currentCpu.toFixed(2)}%, Average (${this.cpuUsageSamples.length} samples): ${avgCpu.toFixed(2)}%`,
        // );

        if (avgCpu > this.CPU_CRITICAL_THRESHOLD) {
          if (!this.isCpuCritical) {
            this.isCpuCritical = true;
            this.logger.warn(`CRITICAL CPU USAGE DETECTED: ${avgCpu.toFixed(2)}%. Applying emergency backpressure.`);
            this.applyEmergencyBackpressure(); // This reduces concurrency and can pause consumers

          }
        } else if (this.isCpuCritical && avgCpu < this.CPU_HIGH_THRESHOLD) {
          // Must drop below HIGH to release critical state
          this.isCpuCritical = false;
          this.logger.log(`CPU usage returned to normal: ${avgCpu.toFixed(2)}%. Releasing emergency backpressure.`);
          this.releaseEmergencyBackpressure();
        }

        if (
          !this.isCpuCritical &&
          Date.now() - this.lastConcurrencyAdjustment > this.CONCURRENCY_ADJUSTMENT_COOLDOWN_MS
        ) {
          this.adjustConcurrencyBasedOnCpu(avgCpu);
        }
      } catch (error) {
        this.logger.error('Error in CPU monitoring interval:', error);
      }
    }, this.CPU_CHECK_INTERVAL_MS);
  }

  /**
   * Gets the current container-aware CPU usage
   * This method uses process.cpuUsage() which is more appropriate for containers than os.cpus()
   */
  private getContainerCpuUsage(): number {
    const currentUsage = process.cpuUsage(this.lastCpuUsage);
    const now = Date.now();
    const elapsedMs = now - this.lastCpuTime;

    if (elapsedMs <= 0) {
      return 0; // Avoid division by zero
    }

    // Convert microseconds to milliseconds (1ms = 1000µs)
    const totalUsageMs = (currentUsage.user + currentUsage.system) / 1000;

    // Calculate usage percentage based on elapsed time and available CPUs
    const cpuLimitStr = process.env.CPU_LIMIT;
    const availableCores = cpuLimitStr ? parseFloat(cpuLimitStr) : this.DEFAULT_MAX_CONCURRENCY * 2; // Estimate as twice the max concurrency if not specified

    // Calculate percentage of total available CPU time used
    // If one core is 100% utilized during the entire interval, this would be 100%
    // If all cores are 100% utilized, this would be (100 * availableCores)%
    // We cap at 100% for consistency with other metrics
    const usagePercent = Math.min(100, ((totalUsageMs / elapsedMs) * 100) / availableCores);

    // Store current values for next calculation
    this.lastCpuUsage = process.cpuUsage();
    this.lastCpuTime = now;

    return usagePercent;
  }

  private getAverageCpuUsage(): number {
    if (this.cpuUsageSamples.length === 0) return 0;
    const sum = this.cpuUsageSamples.reduce((acc, val) => acc + val, 0);
    return sum / this.cpuUsageSamples.length;
  }

  /**
   * Gets container-aware memory usage information
   */
  private getContainerMemoryUsage(): { usedPercent: number; rssBytes: number } {
    const memInfo = process.memoryUsage();

    // RSS (Resident Set Size) is the actual memory footprint of the process
    const rssBytes = memInfo.rss;

    // Calculate percentage of container limit used
    const usedPercent = (rssBytes / this.containerMemoryLimitBytes) * 100;



    return { usedPercent, rssBytes };
  }

  private adjustConcurrencyBasedOnCpu(avgCpuUsage: number): void {
    const currentLimit = this.dynamicConcurrencyLimit;
    let newLimit = currentLimit;
    const maxSystemConcurrency = this.getMaxConcurrency(); // The user-defined or CPU-derived max

    if (avgCpuUsage > this.CPU_HIGH_THRESHOLD) {
      newLimit = Math.max(1, Math.floor(currentLimit * 0.8)); // Reduce by 20%
      this.logger.log(
        `[CONCURRENCY] High CPU (${avgCpuUsage.toFixed(2)}%), reducing concurrency: ${currentLimit} -> ${newLimit}`,
      );
    } else if (avgCpuUsage < this.CPU_LOW_THRESHOLD) {
      newLimit = Math.min(
        Math.ceil(currentLimit * 1.2), // Increase by 20%
        maxSystemConcurrency * this.MAX_CONCURRENCY_MULTIPLIER, // Cap at overall max * multiplier
      );
      if (currentLimit !== newLimit) {
        this.logger.log(
          `[CONCURRENCY] Low CPU (${avgCpuUsage.toFixed(2)}%), increasing concurrency: ${currentLimit} -> ${newLimit}`,
        );
      }
    }

    if (newLimit !== currentLimit) {
      this.dynamicConcurrencyLimit = newLimit;
      this.lastConcurrencyAdjustment = Date.now();

      this.logger.log(
        `[CONCURRENCY] Limit adjusted from ${currentLimit} to ${newLimit}. Current avg CPU: ${avgCpuUsage.toFixed(2)}%`,
      );
    }
  }

  private applyEmergencyBackpressure(): void {
    const oldLimit = this.dynamicConcurrencyLimit;
    this.dynamicConcurrencyLimit = Math.max(1, Math.floor(this.getMaxConcurrency() * 0.25)); // Drastically reduce to 25% of max or 1
    this.logger.warn(
      `EMERGENCY BACKPRESSURE: CPU CRITICAL. Concurrency reduced from ${oldLimit} to ${this.dynamicConcurrencyLimit}.`,
    );

    // Pause all known partitions of all consumers
    this.consumerRegistry.forEach((consumer, topic) => {
      const partitionsToPause = this.topicPartitions.get(topic);
      if (consumer && partitionsToPause && partitionsToPause.length > 0) {
        try {
          this.logger.warn(`Emergency pause for topic ${topic}, partitions: ${partitionsToPause.join(',')}`);
          consumer.pause([{ topic, partitions: partitionsToPause }]);
        } catch (e) {
          this.logger.error(`Error during emergency pause for topic ${topic}: ${e.message}`, e.stack);
        }
      }
    });
  }

  private releaseEmergencyBackpressure(): void {
    const oldLimit = this.dynamicConcurrencyLimit;
    // Restore to default max concurrency, let adjustConcurrencyBasedOnCpu fine-tune it later
    this.dynamicConcurrencyLimit = this.getMaxConcurrency();
    this.logger.log(
      `EMERGENCY BACKPRESSURE RELEASED. Concurrency restored from ${oldLimit} to ${this.dynamicConcurrencyLimit}.`,
    );

    // Resume all known partitions
    this.consumerRegistry.forEach((consumer, topic) => {
      const partitionsToResume = this.topicPartitions.get(topic);
      if (consumer && partitionsToResume && partitionsToResume.length > 0) {
        try {
          this.logger.log(`Emergency resume for topic ${topic}, partitions: ${partitionsToResume.join(',')}`);
          consumer.resume([{ topic, partitions: partitionsToResume }]);
        } catch (e) {
          this.logger.error(`Error during emergency resume for topic ${topic}: ${e.message}`, e.stack);
        }
      }
    });
  }

  /**
   * Apply memory-based backpressure when memory usage is critical
   */
  private applyMemoryBackpressure(): void {
    const oldLimit = this.dynamicConcurrencyLimit;
    this.dynamicConcurrencyLimit = Math.max(1, Math.floor(this.getMaxConcurrency() * 0.25)); // Reduce to 25% of max
    this.logger.warn(
      `MEMORY BACKPRESSURE: Memory usage critical. Concurrency reduced from ${oldLimit} to ${this.dynamicConcurrencyLimit}.`,
    );

    // Attempt to trigger garbage collection if available
    if (global.gc && typeof global.gc === 'function') {
      this.logger.log('Triggering garbage collection due to critical memory usage.');
      global.gc();
    }

    // Pause consumers similar to CPU
    this.consumerRegistry.forEach((consumer, topic) => {
      const partitionsToPause = this.topicPartitions.get(topic);
      if (consumer && partitionsToPause && partitionsToPause.length > 0) {
        try {
          this.logger.warn(`Memory-based pause for topic ${topic}, partitions: ${partitionsToPause.join(',')}`);
          consumer.pause([{ topic, partitions: partitionsToPause }]);
        } catch (e) {
          this.logger.error(`Error during memory-based pause for topic ${topic}: ${e.message}`, e.stack);
        }
      }
    });


  }

  /**
   * Release memory-based backpressure when memory usage returns to normal
   */
  private releaseMemoryBackpressure(): void {
    const oldLimit = this.dynamicConcurrencyLimit;
    // Restore to default max concurrency
    this.dynamicConcurrencyLimit = this.getMaxConcurrency();
    this.logger.log(
      `MEMORY BACKPRESSURE RELEASED. Concurrency restored from ${oldLimit} to ${this.dynamicConcurrencyLimit}.`,
    );

    // Resume all paused partitions
    this.consumerRegistry.forEach((consumer, topic) => {
      const partitionsToResume = this.topicPartitions.get(topic);
      if (consumer && partitionsToResume && partitionsToResume.length > 0) {
        try {
          this.logger.log(`Memory-based resume for topic ${topic}, partitions: ${partitionsToResume.join(',')}`);
          consumer.resume([{ topic, partitions: partitionsToResume }]);
        } catch (e) {
          this.logger.error(`Error during memory-based resume for topic ${topic}: ${e.message}`, e.stack);
        }
      }
    });
  }

  private startMemoryMonitoring(): void {
    this.logger.log(
      `Starting container-aware memory monitoring. Check interval: ${this.GC_INTERVAL_MS}ms, Warning Threshold: ${this.MEMORY_THRESHOLD_PERCENT}%, Critical Threshold: ${this.MEMORY_CRITICAL_THRESHOLD}%`,
    );

    // Take initial reading
    const initialMemory = this.getContainerMemoryUsage();


    // Track memory monitoring state to reduce verbose logging
    let lastLoggedUsage = 0;
    let logCounter = 0;
    const LOG_EVERY_N_CHECKS = 12; // Log every 12 checks (1 hour) when usage is normal

    this.gcTimer = setInterval(() => {
      if (this.isShuttingDown) {
        clearInterval(this.gcTimer!);
        return;
      }

      try {
        const { usedPercent, rssBytes } = this.getContainerMemoryUsage();



        // Determine if we should log based on memory usage and time
        const shouldLog = 
          usedPercent > this.MEMORY_THRESHOLD_PERCENT || // Always log if above warning threshold
          Math.abs(usedPercent - lastLoggedUsage) > 10 || // Log if usage changed significantly (>10%)
          logCounter % LOG_EVERY_N_CHECKS === 0; // Log periodically when stable

        // Log memory usage - use appropriate log level
        if (shouldLog) {
          const memoryMessage = `Memory Usage: ${usedPercent.toFixed(2)}% of container limit (${(rssBytes / (1024 * 1024)).toFixed(2)} MB / ${(this.containerMemoryLimitBytes / (1024 * 1024)).toFixed(2)} MB)`;
          
          if (usedPercent > this.MEMORY_THRESHOLD_PERCENT) {
            this.logger.warn(memoryMessage);
          } else {
            // Use configured log level for normal memory usage
            switch (this.memoryLogLevel) {
              case 'info':
                this.logger.log(memoryMessage);
                break;
              case 'warn':
                this.logger.warn(memoryMessage);
                break;
              case 'debug':
              default:
                this.logger.debug(memoryMessage);
                break;
            }
          }
          lastLoggedUsage = usedPercent;
        }

        logCounter++;

        // Handle warning threshold - trigger GC if possible
        if (usedPercent > this.MEMORY_THRESHOLD_PERCENT && usedPercent < this.MEMORY_CRITICAL_THRESHOLD) {
          this.logger.warn(
            `High memory usage detected: ${usedPercent.toFixed(2)}% of container limit (${(rssBytes / (1024 * 1024)).toFixed(2)} MB / ${(this.containerMemoryLimitBytes / (1024 * 1024)).toFixed(2)} MB)`,
          );

          if (global.gc && typeof global.gc === 'function') {
            this.logger.log('Attempting to trigger garbage collection due to high memory usage.');
            global.gc();
          } else {
            this.logger.warn(
              'Manual garbage collection (global.gc) not available. Start Node.js with --expose-gc flag if manual GC is desired.',
            );
          }
        }

        // Handle critical threshold - apply backpressure
        if (usedPercent > this.MEMORY_CRITICAL_THRESHOLD) {
          if (!this.isMemoryCritical) {
            this.isMemoryCritical = true;
            this.logger.error(
              `CRITICAL memory usage: ${usedPercent.toFixed(2)}% of container limit. Applying memory backpressure.`,
            );
            this.applyMemoryBackpressure();
          }
        } else if (this.isMemoryCritical && usedPercent < this.MEMORY_THRESHOLD_PERCENT) {
          // Only release when it's fallen below the warning threshold
          this.isMemoryCritical = false;
          this.logger.log(
            `Memory usage returned to normal: ${usedPercent.toFixed(2)}% of container limit. Releasing memory backpressure.`,
          );
          this.releaseMemoryBackpressure();
        }
      } catch (error) {
        this.logger.error('Error in memory monitoring interval:', error);
      }
    }, this.GC_INTERVAL_MS);
  }

  private startConsumerWatchdog(groupId: string, consumer: Consumer): void {
    const WATCHDOG_INTERVAL_MS = 300000; // 5 minutes (increased from 1 minute)
    const MAX_IDLE_TIME_MS = 900000; // 15 minutes (increased from 5 minutes)

    const lastMessageTime = new Map<string, number>();
    let hasLoggedIdleWarning = new Map<string, boolean>();

    setInterval(() => {
      if (this.isShuttingDown) return;

      const now = Date.now();
      this.topicPartitions.forEach((partitions, topic) => {
        const topicKey = `${topic}-${groupId}`;
        const lastMsgTime = lastMessageTime.get(topicKey) || now;
        const idleTimeMs = now - lastMsgTime;

        // If no messages have been processed for too long
        if (idleTimeMs > MAX_IDLE_TIME_MS) {
          // Only log once per idle period to reduce spam
          if (!hasLoggedIdleWarning.get(topicKey)) {
            this.logger.warn(
              `Watchdog: No messages processed for ${topic} in consumer group ${groupId} for ${Math.round(idleTimeMs / 1000 / 60)} minutes. Checking health...`,
            );
            hasLoggedIdleWarning.set(topicKey, true);
          }

          // Only perform health check and restart if idle time is excessive (30+ minutes)
          if (idleTimeMs > 1800000) { // 30 minutes
            this.isHealthy().then((healthy) => {
              if (!healthy) {
                this.logger.error(`Consumer appears unhealthy after ${Math.round(idleTimeMs / 1000 / 60)} minutes of inactivity. Attempting to restart consumer for ${groupId}`);
                this.restartConsumer(groupId, consumer);
                lastMessageTime.set(topicKey, Date.now());
                hasLoggedIdleWarning.set(topicKey, false);
              }
            });
          }
        } else {
          // Reset warning flag when messages resume
          if (hasLoggedIdleWarning.get(topicKey)) {
            hasLoggedIdleWarning.set(topicKey, false);
            this.logger.log(`Consumer activity resumed for ${topic} in group ${groupId}`);
          }
        }
      });
    }, WATCHDOG_INTERVAL_MS);

    // Update last message time whenever a message is processed
    // Add this to the message processing logic
  }

  private restartConsumer(groupId: string, consumer: Consumer): void {
    try {
      // Disconnect the unhealthy consumer
      consumer
        .disconnect()
        .then(async () => {
          this.logger.log(`Successfully disconnected unhealthy consumer for ${groupId}`);
          const delay = await setTimeout(5000, 1);
          return consumer.connect();
        })
        .then(() => {
          this.logger.log(`Successfully reconnected consumer for ${groupId}`);

          // Re-subscribe to topics
          return consumer.subscribe({
            topics: Array.from(this.topicPartitions.keys()),
            fromBeginning: false,
          });
        })
        .then(() => {
          this.logger.log(`Successfully resubscribed to topics for ${groupId}`);
        })
        .catch((error: Error) => {
          this.logger.error(`Failed to restart consumer for ${groupId}:`, error);
          // If restart fails, we might want to trigger an alert or take additional action
        });
    } catch (error) {
      this.logger.error(`Error during consumer restart for ${groupId}:`, error);
    }
  }

  private trackProcessingTime(processingTimeMs: number): void {
    // Processing time tracking can be added here if needed
  }



  /**
   * Chunks an array into smaller arrays of specified size
   */
  private chunkArray<T>(array: T[], chunkSize: number): T[][] {
    const chunks: T[][] = [];
    for (let i = 0; i < array.length; i += chunkSize) {
      chunks.push(array.slice(i, i + chunkSize));
    }
    return chunks;
  }
}

export default KafkaClient;
