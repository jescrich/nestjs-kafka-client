# NestJS Kafka - Enterprise Client

A production-ready NestJS module for Kafka client and consumer functionality built on top of [kafkajs](https://kafka.js.org/). This library provides enterprise-grade features including intelligent batch processing, idempotency guarantees, key-based grouping, and automatic pressure management.

## Features

### Core Functionality
- **Kafka Client**: High-performance Kafka producer with intelligent connection management
- **Kafka Consumer**: Enterprise-grade consumer with advanced batch processing capabilities
- **Built on KafkaJS**: Leverages the robust kafkajs library with additional enterprise features

### Advanced Processing
- **Intelligent Batch Processing**: Automatically groups messages for optimal throughput
- **Key-Based Grouping**: Groups messages by key within batches for ordered processing
- **Idempotency Support**: Built-in mechanisms to prevent duplicate message processing
- **Back Pressure Handling**: Automatic throttling when downstream systems are overwhelmed
- **Front Pressure Management**: Smart buffering and flow control for incoming messages

### Reliability & Monitoring
- **Dead Letter Queue (DLQ)**: Automatic handling and routing of failed messages
- **Health Monitoring**: Comprehensive health checks for Kafka connections and consumers
- **Connection Management**: Automatic connection pooling, reconnection, and failover
- **Graceful Shutdown**: Proper cleanup and message completion on application shutdown

## Installation

```bash
npm install @jescrich/nestjs-kafka-client
```

## Usage

### Basic Setup

```typescript
import { KafkaModule, ConsumerModule } from '@jescrich/nestjs-kafka-client';

@Module({
  imports: [
    KafkaModule.register({
      clientId: 'my-app',
      brokers: 'localhost:9092', // String with comma-separated brokers
      options: {
        maxConcurrency: 5,
        batchSizeMultiplier: 10,
        enableCpuMonitoring: true,
        messageRetryLimit: 3,
      },
    }),
    ConsumerModule,
  ],
})
export class AppModule {}
```

### Kafka Producer

```typescript
import { KafkaClient } from '@jescrich/nestjs-kafka-client';

@Injectable()
export class OrderService {
  constructor(private readonly kafkaClient: KafkaClient) {}

  async createOrder(order: Order) {
    // Send with automatic batching and idempotency
    await this.kafkaClient.send('orders', {
      key: order.customerId, // Messages with same key are processed in order
      value: JSON.stringify(order),
      headers: {
        'idempotency-key': order.id, // Prevents duplicate processing
      },
    });
  }

  async sendBatch(orders: Order[]) {
    // Efficient batch sending
    await this.kafkaClient.sendBatch('orders', 
      orders.map(order => ({
        key: order.customerId,
        value: JSON.stringify(order),
      }))
    );
  }
}
```

### Kafka Consumer

#### Simple Consumer
```typescript
import { Consumer } from '@jescrich/nestjs-kafka-client';

@Consumer('orders')
export class OrderConsumer {
  async handleMessage(message: KafkaMessage) {
    const order = JSON.parse(message.value.toString());
    console.log('Processing order:', order);
    
    // Automatic commit after successful processing
    // Built-in error handling with DLQ support
  }
}
```

#### Batch Consumer with Key Grouping
```typescript
@Consumer('orders', {
  batch: true,
  batchSize: 100,
  batchTimeout: 5000, // Process batch every 5 seconds or when full
  groupByKey: true,   // Group messages by key within batch
})
export class OrderBatchConsumer {
  async handleBatch(messages: KafkaMessage[]) {
    // Messages are automatically grouped by key
    // All messages for customer 'A' will be in sequence
    const ordersByCustomer = this.groupByCustomer(messages);
    
    await Promise.all(
      Object.entries(ordersByCustomer).map(([customerId, orders]) =>
        this.processCustomerOrders(customerId, orders)
      )
    );
  }

  private groupByCustomer(messages: KafkaMessage[]) {
    return messages.reduce((acc, msg) => {
      const customerId = msg.key?.toString();
      if (!acc[customerId]) acc[customerId] = [];
      acc[customerId].push(JSON.parse(msg.value.toString()));
      return acc;
    }, {});
  }
}
```

#### Advanced Consumer with Pressure Management
```typescript
@Consumer('high-volume-topic', {
  batch: true,
  batchSize: 500,
  maxConcurrency: 10,        // Limit concurrent batch processing
  backPressureThreshold: 80, // Pause consumption at 80% capacity
  idempotencyKey: (msg) => msg.headers['idempotency-key'], // Custom idempotency
})
export class HighVolumeConsumer {
  async handleBatch(messages: KafkaMessage[]) {
    // Automatic back pressure management
    // If processing falls behind, consumption will pause
    // Front pressure is managed through intelligent buffering
    
    await this.processMessages(messages);
  }
}
```

## Advanced Features

### Batch Processing & Key Grouping

The library automatically handles intelligent batching:

- **Automatic Batching**: Messages are collected into optimal batch sizes
- **Key-Based Ordering**: Messages with the same key are processed in order within batches
- **Configurable Batch Sizes**: Set `batchSize` and `batchTimeout` per consumer
- **Memory Efficient**: Streaming processing prevents memory overflow

### Pressure Management

#### Back Pressure
When your application can't keep up with incoming messages:
- Automatic pause/resume of Kafka consumption
- Configurable thresholds based on queue depth or processing time
- Graceful degradation to prevent system overload

#### Front Pressure  
When Kafka brokers are overwhelmed:
- Intelligent retry mechanisms with exponential backoff
- Circuit breaker patterns for failing brokers
- Automatic connection pooling and load balancing

### Idempotency

Prevent duplicate message processing:

```typescript
@Consumer('payments', {
  idempotencyKey: (message) => message.headers['transaction-id'],
  idempotencyTtl: 3600000, // 1 hour
})
export class PaymentConsumer {
  async handleMessage(message: KafkaMessage) {
    // This message will only be processed once per transaction-id
    // Duplicates are automatically filtered out
  }
}
```

### Dead Letter Queue (DLQ)

Automatic handling of failed messages:

```typescript
@Consumer('orders', {
  dlq: {
    topic: 'orders-dlq',
    maxRetries: 3,
    retryDelay: 1000, // 1 second between retries
  }
})
export class OrderConsumer {
  async handleMessage(message: KafkaMessage) {
    // If this throws an error 3 times, message goes to DLQ
    await this.processOrder(message);
  }
}
```

### Health Monitoring

Built-in health checks for monitoring:

```typescript
import { KafkaHealthIndicator } from '@jescrich/nestjs-kafka-client';

@Controller('health')
export class HealthController {
  constructor(private kafkaHealth: KafkaHealthIndicator) {}

  @Get()
  async check() {
    return this.kafkaHealth.isHealthy('kafka');
  }
}
```

## Configuration Options

### KafkaModule Configuration

```typescript
KafkaModule.register({
  // Required
  clientId: 'my-app',
  brokers: 'localhost:9092', // String with comma-separated brokers
  
  // Optional: Unique identifier for this microservice
  serviceId: 'my-service',
  
  // Optional: Advanced options
  options: {
    // Concurrency Management
    maxConcurrency: 5,              // Default: Math.floor(cpus().length / 2)
    batchSizeMultiplier: 10,        // Default: 10
    
    // Connection Management
    connectionRetryDelay: 5000,     // Default: 5000ms
    connectionMaxRetries: 10,       // Default: 10
    
    // Resource Monitoring
    enableCpuMonitoring: true,      // Default: true
    enableMemoryMonitoring: true,   // Default: false
    memoryLogLevel: 'info',         // 'debug' | 'info' | 'warn'
    containerMemoryLimitMB: 512,    // Auto-detect from env vars
    
    // Message Processing
    messageRetryLimit: 3,           // Default: 3
    messageRetryDelayMs: 30000,     // Default: 30000ms
    dlqSuffix: '-dlq',              // Default: '-dlq'
    
    // Consumer Configuration
    fromBeginning: false,           // Default: false
    sessionTimeout: 30000,          // Default: 30000ms
    heartbeatInterval: 10000,       // Default: 10000ms
    
    // Batch Processing
    batchAccumulationDelayMs: 100,  // Delay to accumulate messages
    minBatchSize: 1,                // Minimum batch size before processing

    // Retry Backoff (consumer redelivery)
    retryBackoffStrategy: 'exponential', // 'fixed' | 'exponential' (default: 'exponential')
    retryBackoffMaxMs: 30000,            // Cap for exponential backoff (default: 30000ms)

    // Idempotent Producer
    producerIdempotent: false,           // Enable idempotent producer (default: false)
    producerMaxInFlightRequests: 5,      // Required <= 5 when idempotent (default: 5)
    producerTransactionalId: undefined,  // Optional; implies idempotent
    producerAcks: -1,                    // Acks per send (default: -1 / all)

    // Cold-boot: ensure topics exist
    ensureTopicsOnConnect: false,        // Create subscribed topics before consuming (default: false)
    autoCreateDlqTopics: true,           // Also create `${topic}${dlqSuffix}` (default: true)
    topicsToEnsure: [                    // Explicit topics to create on init (producer-only topics)
      { topic: 'orders', numPartitions: 3, replicationFactor: 1 },
    ],
    defaultNumPartitions: 1,             // Default partitions for ensured topics (default: 1)
    defaultReplicationFactor: 1,         // Default replication for ensured topics (default: 1)

    // Idempotency / dedup (skip already-processed messages)
    idempotencyStore: new InMemoryIdempotencyStore({ ttlMs: 3_600_000 }), // pluggable (Redis/DB)
    idempotencyKeyExtractor: (msg, topic) => msg.headers?.['x-message-id']?.toString() ?? null,

    // Envelope contract (optional message envelope)
    useEnvelope: false,                  // Unwrap KafkaEnvelope before handing to handler (default: false)
    validateEnvelopeOnConsume: false,    // Reject invalid envelopes to DLQ (default: false)
  }
})
```

### Consumer Configuration

```typescript
@Consumer('my-topic', {
  // Batch processing
  batch: true,
  batchSize: 100,
  batchTimeout: 5000,
  
  // Key grouping and ordering
  groupByKey: true,
  
  // Pressure management
  maxConcurrency: 5,
  backPressureThreshold: 80,
  
  // Idempotency
  idempotencyKey: (msg) => msg.headers['id'],
  idempotencyTtl: 3600000,
  
  // Error handling
  dlq: {
    topic: 'my-topic-dlq',
    maxRetries: 3,
    retryDelay: 1000,
  },
  
  // Consumer group settings
  groupId: 'my-consumer-group',
  sessionTimeout: 30000,
  heartbeatInterval: 3000,
})
```

## Enterprise Features

### Exponential Retry Backoff

When a message fails with a retryable error, the consumer now pauses the affected
topic-partition for an exponentially increasing delay (base = `messageRetryDelayMs`,
doubling per attempt, with jitter, capped at `retryBackoffMaxMs`) before the offset is
redelivered. Set `retryBackoffStrategy: 'fixed'` to keep a constant delay.

### Idempotent Producer

```typescript
KafkaModule.register({
  clientId: 'my-app',
  brokers: 'localhost:9092',
  options: {
    producerIdempotent: true,        // exactly-once-ish delivery to the broker
    producerMaxInFlightRequests: 5,  // must be <= 5 when idempotent
    producerAcks: -1,                // wait for all in-sync replicas
    // producerTransactionalId: 'my-app-tx', // optional; implies idempotent
  },
});
```

### Cold-boot Topic Creation (`ensureTopics`)

```typescript
// Auto-create subscribed topics (and their DLQ) before consuming:
options: { ensureTopicsOnConnect: true, autoCreateDlqTopics: true }

// Or create producer-only topics on startup:
options: {
  topicsToEnsure: [{ topic: 'orders', numPartitions: 3, replicationFactor: 3 }],
}

// Programmatically:
await kafkaClient.ensureTopics([{ topic: 'orders', numPartitions: 3 }]);
```

### Idempotency / Dedup

A pluggable `IdempotencyStore` lets the consumer skip messages it has already processed.
A default in-memory implementation is provided; swap it for Redis/DB in production.

```typescript
import { InMemoryIdempotencyStore } from '@jescrich/nestjs-kafka-client';

options: {
  idempotencyStore: new InMemoryIdempotencyStore({ ttlMs: 3_600_000, maxEntries: 100_000 }),
  // The id resolves from (in order): idempotencyKeyExtractor → envelope.eventId →
  // header `x-message-id` / `x-event-id`. If no id is found, dedup is skipped.
  idempotencyKeyExtractor: (msg, topic) => msg.headers?.['x-message-id']?.toString() ?? null,
}
```

```typescript
// Custom store contract:
interface IdempotencyStore {
  isProcessed(messageId: string): Promise<boolean>;
  markProcessed(messageId: string, ttlMs?: number): Promise<void>;
}
```

### Message Envelope Contract

An optional, generic envelope standardizes event metadata (id, type, tenant, version,
trace). When `useEnvelope` is enabled, the consumer unwraps it and hands the inner
`payload` to your handler; `envelope.eventId` is used as the default idempotency id.

```typescript
import { createEnvelope, KafkaEnvelope } from '@jescrich/nestjs-kafka-client';

// Producer side:
const envelope = createEnvelope({ orderId: 42 }, {
  eventType: 'order.created',
  tenant: 'acme',
  traceId: 'abc-123',
});
await kafkaClient.produce('orders', 'order-42', envelope);

// Consumer side:
options: { useEnvelope: true, validateEnvelopeOnConsume: true }
// Invalid envelopes are routed to the DLQ (non-retryable).
```

## Performance Benefits

### Throughput Improvements
- **Batch Processing**: Up to 10x throughput improvement over single message processing
- **Key Grouping**: Maintains ordering while maximizing parallelism
- **Connection Pooling**: Reduces connection overhead and improves resource utilization

### Reliability Features
- **Automatic Retries**: Built-in retry logic with exponential backoff
- **Circuit Breakers**: Prevent cascade failures in distributed systems
- **Graceful Shutdown**: Ensures all in-flight messages are processed before shutdown

### Memory Management
- **Streaming Processing**: Processes large batches without loading everything into memory
- **Back Pressure**: Prevents memory overflow during high load periods
- **Efficient Serialization**: Optimized message serialization and deserialization

## Best Practices

1. **Use Key-Based Partitioning**: Ensure related messages have the same key for ordering
2. **Configure Appropriate Batch Sizes**: Balance latency vs throughput based on your use case
3. **Monitor Consumer Lag**: Use built-in health checks to monitor system performance
4. **Implement Idempotency**: Always use idempotency keys for critical business operations
5. **Set Up DLQ**: Configure dead letter queues for proper error handling
6. **Use Environment-Specific Configuration**: Different settings for dev/staging/production

## Troubleshooting

### High Consumer Lag
- Increase `batchSize` and `maxConcurrency`
- Check if back pressure is being triggered
- Monitor downstream system performance

### Memory Issues
- Reduce `batchSize` if processing large messages
- Enable back pressure with lower thresholds
- Check for memory leaks in message handlers

### Connection Issues
- Verify broker connectivity and authentication
- Check SSL/SASL configuration
- Monitor connection pool metrics

## License

MIT
