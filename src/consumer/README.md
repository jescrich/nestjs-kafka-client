# Consumer Module - Non-Blocking Kafka Integration

The Consumer Module provides a non-blocking way to integrate Kafka consumers with NestJS applications, preventing Kafka connection issues from blocking the application bootstrap process.

## Key Features

- **Non-blocking Bootstrap**: Consumer initialization happens asynchronously after NestJS bootstrap completes
- **Automatic Retry**: Failed consumer connections are automatically retried with exponential backoff
- **Health Checks**: Built-in health indicators for monitoring consumer status
- **Graceful Degradation**: Application remains functional even when Kafka is unavailable

## Usage

### Basic Setup

```typescript
import { Module } from '@nestjs/common';
import { ConsumerModule } from '@this/consumer';
import { UserEventHandler } from './handlers/user-event.handler';

@Module({
  imports: [
    ConsumerModule.register({
      name: 'my-service',
      brokers: process.env.KAFKA_BROKERS || 'localhost:9092',
      consumers: [UserEventHandler],
      enableMetrics: true,
      options: {
        maxConcurrency: 5,
        connectionRetryDelay: 5000,
        connectionMaxRetries: 10,
      }
    })
  ]
})
export class AppModule {}
```

### Event Handler Implementation

```typescript
import { Injectable } from '@nestjs/common';
import { Handler, IEventHandler } from '@this/consumer';

interface UserEvent {
  id: string;
  type: 'created' | 'updated' | 'deleted';
  userId: string;
  timestamp: string;
}

@Handler({ topic: 'user-events' })
@Injectable()
export class UserEventHandler implements IEventHandler<UserEvent> {
  async handle({ key, event, payload }: {
    key: string;
    event: UserEvent;
    payload: any;
  }): Promise<void> {
    console.log(`Processing user event: ${event.type} for user ${event.userId}`);
    
    // Your business logic here
    switch (event.type) {
      case 'created':
        await this.handleUserCreated(event);
        break;
      case 'updated':
        await this.handleUserUpdated(event);
        break;
      case 'deleted':
        await this.handleUserDeleted(event);
        break;
    }
  }

  // Optional: Batch processing for better performance
  async handleBatch({ key, events }: { 
    key: string; 
    events: UserEvent[] 
  }): Promise<void> {
    console.log(`Processing batch of ${events.length} events for key: ${key}`);
    
    // Process events in batch for better performance
    const createdEvents = events.filter(e => e.type === 'created');
    const updatedEvents = events.filter(e => e.type === 'updated');
    const deletedEvents = events.filter(e => e.type === 'deleted');
    
    await Promise.all([
      this.batchHandleCreated(createdEvents),
      this.batchHandleUpdated(updatedEvents),
      this.batchHandleDeleted(deletedEvents)
    ]);
  }

  private async handleUserCreated(event: UserEvent): Promise<void> {
    // Implementation
  }

  private async handleUserUpdated(event: UserEvent): Promise<void> {
    // Implementation
  }

  private async handleUserDeleted(event: UserEvent): Promise<void> {
    // Implementation
  }

  private async batchHandleCreated(events: UserEvent[]): Promise<void> {
    // Batch implementation
  }

  private async batchHandleUpdated(events: UserEvent[]): Promise<void> {
    // Batch implementation
  }

  private async batchHandleDeleted(events: UserEvent[]): Promise<void> {
    // Batch implementation
  }
}
```

### Health Checks

```typescript
import { Controller, Get } from '@nestjs/common';
import { HealthCheck, HealthCheckService } from '@nestjs/terminus';
import { ConsumerHealthIndicator } from '@this/consumer';

@Controller('health')
export class HealthController {
  constructor(
    private health: HealthCheckService,
    private consumerHealth: ConsumerHealthIndicator,
  ) {}

  @Get()
  @HealthCheck()
  check() {
    return this.health.check([
      () => this.consumerHealth.isHealthy('kafka-consumer'),
    ]);
  }

  @Get('consumer')
  async getConsumerStatus() {
    // Get detailed consumer status
    return await this.consumerService.getAsyncHealthStatus();
  }
}
```

## Configuration Options

### Consumer Module Options

```typescript
interface ConsumerModuleOptions {
  name: string;                    // Service name for consumer group
  brokers: string;                 // Kafka brokers (comma-separated)
  consumers: Type<any>[];          // Array of consumer handler classes
  enableMetrics?: boolean;         // Enable metrics collection
  providers?: Type<any>[];         // Additional providers
  
  options?: {
    maxConcurrency?: number;              // Max concurrent message processing
    batchSizeMultiplier?: number;         // Batch size multiplier
    connectionRetryDelay?: number;        // Delay between connection retries (ms)
    connectionMaxRetries?: number;        // Max connection retry attempts
    enableCpuMonitoring?: boolean;        // Enable CPU monitoring
    enableMemoryMonitoring?: boolean;     // Enable memory monitoring
    memoryLogLevel?: 'debug' | 'info' | 'warn';
    dlqSuffix?: string;                   // Dead letter queue suffix
    messageRetryLimit?: number;           // Max message retry attempts
    messageRetryDelayMs?: number;         // Delay between message retries
    containerMemoryLimitMB?: number;      // Container memory limit
    fromBeginning?: boolean;              // Start from beginning of topic
    sessionTimeout?: number;              // Consumer session timeout
    heartbeatInterval?: number;           // Heartbeat interval
    batchAccumulationDelayMs?: number;    // Batch accumulation delay
    minBatchSize?: number;                // Minimum batch size
  };
  
  metricsOptions?: {
    redis?: {
      host: string;
      port: number;
      password?: string;
      db?: number;
      keyPrefix?: string;
      cluster?: boolean;
    };
    maxHistoryPoints?: number;
    metricsRetentionHours?: number;
    collectionIntervalMs?: number;
  };
}
```

## Behavior Changes

### Before (Blocking)
- Consumer initialization happened during `onModuleInit()`
- Kafka connection failures would block NestJS bootstrap
- Application wouldn't start if Kafka was unavailable
- Long retry cycles could delay startup by several minutes

### After (Non-Blocking)
- Consumer initialization happens asynchronously after bootstrap
- NestJS application starts immediately regardless of Kafka status
- Failed connections are retried automatically with exponential backoff
- Health checks provide visibility into consumer status
- Application remains functional even when consumers are not connected

## Error Handling

The module implements comprehensive error handling:

1. **Connection Failures**: Automatic retry with exponential backoff
2. **Initialization Errors**: Logged and retried after delay
3. **Runtime Errors**: Proper error categorization and DLQ handling
4. **Health Monitoring**: Real-time status reporting

## Monitoring

### Health Status Response

```json
{
  "isInitialized": true,
  "error": null,
  "consumerCount": 3,
  "kafkaHealthy": true
}
```

### Log Messages

The module provides structured logging for:
- Consumer initialization progress
- Connection retry attempts
- Error conditions
- Health status changes

## Best Practices

1. **Always implement health checks** to monitor consumer status
2. **Use batch processing** when possible for better performance
3. **Handle errors gracefully** in your event handlers
4. **Monitor consumer lag** and processing metrics
5. **Test with Kafka unavailable** to ensure graceful degradation
6. **Configure appropriate timeouts** based on your use case

## Migration Guide

If you're upgrading from a blocking consumer implementation:

1. Update your health checks to use `ConsumerHealthIndicator`
2. Ensure your application can handle delayed consumer initialization
3. Test startup behavior when Kafka is unavailable
4. Update monitoring to check consumer health status
5. Consider implementing graceful degradation for critical paths

## Troubleshooting

### Consumer Not Starting
- Check Kafka broker connectivity
- Verify consumer configuration
- Review application logs for initialization errors
- Use health endpoint to check status

### Slow Startup
- Reduce `connectionMaxRetries` for faster failure detection
- Adjust `connectionRetryDelay` for quicker retries
- Consider using health checks to verify readiness

### Memory Issues
- Enable memory monitoring in options
- Adjust `maxConcurrency` based on available resources
- Monitor batch processing performance
