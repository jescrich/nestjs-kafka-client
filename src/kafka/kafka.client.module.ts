import { DynamicModule, Global, Module } from '@nestjs/common';
import { KafkaMessage } from '@nestjs/microservices/external/kafka.interface';
import { randomUUID } from 'crypto';
import { KafkaClient, TopicToEnsure } from './kafka.client';
import { IdempotencyStore } from './idempotency.store';
import { KafkaConnectionManager } from './kafka.connection.manager';
import { KafkaHealthIndicator } from './kafka.health';

@Global()
@Module({})
export class KafkaModule {
  static register(params: {
    serviceId?: string; // Unique identifier for this microservice
    clientId: string;
    brokers: string;
    options?: {
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
      producerIdempotent?: boolean;
      producerMaxInFlightRequests?: number;
      producerTransactionalId?: string;
      producerAcks?: number;
      ensureTopicsOnConnect?: boolean;
      topicsToEnsure?: TopicToEnsure[];
      autoCreateDlqTopics?: boolean;
      defaultNumPartitions?: number;
      defaultReplicationFactor?: number;
      retryBackoffStrategy?: 'fixed' | 'exponential';
      retryBackoffMaxMs?: number;
      idempotencyStore?: IdempotencyStore;
      idempotencyKeyExtractor?: (message: KafkaMessage, topic: string) => string | null;
      useEnvelope?: boolean;
      validateEnvelopeOnConsume?: boolean;
    };
  }): DynamicModule {
    const { clientId, brokers, options } = params;

    if (!brokers) {
      throw new Error('Unable to create KafkaClientModule: missing brokers');
    }

    const providers: any[] = [
      {
        provide: KafkaClient,
        useFactory: () => {
          const fullClientId = clientId + '-' + randomUUID().substring(0, 8);
          return KafkaConnectionManager.getOrCreateClient(
            brokers, 
            fullClientId, 
            options
          );
        },
      },
      {
        provide: KafkaHealthIndicator,
        useFactory: (client: KafkaClient) =>
          new KafkaHealthIndicator(client, {
            clientId,
            brokers,
          }),
        inject: [KafkaClient],
      },
    ];

    const imports: any[] = [];
    const exports: any[] = [KafkaClient, KafkaHealthIndicator];

    return {
      module: KafkaModule,
      providers,
      imports,
      exports,
      global: true,
    };
  }
}
