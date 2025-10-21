import { DynamicModule, Global, Module } from '@nestjs/common';
import { randomUUID } from 'crypto';
import { KafkaClient } from './kafka.client';
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
