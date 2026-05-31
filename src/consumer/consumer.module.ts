import { DynamicModule, Logger, Module, Type } from '@nestjs/common';
import { KafkaMessage } from '@nestjs/microservices/external/kafka.interface';
import { ConsumerService } from './consumer.service';
import { KafkaClient, KafkaModule, TopicToEnsure, IdempotencyStore } from '@this/kafka';
import { ModuleRef } from '@nestjs/core';
import { ConsumerRefService } from './consumer.ref';
import { ConsumerHealthIndicator } from './consumer.health';

@Module({})
export class ConsumerModule {
  static register(params: {
    name: string;
    brokers: string;
    providers?: Type<any>[];
    consumers: Type<any>[];
    /** When set, every consumer is registered once per tenant on its own consumer group. */
    tenants?: string[];
    /** Consumer-group template, supports `{tenant}` and `{name}` placeholders. */
    groupId?: string;
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
    const { name, consumers, providers } = params;

    return {
      module: ConsumerModule,
      imports: [
        KafkaModule.register({
          ...params,
          clientId: name + '-client',
          options: {
            ...params.options,
          },
          serviceId: params.name,
        }),
      ],
      providers: [
        {
          provide: ConsumerRefService,
          useFactory: () => {
            return new ConsumerRefService(consumers);
          },
        },
        {
          provide: ConsumerService,
          useFactory: (kafkaClient: KafkaClient, consumerRef: ConsumerRefService, moduleRef: ModuleRef) => {
            Logger.log('ConsumerService moduleref ' + moduleRef, 'ConsumerService');
            return new ConsumerService(params.name, kafkaClient, consumerRef, moduleRef, {
              tenants: params.tenants,
              groupId: params.groupId,
            });
          },
          inject: [KafkaClient, ConsumerRefService, ModuleRef],
        },
        {
          provide: ConsumerHealthIndicator,
          useFactory: (consumerService: ConsumerService) => {
            return new ConsumerHealthIndicator(consumerService);
          },
          inject: [ConsumerService],
        },
        ...(providers ?? []),
        ...params.consumers,
      ],
      exports: [ConsumerService, ConsumerHealthIndicator],
    };
  }
}
