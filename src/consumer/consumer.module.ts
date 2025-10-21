import { DynamicModule, Logger, Module, Type } from '@nestjs/common';
import { ConsumerService } from './consumer.service';
import { KafkaClient, KafkaModule } from '@this/kafka';
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
            return new ConsumerService(params.name, kafkaClient, consumerRef, moduleRef);
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
