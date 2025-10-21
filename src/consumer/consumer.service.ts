import { IEventHandler, KafkaClient } from '@this/kafka';
import { ConsumerDef } from './consumer.def';
import { ExecutionContext, Injectable, Logger, OnModuleInit, Type } from '@nestjs/common';
import { ModuleRef } from '@nestjs/core';
import { Consumer } from '..';
import { ConsumerRefService } from './consumer.ref';

@Injectable()
export class ConsumerService implements OnModuleInit {
  private readonly logger = new Logger(ConsumerService.name);
  private isInitialized = false;
  private initializationError: Error | null = null;
  private consumerDefinitions: ConsumerDef<any>[] = [];
  
  constructor(
    private readonly name: string,
    private readonly kafkaClient: KafkaClient,
    private readonly consumerRef: ConsumerRefService,
    private readonly moduleRef: ModuleRef,
  ) {
    this.logger.log(`Initializing consumer ${this.name}`);
  }

  async onModuleInit() {
    const providers = this.consumerRef.resolve();

    // Get all providers with @Consumer decorator
    const consumers = providers.filter((provider: Object) => Reflect.hasMetadata('topic-consumer', provider));

    // Create consumer definitions
    const definitions = consumers.map((ConsumerClass: string | symbol | Function | Type<any>) => {
      const instance = this.moduleRef.get(ConsumerClass);
      const topic = Reflect.getMetadata('topic', ConsumerClass);
      return {
        topic,
        handler: instance,
      } as ConsumerDef<any>;
    });

    // Check if all the consumers implements the IEventHandler interface
    const nonEventHandlers = definitions.filter((definition) => !('handle' in definition.handler));

    if (nonEventHandlers.length) {
      this.logger.error(
        `The following consumers do not implement the IEventHandler interface: ${nonEventHandlers
          .map((definition) => definition.handler.constructor.name)
          .join(', ')}`,
      );
      return;
    }

    // Store definitions for later initialization
    this.consumerDefinitions = definitions;

    // Start consuming asynchronously - don't block bootstrap
    this.initializeConsumersAsync();
  }

  /**
   * Initialize consumers asynchronously without blocking the bootstrap process
   */
  private async initializeConsumersAsync(): Promise<void> {
    try {
      this.logger.log(`Starting async initialization of ${this.consumerDefinitions.length} consumers...`);
      
      // Start consuming
      await this.consumeMany(this.consumerDefinitions);

      for (const definition of this.consumerDefinitions) {
        const { topic, handler } = definition;
        this.logger.log(`Consumer started for topic: ${topic}, handler: ${handler.constructor.name}`);
      }

      this.isInitialized = true;
      this.logger.log(`All consumers initialized successfully for service: ${this.name}`);
    } catch (error) {
      this.initializationError = error as Error;
      this.logger.error(
        `Failed to initialize consumers for service: ${this.name}. Error: ${error.message}`,
        error.stack
      );
      
      // Schedule retry after delay
      setTimeout(() => {
        this.logger.log(`Retrying consumer initialization for service: ${this.name}...`);
        this.initializeConsumersAsync();
      }, 30000); // Retry after 30 seconds
    }
  }

  /**
   * Get the health status of the consumer service
   */
  getHealthStatus(): {
    isInitialized: boolean;
    error: string | null;
    consumerCount: number;
    kafkaHealthy: boolean;
  } {
    return {
      isInitialized: this.isInitialized,
      error: this.initializationError?.message || null,
      consumerCount: this.consumerDefinitions.length,
      kafkaHealthy: false, // Will be updated by async method
    };
  }

  /**
   * Get the async health status of the consumer service
   */
  async getAsyncHealthStatus(): Promise<{
    isInitialized: boolean;
    error: string | null;
    consumerCount: number;
    kafkaHealthy: boolean;
    kafkaInitialized: boolean;
    kafkaInitializationError: string | null;
  }> {
    const kafkaHealthy = await this.kafkaClient.isHealthy();
    const kafkaStatus = this.kafkaClient.getInitializationStatus();
    
    return {
      isInitialized: this.isInitialized,
      error: this.initializationError?.message || null,
      consumerCount: this.consumerDefinitions.length,
      kafkaHealthy,
      kafkaInitialized: kafkaStatus.isInitialized,
      kafkaInitializationError: kafkaStatus.error,
    };
  }

  async consume<T>(definition: ConsumerDef<T>): Promise<void> {
    const { name, kafkaClient } = this;
    const groupId = `${name}-consumer`;
    const { topic, handler } = definition;

    return await kafkaClient.consumeMany([{ topic, handler }], groupId);
  }
  
  async consumeMany(definitions: ConsumerDef<any>[]): Promise<void> {
    const { name, kafkaClient } = this;
    const groupId = `${name}-consumer`;

    // Transform definitions to the format expected by kafkaClient.consumeMany
    const topicHandlers = definitions.map(({ topic, handler }) => ({
      topic,
      handler,
    }));

    return await kafkaClient.consumeMany<any>(topicHandlers, groupId);
  }
}
