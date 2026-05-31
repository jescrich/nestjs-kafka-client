import { IEventHandler, KafkaClient, KafkaTopics } from '@this/kafka';
import { ConsumerDef } from './consumer.def';
import { ExecutionContext, Injectable, Logger, OnModuleInit, Type } from '@nestjs/common';
import { ModuleRef } from '@nestjs/core';
import { Consumer } from '..';
import { ConsumerRefService } from './consumer.ref';

/**
 * Optional multi-tenant fan-out. When `tenants` is set, every `@Handler` is registered once per
 * tenant: the `{tenant}` placeholder in its topic is substituted, and each tenant gets its own
 * consumer group (so offsets and rebalances are isolated per tenant).
 */
export interface ConsumerServiceOptions {
  /** Tenants to fan out across. Omit (or empty) for the classic single-group behavior. */
  tenants?: string[];
  /**
   * Consumer-group template. Supports `{tenant}` and `{name}` placeholders. Defaults:
   * `{name}-consumer` (single-group) or `{tenant}.{name}` (per-tenant).
   */
  groupId?: string;
}

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
    private readonly options?: ConsumerServiceOptions,
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
    return this.consumeMany([definition]);
  }

  async consumeMany(definitions: ConsumerDef<any>[]): Promise<void> {
    const tenants = this.options?.tenants;

    // Classic single-group behavior when no tenants are configured.
    if (!tenants || tenants.length === 0) {
      const topicHandlers = definitions.map(({ topic, handler }) => ({ topic, handler }));
      return await this.kafkaClient.consumeMany<any>(topicHandlers, this.resolveGroupId());
    }

    // Per-tenant fan-out: one consumer group per tenant, with `{tenant}` substituted in topics.
    for (const tenant of tenants) {
      const groupId = this.resolveGroupId(tenant);
      const topicHandlers = definitions.map(({ topic, handler }) => ({
        topic: KafkaTopics.withTenant(topic, tenant),
        handler,
      }));
      this.logger.log(
        `Registering ${topicHandlers.length} handler(s) for tenant "${tenant}" on group ${groupId}`,
      );
      await this.kafkaClient.consumeMany<any>(topicHandlers, groupId);
    }
  }

  /** Resolve the consumer-group id from the configured template (or the defaults). */
  private resolveGroupId(tenant?: string): string {
    const template =
      this.options?.groupId ?? (tenant !== undefined ? '{tenant}.{name}' : '{name}-consumer');
    return template.replace(/\{tenant\}/g, tenant ?? '').replace(/\{name\}/g, this.name);
  }
}
