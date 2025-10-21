import { Test, TestingModule } from '@nestjs/testing';
import { ConsumerService } from '../../src/consumer/consumer.service';
import { ConsumerHealthIndicator } from '../../src/consumer/consumer.health';
import { KafkaClient } from '../../src/kafka/kafka.client';
import { ConsumerRefService } from '../../src/consumer/consumer.ref';
import { ModuleRef } from '@nestjs/core';
import { Logger } from '@nestjs/common';

// Mock KafkaClient that simulates connection failures
class MockKafkaClient {
  private shouldFailConnection = true;
  private isHealthyStatus = false;

  async consumeMany(): Promise<void> {
    if (this.shouldFailConnection) {
      throw new Error('Kafka connection failed');
    }
  }

  async isHealthy(): Promise<boolean> {
    return this.isHealthyStatus;
  }

  setConnectionStatus(shouldFail: boolean): void {
    this.shouldFailConnection = shouldFail;
    this.isHealthyStatus = !shouldFail;
  }
}

// Mock consumer handler
class MockConsumerHandler {
  async handle(): Promise<void> {
    // Mock implementation
  }
}

describe('ConsumerService Non-Blocking Initialization', () => {
  let consumerService: ConsumerService;
  let healthIndicator: ConsumerHealthIndicator;
  let mockKafkaClient: MockKafkaClient;
  let moduleRef: ModuleRef;

  beforeEach(async () => {
    mockKafkaClient = new MockKafkaClient();
    
    // Mock ModuleRef
    moduleRef = {
      get: jest.fn().mockReturnValue(new MockConsumerHandler()),
    } as any;

    // Mock ConsumerRefService
    const mockConsumerRef = {
      resolve: jest.fn().mockReturnValue([MockConsumerHandler]),
    };

    // Mock Reflect metadata
    (global as any).Reflect = {
      hasMetadata: jest.fn().mockReturnValue(true),
      getMetadata: jest.fn().mockReturnValue('test-topic'),
    };

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        {
          provide: ConsumerService,
          useFactory: () => new ConsumerService(
            'test-service',
            mockKafkaClient as any,
            mockConsumerRef as any,
            moduleRef
          ),
        },
        {
          provide: ConsumerHealthIndicator,
          useFactory: (consumerService: ConsumerService) => 
            new ConsumerHealthIndicator(consumerService),
          inject: [ConsumerService],
        },
      ],
    }).compile();

    consumerService = module.get<ConsumerService>(ConsumerService);
    healthIndicator = module.get<ConsumerHealthIndicator>(ConsumerHealthIndicator);
  });

  it('should not block during onModuleInit when Kafka is unavailable', async () => {
    // Simulate Kafka being unavailable
    mockKafkaClient.setConnectionStatus(true); // true = should fail

    const startTime = Date.now();
    
    // This should complete quickly without blocking
    await consumerService.onModuleInit();
    
    const duration = Date.now() - startTime;
    
    // Should complete in less than 1 second (not block for minutes)
    expect(duration).toBeLessThan(1000);
  });

  it('should report unhealthy status when initialization fails', async () => {
    // Simulate Kafka being unavailable
    mockKafkaClient.setConnectionStatus(true); // true = should fail

    await consumerService.onModuleInit();
    
    // Give some time for async initialization to attempt and fail
    await new Promise(resolve => setTimeout(resolve, 100));

    const healthStatus = await consumerService.getAsyncHealthStatus();
    
    expect(healthStatus.isInitialized).toBe(false);
    expect(healthStatus.kafkaHealthy).toBe(false);
    expect(healthStatus.consumerCount).toBe(1);
  });

  it('should report healthy status when initialization succeeds', async () => {
    // Simulate Kafka being available
    mockKafkaClient.setConnectionStatus(false); // false = should not fail

    await consumerService.onModuleInit();
    
    // Give some time for async initialization to complete
    await new Promise(resolve => setTimeout(resolve, 100));

    const healthStatus = await consumerService.getAsyncHealthStatus();
    
    expect(healthStatus.isInitialized).toBe(true);
    expect(healthStatus.kafkaHealthy).toBe(true);
    expect(healthStatus.consumerCount).toBe(1);
    expect(healthStatus.error).toBeNull();
  });

  it('should throw HealthCheckError when consumer is unhealthy', async () => {
    // Simulate Kafka being unavailable
    mockKafkaClient.setConnectionStatus(true); // true = should fail

    await consumerService.onModuleInit();
    
    // Give some time for async initialization to attempt and fail
    await new Promise(resolve => setTimeout(resolve, 100));

    await expect(healthIndicator.isHealthy('kafka-consumer')).rejects.toThrow();
  });

  it('should return healthy result when consumer is healthy', async () => {
    // Simulate Kafka being available
    mockKafkaClient.setConnectionStatus(false); // false = should not fail

    await consumerService.onModuleInit();
    
    // Give some time for async initialization to complete
    await new Promise(resolve => setTimeout(resolve, 100));

    const result = await healthIndicator.isHealthy('kafka-consumer');
    
    expect(result).toHaveProperty('kafka-consumer');
    expect(result['kafka-consumer'].status).toBe('up');
  });
});
