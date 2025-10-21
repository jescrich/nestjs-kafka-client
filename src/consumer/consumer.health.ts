import { Injectable } from '@nestjs/common';
import { HealthCheckError, HealthIndicator, HealthIndicatorResult } from '@nestjs/terminus';
import { ConsumerService } from './consumer.service';

@Injectable()
export class ConsumerHealthIndicator extends HealthIndicator {
  constructor(private readonly consumerService: ConsumerService) {
    super();
  }

  async isHealthy(key: string): Promise<HealthIndicatorResult> {
    const status = await this.consumerService.getAsyncHealthStatus();
    
    const isHealthy = status.isInitialized && status.kafkaHealthy && status.kafkaInitialized && !status.error;
    
    const result = this.getStatus(key, isHealthy, {
      consumerInitialized: status.isInitialized,
      consumerError: status.error,
      consumerCount: status.consumerCount,
      kafkaInitialized: status.kafkaInitialized,
      kafkaInitializationError: status.kafkaInitializationError,
      kafkaHealthy: status.kafkaHealthy,
    });

    if (isHealthy) {
      return result;
    }

    throw new HealthCheckError('Consumer service is not healthy', result);
  }
}
