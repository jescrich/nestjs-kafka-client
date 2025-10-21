
import { Injectable } from '@nestjs/common';
import { HealthIndicator, HealthIndicatorResult, HealthCheckError } from '@nestjs/terminus';
import { KafkaClient } from './kafka.client';

@Injectable()
export class KafkaHealthIndicator extends HealthIndicator {
  constructor(private readonly kafkaClient: KafkaClient, private readonly config: { clientId: string; brokers: string }) {
    super();
  }

  async isHealthy(key: string): Promise<HealthIndicatorResult> {
    try {
      const isConnected = await this.kafkaClient.isHealthy();

      return this.getStatus(key, isConnected, {
        kafka: {
          clientId: this.config.clientId,
          brokers: this.config.brokers
        }
      });
    } catch (error) {
      throw new HealthCheckError(
        'Kafka health check failed',
        this.getStatus(key, false)
      );
    }
  }
}
