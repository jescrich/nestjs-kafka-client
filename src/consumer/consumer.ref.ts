import { Injectable, Logger, Type } from '@nestjs/common';

@Injectable()
/**
 * Service to manage and resolve consumer references.
 */
export class ConsumerRefService {
  /**
   * Logger instance for logging messages.
   */
  private readonly logger = new Logger(ConsumerRefService.name);

  /**
   * Creates an instance of ConsumerRefService.
   * @param consumers - Array of consumer types.
   */
  constructor(private readonly consumers: Type<any>[]) {
    this.logger.log(`Initializing consumer references`);
    this.logger.log(`Found ${this.consumers.length} consumers`);
  }

  /**
   * Resolves and returns the array of consumer types.
   * @returns Array of consumer types.
   */
  resolve(): Type<any>[] {
    return this.consumers;
  }
}
