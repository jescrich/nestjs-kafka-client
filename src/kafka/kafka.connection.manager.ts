import { Injectable, Logger } from '@nestjs/common';
import { KafkaClient } from './kafka.client';

/**
 * Manages KafkaClient connections to prevent duplicate connections to the same brokers
 * This helps reduce connection overhead and prevents connection pooling issues with MSK
 */
@Injectable()
export class KafkaConnectionManager {
  private static readonly logger = new Logger(KafkaConnectionManager.name);
  private static readonly connections = new Map<string, KafkaClient>();
  private static readonly connectionUsage = new Map<string, number>(); // Track usage count

  /**
   * Gets or creates a KafkaClient for the given brokers and clientId
   * Uses connection pooling to prevent duplicate connections
   */
  static getOrCreateClient(
    brokers: string,
    clientId: string,
    options?: any
  ): KafkaClient {
    // Create a connection key based on brokers (normalized)
    const normalizedBrokers = brokers.split(',').sort().join(',');
    const connectionKey = `${normalizedBrokers}`;
    
    if (KafkaConnectionManager.connections.has(connectionKey)) {
      const existingClient = KafkaConnectionManager.connections.get(connectionKey)!;
      const currentUsage = KafkaConnectionManager.connectionUsage.get(connectionKey) || 0;
      
      // Increment usage count
      KafkaConnectionManager.connectionUsage.set(connectionKey, currentUsage + 1);
      
      KafkaConnectionManager.logger.log(
        `Reusing existing KafkaClient for brokers: ${normalizedBrokers} (usage: ${currentUsage + 1})`
      );
      
      return existingClient;
    }

    // Create new client with unique ID to avoid conflicts
    const uniqueClientId = `${clientId}-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
    const client = new KafkaClient(uniqueClientId, brokers, options);
    
    // Store the connection
    KafkaConnectionManager.connections.set(connectionKey, client);
    KafkaConnectionManager.connectionUsage.set(connectionKey, 1);
    
    KafkaConnectionManager.logger.log(
      `Created new KafkaClient for brokers: ${normalizedBrokers} with clientId: ${uniqueClientId}`
    );
    
    return client;
  }

  /**
   * Releases a connection (decrements usage count)
   * If usage reaches 0, the connection can be cleaned up
   */
  static releaseConnection(brokers: string): void {
    const normalizedBrokers = brokers.split(',').sort().join(',');
    const connectionKey = `${normalizedBrokers}`;
    
    const currentUsage = KafkaConnectionManager.connectionUsage.get(connectionKey) || 0;
    
    if (currentUsage > 1) {
      KafkaConnectionManager.connectionUsage.set(connectionKey, currentUsage - 1);
      KafkaConnectionManager.logger.log(
        `Decreased usage count for brokers: ${normalizedBrokers} (usage: ${currentUsage - 1})`
      );
    } else {
      // Last usage, can clean up
      const client = KafkaConnectionManager.connections.get(connectionKey);
      if (client) {
        // Don't automatically shutdown - let the client handle its own lifecycle
        KafkaConnectionManager.logger.log(
          `Last usage released for brokers: ${normalizedBrokers}. Connection marked for cleanup.`
        );
      }
      
      KafkaConnectionManager.connections.delete(connectionKey);
      KafkaConnectionManager.connectionUsage.delete(connectionKey);
    }
  }

  /**
   * Gets current connection statistics
   */
  static getConnectionStats(): { connectionKey: string; usage: number; clientId: string }[] {
    const stats: { connectionKey: string; usage: number; clientId: string }[] = [];
    
    for (const [connectionKey, client] of KafkaConnectionManager.connections.entries()) {
      const usage = KafkaConnectionManager.connectionUsage.get(connectionKey) || 0;
      stats.push({
        connectionKey,
        usage,
        clientId: (client as any).clientId || 'unknown'
      });
    }
    
    return stats;
  }

  /**
   * Forces cleanup of all connections (for testing or shutdown)
   */
  static async forceCleanupAll(): Promise<void> {
    KafkaConnectionManager.logger.warn('Force cleanup of all KafkaClient connections');
    
    const shutdownPromises: Promise<void>[] = [];
    
    for (const [connectionKey, client] of KafkaConnectionManager.connections.entries()) {
      shutdownPromises.push(client.shutdown().catch(error => {
        KafkaConnectionManager.logger.error(`Error shutting down connection ${connectionKey}:`, error);
      }));
    }
    
    await Promise.all(shutdownPromises);
    
    KafkaConnectionManager.connections.clear();
    KafkaConnectionManager.connectionUsage.clear();
    
    KafkaConnectionManager.logger.log('All KafkaClient connections cleaned up');
  }
}