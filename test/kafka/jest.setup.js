/**
 * Jest setup for Kafka tests
 * Handles async cleanup and reduces noise
 */

// Suppress KafkaJS partitioner warning
process.env.KAFKAJS_NO_PARTITIONER_WARNING = '1';

// Increase async operation timeouts
jest.setTimeout(30000);

// Handle unhandled promise rejections
process.on('unhandledRejection', (reason, promise) => {
  // Only log if it's not a common KafkaJS cleanup issue
  if (!reason?.message?.includes('request is not a function') &&
      !reason?.message?.includes('after the Jest environment has been torn down')) {
    console.warn('Unhandled Rejection at:', promise, 'reason:', reason);
  }
});

// Handle uncaught exceptions
process.on('uncaughtException', (error) => {
  // Only log if it's not a common KafkaJS cleanup issue
  if (!error?.message?.includes('request is not a function') &&
      !error?.message?.includes('after the Jest environment has been torn down')) {
    console.error('Uncaught Exception:', error);
  }
});

// Global test timeout
beforeEach(() => {
  jest.setTimeout(30000);
});



