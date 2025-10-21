/**
 * Test environment setup for Kafka tests
 * Sets required environment variables
 */

// Set Kafka test environment
process.env.NODE_ENV = 'test';
process.env.TEST_KAFKA_BROKERS = process.env.TEST_KAFKA_BROKERS || 'localhost:29092';
process.env.KAFKAJS_NO_PARTITIONER_WARNING = '1';

// Suppress common async cleanup warnings
const originalConsoleError = console.error;
console.error = (...args) => {
  const message = args.join(' ');
  
  // Suppress specific KafkaJS cleanup errors that occur after tests
  if (message.includes('request is not a function') ||
      message.includes('after the Jest environment has been torn down') ||
      message.includes('Cannot log after tests are done')) {
    return; // Suppress these
  }
  
  originalConsoleError(...args);
};



