/**
 * Suppress KafkaJS warnings and async cleanup errors
 * Include this at the top of test files to reduce noise
 */

// Suppress KafkaJS partitioner warning
process.env.KAFKAJS_NO_PARTITIONER_WARNING = '1';

// Suppress Jest async warnings for known KafkaJS cleanup issues
const originalConsoleError = console.error;
const originalConsoleWarn = console.warn;

console.error = (...args) => {
  const message = args.join(' ');
  
  // Suppress known KafkaJS cleanup errors
  if (message.includes('request is not a function') ||
      message.includes('after the Jest environment has been torn down') ||
      message.includes('Cannot log after tests are done')) {
    return; // Suppress these specific errors
  }
  
  originalConsoleError(...args);
};

console.warn = (...args) => {
  const message = args.join(' ');
  
  // Suppress KafkaJS partitioner warnings
  if (message.includes('switched default partitioner') ||
      message.includes('KAFKAJS_NO_PARTITIONER_WARNING')) {
    return; // Suppress these specific warnings
  }
  
  originalConsoleWarn(...args);
};

// Restore console methods after tests
afterAll(() => {
  console.error = originalConsoleError;
  console.warn = originalConsoleWarn;
});

module.exports = {
  suppressKafkaWarnings: () => {
    process.env.KAFKAJS_NO_PARTITIONER_WARNING = '1';
  }
};



