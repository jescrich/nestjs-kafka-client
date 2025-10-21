module.exports = {
  preset: 'ts-jest',
  testEnvironment: 'node',
  testMatch: [
    '**/test/kafka/**/*.spec.ts',
    '**/test/consumer/**/*.spec.ts'
  ],
  
  // Async cleanup configuration
  detectOpenHandles: true,
  forceExit: true,
  
  // Longer timeouts for Kafka operations
  testTimeout: 30000,
  
  // Setup and teardown
  setupFilesAfterEnv: ['./jest.setup.js'],
  
  // Coverage
  collectCoverage: false, // Disable for faster runs
  
  // Silence console warnings during tests
  silent: false,
  verbose: true,
  
  // Handle async operations better
  maxWorkers: 1, // Single worker to avoid conflicts
  
  // Global teardown
  globalTeardown: './jest.teardown.js'
};
