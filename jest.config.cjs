/** @type {import('ts-jest/dist/types').InitialOptionsTsJest} */
// module.exports = {
//   preset: 'ts-jest',
//   testEnvironment: 'node',
// };

module.exports = {
    preset: 'ts-jest',
    testEnvironment: 'node',
    
    // Test discovery
    roots: ['<rootDir>/test'],
    testMatch: ['**/test/**/*.spec.ts', '**/test/**/*.test.ts'],
    
    // Module resolution
    moduleFileExtensions: ['ts', 'tsx', 'js', 'jsx', 'json', 'node'],
    moduleNameMapper: {
      '@this/(.*)': '<rootDir>/src/$1',
    },
    
    // Transform
    transform: {
      '^.+\\.tsx?$': 'ts-jest',
    },
    
    // Kafka test specific configuration
    testTimeout: 30000,
    detectOpenHandles: false, // Disable for VSCode to reduce noise
    forceExit: true,
    maxWorkers: 1,
    
    // Environment setup
    setupFilesAfterEnv: ['<rootDir>/test/kafka/jest.setup.js'],
    
    // VSCode-friendly output
    verbose: false,
    silent: false,
    
    // Coverage (disabled for faster runs)
    collectCoverage: false,
    
    // Better error handling
    errorOnDeprecated: false,
  };
  