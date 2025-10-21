#!/usr/bin/env ts-node

/**
 * Test Runner for KafkaClient Tests
 * 
 * Usage:
 *   npm run test:kafka
 *   yarn test:kafka
 * 
 * Or directly:
 *   npx ts-node test/kafka/run-tests.ts
 */

import { execSync } from 'child_process';
import { existsSync } from 'fs';
import { join } from 'path';

const testFiles = [
  'test/kafka/kafka.client.spec.ts',
  'test/kafka/kafka.dlq.integration.spec.ts',
  'test/kafka/kafka.batch.integration.spec.ts',
  'test/consumer/consumer.dlq.integration.spec.ts',
  'test/consumer/consumer.batch.integration.spec.ts',
];

function runTests() {
  console.log('🧪 Running KafkaClient Tests...\n');

  // Check if jest is available
  try {
    execSync('npx jest --version', { stdio: 'ignore' });
  } catch (error) {
    console.error('❌ Jest is not installed. Please run: npm install --save-dev jest @types/jest ts-jest');
    process.exit(1);
  }

  // Check if test files exist
  for (const testFile of testFiles) {
    if (!existsSync(testFile)) {
      console.error(`❌ Test file not found: ${testFile}`);
      process.exit(1);
    }
  }

  try {
    // Run tests with Jest
    const jestConfig = {
      preset: 'ts-jest',
      testEnvironment: 'node',
      testMatch: ['**/test/kafka/**/*.spec.ts', '**/test/consumer/**/*.spec.ts'],
      collectCoverage: true,
      coverageDirectory: 'coverage/kafka',
      coverageReporters: ['text', 'lcov', 'html'],
      verbose: true,
      setupFilesAfterEnv: [],
      testTimeout: 30000, // 30 seconds for integration tests
    };

    const jestConfigPath = join(__dirname, 'jest.config.json');
    require('fs').writeFileSync(jestConfigPath, JSON.stringify(jestConfig, null, 2));

    console.log('📋 Running tests with configuration:');
    console.log(JSON.stringify(jestConfig, null, 2));
    console.log('\n');

    execSync(`npx jest --config ${jestConfigPath}`, { 
      stdio: 'inherit',
      env: {
        ...process.env,
        NODE_ENV: 'test',
      }
    });

    console.log('\n✅ All tests completed successfully!');

  } catch (error) {
    console.error('\n❌ Tests failed');
    process.exit(1);
  }
}

// CLI interface
if (require.main === module) {
  const args = process.argv.slice(2);
  
  if (args.includes('--help') || args.includes('-h')) {
    console.log(`
KafkaClient Test Runner

Usage:
  ts-node test/kafka/run-tests.ts [options]

Options:
  --help, -h     Show this help message
  --watch, -w    Run tests in watch mode
  --coverage     Generate coverage report
  --verbose      Verbose output

Examples:
  ts-node test/kafka/run-tests.ts
  ts-node test/kafka/run-tests.ts --coverage
  ts-node test/kafka/run-tests.ts --watch
    `);
    process.exit(0);
  }

  runTests();
}

export { runTests }; 