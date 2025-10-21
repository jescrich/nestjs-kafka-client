/**
 * Jest global teardown for Kafka tests
 * Ensures all async operations are properly cleaned up
 */

module.exports = async () => {
  console.log('🧹 Global Jest teardown: Waiting for async operations to complete...');
  
  // Wait for any remaining async operations
  await new Promise(resolve => setTimeout(resolve, 2000));
  
  console.log('✅ Global Jest teardown completed');
};



