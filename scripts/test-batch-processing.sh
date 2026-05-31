#!/bin/bash

# Batch Processing Test Script
# Tests batch processing functionality

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
MAGENTA='\033[0;35m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Configuration
KAFKA_BROKERS="${TEST_KAFKA_BROKERS:-localhost:29092}"

echo -e "${CYAN}╔════════════════════════════════════════════════════════════╗${NC}"
echo -e "${CYAN}║        📦 Batch Processing Test Suite                     ║${NC}"
echo -e "${CYAN}╔════════════════════════════════════════════════════════════╗${NC}"
echo ""
echo -e "${BLUE}Kafka Brokers: ${KAFKA_BROKERS}${NC}"
echo ""

# Check Kafka connectivity
check_kafka() {
    echo -e "${YELLOW}Checking Kafka connectivity...${NC}"
    
    if command -v nc &> /dev/null; then
        BROKER_HOST=$(echo $KAFKA_BROKERS | cut -d: -f1)
        BROKER_PORT=$(echo $KAFKA_BROKERS | cut -d: -f2)
        
        if nc -z -w5 $BROKER_HOST $BROKER_PORT 2>/dev/null; then
            echo -e "${GREEN}✓ Kafka is reachable${NC}"
            return 0
        else
            echo -e "${RED}✗ Cannot connect to Kafka at ${KAFKA_BROKERS}${NC}"
            return 1
        fi
    else
        echo -e "${YELLOW}⚠ Cannot verify connectivity (nc not installed)${NC}"
        return 0
    fi
}

# Run batch integration tests
run_batch_tests() {
    echo -e "\n${MAGENTA}═══════════════════════════════════════════════════════════${NC}"
    echo -e "${MAGENTA}Running Batch Integration Tests${NC}"
    echo -e "${MAGENTA}═══════════════════════════════════════════════════════════${NC}\n"
    
    jest --config jest.config.cjs \
        test/kafka/kafka.batch.integration.spec.ts \
        test/consumer/consumer.batch.integration.spec.ts \
        --testTimeout=60000 \
        --no-watchman
}

# Run batch unit tests
run_batch_unit_tests() {
    echo -e "\n${MAGENTA}═══════════════════════════════════════════════════════════${NC}"
    echo -e "${MAGENTA}Running Batch Unit Tests${NC}"
    echo -e "${MAGENTA}═══════════════════════════════════════════════════════════${NC}\n"
    
    jest --config jest.config.cjs \
        test/kafka/kafka.client.unit-bug-test.spec.ts \
        --testNamePattern="Batch" \
        --no-watchman
}

# Run specific batch scenario
run_scenario() {
    local scenario=$1
    echo -e "\n${MAGENTA}═══════════════════════════════════════════════════════════${NC}"
    echo -e "${MAGENTA}Running Batch Scenario: ${scenario}${NC}"
    echo -e "${MAGENTA}═══════════════════════════════════════════════════════════${NC}\n"
    
    jest --config jest.config.cjs \
        test/kafka/kafka.batch.integration.spec.ts \
        --testNamePattern="${scenario}" \
        --testTimeout=60000 \
        --no-watchman
}

# Show menu
show_menu() {
    echo ""
    echo -e "${CYAN}Select batch test mode:${NC}"
    echo -e "${GREEN}1)${NC} Run All Batch Integration Tests"
    echo -e "${GREEN}2)${NC} Run Batch Unit Tests"
    echo -e "${GREEN}3)${NC} Run Single Message Processing Test"
    echo -e "${GREEN}4)${NC} Run Same-Key Batch Processing Test"
    echo -e "${GREEN}5)${NC} Run Mixed-Key Batch Processing Test"
    echo -e "${GREEN}6)${NC} Run High-Volume Batch Processing Test"
    echo -e "${GREEN}7)${NC} Run Batch with Failures Test"
    echo -e "${GREEN}8)${NC} Run Batch with Timeouts Test"
    echo -e "${GREEN}9)${NC} Check Kafka Connectivity"
    echo -e "${GREEN}10)${NC} Exit"
    echo ""
    echo -n "Enter choice [1-10]: "
}

# Main loop
main() {
    if ! check_kafka; then
        echo -e "${RED}Kafka is not available. Please start Kafka first.${NC}"
        exit 1
    fi
    
    while true; do
        show_menu
        read choice
        
        case $choice in
            1)
                run_batch_tests
                ;;
            2)
                run_batch_unit_tests
                ;;
            3)
                run_scenario "Single Message Processing"
                ;;
            4)
                run_scenario "Same-Key Batch Processing"
                ;;
            5)
                run_scenario "Mixed-Key Batch Processing"
                ;;
            6)
                run_scenario "High-Volume Batch Processing"
                ;;
            7)
                run_scenario "Batch Processing with Failures"
                ;;
            8)
                run_scenario "Batch Processing with Timeouts"
                ;;
            9)
                check_kafka
                ;;
            10)
                echo -e "\n${CYAN}Goodbye!${NC}\n"
                exit 0
                ;;
            *)
                echo -e "${RED}Invalid option. Please try again.${NC}"
                ;;
        esac
        
        echo ""
        echo -e "${YELLOW}Press Enter to continue...${NC}"
        read
    done
}

# Run main if executed directly
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main
fi
