#!/bin/bash

# DLQ Integration Test Script
# Tests Dead Letter Queue functionality

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
echo -e "${CYAN}║           🔄 DLQ Integration Test Suite                   ║${NC}"
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

# Run DLQ integration tests
run_dlq_tests() {
    echo -e "\n${MAGENTA}═══════════════════════════════════════════════════════════${NC}"
    echo -e "${MAGENTA}Running DLQ Integration Tests${NC}"
    echo -e "${MAGENTA}═══════════════════════════════════════════════════════════${NC}\n"
    
    jest --config jest.config.cjs \
        test/kafka/kafka.dlq.integration.spec.ts \
        test/kafka/kafka.dlq.failure.recovery.spec.ts \
        test/consumer/consumer.dlq.integration.spec.ts \
        --testTimeout=60000 \
        --no-watchman
}

# Run DLQ fix tests
run_dlq_fix_tests() {
    echo -e "\n${MAGENTA}═══════════════════════════════════════════════════════════${NC}"
    echo -e "${MAGENTA}Running DLQ Fix Tests${NC}"
    echo -e "${MAGENTA}═══════════════════════════════════════════════════════════${NC}\n"
    
    jest --config jest.config.cjs \
        test/consumer/consumer.dlq.fix.spec.ts \
        --testTimeout=60000 \
        --no-watchman
}

# Show menu
show_menu() {
    echo ""
    echo -e "${CYAN}Select DLQ test mode:${NC}"
    echo -e "${GREEN}1)${NC} Run DLQ Integration Tests"
    echo -e "${GREEN}2)${NC} Run DLQ Fix Tests"
    echo -e "${GREEN}3)${NC} Run All DLQ Tests"
    echo -e "${GREEN}4)${NC} Check Kafka Connectivity"
    echo -e "${GREEN}5)${NC} Exit"
    echo ""
    echo -n "Enter choice [1-5]: "
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
                run_dlq_tests
                ;;
            2)
                run_dlq_fix_tests
                ;;
            3)
                run_dlq_tests
                run_dlq_fix_tests
                ;;
            4)
                check_kafka
                ;;
            5)
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
