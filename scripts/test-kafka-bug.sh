#!/bin/bash

# Kafka Bug Reproduction Test Script
# Interactive menu for testing different scenarios

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
echo -e "${CYAN}║        🧪 Kafka Bug Reproduction Test Suite              ║${NC}"
echo -e "${CYAN}╔════════════════════════════════════════════════════════════╗${NC}"
echo ""
echo -e "${BLUE}Kafka Brokers: ${KAFKA_BROKERS}${NC}"
echo ""

# Check if Kafka is available
check_kafka() {
    echo -e "${YELLOW}Checking Kafka connectivity...${NC}"
    
    # Simple check - try to connect with kafkajs
    if command -v nc &> /dev/null; then
        BROKER_HOST=$(echo $KAFKA_BROKERS | cut -d: -f1)
        BROKER_PORT=$(echo $KAFKA_BROKERS | cut -d: -f2)
        
        if nc -z -w5 $BROKER_HOST $BROKER_PORT 2>/dev/null; then
            echo -e "${GREEN}✓ Kafka is reachable${NC}"
            return 0
        else
            echo -e "${RED}✗ Cannot connect to Kafka at ${KAFKA_BROKERS}${NC}"
            echo -e "${YELLOW}Please ensure Kafka is running${NC}"
            return 1
        fi
    else
        echo -e "${YELLOW}⚠ Cannot verify Kafka connectivity (nc not installed)${NC}"
        echo -e "${YELLOW}Proceeding anyway...${NC}"
        return 0
    fi
}

# Run unit tests (no Kafka required)
run_unit_tests() {
    echo -e "\n${MAGENTA}═══════════════════════════════════════════════════════════${NC}"
    echo -e "${MAGENTA}Running Unit Tests (No Kafka Required)${NC}"
    echo -e "${MAGENTA}═══════════════════════════════════════════════════════════${NC}\n"
    
    npm run test:kafka-unit-bug
}

# Run standalone bug reproduction script
run_bug_reproduction() {
    echo -e "\n${MAGENTA}═══════════════════════════════════════════════════════════${NC}"
    echo -e "${MAGENTA}Running Standalone Bug Reproduction Test${NC}"
    echo -e "${MAGENTA}═══════════════════════════════════════════════════════════${NC}\n"
    
    if ! check_kafka; then
        echo -e "${RED}Skipping - Kafka not available${NC}"
        return 1
    fi
    
    npm run test:kafka-bug
}

# Run integration tests
run_integration_tests() {
    echo -e "\n${MAGENTA}═══════════════════════════════════════════════════════════${NC}"
    echo -e "${MAGENTA}Running Integration Tests${NC}"
    echo -e "${MAGENTA}═══════════════════════════════════════════════════════════${NC}\n"
    
    if ! check_kafka; then
        echo -e "${RED}Skipping - Kafka not available${NC}"
        return 1
    fi
    
    npm run test:kafka-integration
}

# Run all tests
run_all_tests() {
    echo -e "\n${MAGENTA}═══════════════════════════════════════════════════════════${NC}"
    echo -e "${MAGENTA}Running All Tests${NC}"
    echo -e "${MAGENTA}═══════════════════════════════════════════════════════════${NC}\n"
    
    run_unit_tests
    
    if check_kafka; then
        run_bug_reproduction
        run_integration_tests
    else
        echo -e "${YELLOW}Skipping Kafka-dependent tests${NC}"
    fi
}

# Show menu
show_menu() {
    echo ""
    echo -e "${CYAN}Select test mode:${NC}"
    echo -e "${GREEN}1)${NC} Run Unit Tests (No Kafka Required)"
    echo -e "${GREEN}2)${NC} Run Bug Reproduction Test (Requires Kafka)"
    echo -e "${GREEN}3)${NC} Run Integration Tests (Requires Kafka)"
    echo -e "${GREEN}4)${NC} Run All Tests"
    echo -e "${GREEN}5)${NC} Check Kafka Connectivity"
    echo -e "${GREEN}6)${NC} Exit"
    echo ""
    echo -n "Enter choice [1-6]: "
}

# Main loop
main() {
    while true; do
        show_menu
        read choice
        
        case $choice in
            1)
                run_unit_tests
                ;;
            2)
                run_bug_reproduction
                ;;
            3)
                run_integration_tests
                ;;
            4)
                run_all_tests
                ;;
            5)
                check_kafka
                ;;
            6)
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
