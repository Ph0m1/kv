#!/bin/bash

# KV 存储系统测试脚本

set -e  # 遇到错误立即退出

# 颜色定义
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}================================${NC}"
echo -e "${BLUE}  KV Storage System Tests${NC}"
echo -e "${BLUE}================================${NC}"

# 测试计数
TOTAL_TESTS=0
PASSED_TESTS=0
FAILED_TESTS=0

# 运行单个测试
run_test() {
    local package=$1
    local test_name=$2
    local description=$3
    
    TOTAL_TESTS=$((TOTAL_TESTS + 1))
    
    echo ""
    echo -e "${YELLOW}Running: ${description}${NC}"
    
    if timeout 30s go test -v ${package} -run ${test_name} 2>&1 | tee /tmp/test_output.log; then
        echo -e "${GREEN}✓ ${description} PASSED${NC}"
        PASSED_TESTS=$((PASSED_TESTS + 1))
        return 0
    else
        echo -e "${RED}✗ ${description} FAILED${NC}"
        FAILED_TESTS=$((FAILED_TESTS + 1))
        return 1
    fi
}

# 1. 存储引擎测试
echo ""
echo -e "${BLUE}=== Storage Engine Tests ===${NC}"

run_test "./pkg/storage" "TestSkipListBasicOperations" "SkipList Basic Operations" || true
run_test "./pkg/storage" "TestSkipListUpdate" "SkipList Update" || true
run_test "./pkg/storage" "TestSkipListConcurrent" "SkipList Concurrent" || true

run_test "./pkg/storage" "TestLSMBasicOperations" "LSM Basic Operations" || true
run_test "./pkg/storage" "TestLSMUpdate" "LSM Update" || true

# 2. Raft 测试
echo ""
echo -e "${BLUE}=== Raft Consensus Tests ===${NC}"

run_test "./pkg/replication" "TestRaftCreation" "Raft Creation" || true
run_test "./pkg/replication" "TestRaftInitialState" "Raft Initial State" || true
run_test "./pkg/replication" "TestRaftRequestVote" "Raft Request Vote" || true
run_test "./pkg/replication" "TestCommandEncoding" "Command Encoding" || true

# 3. 分片测试
echo ""
echo -e "${BLUE}=== Sharding Tests ===${NC}"

run_test "./pkg/sharding" "TestConsistentHashBasic" "Consistent Hash Basic" || true
run_test "./pkg/sharding" "TestConsistentHashDistribution" "Load Distribution" || true
run_test "./pkg/sharding" "TestConsistentHashGetN" "Multiple Replicas" || true

# 测试总结
echo ""
echo -e "${BLUE}================================${NC}"
echo -e "${BLUE}  Test Summary${NC}"
echo -e "${BLUE}================================${NC}"
echo -e "Total Tests:  ${TOTAL_TESTS}"
echo -e "${GREEN}Passed:       ${PASSED_TESTS}${NC}"
if [ ${FAILED_TESTS} -gt 0 ]; then
    echo -e "${RED}Failed:       ${FAILED_TESTS}${NC}"
else
    echo -e "Failed:       ${FAILED_TESTS}"
fi

# 如果所有测试通过
if [ ${FAILED_TESTS} -eq 0 ]; then
    echo ""
    echo -e "${GREEN}================================${NC}"
    echo -e "${GREEN}  ✓ All Tests Passed!${NC}"
    echo -e "${GREEN}================================${NC}"
    echo ""
    echo "Next steps:"
    echo "  • Run full test suite: go test ./pkg/..."
    echo "  • Run with race detector: go test -race ./pkg/..."
    echo "  • Run benchmarks: go test -bench=. ./pkg/..."
    echo "  • Generate coverage: go test -coverprofile=coverage.out ./pkg/..."
    exit 0
else
    echo ""
    echo -e "${RED}================================${NC}"
    echo -e "${RED}  ✗ Some Tests Failed${NC}"
    echo -e "${RED}================================${NC}"
    echo ""
    echo "Check the output above for details."
    echo "Common issues:"
    echo "  • Timeout: Test took longer than 30s"
    echo "  • Deadlock: Check for locking issues"
    echo "  • Race condition: Run with -race flag"
    exit 1
fi
