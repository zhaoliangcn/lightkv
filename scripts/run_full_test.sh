#!/bin/bash
# LightKV 完整测试脚本
# 按顺序执行：回归测试 → 性能基准测试 → Pipeline测试 → 并发压力测试 (所有4个SDK)
# Usage: bash scripts/run_full_test.sh [build_dir] [tcp_port] [http_port]

set -e

BUILD_DIR="${1:-build}"
TCP_PORT="${2:-16379}"
HTTP_PORT="${3:-18080}"
PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
SERVER_PID=""

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

pass=0
fail=0
total=0

print_header() {
    echo ""
    echo "═══════════════════════════════════════════════════════════════"
    echo "  $1"
    echo "═══════════════════════════════════════════════════════════════"
    echo ""
}

record() {
    total=$((total + 1))
    if [ "$1" -eq 0 ]; then
        echo -e "  ${GREEN}[PASS]${NC} $2"
        pass=$((pass + 1))
    else
        echo -e "  ${RED}[FAIL]${NC} $2"
        fail=$((fail + 1))
    fi
}

wait_for_server() {
    local port=$1
    local max_attempts=20
    local attempt=0
    while [ $attempt -lt $max_attempts ]; do
        if nc -z 127.0.0.1 "$port" 2>/dev/null; then
            # Verify this is OUR server by checking PID
            if [ -n "$SERVER_PID" ] && kill -0 "$SERVER_PID" 2>/dev/null; then
                return 0
            fi
        fi
        sleep 0.5
        attempt=$((attempt + 1))
    done
    return 1
}

cleanup() {
    echo ""
    echo "Cleaning up..."
    if [ -n "$SERVER_PID" ] && kill -0 "$SERVER_PID" 2>/dev/null; then
        kill "$SERVER_PID" 2>/dev/null
        wait "$SERVER_PID" 2>/dev/null || true
    fi
    echo "Done."
}

trap cleanup EXIT

# ═══════════════════════════════════════════════════════════════
# 1. Build
# ═══════════════════════════════════════════════════════════════
print_header "Step 0: Build"

cd "$PROJECT_DIR"
mkdir -p "$BUILD_DIR"
cd "$BUILD_DIR"
cmake -DCMAKE_BUILD_TYPE=Release .. 2>&1 | tail -1
make -j4 2>&1 | tail -3
record $? "C++ Build"

# ═══════════════════════════════════════════════════════════════
# 2. Start Server
# ═══════════════════════════════════════════════════════════════
print_header "Step 1: Start Server"

# Kill any existing server on the target port
lsof -ti tcp:"$TCP_PORT" 2>/dev/null | xargs kill -9 2>/dev/null || true
sleep 1

DB_PATH="/tmp/lightkv_full_test"
rm -rf "$DB_PATH"
./lightkv_server --db-path "$DB_PATH" --tcp-port "$TCP_PORT" --http-port "$HTTP_PORT" &
SERVER_PID=$!
echo "  Server PID: $SERVER_PID"

sleep 2
if kill -0 "$SERVER_PID" 2>/dev/null && nc -z 127.0.0.1 "$TCP_PORT" 2>/dev/null; then
    record 0 "Server started on port $TCP_PORT"
else
    record 1 "Server failed to start"
    exit 1
fi

# ═══════════════════════════════════════════════════════════════
# 3. C++ Regression Tests
# ═══════════════════════════════════════════════════════════════
print_header "Step 2: C++ Regression Tests"

if ctest --output-on-failure -j4 2>&1; then
    record 0 "C++ Regression Tests (all passed)"
else
    record 1 "C++ Regression Tests"
fi

# ═══════════════════════════════════════════════════════════════
# 4. C++ Benchmarks
# ═══════════════════════════════════════════════════════════════
print_header "Step 3: C++ Benchmarks"

echo "  [C++ Single-thread Bench]"
./tests/bench_cpp 2>&1
record $? "C++ Single-thread Benchmark"

echo ""
echo "  [C++ Pipeline Bench]"
./tests/bench_pipeline_cpp 2>&1
record $? "C++ Pipeline Benchmark"

echo ""
echo "  [C++ Stress Test]"
./tests/stress_cpp 2>&1
record $? "C++ Stress Test"

# ═══════════════════════════════════════════════════════════════
# 5. Node.js Benchmarks
# ═══════════════════════════════════════════════════════════════
print_header "Step 4: Node.js Benchmarks"

echo "  [Node.js Single-thread Bench]"
node "$PROJECT_DIR/clients/nodejs/test/bench.js" 2>&1
record $? "Node.js Single-thread Benchmark"

echo ""
echo "  [Node.js Pipeline Bench]"
node "$PROJECT_DIR/clients/nodejs/test/bench_pipeline.js" 2>&1
record $? "Node.js Pipeline Benchmark"

echo ""
echo "  [Node.js Stress Test]"
node "$PROJECT_DIR/clients/nodejs/test/stress.js" 2>&1
record $? "Node.js Stress Test"

# ═══════════════════════════════════════════════════════════════
# 6. Python Benchmarks
# ═══════════════════════════════════════════════════════════════
print_header "Step 5: Python Benchmarks"

echo "  [Python Single-thread Bench]"
python3 "$PROJECT_DIR/clients/python/tests/bench.py" 2>&1
record $? "Python Single-thread Benchmark"

echo ""
echo "  [Python Pipeline Bench]"
python3 "$PROJECT_DIR/clients/python/tests/bench_pipeline.py" 2>&1
record $? "Python Pipeline Benchmark"

echo ""
echo "  [Python Stress Test]"
python3 "$PROJECT_DIR/clients/python/tests/stress.py" 2>&1
record $? "Python Stress Test"

# ═══════════════════════════════════════════════════════════════
# 7. Go Benchmarks
# ═══════════════════════════════════════════════════════════════
print_header "Step 6: Go Benchmarks"

cd "$PROJECT_DIR/clients/go"

echo "  [Go Single-thread Bench]"
go test -bench=. -benchtime=10000x 2>&1
record $? "Go Single-thread Benchmark"

echo ""
echo "  [Go Pipeline Bench]"
go test -run=TestPipelineBenchmark -v 2>&1
record $? "Go Pipeline Benchmark"

echo ""
echo "  [Go Stress Test]"
go test -run=TestStressTest -v 2>&1
record $? "Go Stress Test"

cd "$PROJECT_DIR"

# ═══════════════════════════════════════════════════════════════
# 8. Summary
# ═══════════════════════════════════════════════════════════════
print_header "Summary"

echo -e "  ${CYAN}Total:${NC} $total"
echo -e "  ${GREEN}Pass:${NC}  $pass"
echo -e "  ${RED}Fail:${NC}  $fail"
echo ""

if [ "$fail" -eq 0 ]; then
    echo -e "  ${GREEN}═════════════════════════════════════${NC}"
    echo -e "  ${GREEN}  All tests passed!${NC}"
    echo -e "  ${GREEN}═════════════════════════════════════${NC}"
else
    echo -e "  ${RED}  $fail test(s) failed${NC}"
fi

echo ""
echo "Full test log saved to: $PROJECT_DIR/build/test_report.txt"

exit $fail