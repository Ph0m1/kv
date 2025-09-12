.PHONY: all build clean test proto run-server run-client fmt lint

# 变量定义
BINARY_DIR=bin
SERVER_BINARY=$(BINARY_DIR)/kv-server
CLIENT_BINARY=$(BINARY_DIR)/kv-client
PROTO_DIR=pkg/proto
GO=go
PROTOC=protoc

# 默认目标
all: build

# 创建bin目录
$(BINARY_DIR):
	mkdir -p $(BINARY_DIR)

# 编译proto文件
proto:
	@echo "Generating protobuf files..."
	$(PROTOC) --go_out=. --go_opt=paths=source_relative \
		--go-grpc_out=. --go-grpc_opt=paths=source_relative \
		$(PROTO_DIR)/kv.proto

# 编译服务器
build-server: $(BINARY_DIR)
	@echo "Building server..."
	$(GO) build -o $(SERVER_BINARY) cmd/server/main.go

# 编译客户端
build-client: $(BINARY_DIR)
	@echo "Building client..."
	$(GO) build -o $(CLIENT_BINARY) cmd/client/main.go

# 编译所有
build: build-server build-client
	@echo "Build complete!"

# 运行服务器
run-server: build-server
	$(SERVER_BINARY)

# 运行客户端
run-client: build-client
	$(CLIENT_BINARY)

# 测试
test:
	@echo "Running tests..."
	$(GO) test -v -race ./...

# 基准测试
benchmark:
	@echo "Running benchmarks..."
	$(GO) test -bench=. -benchmem ./pkg/storage/...

# 代码格式化
fmt:
	@echo "Formatting code..."
	$(GO) fmt ./...

# 代码检查
lint:
	@echo "Linting code..."
	golangci-lint run

# 清理
clean:
	@echo "Cleaning..."
	rm -rf $(BINARY_DIR)
	rm -rf data/
	rm -rf logs/

# 安装依赖
deps:
	@echo "Installing dependencies..."
	$(GO) mod download
	$(GO) mod tidy

# 帮助信息
help:
	@echo "Available targets:"
	@echo "  all              - Build server and client (default)"
	@echo "  build            - Build server and client"
	@echo "  build-server     - Build server only"
	@echo "  build-client     - Build client only"
	@echo "  proto            - Generate protobuf files"
	@echo "  run-server       - Run server"
	@echo "  run-client       - Run client"
	@echo "  test             - Run unit tests"
	@echo "  benchmark        - Run benchmark tests"
	@echo "  fmt              - Format code"
	@echo "  lint             - Lint code"
	@echo "  clean            - Clean build artifacts and data"
	@echo "  deps             - Install dependencies"
