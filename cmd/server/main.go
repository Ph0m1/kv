package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/ph0m1/kv/pkg/common"
	"github.com/ph0m1/kv/pkg/server"
	"github.com/ph0m1/kv/pkg/storage"
)

var (
	configFile = flag.String("config", "configs/server.yaml", "配置文件路径")
	dataDir    = flag.String("data", "", "数据目录（覆盖配置文件）")
	listenAddr = flag.String("listen", "", "监听地址（覆盖配置文件）")
)

func main() {
	flag.Parse()

	// 加载配置
	config := loadConfig(*configFile)

	// 命令行参数覆盖配置
	if *dataDir != "" {
		config.Server.DataDir = *dataDir
	}
	if *listenAddr != "" {
		config.Server.ListenAddr = *listenAddr
	}

	// 验证配置
	if err := config.Validate(); err != nil {
		fmt.Fprintf(os.Stderr, "配置验证失败: %v\n", err)
		os.Exit(1)
	}

	// 初始化日志
	logger := initLogger(config)
	common.SetDefaultLogger(logger)

	logger.Info("===========================================")
	logger.Info("  KV Storage Server Starting")
	logger.Info("===========================================")
	logger.Info("Node ID: %s", config.Server.NodeID)
	logger.Info("Listen: %s", config.Server.ListenAddr)
	logger.Info("Data Dir: %s", config.Server.DataDir)

	// 创建存储引擎
	logger.Info("Initializing storage engine...")
	store, err := storage.NewLSM(config.Server.DataDir)
	if err != nil {
		logger.Error("Failed to create storage engine: %v", err)
		os.Exit(1)
	}
	logger.Info("Storage engine initialized")

	// 创建服务器
	logger.Info("Creating gRPC server...")
	srv, err := server.NewServer(config, store)
	if err != nil {
		logger.Error("Failed to create server: %v", err)
		os.Exit(1)
	}
	logger.Info("gRPC server created")

	// 启动服务器（在goroutine中）
	errChan := make(chan error, 1)
	go func() {
		if err := srv.Start(); err != nil {
			errChan <- err
		}
	}()

	// 等待信号
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	select {
	case err := <-errChan:
		logger.Error("Server error: %v", err)
		os.Exit(1)
	case sig := <-sigChan:
		logger.Info("Received signal: %v", sig)
		logger.Info("Shutting down gracefully...")
		
		if err := srv.Stop(); err != nil {
			logger.Error("Failed to stop server: %v", err)
			os.Exit(1)
		}
		
		logger.Info("Server stopped successfully")
	}
}

// loadConfig 加载配置文件
func loadConfig(configFile string) *common.Config {
	// TODO: 实现YAML配置文件加载
	// 目前返回默认配置
	config := common.DefaultConfig()
	
	// 如果配置文件存在，可以在这里加载
	// 需要添加 yaml 解析库
	
	return config
}

// initLogger 初始化日志器
func initLogger(config *common.Config) *common.Logger {
	var logger *common.Logger
	
	if config.Logging.File != "" {
		// 创建日志目录
		if err := os.MkdirAll("logs", 0755); err != nil {
			fmt.Fprintf(os.Stderr, "Failed to create log directory: %v\n", err)
		}
		
		fileLogger, err := common.NewFileLogger(common.INFO, config.Logging.File)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to create file logger: %v\n", err)
			logger = common.NewLogger(common.INFO, os.Stdout)
		} else {
			logger = fileLogger
		}
	} else {
		logger = common.NewLogger(common.INFO, os.Stdout)
	}
	
	// 设置日志级别
	switch config.Logging.Level {
	case "debug":
		logger.SetLevel(common.DEBUG)
	case "info":
		logger.SetLevel(common.INFO)
	case "warn":
		logger.SetLevel(common.WARN)
	case "error":
		logger.SetLevel(common.ERROR)
	}
	
	return logger
}
