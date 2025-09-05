package server

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"

	"github.com/ph0m1/kv/pkg/common"
	"github.com/ph0m1/kv/pkg/proto"
	"github.com/ph0m1/kv/pkg/storage"
)

// Server KV存储服务器
type Server struct {
	proto.UnimplementedKVServiceServer

	config  *common.Config
	storage storage.Storage
	grpc    *grpc.Server
	logger  *common.Logger

	mu     sync.RWMutex
	closed bool
}

// NewServer 创建新的服务器实例
func NewServer(config *common.Config, store storage.Storage) (*Server, error) {
	if config == nil {
		return nil, fmt.Errorf("config is required")
	}
	if store == nil {
		return nil, fmt.Errorf("storage is required")
	}

	// 创建日志器
	logger := common.GetDefaultLogger()
	if config.Logging.File != "" {
		fileLogger, err := common.NewFileLogger(
			common.INFO,
			config.Logging.File,
		)
		if err != nil {
			logger.Warn("failed to create file logger: %v", err)
		} else {
			logger = fileLogger
		}
	}

	// 配置gRPC服务器选项
	opts := []grpc.ServerOption{
		grpc.MaxRecvMsgSize(16 * 1024 * 1024), // 16MB
		grpc.MaxSendMsgSize(16 * 1024 * 1024), // 16MB
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:    30 * time.Second,
			Timeout: 10 * time.Second,
		}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             10 * time.Second,
			PermitWithoutStream: true,
		}),
	}

	// 添加拦截器
	opts = append(opts,
		grpc.UnaryInterceptor(unaryInterceptor(logger)),
		grpc.StreamInterceptor(streamInterceptor(logger)),
	)

	grpcServer := grpc.NewServer(opts...)

	s := &Server{
		config:  config,
		storage: store,
		grpc:    grpcServer,
		logger:  logger,
		closed:  false,
	}

	// 注册服务
	proto.RegisterKVServiceServer(grpcServer, s)

	return s, nil
}

// Start 启动服务器
func (s *Server) Start() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return fmt.Errorf("server is closed")
	}
	s.mu.Unlock()

	listener, err := net.Listen("tcp", s.config.Server.ListenAddr)
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}

	s.logger.Info("KV Server starting on %s", s.config.Server.ListenAddr)
	s.logger.Info("Node ID: %s", s.config.Server.NodeID)
	s.logger.Info("Data directory: %s", s.config.Server.DataDir)

	if err := s.grpc.Serve(listener); err != nil {
		return fmt.Errorf("failed to serve: %w", err)
	}

	return nil
}

// Stop 停止服务器
func (s *Server) Stop() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}

	s.logger.Info("Stopping KV Server...")

	// 优雅关闭gRPC服务器
	s.grpc.GracefulStop()

	// 关闭存储引擎
	if err := s.storage.Close(); err != nil {
		s.logger.Error("failed to close storage: %v", err)
	}

	s.closed = true
	s.logger.Info("KV Server stopped")

	return nil
}

// Get 实现Get RPC
func (s *Server) Get(ctx context.Context, req *proto.GetRequest) (*proto.GetResponse, error) {
	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return nil, status.Error(codes.Unavailable, "server is closed")
	}
	s.mu.RUnlock()

	if len(req.Key) == 0 {
		return &proto.GetResponse{
			Found: false,
			Error: "key is empty",
		}, nil
	}

	value, found, err := s.storage.Get(req.Key)
	if err != nil {
		s.logger.Error("Get error: %v", err)
		return &proto.GetResponse{
			Found: false,
			Error: err.Error(),
		}, nil
	}

	return &proto.GetResponse{
		Value: value,
		Found: found,
	}, nil
}

// Put 实现Put RPC
func (s *Server) Put(ctx context.Context, req *proto.PutRequest) (*proto.PutResponse, error) {
	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return nil, status.Error(codes.Unavailable, "server is closed")
	}
	s.mu.RUnlock()

	if len(req.Key) == 0 {
		return &proto.PutResponse{
			Success: false,
			Error:   "key is empty",
		}, nil
	}

	err := s.storage.Put(req.Key, req.Value)
	if err != nil {
		s.logger.Error("Put error: %v", err)
		return &proto.PutResponse{
			Success: false,
			Error:   err.Error(),
		}, nil
	}

	return &proto.PutResponse{
		Success: true,
	}, nil
}

// Delete 实现Delete RPC
func (s *Server) Delete(ctx context.Context, req *proto.DeleteRequest) (*proto.DeleteResponse, error) {
	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return nil, status.Error(codes.Unavailable, "server is closed")
	}
	s.mu.RUnlock()

	if len(req.Key) == 0 {
		return &proto.DeleteResponse{
			Success: false,
			Error:   "key is empty",
		}, nil
	}

	err := s.storage.Delete(req.Key)
	if err != nil {
		s.logger.Error("Delete error: %v", err)
		return &proto.DeleteResponse{
			Success: false,
			Error:   err.Error(),
		}, nil
	}

	return &proto.DeleteResponse{
		Success: true,
	}, nil
}

// Scan 实现Scan RPC（流式）
func (s *Server) Scan(req *proto.ScanRequest, stream proto.KVService_ScanServer) error {
	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return status.Error(codes.Unavailable, "server is closed")
	}
	s.mu.RUnlock()

	// TODO: 实现范围扫描
	// 需要存储引擎支持迭代器
	return status.Error(codes.Unimplemented, "scan not implemented yet")
}

// BatchGet 实现BatchGet RPC
func (s *Server) BatchGet(ctx context.Context, req *proto.BatchGetRequest) (*proto.BatchGetResponse, error) {
	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return nil, status.Error(codes.Unavailable, "server is closed")
	}
	s.mu.RUnlock()

	items := make([]*proto.KeyValue, 0, len(req.Keys))

	for _, key := range req.Keys {
		value, found, err := s.storage.Get(key)
		if err != nil {
			s.logger.Error("BatchGet error for key: %v", err)
			return &proto.BatchGetResponse{
				Error: err.Error(),
			}, nil
		}

		items = append(items, &proto.KeyValue{
			Key:   key,
			Value: value,
			Found: found,
		})
	}

	return &proto.BatchGetResponse{
		Items: items,
	}, nil
}

// BatchPut 实现BatchPut RPC
func (s *Server) BatchPut(ctx context.Context, req *proto.BatchPutRequest) (*proto.BatchPutResponse, error) {
	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return nil, status.Error(codes.Unavailable, "server is closed")
	}
	s.mu.RUnlock()

	for _, item := range req.Items {
		if len(item.Key) == 0 {
			return &proto.BatchPutResponse{
				Success: false,
				Error:   "key is empty",
			}, nil
		}

		err := s.storage.Put(item.Key, item.Value)
		if err != nil {
			s.logger.Error("BatchPut error: %v", err)
			return &proto.BatchPutResponse{
				Success: false,
				Error:   err.Error(),
			}, nil
		}
	}

	return &proto.BatchPutResponse{
		Success: true,
	}, nil
}
