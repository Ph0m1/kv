package server

import (
	"context"
	"time"

	"google.golang.org/grpc"

	"github.com/ph0m1/kv/pkg/common"
)

// unaryInterceptor 一元RPC拦截器
func unaryInterceptor(logger *common.Logger) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req interface{},
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (interface{}, error) {
		start := time.Now()

		// 调用实际的处理函数
		resp, err := handler(ctx, req)

		// 记录日志
		duration := time.Since(start)
		if err != nil {
			logger.Error("[%s] failed in %v: %v", info.FullMethod, duration, err)
		} else {
			logger.Info("[%s] completed in %v", info.FullMethod, duration)
		}

		return resp, err
	}
}

// streamInterceptor 流式RPC拦截器
func streamInterceptor(logger *common.Logger) grpc.StreamServerInterceptor {
	return func(
		srv interface{},
		ss grpc.ServerStream,
		info *grpc.StreamServerInfo,
		handler grpc.StreamHandler,
	) error {
		start := time.Now()

		// 调用实际的处理函数
		err := handler(srv, ss)

		// 记录日志
		duration := time.Since(start)
		if err != nil {
			logger.Error("[%s] stream failed in %v: %v", info.FullMethod, duration, err)
		} else {
			logger.Info("[%s] stream completed in %v", info.FullMethod, duration)
		}

		return err
	}
}
