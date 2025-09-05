package client

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"

	"github.com/ph0m1/kv/pkg/proto"
)

// Client KV存储客户端
type Client struct {
	conn   *grpc.ClientConn
	client proto.KVServiceClient
	addr   string
}

// ClientOptions 客户端配置选项
type ClientOptions struct {
	// DialTimeout 连接超时时间
	DialTimeout time.Duration
	// RequestTimeout 请求超时时间
	RequestTimeout time.Duration
	// MaxRetries 最大重试次数
	MaxRetries int
}

// DefaultClientOptions 返回默认客户端选项
func DefaultClientOptions() *ClientOptions {
	return &ClientOptions{
		DialTimeout:    5 * time.Second,
		RequestTimeout: 10 * time.Second,
		MaxRetries:     3,
	}
}

// NewClient 创建新的客户端
func NewClient(addr string, opts *ClientOptions) (*Client, error) {
	if opts == nil {
		opts = DefaultClientOptions()
	}

	// 配置gRPC连接选项
	dialOpts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                30 * time.Second,
			Timeout:             10 * time.Second,
			PermitWithoutStream: true,
		}),
	}

	// 连接服务器
	ctx, cancel := context.WithTimeout(context.Background(), opts.DialTimeout)
	defer cancel()

	conn, err := grpc.DialContext(ctx, addr, dialOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to %s: %w", addr, err)
	}

	return &Client{
		conn:   conn,
		client: proto.NewKVServiceClient(conn),
		addr:   addr,
	}, nil
}

// Close 关闭客户端连接
func (c *Client) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// Get 获取键对应的值
func (c *Client) Get(ctx context.Context, key []byte) ([]byte, bool, error) {
	req := &proto.GetRequest{
		Key: key,
	}

	resp, err := c.client.Get(ctx, req)
	if err != nil {
		return nil, false, fmt.Errorf("get failed: %w", err)
	}

	if resp.Error != "" {
		return nil, false, fmt.Errorf("get error: %s", resp.Error)
	}

	return resp.Value, resp.Found, nil
}

// Put 设置键值对
func (c *Client) Put(ctx context.Context, key, value []byte) error {
	req := &proto.PutRequest{
		Key:   key,
		Value: value,
	}

	resp, err := c.client.Put(ctx, req)
	if err != nil {
		return fmt.Errorf("put failed: %w", err)
	}

	if !resp.Success {
		return fmt.Errorf("put error: %s", resp.Error)
	}

	return nil
}

// Delete 删除键
func (c *Client) Delete(ctx context.Context, key []byte) error {
	req := &proto.DeleteRequest{
		Key: key,
	}

	resp, err := c.client.Delete(ctx, req)
	if err != nil {
		return fmt.Errorf("delete failed: %w", err)
	}

	if !resp.Success {
		return fmt.Errorf("delete error: %s", resp.Error)
	}

	return nil
}

// BatchGet 批量获取
func (c *Client) BatchGet(ctx context.Context, keys [][]byte) (map[string][]byte, error) {
	req := &proto.BatchGetRequest{
		Keys: keys,
	}

	resp, err := c.client.BatchGet(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("batch get failed: %w", err)
	}

	if resp.Error != "" {
		return nil, fmt.Errorf("batch get error: %s", resp.Error)
	}

	result := make(map[string][]byte)
	for _, item := range resp.Items {
		if item.Found {
			result[string(item.Key)] = item.Value
		}
	}

	return result, nil
}

// BatchPut 批量写入
func (c *Client) BatchPut(ctx context.Context, items map[string][]byte) error {
	kvs := make([]*proto.KeyValue, 0, len(items))
	for key, value := range items {
		kvs = append(kvs, &proto.KeyValue{
			Key:   []byte(key),
			Value: value,
		})
	}

	req := &proto.BatchPutRequest{
		Items: kvs,
	}

	resp, err := c.client.BatchPut(ctx, req)
	if err != nil {
		return fmt.Errorf("batch put failed: %w", err)
	}

	if !resp.Success {
		return fmt.Errorf("batch put error: %s", resp.Error)
	}

	return nil
}

// Scan 范围扫描
func (c *Client) Scan(ctx context.Context, startKey, endKey []byte, limit int32) (map[string][]byte, error) {
	req := &proto.ScanRequest{
		StartKey: startKey,
		EndKey:   endKey,
		Limit:    limit,
	}

	stream, err := c.client.Scan(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("scan failed: %w", err)
	}

	result := make(map[string][]byte)
	for {
		resp, err := stream.Recv()
		if err != nil {
			// 流结束
			break
		}

		if resp.Error != "" {
			return nil, fmt.Errorf("scan error: %s", resp.Error)
		}

		result[string(resp.Key)] = resp.Value
	}

	return result, nil
}
