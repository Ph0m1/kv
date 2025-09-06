package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/ph0m1/kv/pkg/client"
)

var (
	serverAddr = flag.String("server", "localhost:9090", "服务器地址")
	timeout    = flag.Duration("timeout", 10*time.Second, "请求超时时间")
)

func main() {
	flag.Parse()

	if flag.NArg() < 1 {
		printUsage()
		os.Exit(1)
	}

	// 创建客户端
	opts := client.DefaultClientOptions()
	opts.RequestTimeout = *timeout

	cli, err := client.NewClient(*serverAddr, opts)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to connect to server: %v\n", err)
		os.Exit(1)
	}
	defer cli.Close()

	// 解析命令
	command := flag.Arg(0)
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	switch command {
	case "get":
		handleGet(ctx, cli)
	case "put", "set":
		handlePut(ctx, cli)
	case "delete", "del":
		handleDelete(ctx, cli)
	case "batch-get":
		handleBatchGet(ctx, cli)
	case "batch-put":
		handleBatchPut(ctx, cli)
	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n", command)
		printUsage()
		os.Exit(1)
	}
}

func handleGet(ctx context.Context, cli *client.Client) {
	if flag.NArg() < 2 {
		fmt.Fprintln(os.Stderr, "Usage: client get <key>")
		os.Exit(1)
	}

	key := flag.Arg(1)
	value, found, err := cli.Get(ctx, []byte(key))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Get failed: %v\n", err)
		os.Exit(1)
	}

	if found {
		fmt.Printf("%s\n", string(value))
	} else {
		fmt.Println("Key not found")
		os.Exit(1)
	}
}

func handlePut(ctx context.Context, cli *client.Client) {
	if flag.NArg() < 3 {
		fmt.Fprintln(os.Stderr, "Usage: client put <key> <value>")
		os.Exit(1)
	}

	key := flag.Arg(1)
	value := flag.Arg(2)

	err := cli.Put(ctx, []byte(key), []byte(value))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Put failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("OK")
}

func handleDelete(ctx context.Context, cli *client.Client) {
	if flag.NArg() < 2 {
		fmt.Fprintln(os.Stderr, "Usage: client delete <key>")
		os.Exit(1)
	}

	key := flag.Arg(1)
	err := cli.Delete(ctx, []byte(key))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Delete failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("OK")
}

func handleBatchGet(ctx context.Context, cli *client.Client) {
	if flag.NArg() < 2 {
		fmt.Fprintln(os.Stderr, "Usage: client batch-get <key1> <key2> ...")
		os.Exit(1)
	}

	keys := make([][]byte, 0, flag.NArg()-1)
	for i := 1; i < flag.NArg(); i++ {
		keys = append(keys, []byte(flag.Arg(i)))
	}

	result, err := cli.BatchGet(ctx, keys)
	if err != nil {
		fmt.Fprintf(os.Stderr, "BatchGet failed: %v\n", err)
		os.Exit(1)
	}

	for key, value := range result {
		fmt.Printf("%s: %s\n", key, string(value))
	}
}

func handleBatchPut(ctx context.Context, cli *client.Client) {
	if flag.NArg() < 3 || (flag.NArg()-1)%2 != 0 {
		fmt.Fprintln(os.Stderr, "Usage: client batch-put <key1> <value1> <key2> <value2> ...")
		os.Exit(1)
	}

	items := make(map[string][]byte)
	for i := 1; i < flag.NArg(); i += 2 {
		key := flag.Arg(i)
		value := flag.Arg(i + 1)
		items[key] = []byte(value)
	}

	err := cli.BatchPut(ctx, items)
	if err != nil {
		fmt.Fprintf(os.Stderr, "BatchPut failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("OK")
}

func printUsage() {
	fmt.Println("KV Storage Client")
	fmt.Println()
	fmt.Println("Usage:")
	fmt.Println("  client [options] <command> [arguments]")
	fmt.Println()
	fmt.Println("Options:")
	fmt.Println("  -server string")
	fmt.Println("        服务器地址 (default \"localhost:9090\")")
	fmt.Println("  -timeout duration")
	fmt.Println("        请求超时时间 (default 10s)")
	fmt.Println()
	fmt.Println("Commands:")
	fmt.Println("  get <key>")
	fmt.Println("        获取键对应的值")
	fmt.Println()
	fmt.Println("  put <key> <value>")
	fmt.Println("        设置键值对")
	fmt.Println()
	fmt.Println("  delete <key>")
	fmt.Println("        删除键")
	fmt.Println()
	fmt.Println("  batch-get <key1> <key2> ...")
	fmt.Println("        批量获取")
	fmt.Println()
	fmt.Println("  batch-put <key1> <value1> <key2> <value2> ...")
	fmt.Println("        批量写入")
	fmt.Println()
	fmt.Println("Examples:")
	fmt.Println("  client put mykey myvalue")
	fmt.Println("  client get mykey")
	fmt.Println("  client delete mykey")
	fmt.Println("  client batch-put key1 value1 key2 value2")
	fmt.Println("  client batch-get key1 key2 key3")
}
