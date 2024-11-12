package main

//
// start a worker process, which is implemented
// in ../mr/worker.go. typically there will be
// multiple worker processes, talking to one coordinator.
//
// go run mrworker.go wc.so
//
// Please do not change this file.
//

import (
	"fmt"
	"log"
	"mapreduce/mr"
	"os"
	"plugin"
	pb "mapreduce/proto"
)

func main() {
	//命令行参数个数不等于2，也就是说没有加载插件
	if len(os.Args) != 2 {
		fmt.Fprintf(os.Stderr, "Usage: mrworker xxx.so\n")
		os.Exit(1)
	}

	//加载插件，获得MapFunc和ReduceFunc
	mapf, reducef := loadPlugin(os.Args[1])

	//获取grpc conn
	conn, err := grpc.Dial("localhost:1234", grpc.WithInsecure())
	if err != nil {
			log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()

	// 创建 gRPC 客户端
	client := pb.NewMapReduceClient(conn)
	ctx, cancel := context.with(context.Background(), time.Second)
	defer cancel()

	//worker执行任务
	mr.Worker(mapf, reducef)
}

// load the application Map and Reduce functions
// from a plugin file, e.g. ../mrapps/wc.so
func loadPlugin(filename string) (func(string, string) []mr.KeyValue, func(string, []string) string) {
	p, err := plugin.Open(filename)
	if err != nil {
		log.Fatalf("cannot load plugin %v", filename)
	}
	xmapf, err := p.Lookup("Map")
	if err != nil {
		log.Fatalf("cannot find Map in %v", filename)
	}
	mapf := xmapf.(func(string, string) []mr.KeyValue)
	xreducef, err := p.Lookup("Reduce")
	if err != nil {
		log.Fatalf("cannot find Reduce in %v", filename)
	}
	reducef := xreducef.(func(string, []string) string)

	return mapf, reducef
}
