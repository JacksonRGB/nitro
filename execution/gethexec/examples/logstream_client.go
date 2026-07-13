// Connects to the local Nitro gRPC log stream and writes one JSON object per line.
//
// Run from the repository root:
//
//	go run ./execution/gethexec/examples/logstream_client.go
//
// Override the endpoint when needed:
//
//	go run ./execution/gethexec/examples/logstream_client.go -addr 127.0.0.1:18888
package main

import (
	"context"
	"encoding/json"
	"flag"
	"io"
	"log"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/structpb"
)

const logStreamSubscribeMethod = "/nitro.logs.v1.LogStream/Subscribe"

func main() {
	address := flag.String("addr", "127.0.0.1:18888", "Nitro gRPC log stream address")
	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	connection, err := grpc.DialContext(ctx, *address, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		log.Fatalf("connect to %s: %v", *address, err)
	}
	defer connection.Close()

	stream, err := connection.NewStream(context.Background(), &grpc.StreamDesc{ServerStreams: true}, logStreamSubscribeMethod)
	if err != nil {
		log.Fatalf("open log stream: %v", err)
	}
	if err := stream.SendMsg(&emptypb.Empty{}); err != nil {
		log.Fatalf("subscribe to log stream: %v", err)
	}
	if err := stream.CloseSend(); err != nil {
		log.Fatalf("close subscription request: %v", err)
	}

	encoder := json.NewEncoder(os.Stdout)
	for {
		event := new(structpb.Struct)
		if err := stream.RecvMsg(event); err != nil {
			if err == io.EOF {
				return
			}
			log.Fatalf("receive log event: %v", err)
		}
		if err := encoder.Encode(event.AsMap()); err != nil {
			log.Fatalf("encode log event: %v", err)
		}
	}
}
