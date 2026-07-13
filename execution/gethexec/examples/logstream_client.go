// Connects to the local Nitro gRPC log stream and prints every receipt log.
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
	"flag"
	"fmt"
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

	for {
		event := new(structpb.Struct)
		if err := stream.RecvMsg(event); err != nil {
			if err == io.EOF {
				return
			}
			log.Fatalf("receive log event: %v", err)
		}
		printLogEvent(event)
	}
}

func printLogEvent(event *structpb.Struct) {
	fields := event.GetFields()
	fmt.Fprintf(os.Stdout, "address: %s\n", stringField(fields, "address"))
	fmt.Fprintln(os.Stdout, "topics:")
	for index, topic := range fields["topics"].GetListValue().GetValues() {
		fmt.Fprintf(os.Stdout, "  [%d]: %s\n", index, topic.GetStringValue())
	}
	fmt.Fprintf(os.Stdout, "data: %s\n", stringField(fields, "data"))
	fmt.Fprintf(os.Stdout, "block_number: %s\n", stringField(fields, "block_number"))
	fmt.Fprintf(os.Stdout, "previous_block_number: %s\n", stringField(fields, "previous_block_number"))
	fmt.Fprintf(os.Stdout, "block_hash (provisional): %s\n", stringField(fields, "block_hash"))
	fmt.Fprintf(os.Stdout, "transaction_hash: %s\n", stringField(fields, "transaction_hash"))
	fmt.Fprintf(os.Stdout, "sender: %s\n", stringField(fields, "sender"))
	fmt.Fprintf(os.Stdout, "transaction_index: %s\n", stringField(fields, "transaction_index"))
	fmt.Fprintf(os.Stdout, "log_index: %s\n", stringField(fields, "log_index"))
	fmt.Fprintf(os.Stdout, "removed: %t\n", boolField(fields, "removed"))
	fmt.Fprintf(os.Stdout, "phase: %s\n", stringField(fields, "phase"))
	fmt.Fprintf(os.Stdout, "emitted_at_unix_nano: %s\n\n", stringField(fields, "emitted_at_unix_nano"))
}

func stringField(fields map[string]*structpb.Value, name string) string {
	if field, ok := fields[name]; ok {
		return field.GetStringValue()
	}
	return "<missing>"
}

func boolField(fields map[string]*structpb.Value, name string) bool {
	if field, ok := fields[name]; ok {
		return field.GetBoolValue()
	}
	return false
}
