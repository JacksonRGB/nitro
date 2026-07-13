// Compares the arrival time of an ERC-20 Transfer log from Nitro's local gRPC
// pending-log stream and eth_subscribe.
//
// Run from the repository root:
//
//	go run ./execution/gethexec/examples/transfer_latency_compare
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/structpb"
)

const (
	defaultTokenAddress      = "0x7e3230934318979d62e0432faf4a4f75cb483534"
	transferTopic            = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
	logStreamSubscribeMethod = "/nitro.logs.v1.LogStream/Subscribe"
)

type source string

const (
	sourceGRPC source = "grpc"
	sourceETH  source = "eth_subscribe"
)

type observedLog struct {
	source              source
	key                 string
	txHash              string
	logIndex            string
	blockNumber         string
	previousBlockNumber string
	phase               string
	receivedAt          time.Time
	grpcEnqueuedAt      time.Time
}

func main() {
	grpcAddress := flag.String("grpc-addr", "127.0.0.1:18888", "Nitro gRPC pending-log stream address")
	ethWS := flag.String("eth-ws", "ws://127.0.0.1:8548", "Ethereum WebSocket RPC address")
	tokenAddress := flag.String("token", defaultTokenAddress, "ERC-20 token contract address")
	reconnectDelay := flag.Duration("reconnect-delay", time.Second, "delay before reconnecting a failed subscription")
	flag.Parse()

	token := common.HexToAddress(*tokenAddress)
	topic := common.HexToHash(transferTopic)
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	events := make(chan observedLog, 256)
	errors := make(chan error, 2)
	var workers sync.WaitGroup
	workers.Add(2)
	go func() {
		defer workers.Done()
		runGRPCSubscription(ctx, *grpcAddress, token, topic, *reconnectDelay, events, errors)
	}()
	go func() {
		defer workers.Done()
		runETHSubscription(ctx, *ethWS, token, topic, *reconnectDelay, events, errors)
	}()

	log.Printf("monitoring Transfer(address,address,uint256), token=%s grpc=%s eth-ws=%s", token, *grpcAddress, *ethWS)
	compareEvents(ctx, events, errors)
	cancel()
	workers.Wait()
}

func runGRPCSubscription(ctx context.Context, address string, token common.Address, topic common.Hash, reconnectDelay time.Duration, events chan<- observedLog, errors chan<- error) {
	for ctx.Err() == nil {
		if err := subscribeGRPC(ctx, address, token, topic, events); err != nil && ctx.Err() == nil {
			reportError(errors, fmt.Errorf("gRPC subscription: %w", err))
		}
		waitForReconnect(ctx, reconnectDelay)
	}
}

func subscribeGRPC(ctx context.Context, address string, token common.Address, topic common.Hash, events chan<- observedLog) error {
	connection, err := grpc.DialContext(ctx, address, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		return err
	}
	defer connection.Close()

	stream, err := connection.NewStream(ctx, &grpc.StreamDesc{ServerStreams: true}, logStreamSubscribeMethod)
	if err != nil {
		return err
	}
	if err := stream.SendMsg(&emptypb.Empty{}); err != nil {
		return err
	}
	if err := stream.CloseSend(); err != nil {
		return err
	}

	for {
		event := new(structpb.Struct)
		if err := stream.RecvMsg(event); err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		observed, ok := grpcTransferLog(event, token, topic, time.Now())
		if ok {
			select {
			case events <- observed:
			case <-ctx.Done():
				return nil
			}
		}
	}
}

func grpcTransferLog(event *structpb.Struct, token common.Address, topic common.Hash, receivedAt time.Time) (observedLog, bool) {
	fields := event.GetFields()
	if common.HexToAddress(stringField(fields, "address")) != token {
		return observedLog{}, false
	}
	topics := fields["topics"].GetListValue().GetValues()
	if len(topics) == 0 || common.HexToHash(topics[0].GetStringValue()) != topic {
		return observedLog{}, false
	}
	txHash := stringField(fields, "transaction_hash")
	logIndex := stringField(fields, "log_index")
	if txHash == "" || logIndex == "" {
		return observedLog{}, false
	}
	return observedLog{
		source:              sourceGRPC,
		key:                 eventKey(txHash, logIndex),
		txHash:              txHash,
		logIndex:            logIndex,
		blockNumber:         stringField(fields, "block_number"),
		previousBlockNumber: stringField(fields, "previous_block_number"),
		phase:               stringField(fields, "phase"),
		receivedAt:          receivedAt,
		grpcEnqueuedAt:      unixNanoField(fields, "emitted_at_unix_nano"),
	}, true
}

func runETHSubscription(ctx context.Context, endpoint string, token common.Address, topic common.Hash, reconnectDelay time.Duration, events chan<- observedLog, errors chan<- error) {
	for ctx.Err() == nil {
		if err := subscribeETH(ctx, endpoint, token, topic, events); err != nil && ctx.Err() == nil {
			reportError(errors, fmt.Errorf("eth_subscribe logs: %w", err))
		}
		waitForReconnect(ctx, reconnectDelay)
	}
}

func subscribeETH(ctx context.Context, endpoint string, token common.Address, topic common.Hash, events chan<- observedLog) error {
	client, err := ethclient.DialContext(ctx, endpoint)
	if err != nil {
		return err
	}
	defer client.Close()

	logs := make(chan types.Log, 128)
	subscription, err := client.SubscribeFilterLogs(ctx, ethereum.FilterQuery{
		Addresses: []common.Address{token},
		Topics:    [][]common.Hash{{topic}},
	}, logs)
	if err != nil {
		return err
	}
	defer subscription.Unsubscribe()

	for {
		select {
		case eventLog := <-logs:
			observed := observedLog{
				source:      sourceETH,
				key:         eventKey(eventLog.TxHash.Hex(), strconv.FormatUint(uint64(eventLog.Index), 10)),
				txHash:      eventLog.TxHash.Hex(),
				logIndex:    strconv.FormatUint(uint64(eventLog.Index), 10),
				blockNumber: strconv.FormatUint(eventLog.BlockNumber, 10),
				receivedAt:  time.Now(),
			}
			select {
			case events <- observed:
			case <-ctx.Done():
				return nil
			}
		case err := <-subscription.Err():
			return err
		case <-ctx.Done():
			return nil
		}
	}
}

func compareEvents(ctx context.Context, events <-chan observedLog, errors <-chan error) {
	pending := make(map[string]observedLog)
	cleanupTicker := time.NewTicker(5 * time.Minute)
	defer cleanupTicker.Stop()

	for {
		select {
		case event := <-events:
			fmt.Printf("[%s] tx=%s log=%s block=%s received=%s\n", event.source, event.txHash, event.logIndex, event.blockNumber, event.receivedAt.Format(time.RFC3339Nano))
			if earlier, ok := pending[event.key]; ok && earlier.source != event.source {
				printComparison(earlier, event)
				delete(pending, event.key)
			} else {
				pending[event.key] = event
			}
		case err := <-errors:
			log.Printf("%v; reconnecting", err)
		case <-cleanupTicker.C:
			cutoff := time.Now().Add(-5 * time.Minute)
			for key, event := range pending {
				if event.receivedAt.Before(cutoff) {
					delete(pending, key)
				}
			}
		case <-ctx.Done():
			return
		}
	}
}

func printComparison(first, second observedLog) {
	var grpcEvent, ethEvent observedLog
	if first.source == sourceGRPC {
		grpcEvent, ethEvent = first, second
	} else {
		grpcEvent, ethEvent = second, first
	}
	fmt.Printf(
		"MATCH tx=%s log=%s previous_block=%s candidate_block=%s grpc_received=%s eth_received=%s eth_minus_grpc=%s grpc_queue_to_receive=%s\n",
		grpcEvent.txHash,
		grpcEvent.logIndex,
		grpcEvent.previousBlockNumber,
		grpcEvent.blockNumber,
		grpcEvent.receivedAt.Format(time.RFC3339Nano),
		ethEvent.receivedAt.Format(time.RFC3339Nano),
		ethEvent.receivedAt.Sub(grpcEvent.receivedAt),
		grpcEvent.receivedAt.Sub(grpcEvent.grpcEnqueuedAt),
	)
}

func eventKey(txHash, logIndex string) string {
	return strings.ToLower(txHash) + ":" + logIndex
}

func stringField(fields map[string]*structpb.Value, name string) string {
	if field, ok := fields[name]; ok {
		return field.GetStringValue()
	}
	return ""
}

func unixNanoField(fields map[string]*structpb.Value, name string) time.Time {
	nanos, err := strconv.ParseInt(stringField(fields, name), 10, 64)
	if err != nil {
		return time.Time{}
	}
	return time.Unix(0, nanos)
}

func waitForReconnect(ctx context.Context, delay time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(delay):
	}
}

func reportError(errors chan<- error, err error) {
	select {
	case errors <- err:
	default:
	}
}
