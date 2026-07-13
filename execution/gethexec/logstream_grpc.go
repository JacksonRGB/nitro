// Copyright 2026, Offchain Labs, Inc.
// For license information, see https://github.com/offchainlabs/nitro/blob/master/LICENSE.md

package gethexec

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/structpb"
)

const (
	grpcLogStreamListenAddress = "0.0.0.0:18888"

	// LogStreamSubscribeFullMethodName is the server-streaming gRPC method exposed
	// by the execution node. Its request is google.protobuf.Empty and each response
	// is a google.protobuf.Struct containing one Ethereum log.
	LogStreamSubscribeFullMethodName = "/nitro.logs.v1.LogStream/Subscribe"

	grpcLogSubscriberQueueSize = 1024
)

// LogStreamServer is the server API for the local execution-log stream.
//
// The response fields are address, topics, data, block_number,
// previous_block_number, block_hash, transaction_hash, transaction_index,
// log_index, removed, phase, and
// emitted_at_unix_nano. Numeric fields are decimal strings so clients do not
// lose precision when decoding google.protobuf.Struct. phase is always
// receipt: execution completed, but the block is still being assembled, so
// block_hash is provisional.
type LogStreamServer interface {
	Subscribe(*emptypb.Empty, LogStream_SubscribeServer) error
}

// LogStream_SubscribeServer is the server side of LogStream.Subscribe.
type LogStream_SubscribeServer interface {
	Send(*structpb.Struct) error
	grpc.ServerStream
}

type logStreamSubscribeServer struct {
	grpc.ServerStream
}

func (s *logStreamSubscribeServer) Send(event *structpb.Struct) error {
	return s.ServerStream.SendMsg(event)
}

// LogStreamClient is a minimal client for the local execution-log stream.
type LogStreamClient interface {
	Subscribe(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (LogStream_SubscribeClient, error)
}

// LogStream_SubscribeClient is the client side of LogStream.Subscribe.
type LogStream_SubscribeClient interface {
	Recv() (*structpb.Struct, error)
	grpc.ClientStream
}

type logStreamClient struct {
	cc grpc.ClientConnInterface
}

type logStreamSubscribeClient struct {
	grpc.ClientStream
}

func (c *logStreamSubscribeClient) Recv() (*structpb.Struct, error) {
	event := new(structpb.Struct)
	if err := c.ClientStream.RecvMsg(event); err != nil {
		return nil, err
	}
	return event, nil
}

// NewLogStreamClient creates a client for LogStreamSubscribeFullMethodName.
func NewLogStreamClient(cc grpc.ClientConnInterface) LogStreamClient {
	return &logStreamClient{cc: cc}
}

func (c *logStreamClient) Subscribe(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (LogStream_SubscribeClient, error) {
	stream, err := c.cc.NewStream(ctx, &LogStream_ServiceDesc.Streams[0], LogStreamSubscribeFullMethodName, opts...)
	if err != nil {
		return nil, err
	}
	client := &logStreamSubscribeClient{ClientStream: stream}
	if err := client.SendMsg(in); err != nil {
		return nil, err
	}
	if err := client.CloseSend(); err != nil {
		return nil, err
	}
	return client, nil
}

// RegisterLogStreamServer registers the local execution-log streaming service.
func RegisterLogStreamServer(registrar grpc.ServiceRegistrar, server LogStreamServer) {
	registrar.RegisterService(&LogStream_ServiceDesc, server)
}

// LogStream_ServiceDesc is equivalent to the generated descriptor for:
//
//	service LogStream {
//	  rpc Subscribe(google.protobuf.Empty) returns (stream google.protobuf.Struct);
//	}
var LogStream_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "nitro.logs.v1.LogStream",
	HandlerType: (*LogStreamServer)(nil),
	Streams: []grpc.StreamDesc{{
		StreamName:    "Subscribe",
		Handler:       logStreamSubscribeHandler,
		ServerStreams: true,
	}},
}

func logStreamSubscribeHandler(server interface{}, stream grpc.ServerStream) error {
	request := new(emptypb.Empty)
	if err := stream.RecvMsg(request); err != nil {
		return err
	}
	return server.(LogStreamServer).Subscribe(request, &logStreamSubscribeServer{ServerStream: stream})
}

type grpcLogBatch struct {
	blockHash           common.Hash
	blockNum            uint64
	previousBlockNumber uint64
	logs                []*types.Log
	emittedAt           time.Time
	phase               string
}

// GRPCLogPublisher publishes every EVM receipt log to local gRPC subscribers.
// Publishing from the block-creation goroutine is deliberately non-blocking.
type GRPCLogPublisher struct {
	listenAddress string
	queueMutex    sync.Mutex
	queuedBatches []grpcLogBatch
	queueWakeup   chan struct{}
	done          chan struct{}

	startMutex sync.Mutex
	started    bool
	stopOnce   sync.Once
	stopped    atomic.Bool
	server     *grpc.Server
	listener   net.Listener
	wg         sync.WaitGroup

	subscribersMutex sync.Mutex
	subscribers      map[uint64]chan *structpb.Struct
	nextSubscriber   uint64
}

// NewGRPCLogPublisher creates the publisher used by an ExecutionEngine. Its
// externally visible entry point is intentionally fixed at 0.0.0.0:18888.
func NewGRPCLogPublisher() *GRPCLogPublisher {
	return newGRPCLogPublisher(grpcLogStreamListenAddress)
}

func newGRPCLogPublisher(listenAddress string) *GRPCLogPublisher {
	return &GRPCLogPublisher{
		listenAddress: listenAddress,
		queueWakeup:   make(chan struct{}, 1),
		done:          make(chan struct{}),
		subscribers:   make(map[uint64]chan *structpb.Struct),
	}
}

// Start binds the listener and starts the gRPC server and its background
// dispatcher. It must be called once.
func (p *GRPCLogPublisher) Start(ctx context.Context) error {
	p.startMutex.Lock()
	defer p.startMutex.Unlock()
	if p.started {
		return errors.New("gRPC log publisher already started")
	}

	listener, err := net.Listen("tcp", p.listenAddress)
	if err != nil {
		return fmt.Errorf("listen for gRPC log stream on %s: %w", p.listenAddress, err)
	}
	p.listener = listener
	p.server = grpc.NewServer()
	RegisterLogStreamServer(p.server, p)
	p.started = true

	p.wg.Add(3)
	go func() {
		defer p.wg.Done()
		if err := p.server.Serve(listener); err != nil && !p.stopped.Load() {
			log.Error("gRPC log stream server stopped unexpectedly", "err", err)
		}
	}()
	go func() {
		defer p.wg.Done()
		p.dispatch()
	}()
	go func() {
		defer p.wg.Done()
		select {
		case <-ctx.Done():
		case <-p.done:
		}
		p.StopOnly()
	}()

	log.Info("gRPC log stream server started", "address", p.listenAddress)
	return nil
}

// PublishReceipt queues logs as soon as their transaction has been executed.
// It never waits for a network operation or a subscriber.
func (p *GRPCLogPublisher) PublishReceipt(receipt *types.Receipt, previousBlockNumber uint64) {
	if p == nil || p.stopped.Load() || receipt == nil || len(receipt.Logs) == 0 {
		return
	}

	var blockNum uint64
	if receipt.BlockNumber != nil {
		blockNum = receipt.BlockNumber.Uint64()
	}
	batch := grpcLogBatch{
		blockHash:           receipt.BlockHash,
		blockNum:            blockNum,
		previousBlockNumber: previousBlockNumber,
		logs:                append([]*types.Log(nil), receipt.Logs...),
		emittedAt:           time.Now().UTC(),
		phase:               "receipt",
	}
	p.queueMutex.Lock()
	p.queuedBatches = append(p.queuedBatches, batch)
	p.queueMutex.Unlock()
	select {
	case p.queueWakeup <- struct{}{}:
	default:
	}
}

// Subscribe streams every log committed after the client connects.
func (p *GRPCLogPublisher) Subscribe(_ *emptypb.Empty, stream LogStream_SubscribeServer) error {
	subscriber, err := p.addSubscriber()
	if err != nil {
		return err
	}
	defer p.removeSubscriber(subscriber.id)

	for {
		select {
		case event, ok := <-subscriber.events:
			if !ok {
				return status.Error(codes.ResourceExhausted, "gRPC log stream subscriber is too slow")
			}
			if err := stream.Send(event); err != nil {
				return err
			}
		case <-stream.Context().Done():
			return stream.Context().Err()
		case <-p.done:
			return status.Error(codes.Unavailable, "gRPC log stream server stopped")
		}
	}
}

type grpcLogSubscriber struct {
	id     uint64
	events <-chan *structpb.Struct
}

func (p *GRPCLogPublisher) addSubscriber() (grpcLogSubscriber, error) {
	p.subscribersMutex.Lock()
	defer p.subscribersMutex.Unlock()
	if p.stopped.Load() {
		return grpcLogSubscriber{}, status.Error(codes.Unavailable, "gRPC log stream server stopped")
	}
	p.nextSubscriber++
	events := make(chan *structpb.Struct, grpcLogSubscriberQueueSize)
	p.subscribers[p.nextSubscriber] = events
	return grpcLogSubscriber{id: p.nextSubscriber, events: events}, nil
}

func (p *GRPCLogPublisher) removeSubscriber(id uint64) {
	p.subscribersMutex.Lock()
	defer p.subscribersMutex.Unlock()
	if events, ok := p.subscribers[id]; ok {
		delete(p.subscribers, id)
		close(events)
	}
}

func (p *GRPCLogPublisher) dispatch() {
	for {
		select {
		case <-p.done:
			return
		case <-p.queueWakeup:
			for {
				batch, ok := p.dequeueBatch()
				if !ok {
					break
				}
				p.dispatchBatch(batch)
			}
		}
	}
}

func (p *GRPCLogPublisher) dequeueBatch() (grpcLogBatch, bool) {
	p.queueMutex.Lock()
	defer p.queueMutex.Unlock()
	if len(p.queuedBatches) == 0 {
		return grpcLogBatch{}, false
	}
	batch := p.queuedBatches[0]
	p.queuedBatches[0] = grpcLogBatch{}
	p.queuedBatches = p.queuedBatches[1:]
	return batch, true
}

func (p *GRPCLogPublisher) dispatchBatch(batch grpcLogBatch) {
	p.subscribersMutex.Lock()
	defer p.subscribersMutex.Unlock()
	for _, eventLog := range batch.logs {
		if eventLog == nil {
			continue
		}
		event := grpcLogEvent(batch, eventLog)
		for id, subscriber := range p.subscribers {
			select {
			case subscriber <- event:
			default:
				delete(p.subscribers, id)
				close(subscriber)
				log.Warn("disconnecting slow gRPC log stream subscriber", "subscriber", id)
			}
		}
	}
}

func grpcLogEvent(batch grpcLogBatch, eventLog *types.Log) *structpb.Struct {
	topics := make([]*structpb.Value, len(eventLog.Topics))
	for i, topic := range eventLog.Topics {
		topics[i] = structpb.NewStringValue(topic.Hex())
	}
	return &structpb.Struct{Fields: map[string]*structpb.Value{
		"address":               structpb.NewStringValue(eventLog.Address.Hex()),
		"topics":                structpb.NewListValue(&structpb.ListValue{Values: topics}),
		"data":                  structpb.NewStringValue(hexutil.Encode(eventLog.Data)),
		"block_number":          structpb.NewStringValue(strconv.FormatUint(batch.blockNum, 10)),
		"previous_block_number": structpb.NewStringValue(strconv.FormatUint(batch.previousBlockNumber, 10)),
		"block_hash":            structpb.NewStringValue(hexutil.Encode(batch.blockHash[:])),
		"transaction_hash":      structpb.NewStringValue(eventLog.TxHash.Hex()),
		"transaction_index":     structpb.NewStringValue(strconv.FormatUint(uint64(eventLog.TxIndex), 10)),
		"log_index":             structpb.NewStringValue(strconv.FormatUint(uint64(eventLog.Index), 10)),
		"removed":               structpb.NewBoolValue(eventLog.Removed),
		"phase":                 structpb.NewStringValue(batch.phase),
		"emitted_at_unix_nano":  structpb.NewStringValue(strconv.FormatInt(batch.emittedAt.UnixNano(), 10)),
	}}
}

// StopOnly stops accepting connections and unblocks active streams.
func (p *GRPCLogPublisher) StopOnly() {
	p.stopOnce.Do(func() {
		p.stopped.Store(true)
		close(p.done)
		if p.server != nil {
			p.server.Stop()
		}
		if p.listener != nil {
			_ = p.listener.Close()
		}

		p.subscribersMutex.Lock()
		for id, subscriber := range p.subscribers {
			delete(p.subscribers, id)
			close(subscriber)
		}
		p.subscribersMutex.Unlock()
	})
}

// StopAndWait stops the server and waits for its background goroutines.
func (p *GRPCLogPublisher) StopAndWait() {
	p.StopOnly()
	p.wg.Wait()
}
