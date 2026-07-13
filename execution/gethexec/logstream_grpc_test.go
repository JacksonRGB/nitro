// Copyright 2026, Offchain Labs, Inc.
// For license information, see https://github.com/offchainlabs/nitro/blob/master/LICENSE.md

package gethexec

import (
	"context"
	"math/big"
	"strconv"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestGRPCLogPublisherStreamsAllBlockLogs(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	publisher := newGRPCLogPublisher("127.0.0.1:0")
	if err := publisher.Start(ctx); err != nil {
		t.Fatalf("start publisher: %v", err)
	}
	defer func() {
		if err := publisher.StopAndWait(); err != nil {
			t.Errorf("stop publisher: %v", err)
		}
	}()

	connection, err := grpc.DialContext(ctx, publisher.listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		t.Fatalf("dial publisher: %v", err)
	}
	defer connection.Close()

	stream, err := NewLogStreamClient(connection).Subscribe(ctx, &emptypb.Empty{})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	waitForLogStreamSubscriber(t, publisher)

	block := types.NewBlockWithHeader(&types.Header{Number: big.NewInt(42)})
	firstLog := &types.Log{
		Address: common.HexToAddress("0x1111111111111111111111111111111111111111"),
		Topics: []common.Hash{
			common.HexToHash("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
		},
		Data:    []byte{0x01, 0x02},
		TxHash:  common.HexToHash("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
		TxIndex: 3,
		Index:   7,
	}
	secondLog := &types.Log{
		Address: common.HexToAddress("0x2222222222222222222222222222222222222222"),
		Data:    []byte{0x03},
		TxHash:  common.HexToHash("0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"),
		TxIndex: 4,
		Index:   8,
	}
	publisher.Publish(block, []*types.Log{firstLog, secondLog})

	firstEvent, err := stream.Recv()
	if err != nil {
		t.Fatalf("receive first log: %v", err)
	}
	assertLogStreamEvent(t, firstEvent.GetFields(), block, firstLog)

	secondEvent, err := stream.Recv()
	if err != nil {
		t.Fatalf("receive second log: %v", err)
	}
	assertLogStreamEvent(t, secondEvent.GetFields(), block, secondLog)
}

func waitForLogStreamSubscriber(t *testing.T, publisher *GRPCLogPublisher) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		publisher.subscribersMutex.Lock()
		subscriberCount := len(publisher.subscribers)
		publisher.subscribersMutex.Unlock()
		if subscriberCount == 1 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("timed out waiting for gRPC log stream subscriber")
}

func assertLogStreamEvent(t *testing.T, fields map[string]*structpb.Value, block *types.Block, eventLog *types.Log) {
	t.Helper()
	if got := fields["address"].GetStringValue(); got != eventLog.Address.Hex() {
		t.Errorf("address: got %s want %s", got, eventLog.Address.Hex())
	}
	if got := fields["block_number"].GetStringValue(); got != block.Number().String() {
		t.Errorf("block number: got %s want %s", got, block.Number())
	}
	if got := fields["block_hash"].GetStringValue(); got != block.Hash().Hex() {
		t.Errorf("block hash: got %s want %s", got, block.Hash())
	}
	if got := fields["transaction_hash"].GetStringValue(); got != eventLog.TxHash.Hex() {
		t.Errorf("transaction hash: got %s want %s", got, eventLog.TxHash.Hex())
	}
	if got := fields["transaction_index"].GetStringValue(); got != strconv.FormatUint(uint64(eventLog.TxIndex), 10) {
		t.Errorf("transaction index: got %s want %d", got, eventLog.TxIndex)
	}
	if got := fields["log_index"].GetStringValue(); got != strconv.FormatUint(uint64(eventLog.Index), 10) {
		t.Errorf("log index: got %s want %d", got, eventLog.Index)
	}
	if got := fields["data"].GetStringValue(); got != hexutil.Encode(eventLog.Data) {
		t.Errorf("data: got %s want %s", got, hexutil.Encode(eventLog.Data))
	}
	topics := fields["topics"].GetListValue().GetValues()
	if len(topics) != len(eventLog.Topics) {
		t.Errorf("topics count: got %d want %d", len(topics), len(eventLog.Topics))
	}
	for i, topic := range eventLog.Topics {
		if i < len(topics) && topics[i].GetStringValue() != topic.Hex() {
			t.Errorf("topic %d: got %s want %s", i, topics[i].GetStringValue(), topic.Hex())
		}
	}
}
