// Copyright 2026, Offchain Labs, Inc.
// For license information, see https://github.com/OffchainLabs/nitro/blob/master/LICENSE.md

package forwarder

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"

	"github.com/offchainlabs/nitro/cmd/filtering-report/api"
	"github.com/offchainlabs/nitro/execution/gethexec/addressfilter"
	"github.com/offchainlabs/nitro/util/sqsclient"
)

func TestForwarder_ForwardsMessages(t *testing.T) {
	endpoint := NewMockExternalEndpoint(t)

	queueClient := &sqsclient.MockQueueClient{}
	stack := api.NewTestStack(t, queueClient)
	filteringReportClient := stack.Attach()
	t.Cleanup(func() { filteringReportClient.Close() })

	reports := []addressfilter.FilteredTxReport{
		{
			ID:                "",
			TxHash:            common.HexToHash("0x01"),
			TxRLP:             hexutil.Bytes{},
			FilteredAddresses: nil,
			ChainID:           0,
			BlockNumber:       0,
			ParentBlockHash:   common.Hash{},
			PositionInBlock:   0,
			FilteredAt:        time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC),
			IsDelayed:         false,
			DelayedReportData: nil,
		},
		{
			ID:                "",
			TxHash:            common.HexToHash("0x02"),
			TxRLP:             hexutil.Bytes{},
			FilteredAddresses: nil,
			ChainID:           0,
			BlockNumber:       0,
			ParentBlockHash:   common.Hash{},
			PositionInBlock:   0,
			FilteredAt:        time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC),
			IsDelayed:         false,
			DelayedReportData: nil,
		},
	}
	if err := filteringReportClient.Call(nil, "filteringreport_reportFilteredTransactions", reports); err != nil {
		t.Fatal(err)
	}

	ctx := t.Context()
	forwarder := NewTestForwarder(t, queueClient, nil, endpoint.URL())
	var consecutiveRetryableHTTPErrors int
	forwarder.pollAndForward(ctx, &consecutiveRetryableHTTPErrors)
	forwarder.pollAndForward(ctx, &consecutiveRetryableHTTPErrors)

	received := []addressfilter.FilteredTxReport{
		*endpoint.NextReport(t),
		*endpoint.NextReport(t),
	}

	sort.Slice(reports, func(i, j int) bool { return reports[i].TxHash.Cmp(reports[j].TxHash) < 0 })
	sort.Slice(received, func(i, j int) bool { return received[i].TxHash.Cmp(received[j].TxHash) < 0 })
	for i := range reports {
		if !reflect.DeepEqual(received[i], reports[i]) {
			t.Fatalf("report mismatch at index %d: expected %+v, got %+v", i, reports[i], received[i])
		}
	}

	deleted := queueClient.DeletedReceiptHandles()
	if len(deleted) != 2 {
		t.Fatalf("expected 2 deletes, got %d", len(deleted))
	}
}

func TestForwarder_EndpointFailure_DoesNotDelete(t *testing.T) {
	externalEndpointServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer externalEndpointServer.Close()

	queueClient := &sqsclient.MockQueueClient{}
	stack := api.NewTestStack(t, queueClient)
	filteringReportClient := stack.Attach()
	t.Cleanup(func() { filteringReportClient.Close() })

	reports := []addressfilter.FilteredTxReport{
		{
			ID:                "",
			TxHash:            common.HexToHash("0x01"),
			TxRLP:             nil,
			FilteredAddresses: nil,
			ChainID:           0,
			BlockNumber:       0,
			ParentBlockHash:   common.Hash{},
			PositionInBlock:   0,
			FilteredAt:        time.Time{},
			IsDelayed:         false,
			DelayedReportData: nil,
		},
	}
	if err := filteringReportClient.Call(nil, "filteringreport_reportFilteredTransactions", reports); err != nil {
		t.Fatal(err)
	}

	ctx := t.Context()
	forwarder := NewTestForwarder(t, queueClient, nil, externalEndpointServer.URL)
	var consecutiveRetryableHTTPErrors int
	forwarder.pollAndForward(ctx, &consecutiveRetryableHTTPErrors)

	deleted := queueClient.DeletedReceiptHandles()
	if len(deleted) != 0 {
		t.Fatalf("expected 0 deletes on endpoint failure, got %d", len(deleted))
	}
}

func TestForwarder_EmptyQueue(t *testing.T) {
	externalEndpointServerCalled := false
	externalEndpointServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		externalEndpointServerCalled = true
		w.WriteHeader(http.StatusOK)
	}))
	defer externalEndpointServer.Close()

	queueClient := &sqsclient.MockQueueClient{}

	forwarder := NewTestForwarder(t, queueClient, nil, externalEndpointServer.URL)
	var consecutiveRetryableHTTPErrors int
	interval := forwarder.pollAndForward(t.Context(), &consecutiveRetryableHTTPErrors)

	if externalEndpointServerCalled {
		t.Fatal("expected no HTTP calls on empty queue")
	}
	deleted := queueClient.DeletedReceiptHandles()
	if len(deleted) != 0 {
		t.Fatalf("expected 0 deletes on empty queue, got %d", len(deleted))
	}
	if interval != forwarder.config.PollInterval {
		t.Fatalf("expected poll interval %v on empty queue, got %v", forwarder.config.PollInterval, interval)
	}
}

func TestForwarder_ReceiveError(t *testing.T) {
	externalEndpointServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("expected no HTTP calls when Receive fails")
	}))
	defer externalEndpointServer.Close()

	queueClient := &sqsclient.MockQueueClient{
		ReceiveErr: fmt.Errorf("simulated SQS error"),
	}

	forwarder := NewTestForwarder(t, queueClient, nil, externalEndpointServer.URL)
	var consecutiveRetryableHTTPErrors int
	interval := forwarder.pollAndForward(t.Context(), &consecutiveRetryableHTTPErrors)

	if interval != forwarder.config.PollInterval {
		t.Fatalf("expected poll interval %v on receive error, got %v", forwarder.config.PollInterval, interval)
	}
}

func TestForwarder_DeleteError(t *testing.T) {
	endpoint := NewMockExternalEndpoint(t)

	queueClient := &sqsclient.MockQueueClient{
		DeleteErr: fmt.Errorf("simulated SQS delete error"),
	}
	stack := api.NewTestStack(t, queueClient)
	rpcClient := stack.Attach()
	t.Cleanup(func() { rpcClient.Close() })

	reports := []addressfilter.FilteredTxReport{{
		ID:                "",
		TxHash:            common.HexToHash("0x01"),
		TxRLP:             nil,
		FilteredAddresses: nil,
		ChainID:           0,
		BlockNumber:       0,
		ParentBlockHash:   common.Hash{},
		PositionInBlock:   0,
		FilteredAt:        time.Time{},
		IsDelayed:         false,
		DelayedReportData: nil,
	}}
	if err := rpcClient.Call(nil, "filteringreport_reportFilteredTransactions", reports); err != nil {
		t.Fatal(err)
	}

	forwarder := NewTestForwarder(t, queueClient, nil, endpoint.URL())
	var consecutiveRetryableHTTPErrors int
	interval := forwarder.pollAndForward(t.Context(), &consecutiveRetryableHTTPErrors)

	received := endpoint.NextReport(t)
	if received.TxHash != reports[0].TxHash {
		t.Fatalf("expected tx hash %v, got %v", reports[0].TxHash, received.TxHash)
	}
	deleted := queueClient.DeletedReceiptHandles()
	if len(deleted) != 0 {
		t.Fatalf("expected 0 deletes on delete error, got %d", len(deleted))
	}
	if interval != 0 {
		t.Fatalf("expected immediate re-poll (0) on delete error, got %v", interval)
	}
}

func TestForwarder_RetryableHTTPErrorSlowdown_AfterThreshold(t *testing.T) {
	externalEndpointServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer externalEndpointServer.Close()

	queueClient := &sqsclient.MockQueueClient{}
	stack := api.NewTestStack(t, queueClient)
	rpcClient := stack.Attach()
	t.Cleanup(func() { rpcClient.Close() })

	forwarder := NewTestForwarder(t, queueClient, nil, externalEndpointServer.URL)
	threshold := forwarder.config.ExternalEndpointRetryableHTTPErrorSlowdown.ConsecutiveRetryableHTTPErrors

	// Enqueue enough messages to exceed the threshold.
	reports := make([]addressfilter.FilteredTxReport, threshold)
	for i := range reports {
		reports[i] = addressfilter.FilteredTxReport{
			ID:                "",
			TxHash:            common.HexToHash(fmt.Sprintf("0x%02x", i+1)),
			TxRLP:             nil,
			FilteredAddresses: nil,
			ChainID:           0,
			BlockNumber:       0,
			ParentBlockHash:   common.Hash{},
			PositionInBlock:   0,
			FilteredAt:        time.Time{},
			IsDelayed:         false,
			DelayedReportData: nil,
		}
	}
	if err := rpcClient.Call(nil, "filteringreport_reportFilteredTransactions", reports); err != nil {
		t.Fatal(err)
	}

	ctx := t.Context()
	var consecutiveRetryableHTTPErrors int

	// First threshold-1 errors should return 0 (immediate re-poll).
	for i := 0; i < threshold-1; i++ {
		interval := forwarder.pollAndForward(ctx, &consecutiveRetryableHTTPErrors)
		if interval != 0 {
			t.Fatalf("call %d: expected 0 before threshold, got %v", i+1, interval)
		}
	}

	// The threshold-th error should trigger the slowdown.
	interval := forwarder.pollAndForward(ctx, &consecutiveRetryableHTTPErrors)
	expected := forwarder.config.ExternalEndpointRetryableHTTPErrorSlowdown.Duration
	if interval != expected {
		t.Fatalf("expected slowdown duration %v at threshold, got %v", expected, interval)
	}
}

func TestForwarder_RetryableHTTPErrorSlowdown_ResetOnSuccess(t *testing.T) {
	var mu sync.Mutex
	callCount := 0
	failUntil := 2 // first 2 calls fail, third succeeds
	externalEndpointServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		callCount++
		n := callCount
		mu.Unlock()
		if n <= failUntil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer externalEndpointServer.Close()

	queueClient := &sqsclient.MockQueueClient{}
	stack := api.NewTestStack(t, queueClient)
	rpcClient := stack.Attach()
	t.Cleanup(func() { rpcClient.Close() })

	reports := make([]addressfilter.FilteredTxReport, 3)
	for i := range reports {
		reports[i] = addressfilter.FilteredTxReport{
			ID:                "",
			TxHash:            common.HexToHash(fmt.Sprintf("0x%02x", i+1)),
			TxRLP:             nil,
			FilteredAddresses: nil,
			ChainID:           0,
			BlockNumber:       0,
			ParentBlockHash:   common.Hash{},
			PositionInBlock:   0,
			FilteredAt:        time.Time{},
			IsDelayed:         false,
			DelayedReportData: nil,
		}
	}
	if err := rpcClient.Call(nil, "filteringreport_reportFilteredTransactions", reports); err != nil {
		t.Fatal(err)
	}

	forwarder := NewTestForwarder(t, queueClient, nil, externalEndpointServer.URL)
	ctx := t.Context()
	var consecutiveRetryableHTTPErrors int

	// Two retryable errors.
	forwarder.pollAndForward(ctx, &consecutiveRetryableHTTPErrors)
	forwarder.pollAndForward(ctx, &consecutiveRetryableHTTPErrors)
	if consecutiveRetryableHTTPErrors != 2 {
		t.Fatalf("expected 2 consecutive retryable errors, got %d", consecutiveRetryableHTTPErrors)
	}

	// Success should reset counter.
	forwarder.pollAndForward(ctx, &consecutiveRetryableHTTPErrors)
	if consecutiveRetryableHTTPErrors != 0 {
		t.Fatalf("expected counter reset to 0 after success, got %d", consecutiveRetryableHTTPErrors)
	}
}

func TestForwarder_RetryableHTTPErrorSlowdown_NonRetryableErrorDoesNotCount(t *testing.T) {
	externalEndpointServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest) // 400 - non-retryable client error
	}))
	defer externalEndpointServer.Close()

	queueClient := &sqsclient.MockQueueClient{}
	stack := api.NewTestStack(t, queueClient)
	rpcClient := stack.Attach()
	t.Cleanup(func() { rpcClient.Close() })

	forwarder := NewTestForwarder(t, queueClient, nil, externalEndpointServer.URL)
	threshold := forwarder.config.ExternalEndpointRetryableHTTPErrorSlowdown.ConsecutiveRetryableHTTPErrors

	reports := make([]addressfilter.FilteredTxReport, threshold+1)
	for i := range reports {
		reports[i] = addressfilter.FilteredTxReport{
			ID:                "",
			TxHash:            common.HexToHash(fmt.Sprintf("0x%02x", i+1)),
			TxRLP:             nil,
			FilteredAddresses: nil,
			ChainID:           0,
			BlockNumber:       0,
			ParentBlockHash:   common.Hash{},
			PositionInBlock:   0,
			FilteredAt:        time.Time{},
			IsDelayed:         false,
			DelayedReportData: nil,
		}
	}
	if err := rpcClient.Call(nil, "filteringreport_reportFilteredTransactions", reports); err != nil {
		t.Fatal(err)
	}

	ctx := t.Context()
	var consecutiveRetryableHTTPErrors int

	// Even after many 400 errors, should never trigger slowdown.
	for i := 0; i <= threshold; i++ {
		interval := forwarder.pollAndForward(ctx, &consecutiveRetryableHTTPErrors)
		if interval != 0 {
			t.Fatalf("call %d: expected 0 for non-retryable error, got %v", i+1, interval)
		}
	}
	if consecutiveRetryableHTTPErrors != 0 {
		t.Fatalf("expected 0 consecutive retryable errors for non-retryable errors, got %d", consecutiveRetryableHTTPErrors)
	}
}

func TestForwarder_DLQ_NonRetryableErrorSentToDLQ(t *testing.T) {
	externalEndpointServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer externalEndpointServer.Close()

	queueClient := &sqsclient.MockQueueClient{}
	dlqClient := &sqsclient.MockQueueClient{}

	stack := api.NewTestStack(t, queueClient)
	rpcClient := stack.Attach()
	t.Cleanup(func() { rpcClient.Close() })

	reports := []addressfilter.FilteredTxReport{{
		ID:                "",
		TxHash:            common.HexToHash("0x01"),
		TxRLP:             nil,
		FilteredAddresses: nil,
		ChainID:           0,
		BlockNumber:       0,
		ParentBlockHash:   common.Hash{},
		PositionInBlock:   0,
		FilteredAt:        time.Time{},
		IsDelayed:         false,
		DelayedReportData: nil,
	}}
	if err := rpcClient.Call(nil, "filteringreport_reportFilteredTransactions", reports); err != nil {
		t.Fatal(err)
	}

	forwarder := NewTestForwarder(t, queueClient, dlqClient, externalEndpointServer.URL)
	var consecutiveRetryableHTTPErrors int
	forwarder.pollAndForward(t.Context(), &consecutiveRetryableHTTPErrors)

	// Message should have been sent to DLQ.
	sentBodies := dlqClient.SentBodies()
	if len(sentBodies) != 1 {
		t.Fatalf("expected 1 message sent to DLQ, got %d", len(sentBodies))
	}

	// Message should have been deleted from main queue.
	deleted := queueClient.DeletedReceiptHandles()
	if len(deleted) != 1 {
		t.Fatalf("expected 1 delete from main queue after DLQ send, got %d", len(deleted))
	}
}

func TestForwarder_DLQ_SendFailureLeavesMessageInQueue(t *testing.T) {
	externalEndpointServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer externalEndpointServer.Close()

	queueClient := &sqsclient.MockQueueClient{}
	dlqClient := &sqsclient.MockQueueClient{
		SendErr: fmt.Errorf("simulated DLQ send error"),
	}

	stack := api.NewTestStack(t, queueClient)
	rpcClient := stack.Attach()
	t.Cleanup(func() { rpcClient.Close() })

	reports := []addressfilter.FilteredTxReport{{
		ID:                "",
		TxHash:            common.HexToHash("0x01"),
		TxRLP:             nil,
		FilteredAddresses: nil,
		ChainID:           0,
		BlockNumber:       0,
		ParentBlockHash:   common.Hash{},
		PositionInBlock:   0,
		FilteredAt:        time.Time{},
		IsDelayed:         false,
		DelayedReportData: nil,
	}}
	if err := rpcClient.Call(nil, "filteringreport_reportFilteredTransactions", reports); err != nil {
		t.Fatal(err)
	}

	forwarder := NewTestForwarder(t, queueClient, dlqClient, externalEndpointServer.URL)
	var consecutiveRetryableHTTPErrors int
	forwarder.pollAndForward(t.Context(), &consecutiveRetryableHTTPErrors)

	// DLQ send failed, so message should NOT have been deleted from main queue.
	deleted := queueClient.DeletedReceiptHandles()
	if len(deleted) != 0 {
		t.Fatalf("expected 0 deletes when DLQ send fails, got %d", len(deleted))
	}
}
