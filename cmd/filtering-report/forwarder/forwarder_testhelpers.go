// Copyright 2026, Offchain Labs, Inc.
// For license information, see https://github.com/OffchainLabs/nitro/blob/master/LICENSE.md

package forwarder

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/offchainlabs/nitro/cmd/filtering-report/signer"
	"github.com/offchainlabs/nitro/cmd/genericconf"
	"github.com/offchainlabs/nitro/execution/gethexec/addressfilter"
	"github.com/offchainlabs/nitro/util/sqsclient"
)

type MockExternalEndpoint struct {
	server       *httptest.Server
	reports      chan *addressfilter.FilteredTxReport
	requestCount atomic.Int64
}

func NewMockExternalEndpoint(t *testing.T) *MockExternalEndpoint {
	t.Helper()
	return newMockExternalEndpoint(t, nil)
}

func NewMockExternalEndpointWithVerifier(t *testing.T, v *signer.Verifier) *MockExternalEndpoint {
	t.Helper()
	return newMockExternalEndpoint(t, v)
}

func newMockExternalEndpoint(t *testing.T, v *signer.Verifier) *MockExternalEndpoint {
	t.Helper()
	m := &MockExternalEndpoint{
		reports: make(chan *addressfilter.FilteredTxReport, 100),
	}
	m.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		m.requestCount.Add(1)
		body, err := io.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		if v != nil {
			if err := v.VerifyHTTPRequest(r, body); err != nil {
				t.Errorf("verifier rejected signed request: %v", err)
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
		}
		var report addressfilter.FilteredTxReport
		if err := json.Unmarshal(body, &report); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		m.reports <- &report
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(func() { m.server.Close() })
	return m
}

func (m *MockExternalEndpoint) NextReport(t *testing.T) *addressfilter.FilteredTxReport {
	t.Helper()
	select {
	case r := <-m.reports:
		return r
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for report")
		return nil
	}
}

func (m *MockExternalEndpoint) URL() string {
	return m.server.URL
}

func (m *MockExternalEndpoint) ReceivedCount() int {
	return int(m.requestCount.Load())
}

func NewTestForwarder(t *testing.T, queueClient sqsclient.QueueClient, endpointURL string) *Forwarder {
	t.Helper()
	return newTestForwarder(t, queueClient, endpointURL, signer.Config{})
}

func NewTestForwarderWithSigner(t *testing.T, queueClient sqsclient.QueueClient, endpointURL string, signerCfg signer.Config) *Forwarder {
	t.Helper()
	return newTestForwarder(t, queueClient, endpointURL, signerCfg)
}

func newTestForwarder(t *testing.T, queueClient sqsclient.QueueClient, endpointURL string, signerCfg signer.Config) *Forwarder {
	t.Helper()
	config := &Config{
		Workers:            1,
		PollInterval:       10 * time.Millisecond,
		SQSWaitTimeSeconds: DefaultConfig.SQSWaitTimeSeconds,
		ExternalEndpoint: genericconf.HTTPClientConfig{
			URL:     endpointURL,
			Timeout: genericconf.HTTPClientConfigDefault.Timeout,
		},
		Signer: signerCfg,
	}
	fwd, err := New(config, queueClient)
	if err != nil {
		t.Fatal(err)
	}
	return fwd
}
