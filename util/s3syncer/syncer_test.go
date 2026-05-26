// Copyright 2026, Offchain Labs, Inc.
// For license information, see https://github.com/OffchainLabs/nitro/blob/master/LICENSE.md

package s3syncer

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/offchainlabs/nitro/util/s3client"
	"github.com/offchainlabs/nitro/util/s3syncer/s3syncertest"
)

func TestConfigValidate(t *testing.T) {
	tests := []struct {
		name    string
		config  Config
		wantErr bool
	}{
		{
			name: "valid config",
			config: Config{
				Config:    s3client.Config{Region: "us-east-1"},
				Bucket:    "test-bucket",
				ObjectKey: "path/to/file.json",
			},
			wantErr: false,
		},
		{
			name: "missing bucket",
			config: Config{
				Config:    s3client.Config{Region: "us-east-1"},
				ObjectKey: "path/to/file.json",
			},
			wantErr: true,
		},
		{
			name: "missing region",
			config: Config{
				Bucket:    "test-bucket",
				ObjectKey: "path/to/file.json",
			},
			wantErr: true,
		},
		{
			name: "missing object key",
			config: Config{
				Config: s3client.Config{Region: "us-east-1"},
				Bucket: "test-bucket",
			},
			wantErr: true,
		},
		{
			name: "valid config with credentials",
			config: Config{
				Config: s3client.Config{
					Region:    "us-east-1",
					AccessKey: "AKIAIOSFODNN7EXAMPLE",
					SecretKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
				},
				Bucket:    "test-bucket",
				ObjectKey: "path/to/file.json",
			},
			wantErr: false,
		},
		{
			name: "negative max-file-size-mb",
			config: Config{
				Config:        s3client.Config{Region: "us-east-1"},
				Bucket:        "test-bucket",
				ObjectKey:     "path/to/file.json",
				MaxFileSizeMB: -1,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Config.Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

const testBucket = "test-bucket"

func newTestConfig(endpoint, key string, maxFileSizeMB int) *Config {
	return &Config{
		Config: s3client.Config{
			Region:    "us-east-1",
			AccessKey: "dummy-access-key",
			SecretKey: "dummy-secret-key",
			Endpoint:  endpoint,
		},
		Bucket:        testBucket,
		ObjectKey:     key,
		ChunkSizeMB:   DefaultS3Config.ChunkSizeMB,
		MaxRetries:    DefaultS3Config.MaxRetries,
		Concurrency:   DefaultS3Config.Concurrency,
		MaxFileSizeMB: maxFileSizeMB,
	}
}

type syncerRecorder struct {
	observedSizes []int64
	handlerCalls  int
	lastBody      []byte
	lastDigest    string
}

func (r *syncerRecorder) onObjectSize(size int64) {
	r.observedSizes = append(r.observedSizes, size)
}

func (r *syncerRecorder) handleData(body []byte, digest string) error {
	r.handlerCalls++
	r.lastBody = bytes.Clone(body)
	r.lastDigest = digest
	return nil
}

var syncerMethodCases = []struct {
	name string
	run  func(*Syncer, context.Context) error
}{
	{"DownloadAndLoad", (*Syncer).DownloadAndLoad},
	{"CheckAndSync", (*Syncer).CheckAndSync},
}

func TestSyncer_RejectsOversizedObject(t *testing.T) {
	for _, tt := range syncerMethodCases {
		t.Run(tt.name, func(t *testing.T) {
			key := "oversized.json"
			body := bytes.Repeat([]byte("A"), 2*1024*1024) // 2 MB
			endpoint, _ := s3syncertest.NewFakeS3(t, testBucket, map[string][]byte{key: body})

			rec := &syncerRecorder{}
			syncer := NewSyncer(newTestConfig(endpoint, key, 1), rec.handleData, rec.onObjectSize)
			if err := syncer.Initialize(t.Context()); err != nil {
				t.Fatalf("Initialize: %v", err)
			}

			err := tt.run(syncer, t.Context())
			if !errors.Is(err, ErrObjectTooLarge) {
				t.Fatalf("expected ErrObjectTooLarge, got %v", err)
			}
			if got, want := len(rec.observedSizes), 1; got != want {
				t.Fatalf("onObjectSize call count: got %d, want %d", got, want)
			}
			if got, want := rec.observedSizes[0], int64(len(body)); got != want {
				t.Errorf("onObjectSize size: got %d, want %d", got, want)
			}
			if rec.handlerCalls != 0 {
				t.Errorf("data handler should not be called when object too large; got %d calls", rec.handlerCalls)
			}
		})
	}
}

func TestSyncer_AcceptsWithinLimit(t *testing.T) {
	for _, tt := range syncerMethodCases {
		t.Run(tt.name, func(t *testing.T) {
			key := "filter.json"
			body := []byte(`{"id":"0fa6d8c0-0000-0000-0000-000000000001","salt":"00000000-0000-0000-0000-000000000000","hashes":[]}`)
			endpoint, _ := s3syncertest.NewFakeS3(t, testBucket, map[string][]byte{key: body})

			rec := &syncerRecorder{}
			syncer := NewSyncer(newTestConfig(endpoint, key, 10), rec.handleData, rec.onObjectSize)
			if err := syncer.Initialize(t.Context()); err != nil {
				t.Fatalf("Initialize: %v", err)
			}

			if err := tt.run(syncer, t.Context()); err != nil {
				t.Fatalf("%s: %v", tt.name, err)
			}
			if got, want := len(rec.observedSizes), 1; got != want {
				t.Fatalf("onObjectSize call count: got %d, want %d", got, want)
			}
			if got, want := rec.observedSizes[0], int64(len(body)); got != want {
				t.Errorf("onObjectSize size: got %d, want %d", got, want)
			}
			if rec.handlerCalls != 1 {
				t.Fatalf("data handler call count: got %d, want 1", rec.handlerCalls)
			}
			if !bytes.Equal(rec.lastBody, body) {
				t.Errorf("data handler body mismatch:\n got  %q\n want %q", rec.lastBody, body)
			}
			if rec.lastDigest == "" {
				t.Error("data handler should receive a non-empty etag digest")
			}
		})
	}
}

func TestSyncer_LimitDisabled(t *testing.T) {
	for _, tt := range syncerMethodCases {
		t.Run(tt.name, func(t *testing.T) {
			key := "big.json"
			body := bytes.Repeat([]byte("B"), 2*1024*1024) // 2 MB
			endpoint, _ := s3syncertest.NewFakeS3(t, testBucket, map[string][]byte{key: body})

			rec := &syncerRecorder{}
			syncer := NewSyncer(newTestConfig(endpoint, key, 0), rec.handleData, rec.onObjectSize)
			if err := syncer.Initialize(t.Context()); err != nil {
				t.Fatalf("Initialize: %v", err)
			}

			if err := tt.run(syncer, t.Context()); err != nil {
				t.Fatalf("%s with MaxFileSizeMB=0: %v", tt.name, err)
			}
			if got, want := len(rec.observedSizes), 1; got != want {
				t.Fatalf("onObjectSize call count: got %d, want %d", got, want)
			}
			if got, want := rec.observedSizes[0], int64(len(body)); got != want {
				t.Errorf("onObjectSize size: got %d, want %d", got, want)
			}
			if rec.handlerCalls != 1 {
				t.Fatalf("data handler call count: got %d, want 1", rec.handlerCalls)
			}
			if !bytes.Equal(rec.lastBody, body) {
				t.Errorf("data handler body mismatch:\n got  %q\n want %q", rec.lastBody, body)
			}
			if rec.lastDigest == "" {
				t.Error("data handler should receive a non-empty etag digest")
			}
		})
	}
}

func TestSyncer_HeadObjectError(t *testing.T) {
	for _, tt := range syncerMethodCases {
		t.Run(tt.name, func(t *testing.T) {
			endpoint, _ := s3syncertest.NewFakeS3(t, testBucket, nil) // bucket exists, key does not
			rec := &syncerRecorder{}
			syncer := NewSyncer(newTestConfig(endpoint, "missing.json", 1), rec.handleData, rec.onObjectSize)
			if err := syncer.Initialize(t.Context()); err != nil {
				t.Fatalf("Initialize: %v", err)
			}

			err := tt.run(syncer, t.Context())
			if err == nil {
				t.Fatal("expected error for missing key, got nil")
			}
			if errors.Is(err, ErrObjectTooLarge) {
				t.Errorf("missing-key error should not match ErrObjectTooLarge: %v", err)
			}
			if len(rec.observedSizes) != 0 {
				t.Errorf("onObjectSize should not be called on HEAD failure; got %d calls", len(rec.observedSizes))
			}
			if rec.handlerCalls != 0 {
				t.Errorf("data handler should not be called on HEAD failure; got %d calls", rec.handlerCalls)
			}
		})
	}
}
