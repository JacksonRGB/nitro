// Copyright 2026, Offchain Labs, Inc.
// For license information, see https://github.com/OffchainLabs/nitro/blob/master/LICENSE.md

package forwarder

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/spf13/pflag"

	"github.com/ethereum/go-ethereum/log"

	"github.com/offchainlabs/nitro/cmd/genericconf"
	"github.com/offchainlabs/nitro/util/httperror"
	"github.com/offchainlabs/nitro/util/sqsclient"
	"github.com/offchainlabs/nitro/util/stopwaiter"
)

type ExternalEndpointRetryableErrorSlowdownConfig struct {
	Duration                       time.Duration `koanf:"duration"`
	ConsecutiveRetryableErrors int           `koanf:"consecutive-retryable-errors"`
}

var DefaultExternalEndpointRetryableErrorSlowdownConfig = ExternalEndpointRetryableErrorSlowdownConfig{
	Duration:                       2 * time.Minute,
	ConsecutiveRetryableErrors: 3,
}

func (c *ExternalEndpointRetryableErrorSlowdownConfig) Validate() error {
	if c.Duration < 0 {
		return fmt.Errorf("duration must be non-negative, got %s", c.Duration)
	}
	if c.ConsecutiveRetryableErrors <= 0 {
		return fmt.Errorf("consecutive-retryable-errors must be positive, got %d", c.ConsecutiveRetryableErrors)
	}
	return nil
}

func ExternalEndpointRetryableErrorSlowdownConfigAddOptions(prefix string, f *pflag.FlagSet) {
	f.Duration(prefix+".duration", DefaultExternalEndpointRetryableErrorSlowdownConfig.Duration, "how long a worker sleeps when consecutive retryable errors threshold is reached")
	f.Int(prefix+".consecutive-retryable-errors", DefaultExternalEndpointRetryableErrorSlowdownConfig.ConsecutiveRetryableErrors, "number of consecutive retryable errors a worker encounters before slowing down")
}

type Config struct {
	Workers                                    uint                                             `koanf:"workers"`
	PollInterval                               time.Duration                                    `koanf:"poll-interval"`
	SQSWaitTimeSeconds                         int32                                            `koanf:"sqs-wait-time-seconds"`
	ExternalEndpoint                           genericconf.HTTPClientConfig                     `koanf:"external-endpoint"`
	ExternalEndpointRetryableErrorSlowdown ExternalEndpointRetryableErrorSlowdownConfig `koanf:"external-endpoint-retryable-error-slowdown"`
	PoisonQueue                                sqsclient.QueueConfig                            `koanf:"poison-queue"`
}

var DefaultConfig = Config{
	Workers:            1,
	PollInterval:       1 * time.Second,
	SQSWaitTimeSeconds: 5,
	ExternalEndpoint:   genericconf.HTTPClientConfigDefault,
	ExternalEndpointRetryableErrorSlowdown: DefaultExternalEndpointRetryableErrorSlowdownConfig,
	PoisonQueue: sqsclient.DefaultQueueConfig,
}

func (c *Config) Validate() error {
	if c.PollInterval < 0 {
		return fmt.Errorf("poll-interval must be non-negative, got %s", c.PollInterval)
	}
	if c.SQSWaitTimeSeconds < 0 {
		return fmt.Errorf("sqs-wait-time-seconds must be non-negative, got %d", c.SQSWaitTimeSeconds)
	}
	if err := c.ExternalEndpointRetryableErrorSlowdown.Validate(); err != nil {
		return err
	}
	return c.ExternalEndpoint.Validate()
}

func ConfigAddOptions(prefix string, f *pflag.FlagSet) {
	f.Uint(prefix+".workers", DefaultConfig.Workers, "number of workers")
	f.Duration(prefix+".poll-interval", DefaultConfig.PollInterval, "interval between SQS polls when queue is empty")
	f.Int32(prefix+".sqs-wait-time-seconds", DefaultConfig.SQSWaitTimeSeconds, "SQS long polling wait time in seconds")
	genericconf.HTTPClientConfigAddOptions(prefix+".external-endpoint", f)
	ExternalEndpointRetryableErrorSlowdownConfigAddOptions(prefix+".external-endpoint-retryable-error-slowdown", f)
	sqsclient.QueueConfigAddOptions(prefix+".poison-queue", f, "SQS queue URL for messages that failed with non-retryable errors")
}

// Forwarder polls messages from an SQS queue and forwards them to an external
// HTTP endpoint. Error handling follows two paths:
//
//   - Non-retryable HTTP errors (4xx client errors, except 408/425/429) indicate
//     the message itself is permanently invalid. These are sent directly to an
//     optional poison queue and deleted from the main queue. The poison queue can
//     be used for manual inspection, reprocessing, or simply discarded.
//
//   - Retryable HTTP errors (5xx, 408, 425, 429) leave the message in the queue
//     for SQS to redeliver after its visibility timeout expires. Since all reports
//     target the same endpoint, a per-worker slowdown kicks in after a configurable
//     number of consecutive retryable errors, preventing workers from hammering a
//     degraded endpoint and avoiding unnecessary consumption of the SQS max receive
//     count quota.
//
// The SQS queue should be configured with a small default visibility timeout
// (e.g. 1-3 minutes) and a high max receive count (e.g. 300+). The visibility
// timeout only needs to be long enough for a single forward attempt; the
// worker-level slowdown handles rate limiting during outages. A small visibility
// timeout ensures messages become available quickly once the endpoint recovers.
// For example, with a visibility timeout of 2 minutes and a max receive count of
// 300, the minimum retry window per message is 300 × 2 min = 600 min = 10 hours.
// In practice the effective interval is longer because the worker slowdown spaces
// out attempts, so the actual retry window will be considerably larger.
type Forwarder struct {
	stopwaiter.StopWaiter
	config            *Config
	queueClient       sqsclient.QueueClient
	poisonQueueClient sqsclient.QueueClient
	httpClient        *http.Client
}

func New(config *Config, queueClient sqsclient.QueueClient, poisonQueueClient sqsclient.QueueClient) (*Forwarder, error) {
	if config == nil {
		return nil, errors.New("config must not be nil")
	}
	if queueClient == nil {
		return nil, errors.New("queueClient must not be nil")
	}
	return &Forwarder{
		config:            config,
		queueClient:       queueClient,
		poisonQueueClient: poisonQueueClient,
		httpClient:        &http.Client{Timeout: config.ExternalEndpoint.Timeout},
	}, nil
}

func (r *Forwarder) Start(ctx context.Context) {
	r.StopWaiter.Start(ctx, r)
	for i := uint(0); i < r.config.Workers; i++ {
		var consecutiveRetryableErrors int
		r.CallIteratively(func(ctx context.Context) time.Duration {
			return r.pollAndForward(ctx, &consecutiveRetryableErrors)
		})
	}
}

func (r *Forwarder) pollAndForward(ctx context.Context, consecutiveRetryableErrors *int) time.Duration {
	msgs, err := r.queueClient.Receive(ctx, r.config.SQSWaitTimeSeconds, 1)
	if err != nil {
		log.Error("Failed to receive SQS messages", "err", err)
		return r.config.PollInterval
	}
	if len(msgs) == 0 {
		return r.config.PollInterval
	}
	msg := msgs[0]
	if err := r.forwardToEndpoint(ctx, *msg.Body); err != nil {
		log.Error("Failed to forward report to external endpoint", "err", err, "messageId", *msg.MessageId)
		var httpErr *httperror.HTTPError
		if errors.As(err, &httpErr) {
			if httpErr.IsRetryable() {
				*consecutiveRetryableErrors++
				if *consecutiveRetryableErrors >= r.config.ExternalEndpointRetryableErrorSlowdown.ConsecutiveRetryableErrors {
					return r.config.ExternalEndpointRetryableErrorSlowdown.Duration
				}
			} else {
				*consecutiveRetryableErrors = 0
				r.sendToPoisonQueue(ctx, msg)
			}
		}
		return 0
	}
	*consecutiveRetryableErrors = 0
	if err = r.queueClient.Delete(ctx, *msg.ReceiptHandle); err != nil {
		log.Error("Failed to delete SQS message after forwarding", "err", err, "messageId", *msg.MessageId)
	}
	return 0
}

func (r *Forwarder) sendToPoisonQueue(ctx context.Context, msg sqstypes.Message) {
	if r.poisonQueueClient == nil {
		return
	}
	if err := r.poisonQueueClient.Send(ctx, *msg.Body); err != nil {
		log.Error("Failed to send message to poison queue", "err", err, "messageId", *msg.MessageId)
		return
	}
	if err := r.queueClient.Delete(ctx, *msg.ReceiptHandle); err != nil {
		log.Error("Failed to delete SQS message after sending to poison queue", "err", err, "messageId", *msg.MessageId)
	}
}

func (r *Forwarder) forwardToEndpoint(ctx context.Context, body string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, r.config.ExternalEndpoint.URL, strings.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := r.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer func() {
		if _, drainErr := io.Copy(io.Discard, resp.Body); drainErr != nil {
			log.Warn("Failed draining response body", "err", drainErr)
		}
		resp.Body.Close()
	}()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		respBody, readErr := io.ReadAll(io.LimitReader(resp.Body, 1024)) // cap error body to avoid unbounded reads
		if readErr != nil {
			return fmt.Errorf("external endpoint returned status %d (body read error: %w)", resp.StatusCode, readErr)
		}
		return &httperror.HTTPError{StatusCode: resp.StatusCode, Body: string(respBody)}
	}
	return nil
}
