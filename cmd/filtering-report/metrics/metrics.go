// Copyright 2026, Offchain Labs, Inc.
// For license information, see https://github.com/OffchainLabs/nitro/blob/master/LICENSE.md

package metrics

import gethmetrics "github.com/ethereum/go-ethereum/metrics"

var (
	SQSSuccessesCounter = gethmetrics.NewRegisteredCounter(
		"arb/filtering-report/sqs_successes_total", nil,
	)
	SQSFailuresCounter = gethmetrics.NewRegisteredCounter(
		"arb/filtering-report/sqs_failures_total", nil,
	)
)
