// Copyright 2023-2026, Offchain Labs, Inc.
// For license information, see https://github.com/OffchainLabs/nitro/blob/master/LICENSE.md

package e2e

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"

	"github.com/offchainlabs/nitro/bold/protocol"
	"github.com/offchainlabs/nitro/bold/retry"
	"github.com/offchainlabs/nitro/bold/testing/setup"
	"github.com/offchainlabs/nitro/solgen/go/challengeV2gen"
	"github.com/offchainlabs/nitro/solgen/go/rollupgen"
)

// expect is a function that will be called asynchronously to verify some success criteria
// for the given scenario.
type expect func(t *testing.T, ctx context.Context, addresses *setup.RollupAddresses, be protocol.ChainBackend, honestValidatorAddress common.Address) error

// Expects that an assertion is confirmed by challenge win.
func expectChallengeWinWithAllHonestEssentialEdgesConfirmed(
	t *testing.T,
	ctx context.Context,
	addresses *setup.RollupAddresses,
	backend protocol.ChainBackend,
	honestValidatorAddress common.Address,
) error {
	t.Run("honest essential edges confirmed by challenge win", func(t *testing.T) {
		rc, err := rollupgen.NewRollupCore(addresses.Rollup, backend)
		require.NoError(t, err)
		cmAddr, err := rc.ChallengeManager(&bind.CallOpts{})
		require.NoError(t, err)

		// Wait until a challenged assertion is confirmed by time.
		var confirmed bool
		for ctx.Err() == nil && !confirmed {
			var i *rollupgen.RollupCoreAssertionConfirmedIterator
			i, err = retry.UntilSucceeds(ctx, func() (*rollupgen.RollupCoreAssertionConfirmedIterator, error) {
				return rc.FilterAssertionConfirmed(nil, nil)
			})
			require.NoError(t, err)
			for i.Next() {
				var assertionNode rollupgen.AssertionNode
				assertionNode, err = retry.UntilSucceeds(ctx, func() (rollupgen.AssertionNode, error) {
					return rc.GetAssertion(&bind.CallOpts{Context: ctx}, i.Event.AssertionHash)
				})
				require.NoError(t, err)
				isChallengeParent := assertionNode.FirstChildBlock > 0 && assertionNode.SecondChildBlock > 0
				if isChallengeParent && assertionNode.Status != uint8(protocol.AssertionConfirmed) {
					t.Fatal("Confirmed assertion with unfinished state")
				}
				confirmed = true
				break
			}
			time.Sleep(500 * time.Millisecond) // Don't spam the backend.
		}

		if !confirmed {
			t.Fatal("assertion was not confirmed")
		}

		// Scrape the edges that have been added to the challenge so far, then
		// wait until all of the essential root edges among them are confirmed.
		cm, err := challengeV2gen.NewEdgeChallengeManager(cmAddr, backend)
		require.NoError(t, err)

		// Scrape all the honest edges onchain (the ones made by the honest address).
		// Check if the edges that have claim id != None are confirmed (those are essential root edges)
		// and also check one step edges from honest party are confirmed.
		honestEssentialRootIds := make(map[common.Hash]bool, 0)
		chainId, err := backend.ChainID(ctx)
		require.NoError(t, err)
		it, err := cm.FilterEdgeAdded(nil, nil, nil, nil)
		require.NoError(t, err)
		for it.Next() {
			txHash := it.Event.Raw.TxHash
			tx, _, err := backend.TransactionByHash(ctx, txHash)
			require.NoError(t, err)
			sender, err := types.Sender(types.NewCancunSigner(chainId), tx)
			require.NoError(t, err)
			if sender != honestValidatorAddress {
				continue
			}
			// Skip edges that are not essential roots or the top-level challenge root.
			if it.Event.ClaimId == (common.Hash{}) || it.Event.Level == 0 {
				continue
			}
			honestEssentialRootIds[it.Event.EdgeId] = false
		}

		// Wait until all of the honest essential root ids are confirmed, with a
		// per-test deadline well below the package's global test timeout so a
		// stall surfaces as a structured failure (with the unconfirmed edge
		// IDs) instead of silently consuming the entire CI budget.
		waitCtx, cancel := context.WithTimeout(ctx, 30*time.Minute)
		defer cancel()
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()
		confirmedCount := 0
		for confirmedCount < len(honestEssentialRootIds) {
			for k, markedConfirmed := range honestEssentialRootIds {
				if markedConfirmed {
					continue
				}
				// retry.UntilSucceeds only returns an error when ctx is done,
				// so on err we just exit the inner loop and let the outer
				// select emit the structured diagnostic.
				edge, err := retry.UntilSucceeds(waitCtx, func() (challengeV2gen.ChallengeEdge, error) {
					return cm.GetEdge(&bind.CallOpts{Context: waitCtx}, k)
				})
				if err != nil {
					break
				}
				if edge.Status == uint8(protocol.EdgeConfirmed) {
					confirmedCount += 1
					honestEssentialRootIds[k] = true
					t.Logf("Confirmed %d/%d honest essential edges, got edge at level %d", confirmedCount, len(honestEssentialRootIds), edge.Level)
				}
			}
			if confirmedCount >= len(honestEssentialRootIds) {
				break
			}
			select {
			case <-waitCtx.Done():
				unconfirmed := make([]string, 0, len(honestEssentialRootIds)-confirmedCount)
				for k, markedConfirmed := range honestEssentialRootIds {
					if !markedConfirmed {
						unconfirmed = append(unconfirmed, k.Hex())
					}
				}
				t.Fatalf("timed out waiting for honest essential edges: %d/%d confirmed, unconfirmed=%v", confirmedCount, len(honestEssentialRootIds), unconfirmed)
			case <-ticker.C:
			}
		}
	})
	return nil
}
