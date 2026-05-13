// Copyright 2026, Offchain Labs, Inc.
// For license information, see https://github.com/OffchainLabs/nitro/blob/master/LICENSE.md

package assertions

import (
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/offchainlabs/nitro/bold/containers/threadsafe"
	"github.com/offchainlabs/nitro/bold/protocol"
)

// TestAdvanceChainPointerDoesNotDowngradeLatestAgreedAssertion pins the
// structural invariant that fixes the prod "honest validator self-challenge"
// incident: latestAgreedAssertion must never move backward, even when the
// catchup goroutine writes an assertion that is an ancestor of (rather than a
// direct child of) the current value.
//
// Failure mode this test guards against: sync advances latestAgreedAssertion
// to A2; the slow RPC-bound catchup loop later finishes its write for A1
// (which is A2's ancestor). An unconditional advance would set
// latestAgreedAssertion back to A1, causing the next sync chunk to start its
// agreement-cursor at A1 instead of A2 and therefore skip the agreement check
// for any assertion whose parent is A2. Skipped assertions hit
// respondToAnyInvalidAssertions' "canonical parent, non-canonical self" branch
// and trigger a self-challenge against our own canonical assertion.
func TestAdvanceChainPointerDoesNotDowngradeLatestAgreedAssertion(t *testing.T) {
	a0 := protocol.AssertionHash{Hash: hashFromString("A0")}
	a1 := protocol.AssertionHash{Hash: hashFromString("A1")}
	a2 := protocol.AssertionHash{Hash: hashFromString("A2")}

	a0Info := &protocol.AssertionCreatedInfo{AssertionHash: a0, InboxMaxCount: big.NewInt(1)}
	a1Info := &protocol.AssertionCreatedInfo{AssertionHash: a1, ParentAssertionHash: a0, InboxMaxCount: big.NewInt(2)}
	a2Info := &protocol.AssertionCreatedInfo{AssertionHash: a2, ParentAssertionHash: a1, InboxMaxCount: big.NewInt(3)}

	m := managerWithCanonical(t, a2, map[protocol.AssertionHash]*protocol.AssertionCreatedInfo{
		a0: a0Info,
		a1: a1Info,
		a2: a2Info,
	})

	// Slow catchup belatedly applies A1 (an ancestor of latestAgreedAssertion).
	// A1's parent is A0, not A2, so the cursor must NOT move.
	m.applyChainPointerAdvance(a1Info)

	require.Equal(t,
		a2,
		m.assertionChainData.latestAgreedAssertion,
		"latestAgreedAssertion was downgraded from A2 back to an ancestor — "+
			"this is the bug that triggers honest-validator self-challenge")

	// canonicalAssertions/submittedAssertions must still be append-only.
	_, hasA1 := m.assertionChainData.canonicalAssertions[a1]
	require.True(t, hasA1, "A1 must still be present in canonicalAssertions")
	require.True(t, m.submittedAssertions.Has(a1), "A1 must be tracked in submittedAssertions")
}

// TestAdvanceChainPointerAdvancesOnDirectChild pins the positive side of the
// invariant: when the supplied assertion IS a direct child of the current
// latestAgreedAssertion, the advance must succeed. Without this, the catchup
// loop can't make progress.
func TestAdvanceChainPointerAdvancesOnDirectChild(t *testing.T) {
	a0 := protocol.AssertionHash{Hash: hashFromString("A0")}
	a1 := protocol.AssertionHash{Hash: hashFromString("A1")}

	a0Info := &protocol.AssertionCreatedInfo{AssertionHash: a0, InboxMaxCount: big.NewInt(1)}
	a1Info := &protocol.AssertionCreatedInfo{AssertionHash: a1, ParentAssertionHash: a0, InboxMaxCount: big.NewInt(2)}

	m := managerWithCanonical(t, a0, map[protocol.AssertionHash]*protocol.AssertionCreatedInfo{a0: a0Info})

	m.applyChainPointerAdvance(a1Info)

	require.Equal(t, a1, m.assertionChainData.latestAgreedAssertion,
		"latestAgreedAssertion should advance to a direct child of the current value")
	_, hasA1 := m.assertionChainData.canonicalAssertions[a1]
	require.True(t, hasA1)
}

// TestAdvanceChainPointerAllowsOverflowAdvance pins the choice of parent-hash
// linkage (not numeric ordering on InboxMaxCount) for the advance check.
//
// Overflow assertions are created when the machine stops mid-batch because it
// hit the per-assertion block-height cap before consuming the next inbox
// position. Their InboxMaxCount equals their parent's. A numeric check like
// "child.InboxMaxCount > parent.InboxMaxCount" would refuse the advance and
// pin catchup at the overflow parent forever, never making progress.
//
// This test would FAIL under a numeric implementation and PASSES under the
// parent-hash implementation.
func TestAdvanceChainPointerAllowsOverflowAdvance(t *testing.T) {
	parent := protocol.AssertionHash{Hash: hashFromString("parent")}
	child := protocol.AssertionHash{Hash: hashFromString("child-overflow")}

	const sharedInboxMaxCount = 42
	parentInfo := &protocol.AssertionCreatedInfo{
		AssertionHash: parent,
		InboxMaxCount: big.NewInt(sharedInboxMaxCount),
	}
	childInfo := &protocol.AssertionCreatedInfo{
		AssertionHash:       child,
		ParentAssertionHash: parent,
		// Same as parent — the overflow case. A numeric check would refuse.
		InboxMaxCount: big.NewInt(sharedInboxMaxCount),
	}

	m := managerWithCanonical(t, parent, map[protocol.AssertionHash]*protocol.AssertionCreatedInfo{parent: parentInfo})

	m.applyChainPointerAdvance(childInfo)

	require.Equal(t, child, m.assertionChainData.latestAgreedAssertion,
		"overflow child (same InboxMaxCount as parent) must be allowed to advance "+
			"the chain pointer — a numeric-ordering check would have left catchup stuck")
}

// managerWithCanonical builds a Manager with only the fields advanceChainPointer
// touches, pre-seeded with the supplied canonical map and latestAgreedAssertion.
func managerWithCanonical(
	t *testing.T,
	latestAgreed protocol.AssertionHash,
	canonical map[protocol.AssertionHash]*protocol.AssertionCreatedInfo,
) *Manager {
	t.Helper()
	return &Manager{
		assertionChainData: &assertionChainData{
			latestAgreedAssertion: latestAgreed,
			canonicalAssertions:   canonical,
		},
		submittedAssertions: threadsafe.NewLruSet(
			1024,
			threadsafe.LruSetWithMetric[protocol.AssertionHash]("submittedAssertions"),
		),
	}
}

func hashFromString(s string) [32]byte {
	var h [32]byte
	copy(h[:], s)
	return h
}
