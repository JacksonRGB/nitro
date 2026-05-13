// Copyright 2026, Offchain Labs, Inc.
// For license information, see https://github.com/OffchainLabs/nitro/blob/master/LICENSE.md

//go:build challengetest && !race

package arbtest

import (
	"context"
	"math/big"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/node"
	"github.com/ethereum/go-ethereum/params"

	"github.com/offchainlabs/nitro/arbnode"
	"github.com/offchainlabs/nitro/arbos/l2pricing"
	"github.com/offchainlabs/nitro/bold/assertions"
	"github.com/offchainlabs/nitro/bold/challenge"
	modes "github.com/offchainlabs/nitro/bold/challenge/types"
	"github.com/offchainlabs/nitro/bold/protocol"
	"github.com/offchainlabs/nitro/bold/protocol/sol"
	"github.com/offchainlabs/nitro/bold/state"
	"github.com/offchainlabs/nitro/bold/testing/setup"
	"github.com/offchainlabs/nitro/cmd/chaininfo"
	"github.com/offchainlabs/nitro/execution_consensus"
	"github.com/offchainlabs/nitro/solgen/go/bridgegen"
	"github.com/offchainlabs/nitro/solgen/go/mocksgen"
	"github.com/offchainlabs/nitro/staker"
	"github.com/offchainlabs/nitro/staker/bold"
	"github.com/offchainlabs/nitro/util"
	"github.com/offchainlabs/nitro/validator/server_common"
	"github.com/offchainlabs/nitro/validator/valnode"
)

// TestBoldSelfChallengeRepro is the system-level analogue of
// bold/assertions/self_challenge_repro_test.go. It stands up a real L1 + L2 +
// BoLD rollup stack, posts one canonical child assertion Y on-chain, then runs
// a full challenge.Stack whose ExecutionProvider is wrapped to return a
// transient-wrong EndHistoryRoot on the first call for Y's batch — exactly the
// non-deterministic state-provider race observed in production.
//
// The detection mechanism is a thin recording wrapper around the rival handler:
// after challenge.NewChallengeStack wires the challenge manager into the
// assertion manager as the RivalHandler, the test reinstalls a wrapping
// handler that records every HandleCorrectRival(hash) call before delegating
// to the real challenge manager. The bug's signature is that this method is
// invoked with the canonical assertion Y's own hash — the validator's "correct
// rival" being byte-identical to the assertion it just declared invalid. That
// is exactly what the production log line "correctRivalAssertionHash == detectedAssertionHash"
// is reporting.
//
// On master the validator's sync loop runs the full prod log chain:
//
//	WARN  Disagreed with an observed assertion onchain  detectedAssertionHash=Y
//	INFO  Rival assertion already exists onchain        assertionHash=Y
//	INFO  Posted rival assertion to another that we disagreed with
//	        correctRivalAssertionHash=Y                  ← equals detectedAssertionHash
//	ERROR could not add block challenge level zero edge …  execution reverted
//
// The reverting tx at the bottom of the chain is the on-chain manifestation
// (in prod, OnlyOwnerDestination from the validator wallet's whitelist; in
// this test harness, AssertionNoSibling() from the challenge manager itself
// since the canonical assertion has no rival). Either revert confirms that
// the validator just tried to challenge a canonical assertion — that is the
// stake-threatening bug we are reproducing.
//
// Expected on master: the recording handler captures one or more calls whose
// hash equals Y's, and the require.Empty assertion at the bottom FAILS, with a
// diagnostic message that names the canonical hash and the inappropriate
// invocation. Expected after the sync.go:441 self-check fix: the rival path
// detects the same-hash condition and bails before HandleCorrectRival is
// invoked. The recorder stays empty, the require.Empty assertion passes, and
// the test goes green — this is the regression gate for the system path.
func TestBoldSelfChallengeRepro(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	repro := setupBoldSelfChallengeRepro(t, ctx)
	defer requireClose(t, repro.l1stack)
	defer repro.l2node.StopAndWait()

	// Post two batches so we have enough material for an assertion past genesis,
	// and grant the sequencer batch-poster rights on the inbox.
	sequencerTxOpts := repro.l1info.GetDefaultTransactOpts("Sequencer", ctx)
	seqInbox := repro.l1info.GetAddress("SequencerInbox")
	seqInboxBinding, err := bridgegen.NewSequencerInbox(seqInbox, repro.l1client)
	Require(t, err)
	seqInboxABI, err := abi.JSON(strings.NewReader(bridgegen.SequencerInboxABI))
	Require(t, err)
	upgradeExec, err := mocksgen.NewUpgradeExecutorMock(repro.l1info.GetAddress("UpgradeExecutor"), repro.l1client)
	Require(t, err)
	data, err := seqInboxABI.Pack("setIsBatchPoster", sequencerTxOpts.From, true)
	Require(t, err)
	rollupOwnerOpts := repro.l1info.GetDefaultTransactOpts("RollupOwner", ctx)
	_, err = upgradeExec.ExecuteCall(&rollupOwnerOpts, seqInbox, data)
	Require(t, err)

	repro.l2info.GenerateAccount("Destination")
	makeBoldBatch(t, repro.l2node, repro.l2info, repro.l1client, &sequencerTxOpts, seqInboxBinding, seqInbox, 5, -1)
	makeBoldBatch(t, repro.l2node, repro.l2info, repro.l1client, &sequencerTxOpts, seqInboxBinding, seqInbox, 5, -1)

	bridgeBinding, err := bridgegen.NewBridge(repro.l1info.GetAddress("Bridge"), repro.l1client)
	Require(t, err)
	totalBatchesBig, err := bridgeBinding.SequencerMessageCount(&bind.CallOpts{Context: ctx})
	Require(t, err)
	totalBatches := totalBatchesBig.Uint64()

	pollUntil(t, ctx, 5*time.Minute, 100*time.Millisecond, "validator to validate batches", func() bool {
		lastInfo, err := repro.blockValidator.ReadLastValidatedInfo()
		if err != nil || lastInfo == nil {
			return false
		}
		return lastInfo.GlobalState.Batch >= totalBatches
	})

	// Post one canonical child Y on-chain via the real state provider, using
	// the configured asserter account. This is the assertion the validator's
	// flaky-wrapped sync loop will later see and erroneously flag as invalid.
	genesisHash, err := repro.assertionChain.GenesisAssertionHash(ctx)
	Require(t, err)
	genesisInfo, err := repro.assertionChain.ReadAssertionCreationInfo(ctx, protocol.AssertionHash{Hash: genesisHash})
	Require(t, err)
	parentGlobalState := protocol.GoGlobalStateFromSolidity(genesisInfo.AfterState.GlobalState)
	yState, err := repro.stateManager.ExecutionStateAfterPreviousState(
		ctx, genesisInfo.InboxMaxCount.Uint64(), parentGlobalState,
	)
	Require(t, err)
	yAssertion, err := repro.assertionChain.NewStakeOnNewAssertion(ctx, genesisInfo, yState)
	Require(t, err)
	yHash := yAssertion.Id()
	t.Logf("posted canonical child Y on-chain: %s", yHash)

	// Wrap the real state provider so the FIRST call to
	// ExecutionStateAfterPreviousState for Y's batch returns a state with a
	// wrong EndHistoryRoot; subsequent calls return the truth. This is the
	// minimal model of the prod race in [staker/bold/bold_state_provider.go].
	flaky := newFlakySystemExecutionProvider(repro.stateManager, genesisInfo.InboxMaxCount.Uint64())

	// Build a HistoryCommitmentProvider where ONLY the ExecutionProvider role
	// (5th arg) is wrapped — everything else uses the real provider.
	provider := state.NewHistoryCommitmentProvider(
		repro.stateManager,
		repro.stateManager,
		repro.stateManager,
		[]state.Height{state.Height(repro.blockChallengeLeafHeight)},
		flaky,
		nil,
	)

	// Build the assertion manager manually with posting disabled. The poster
	// would otherwise fire ExecutionStateAfterParent immediately at startup —
	// from the same (genesis-parent, maxInboxCount=1) cell that sync's
	// findCanonicalAssertionBranch evaluates Y under — and consume the flaky
	// provider's first-fire trigger before sync gets to it.
	asm, err := assertions.NewManager(
		repro.assertionChain,
		provider,
		"self-challenge-repro",
		modes.MakeMode,
		assertions.WithPostingDisabled(),
		assertions.WithPollingInterval(500*time.Millisecond),
		assertions.WithConfirmationInterval(time.Hour),
		assertions.WithAverageBlockCreationTime(time.Second),
		assertions.WithMinimumGapToParentAssertion(0),
		assertions.WithPostingInterval(time.Hour),
		assertions.WithMaxGetLogBlocks(5000),
	)
	Require(t, err)

	manager, err := challenge.NewChallengeStack(
		repro.assertionChain,
		provider,
		challenge.StackWithName("self-challenge-repro"),
		challenge.StackWithMode(modes.MakeMode),
		challenge.StackWithPostingInterval(time.Hour),
		challenge.StackWithPollingInterval(500*time.Millisecond),
		challenge.StackWithConfirmationInterval(time.Hour),
		challenge.StackWithMinimumGapToParentAssertion(0),
		challenge.StackWithAverageBlockCreationTime(time.Second),
		challenge.StackWithHeaderProvider(repro.l2node.L1Reader),
		challenge.StackWithSyncMaxGetLogBlocks(5000),
		challenge.OverrideAssertionManager(asm),
	)
	Require(t, err)

	recorder := &recordingRivalHandler{inner: manager}
	asm.SetRivalHandler(recorder)

	manager.Start(ctx)

	deadline := time.Now().Add(2 * time.Minute)
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for time.Now().Before(deadline) {
		if len(recorder.snapshot()) > 0 {
			break
		}
		select {
		case <-ctx.Done():
			break
		case <-ticker.C:
		}
	}

	calls := recorder.snapshot()
	for _, h := range calls {
		t.Logf(
			"self-challenge bug fired end-to-end: HandleCorrectRival(%s); "+
				"args.invalidAssertion.AssertionHash=%s (equal=%v)",
			h, yHash, h == yHash,
		)
	}

	// Regression gate. With the bug present the recorder captures one or more
	// invocations whose hash equals Y's, and require.Empty fails with the
	// diagnostic above. With the proposed sync.go:441 self-check fix the rival
	// path bails before HandleCorrectRival is ever called, the recorder stays
	// empty, and the test goes green.
	require.Empty(
		t,
		calls,
		"self-challenge bug reproduced end-to-end: the challenge manager's "+
			"HandleCorrectRival was invoked when it should have been short-circuited. "+
			"The supposedly-invalid assertion %s has the same hash as the 'correct rival' "+
			"the validator computed, so the rival path is trying to challenge a canonical "+
			"assertion against itself. Captured calls: %v",
		yHash,
		calls,
	)
}

// recordingRivalHandler captures every HandleCorrectRival call before
// delegating to a wrapped handler. The wrapped handler is the real
// challenge.Manager — calls keep flowing through it so the test exercises the
// full production code path (createLayerZeroEdge attempt, on-chain revert),
// but the recorder gives the test a direct, fast, hash-level assertion target
// without needing to scrape logs.
type recordingRivalHandler struct {
	inner modes.RivalHandler

	mu    sync.Mutex
	calls []protocol.AssertionHash
}

func (r *recordingRivalHandler) HandleCorrectRival(ctx context.Context, hash protocol.AssertionHash) error {
	r.mu.Lock()
	r.calls = append(r.calls, hash)
	r.mu.Unlock()
	if r.inner != nil {
		return r.inner.HandleCorrectRival(ctx, hash)
	}
	return nil
}

func (r *recordingRivalHandler) snapshot() []protocol.AssertionHash {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]protocol.AssertionHash, len(r.calls))
	copy(out, r.calls)
	return out
}

var _ modes.RivalHandler = (*recordingRivalHandler)(nil)

// flakySystemExecutionProvider mirrors the wrapper used by the unit test in
// bold/assertions/self_challenge_repro_test.go but lives in the system_tests
// package so the test is self-contained.
//
// On the first call to ExecutionStateAfterPreviousState matching the target
// maxInboxCount, it delegates to the inner provider, then replaces the returned
// EndHistoryRoot with a deterministic-but-wrong value before returning. Every
// subsequent call delegates unchanged.
type flakySystemExecutionProvider struct {
	inner            state.ExecutionProvider
	targetInboxCount uint64

	mu        sync.Mutex
	triggered bool
}

func newFlakySystemExecutionProvider(inner state.ExecutionProvider, targetInboxCount uint64) *flakySystemExecutionProvider {
	return &flakySystemExecutionProvider{inner: inner, targetInboxCount: targetInboxCount}
}

func (f *flakySystemExecutionProvider) ExecutionStateAfterPreviousState(
	ctx context.Context,
	maxInboxCount uint64,
	previousGlobalState protocol.GoGlobalState,
) (*protocol.ExecutionState, error) {
	real, err := f.inner.ExecutionStateAfterPreviousState(ctx, maxInboxCount, previousGlobalState)
	if err != nil {
		return real, err
	}
	f.mu.Lock()
	corrupt := maxInboxCount == f.targetInboxCount && !f.triggered
	if corrupt {
		f.triggered = true
	}
	f.mu.Unlock()
	if !corrupt {
		return real, nil
	}
	corrupted := *real
	corrupted.EndHistoryRoot = common.Hash(crypto.Keccak256Hash([]byte("transient-wrong-end-history-root")))
	return &corrupted, nil
}

var _ state.ExecutionProvider = (*flakySystemExecutionProvider)(nil)

// reproRig bundles everything the test needs from the L1+L2 setup, including
// the real BOLDStateProvider, the real AssertionChain, and the validator. The
// fields are unexported because nothing outside this file uses them.
type reproRig struct {
	l1stack                  *node.Node
	l1client                 *ethclient.Client
	l1info                   info
	l2node                   *arbnode.Node
	l2info                   info
	stateManager             *bold.BOLDStateProvider
	blockValidator           *staker.BlockValidator
	assertionChain           *sol.AssertionChain
	blockChallengeLeafHeight uint64
}

func setupBoldSelfChallengeRepro(t *testing.T, ctx context.Context) *reproRig {
	t.Helper()
	transferGas := util.NormalizeL2GasForL1GasInitial(800_000, params.GWei)
	l2chainConfig := chaininfo.ArbitrumDevTestChainConfig()
	l2info := NewBlockChainTestInfo(
		t,
		types.NewArbitrumSigner(types.NewLondonSigner(l2chainConfig.ChainID)),
		big.NewInt(l2pricing.InitialBaseFeeWei*2),
		transferGas,
	)
	ownerBal := big.NewInt(params.Ether)
	ownerBal.Mul(ownerBal, big.NewInt(1_000_000))
	l2info.GenerateGenesisAccount("Owner", ownerBal)
	sconf := setup.RollupStackConfig{
		UseMockBridge:          false,
		UseMockOneStepProver:   false,
		UseBlobs:               true,
		MinimumAssertionPeriod: 0,
	}

	l2info, l2node, l2execNode, _, l2stack, l1info, _, l1client, l1stack, assertionChain, _, _, _, _ := createCompleteTestNodeOnL1(
		t, ctx, false, nil, l2chainConfig, nil, sconf, l2info, false, false,
	)

	valnode.TestValidationConfig.UseJit = false
	_, valStack := createTestValidationNode(t, ctx, &valnode.TestValidationConfig)
	blockValidatorConfig := staker.TestBlockValidatorConfig

	locator, err := server_common.NewMachineLocator(valnode.TestValidationConfig.Wasm.RootPath)
	Require(t, err)
	pcds := l2node.GetParentChainDataSource()
	stateless, err := staker.NewStatelessBlockValidator(
		pcds, pcds, l2node.TxStreamer, l2node.ExecutionRecorder, l2node.ConsensusDB, nil,
		StaticFetcherFrom(t, &blockValidatorConfig), valStack, locator.LatestWasmModuleRoot(),
	)
	Require(t, err)
	Require(t, stateless.Start(ctx))

	blockValidator, err := staker.NewBlockValidator(
		stateless, pcds, l2node.TxStreamer, StaticFetcherFrom(t, &blockValidatorConfig), nil,
	)
	Require(t, err)
	Require(t, blockValidator.Initialize(ctx))
	Require(t, blockValidator.Start(ctx))

	blockChallengeLeafHeight := uint64(1 << 5)
	dir := t.TempDir()
	stateManager, err := bold.NewBOLDStateProvider(
		blockValidator, stateless, state.Height(blockChallengeLeafHeight),
		&bold.StateProviderConfig{
			ValidatorName:          "self-challenge-repro",
			MachineLeavesCachePath: dir,
			CheckBatchFinality:     false,
		},
		dir, pcds, l2node.TxStreamer, pcds, nil,
	)
	Require(t, err)

	_, err = execution_consensus.InitAndStartExecutionAndConsensusNodes(ctx, l2stack, l2execNode, l2node)
	Require(t, err)

	return &reproRig{
		l1stack:                  l1stack,
		l1client:                 l1client,
		l1info:                   l1info,
		l2node:                   l2node,
		l2info:                   l2info,
		stateManager:             stateManager,
		blockValidator:           blockValidator,
		assertionChain:           assertionChain,
		blockChallengeLeafHeight: blockChallengeLeafHeight,
	}
}
