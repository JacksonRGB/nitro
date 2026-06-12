### Fixed
- Sequencing now stays in-process when consensus connects to execution over a same-process loopback RPC (`--node.execution-rpc-client.url=self` or `self-auth`), instead of being disabled as in the remote RPC case.
- Consensus RPC client now restores the `ErrRetrySequencer` sentinel across the RPC boundary, so the sequencer requeues transactions during transient coordinator handovers (e.g. Redis switchover) instead of surfacing the error to `eth_sendRawTransaction` clients.

### Internal
- Re-enable CI tests for consensus and execution nodes connected over JSON RPC, split into `defaults-A-consensus-execution-rpc` / `defaults-B-consensus-execution-rpc` modes of the standard go test suite, gated by the `run-defaults-a-consensus-execution-rpc` / `run-defaults-b-consensus-execution-rpc` workflow inputs.
