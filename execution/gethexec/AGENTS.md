# gethexec 工作说明

- `ExecutionEngine` 通过 ArbOS 的 `ReceiptProducedHook` 在每笔交易执行完成后立刻推送 `receipt` logs；直接 sequencer、延迟消息和 feed digest 路径都必须挂接 receipt publisher。事件同时带候选 `block_number` 与建块前的 `previous_block_number`；此时最终 `BlockHash` 尚未确定。
- 本地 gRPC 日志流由 `logstream_grpc.go` 管理，固定监听 `0.0.0.0:18888`，服务方法为 `nitro.logs.v1.LogStream/Subscribe`。请求为 `google.protobuf.Empty`，每条响应为包含一个 EVM log 的 `google.protobuf.Struct`。
- 建块协程只能通过 `GRPCLogPublisher.PublishReceipt` 非阻塞入队，不能在 `appendBlock` 中进行网络发送或等待客户端；慢客户端应被断开，不得反压出块。
- 独立客户端示例位于 `execution/gethexec/examples/logstream_client.go`，使用 gRPC 全限定方法直接订阅，不依赖 `gethexec` 包。
- 最小独立验证：`go test execution/gethexec/logstream_grpc.go execution/gethexec/logstream_grpc_test.go`。完整包还需要先生成被忽略的 `solgen/go/` 合约绑定。
