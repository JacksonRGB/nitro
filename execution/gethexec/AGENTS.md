# gethexec 工作说明

- `ExecutionEngine.appendBlock` 是所有已提交 L2 区块写入本地链的汇合点；在此之后处理 receipt logs 可同时覆盖直接 sequencer、延迟消息和 feed digest 路径。
- 本地 gRPC 日志流由 `logstream_grpc.go` 管理，固定监听 `0.0.0.0:18888`，服务方法为 `nitro.logs.v1.LogStream/Subscribe`。请求为 `google.protobuf.Empty`，每条响应为包含一个 EVM log 的 `google.protobuf.Struct`。
- 建块协程只能通过 `GRPCLogPublisher.Publish` 非阻塞入队，不能在 `appendBlock` 中进行网络发送或等待客户端；慢客户端应被断开，不得反压出块。
- 独立客户端示例位于 `examples/logstream_client.go`，使用 gRPC 全限定方法直接订阅，不依赖 `gethexec` 包。
- 最小独立验证：`go test execution/gethexec/logstream_grpc.go execution/gethexec/logstream_grpc_test.go`。完整包还需要先生成被忽略的 `solgen/go/` 合约绑定。
