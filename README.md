# edge-fleet-sync-go

进程内（in-process）云边协同模拟组件：一个云侧 `Registry` 控制器与多个边侧
`edge.Device` 设备通过内存传输 `transport.InProc` 组网，不依赖任何外部服务、
真实网络或凭据。所有时间由可注入的 `clock.Clock` 提供，测试使用 `FakeClock`
手动推进时间，因此测试快速、稳定、无 `sleep` 与真实网络等待。

## 模块划分

| 包 | 职责 |
| --- | --- |
| `protocol` | 设备 ID、租约、信封、心跳、配置、遥测等数据结构，以及带稳定错误码的 `RejectError` |
| `clock` | `Clock` 接口、真实时钟 `WallClock`、测试时钟 `FakeClock` |
| `cloud` | 云侧权威状态：注册表、租约/心跳、版本化配置编排、遥测入库与缺口标记 |
| `edge` | 边侧代理：会话/租约持有、心跳计数、配置幂等应用、有界遥测缓存 |
| `transport` | 进程内传输：设备邮箱、可切换的上下行链路故障、投递审计 |
| `fleet` | 组装层：`Fleet` 与确定性的单步调度 `RunOnce`（心跳/过期扫描/投递/续传） |
| `cmd/fleetdemo` | 可运行的端到端脚本演示 |

## 心跳与租约语义

- 设备先 `Register`（身份为 `(DeviceID, key)`），再 `Connect` 才会上线。
- 每次 `Connect`/`Reconnect` 颁发严格递增的 `Epoch`，旧租约立即失效。
- 心跳必须携带匹配的租约 `Epoch` 和设备固件侧单调递增的 `Counter`。
- 云侧每次接受心跳把过期时间刷新为 `now + LeaseTTLMs`；`ExpireSweep`
  （由 `Fleet.RunOnce` 或外部 tick 驱动）把超期设备置为离线。
- 离线/过期设备不接受新的实时下发；配置进入该设备的 FIFO 队列。
- 过期设备用旧租约回连或发心跳会被拒绝，且不会覆盖当前有效状态：
  - `unknown_device`：未注册；
  - `identity_conflict`：同一 ID 使用不同 key；
  - `stale_lease`：旧 `Epoch`、已过期或已断开的会话；
  - `heartbeat_out_of_order`：`Counter` 不大于该会话已接受的最大值；
  - `duplicate_registration`：相同 ID+key 的重复注册（幂等 no-op，仍给出可区分原因）。
- 所有状态变更在 `Registry` 的单把互斥锁内完成，`RunOnce` 与并发写入、
  并发接入交错执行时状态不会错乱（见并发测试）。

## 配置版本与兼容规则

- 配置带整数 `Version`，同一设备只能严格向前推进：
  - 新版本必须大于云侧 `desiredVersion`；更小版本返回 `config_version_rollback`；
  - 对当前已生效版本的完全重复推送是幂等操作，返回同一投递信封
    （`DedupKey = config:<version>`），可用于确认丢失后的重投。
- 兼容窗口 `FirmwareMin/FirmwareMax` 必须包含设备固件版本，否则返回
  `config_incompatible`；边侧应用前也做同样校验。
- 设备离线期间的下发进入 FIFO 队列；重连后云侧按入队顺序依次投递，
  边侧按序应用。云侧在收到对应确认前一直保留该投递。
- 幂等确认：
  - 边侧以 `DedupKey` 去重，重复投递只生效一次，但始终补回确认；
  - 云侧对 `version <= appliedVersion` 的确认按幂等成功处理；
  - 云侧 `appliedVersion` 与边侧应用版本因此不会因重复投递产生分歧；
  - 从未下发过的版本收到确认返回 `config_ack_stale`。

## 遥测续传、去重与缺口约定

- 每条记录由边侧分配设备生命周期内**连续且单调递增**的 `Seq`（含离线期间）。
- 边侧缓存有界（构造时指定容量）。缓存写满时采用确定的
  **`drop_oldest`（丢最旧）** 策略：新值保留，序号最小的未投递记录被挤出；
  被挤出的序号记录在 `DroppedSeqs()` 审计列表中，可解释、可追溯。
- 缓存写入**永不阻塞、永不返回错误**：上行链路不可用（`FlushTelemetry`
  返回错误）时，数据原样保留，稍后整批复传。
- 云侧入库以 `(DeviceID, Seq)` 去重：
  - 重复序号返回在 `DupSeqs`，不重复入库；
  - `Seq > nextSeq` 时，中间缺口序号进入 `MissingSeqs` 并被标记（记录本身保留）；
  - 低于连续水位线的迟到未知记录入库并标记为乱序；已知记录按重复处理；
  - 缺口被连续补齐后清除对应标记；标记可通过 `MarkedSeqs()` 查询，
    数据被标记而不是静默丢弃。

## 本地验证

要求 Go 1.25+（仓库在 Go 1.26 上验证）。

```bash
# 全量单测（竞态检测）。测试用 FakeClock，毫秒级完成，无网络、无真实等待。
CGO_ENABLED=1 go test -race -count=1 ./...

# 查看每个业务用例与断言情况，日志带 [biz=<业务> case=<用例>] 前缀：
CGO_ENABLED=1 go test -race -count=1 -v ./...

# 稳定性重复执行：
CGO_ENABLED=1 go test -race -count=20 ./...

# 端到端演示（注册、心跳、过期、离线排队、重连 FIFO、重复投递）：
go run ./cmd/fleetdemo
```

### 测试覆盖的关键链路

| 业务 | 测试 | 覆盖点 |
| --- | --- | --- |
| 注册/身份 | `TestRegister_DuplicateAndConflict` | 首次注册、同 key 重复、身份冲突 |
| 租约 | `TestLease_ExpireOfflineAndStaleReconnect` | 心跳续约、超期离线、旧租约拒绝复活、离线不实时下发、重连补发 |
| 心跳顺序 | `TestHeartbeat_OutOfOrderCounter` | 乱序/重复 Counter 拒绝原因可区分 |
| 配置 | `TestConfig_VersionRulesAndRedelivery` | 前进推进、回退拒绝、固件不兼容、确认丢失重复投递、离线 FIFO、重复确认 |
| 遥测 | `TestTelemetry_OfflineResumeDedupAndGaps` | 断连缓存、恢复按序补齐、重复不入库、缺口/乱序标记 |
| 缓存溢出 | `TestTelemetry_CacheOverflowDeterministic` | `drop_oldest` 确定丢弃、审计、云端缺口标记 |
| 边侧语义 | `edge/device_test.go` | 后端不可用时写入不阻塞、失败 flush 保留、重复投递生效一次 |
| 并发接入 | `TestConcurrent_ManyDevicesWritersAndTicks` | 12 设备 × 4 写者并发记录、心跳 tick、配置下发与重连交错，无丢失/重复/状态错乱 |

## 编程接口速览

```go
c := clock.NewFakeClock(1000)
f := fleet.New(c, 300 /*ttl ms*/, 100 /*heartbeat ms*/)

dev, _ := f.Register("sensor-1", "secret", 2 /*firmware*/, 128 /*buffer*/)
_, _ = f.Cloud.PushConfig("sensor-1", protocol.Config{
    Version: 1, FirmwareMin: 1, FirmwareMax: 3, Body: "baseline",
})
dev.Record(c.NowMs(), "temp", 21.5)

f.RunOnce()                 // 心跳/扫描/投递/遥测 flush 的确定性一步
c.Advance(401); f.RunOnce() // 推进时间，触发租约过期
_ = f.Reconnect("sensor-1") // 新 epoch，FIFO 补发配置
```
