# edge-fleet-sync-go

在单进程内模拟真实云边组网：一个云侧控制器（注册中心 + 配置管理 + 遥测入库）
与多个边侧设备代理通过可断连、可重复投递的链路交互，全程不依赖外部服务或真实凭据。

## 工程结构

```
pkg/fleet/
  types.go      核心类型与可区分的拒绝原因（哨兵错误 + RejectionError）
  clock.go      Clock 抽象；测试用 ManualClock，无需真实等待
  registry.go   Registry：设备注册、心跳、租约管理（并发安全）
  config.go     ConfigManager：带版本的配置下发、离线排队、确认与重投
  telemetry.go  TelemetryStore：遥测入库、去重、GAP/乱序标记、可用性模拟
  device.go     Device 边侧代理、Link 链路模拟、TelemetryCache 有界本地缓存
```

## 心跳与租约语义

- 设备通过 `Register(deviceID, identity)` 注册，控制器签发租约 `Lease{ID, ExpiresAt}`（TTL 可配）。
- `Heartbeat(deviceID, leaseID, hbSeq)` 刷新租约；`hbSeq` 必须严格递增。
- 心跳超期（当前时间超过 `ExpiresAt`）设备即被判离线，`ConfigManager.NextConfig`
  对离线设备一律返回"无任务"，不再下发。
- 拒绝原因可区分（`errors.Is` 可判定）：
  - `ErrAlreadyRegistered`：同一身份重复注册且租约仍有效；
  - `ErrIdentityConflict`：同一 deviceID 被不同身份占用；
  - `ErrStaleLease`：携带旧租约回连——不会覆盖控制器当前有效租约；
  - `ErrLeaseExpired`：租约已过期，设备被判离线；
  - `ErrHeartbeatOutOfOrder`：心跳序号回退/重复，拒绝但不影响在线状态。
- 所有状态变更在单把互斥锁下完成，并发接入下恰好一个注册者获胜。

## 配置版本兼容规则

- 每个设备的配置版本**只能严格向前**：`Version` 大于设备已应用版本且大于当前期望版本，
  否则拒绝 `ErrVersionRegression`。
- `RequiresVersion` 表示设备已应用版本的下限，不满足则拒绝 `ErrIncompatibleConfig`。
- 设备离线期间 `Push` 的配置进入每设备队列，恢复上线后按版本顺序逐条送达。
- 投递为拉取式至少一次：`NextConfig` 返回队首（未确认前重复返回同一版本），
  设备应用后 `Ack`。确认丢失导致重复投递时，设备对 `Version <= 已应用版本` 的配置
  只再次确认、不重复生效，两侧状态始终收敛。

## 遥测续传与去重约定

- 设备为每条遥测分配严格递增的 `Seq`，先写入本地有界缓存 `TelemetryCache`，
  链路可用时按序 `FlushTelemetry` 上传。
- 入库按 `(deviceID, seq)` 去重：重复投递是幂等空操作（计入 `Duplicates()`），不报错。
- 序号异常**标记而非丢弃**：首条记录 `Seq > 1` 或 `Seq > maxSeq+1` 标记 `GAP`；
  `Seq < maxSeq` 的迟到记录标记 `OUT_OF_ORDER`。
- 缓存溢出策略确定：容量满时**丢弃最旧记录**（DropOldest）并累计 `Dropped()`；
  溢出造成的序号缺失会在入库时体现为 `GAP` 标记，可解释。
- 缓存写入是纯本地操作，不触碰后端；后端不可用（`SetAvailable(false)`）时
  `FlushTelemetry` 保留全部数据并正常返回，恢复后按序补齐。

## 本地验证

```bash
go build ./...
go test ./... -count=1 -v          # 日志中每条用例都标注了覆盖的业务
CGO_ENABLED=1 go test ./... -race  # 并发接入的竞态检测
```

测试全部使用 `ManualClock` 与进程内链路，不依赖真实网络、不需要长时间等待，
覆盖：断连重连、重复投递（配置与遥测）、租约过期与旧租约回连、缓存溢出、
后端不可用、并发注册与并发心跳。
