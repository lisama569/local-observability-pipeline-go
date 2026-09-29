# 本地可观测数据管道（Go）

一套完全本地可运行、不依赖任何外部服务或真实凭据的观测数据管道。它统一接入
**指标（metric）、日志（log）、追踪（trace）** 三类事件，对同一故障做
聚合、抑制、恢复与重新打开，并在下游变慢或短暂失败时提供有界背压、重试与
重启恢复。所有数据只落在本地临时目录（WAL + 状态快照）。

## 快速开始

```bash
# 运行本地演示（自动使用临时目录，退出后清理）
go run ./cmd/pipeline

# 全部单测与集成测试（建议加 -race）
go test -race ./...
go test -v ./...          # 查看每个用例打印的关键输入、事件标识与判定依据
```

要求 Go 1.25+，无第三方依赖。

## 目录结构

```
internal/model     统一事件模型 Event 与可追踪事件 Incident（含校验与故障判定）
internal/dedup    有界 LRU：内存快速去重（容量受限，防内存无界增长）
internal/wal       本地只追加日志 events.log + checkpoint + seen.log（崩溃/重启恢复）
internal/store     有界、可统一查询的事件视图（结果确定性排序）
internal/alert     告警状态机：聚合/抑制/恢复/重开，转换幂等
internal/sink      下游接口 + 可控本地 FakeSink（脚本化失败、延迟、恢复）
internal/pipeline  接线、有界队列背压、下游重试、重启重放与快照恢复
cmd/pipeline      本地端到端演示
```

## 数据模型

### 统一事件 `model.Event`

三类事件归一为同一结构，关键字段：

- `event_id`（必填）：全局唯一标识，是去重主键。重复投递（含重启后）按
  同一事件处理，最多计入一次，绝不会算成新故障。
- `kind`：`metric` / `log` / `trace`。
- `source`、`service`、`fingerprint`：来源、服务、故障指纹。
  **关联键 = `service|fingerprint`**；同一故障的三类信号聚合到同一条 Incident。
- `trace_id`：把同一请求的 metric/log/trace 进一步关联，可直接用
  `store.Query{TraceID: ...}` 统一查询。
- `timestamp`：源端事件时间，允许乱序到达（见下）。
- 类别字段：metric 的 `value/fault`；log 的 `severity/message`；
  trace 的 `span_name/is_error/duration_ms`。
- `resolved`：显式恢复信号；`suppressed`：抑制窗口标记。

“故障信号”判定（`Event.IsFaultSignal`）：metric 的 `fault=true`；
log 的 `severity in {error,fatal}`；trace 的 `is_error=true`。

字段校验 `Event.Validate()` 返回 `*ValidationError`，每种失败有**可区分**的
稳定原因：`missing event_id` / `unknown kind` / `missing source` /
`missing service` / `missing fingerprint` / `missing timestamp` /
`log event missing message` / `metric event missing metric_name` /
`trace event missing span_name`。

### 可追踪事件 `model.Incident`

一条故障生命周期内所有事件挂在同一条记录上：关联键、全部 `event_ids`、
出现过的信号种类集合、关联的 `trace_id`、状态、打开/触发/恢复时间、
通知次数、重开次数、抑制标记，以及 `reason`——最近一次状态转换的
**人类可读判定依据**（测试日志与可解释性输出都打印它）。

## 统一查询

`store.Store` 是有界内存视图，`pipeline.Query(store.Query{...})` 支持按
种类、service、source、fingerprint、trace_id 与时间区间过滤。结果**与到达
顺序无关**：按 `(timestamp, event_id)` 确定性排序，同一输入多次查询结果
完全一致、可复现。每行带 `Reason` 说明命中了哪些条件。容量满时淘汰最旧
记录（有界内存）；持久去重不依赖它（见恢复语义）。

## 告警状态规则

状态机（`internal/alert`）：

```
pending ──满足触发规则──▶ firing ──恢复信号──▶ resolved
   │                        │  ▲
   │ suppressed 标记         │  └── 更多证据：不重复通知
   ▼                        ▼
suppressed（满足触发规则但被抑制，不通知）
resolved ──新故障信号──▶ pending（同一条 Incident 重新打开，reopen#++）
```

规则与不变量：

1. **聚合**：同一 `service|fingerprint` 的所有 metric/log/trace 信号聚合到
   同一条 Incident。默认需要 **2 种不同信号种类**（`FireAfterKinds`）
   才触发；同种类的重复信号、重复投递**绝不重复计数**。可配触发时间窗
   `FireWindow`，窗口内的种类才计入。
2. **抑制**：`suppressed=true` 时，pending 被标记；若随后满足触发规则，
   进入 `suppressed` 而不是 `firing`，**不发送通知**。抑制只作用于当前
   生命周期，恢复后重开的新生命周期重新开始。
3. **恢复**：显式 `resolved=true` 转 `resolved` 并发送一次 resolve 通知。
4. **重新打开**：resolved 后再来新故障信号，在**同一条可追踪事件**上重开，
   重新收集证据、重新计一次通知；`reopen_count++`，历史 `event_ids` 保留。
5. **乱序不抖动**：以最新事件时间为水位线。早于最新水位线的“迟到恢复”
   只作证据，不能把 firing 拉回 resolved；迟到信号也不能单独触发，避免状态
   来回抖动。
6. **幂等**：`Apply` 对已挂在该 Incident 上的 `event_id` 直接返回当前
   状态。因此 WAL 重放、重启、重复投递产生相同最终状态；每个生命周期
   fire/resolve 通知最多各一次。
7. **下游失败时**：通知失败则不进入 firing，保持 pending（`reason` 含
   `downstream`），由上层重试，绝不丢结论也不假装已通知。

## 背压、重试与恢复语义（pipeline）

### 接入路径（不静默丢数据）

`Ingest` / `TryIngest` 的处理顺序：

1. 校验（失败返回可区分的 validation 原因，计数 rejected）。
2. 去重：先查有界内存 LRU，再查**持久 seen 日志**（`seen.log`，
   fsync）。重复投递返回 `duplicate=true` 与原因，不重复计入、不重放。
3. 事件先 fsync 写入 WAL（`events.log`），再入有界队列，然后才向调用方
   确认接收。**已确认 = 已落盘**。
4. 后台单 worker 顺序处理：写 store → 状态机 `Apply`（含下游通知重试）→
   更新 checkpoint → 写 Incident 状态快照。

### 背压（内存有界）

- 队列容量固定。`Ingest` 在队列满时**阻塞**直到有空位或 ctx 结束；
  `TryIngest` **立即失败**并返回哨兵错误 `pipeline.ErrQueueFull`
  （原因可区分于校验错误与下游错误）。
- 去重缓存（LRU）、store 都有容量上限；持久去重交给 `seen.log`，
  内存不会无界增长。
- 下游变慢时，通知在处理单个事件期间同步重试（有界指数退避
  `NotifyBackoff/NotifyBackoffMax/MaxNotifyAttempts`），队列因此自然回压给上游。

### 失败与重试（可区分原因）

- 校验失败：`*model.ValidationError`（具体原因）。
- 背压：`pipeline.ErrQueueFull` / `pipeline.ErrClosed`。
- 下游失败：`sink.ErrDownstream`（FakeSink 可脚本化失败与延迟）。
- 通知发送在**处理一个事件的事务边界内**完成；成功后才推进 checkpoint，
  因此崩溃后该事件会重放并重试，而不是静默丢失通知。

### 重启恢复（不丢、不重复计入结论）

WAL 目录中：

- `events.log`：每行一个事件信封（offset + event），fsync；崩溃产生的半行
  在 Open 时检测并截断尾部（`ErrTornTail` 语义），**不影响更早的有效数据**。
- `checkpoint`：已完整处理（含通知）的最大 offset，原子替换。
- `seen.log`：所有已确认 event_id（base64 每行），重启后继续判重。
- `incidents.json`：状态机快照（checkpoint 之后原子写），恢复已提交事件
  的 Incident 状态（这些事件不再由 WAL 重放）。

重启时：恢复 incidents 快照 → 重放 checkpoint 之后的事件（用静默 notifier
重建状态，**不会重复发送历史通知**）→ 推进 checkpoint → 安装真实 notifier
开始接新事件。新事件继续关联到恢复出来的同一条 Incident。结合 event_id
幂等，重放/重启/重复投递后结论不丢、不重复、状态不抖动。

崩溃窗口语义：checkpoint 与状态快照分两步落盘；若在两者之间崩溃，重启时该
事件由 WAL 重放并幂等地重建相同状态；通知在事件处理事务内完成，checkpoint
只在通知成功后推进，从而保证“已确认接收的数据恢复后不丢失，也不重复
计入最终结论”。

## 本地复现与验证

`go run ./cmd/pipeline` 会用一个临时目录依次演示：单信号 pending → 第二类
信号 firing → 第三类信号（不重复通知）→ 同 ID 重复投递（判重）→ 乱序
迟到日志（不抖动）→ 恢复 resolved → 新故障在同一 Incident 重开，并打印
统一查询结果、incident 状态/次数/判定依据与统计计数。

测试覆盖（运行 `go test -race -v ./...` 可见关键输入、事件标识与判定依据）：

- `wal`：追加/重放/检查点、崩溃半行尾部修复、seen 日志跨重启。
- `dedup` / `store`：有界淘汰、重复不计数、乱序到达查询仍确定。
- `alert`：聚合一次触发、恢复、重开到同一事件、重放幂等不重复通知、
  迟到恢复不抖动、下游失败保持 pending、抑制不通知。
- `pipeline`（集成）：重复投递不算新故障；失败原因可区分；乱序不抖动；
  **重启恢复**（新 sink 验证不重发历史 fire、新 resolve 恰好一次、
  重投判重）；下游短暂失败后重试成功且只计一次；8 路并发 320 事件不丢
  不重、恰好一条告警；背压 `ErrQueueFull` 且已接收数据不丢；trace 维度
  统一查询三类事件。

FakeSink（`internal/sink`）提供 `FailNext` / `SetLatency` / `Recover`
与成功、失败调用计数，专用于在不接外部服务的前提下确定性地复现变慢与
短暂失败。
