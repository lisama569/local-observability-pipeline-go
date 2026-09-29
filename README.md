# local-observability-pipeline-go

本地可运行的观测数据管道：统一接入 **指标 / 日志 / 追踪** 三类事件，围绕同一故障
（同一 `Fingerprint`）做跨信号关联，产出可追踪、可解释的告警状态。不依赖任何外部
服务或真实凭据，数据落在本地目录的 WAL 文件中。

## 构建与运行

```bash
go build ./...
go test ./...          # 单元 + 集成测试（含 -race 可通过 go test -race ./...）
go run ./cmd/pipeline  # 本地演示：接入→关联告警→重启恢复
```

`cmd/pipeline` 默认使用临时目录，也可用 `-dir <path>` 指定 WAL 目录以观察重启行为。

## 数据模型（internal/model）

三类事件归一化为统一的 `Event`：

| 字段 | 说明 |
|---|---|
| `ID` | 生产侧唯一 ID，与 `Source` 组成幂等去重键 |
| `Kind` | `metric` / `log` / `trace` |
| `Source` | 来源（服务/主机） |
| `Fingerprint` | **关联键**：同一故障的跨信号事件携带相同 fingerprint |
| `Timestamp` | 事件时间（告警窗口按事件时间判定） |
| `Severity` | `info` / `warn` / `error`，聚合只统计 `error` |
| `Message` / `Value` / `TraceID` / `Attributes` | 各信号类型的负载 |

- **校验**：缺 `ID/Kind/Source/Fingerprint/Timestamp` 会被拒绝，错误为可判别的哨兵
  错误（`model.ErrMissingID` 等，`errors.Is` 可区分），不会静默丢数据。
- **归一化**：可选字段缺失时补齐（如 trace 事件缺 `TraceID` 用事件 ID 顶替），
  被补齐的字段名通过 `Result.Defaulted` 返回，结果可判别。
- **去重**：`(Source, ID)` 为幂等键。同一来源短时间重复投递返回
  `Result.Duplicate=true`，不计为新事件、不参与告警判定。

## 存储与查询（internal/store）

- 所有被接受的事件先**追加写 WAL**（可选 fsync）再入内存索引：确认接收的数据
  重启后不丢失。
- 乱序事件：内存索引按 `(事件时间, 接收序)` 排序，`Query(from, to, kind)` 的输出
  与到达顺序无关，可复现。
- 内存有界：`MaxInMemory` 限制索引大小（WAL 仍完整），`DedupWindow` 限制去重表
  存活窗口。
- 告警通知确认（`MarkNotified`）也写入 WAL，重启后不会重复通知已确认的迁移。

## 告警状态规则（internal/alert）

每条 `Rule` 按 `(rule, fingerprint)` 维护状态机：

```
ok --(窗口内 error 数 >= Threshold)--> firing
firing --(持续触发)--> 抑制（只计数，不重复通知）
firing --(水位线超过最近触发 ResolveAfter)--> resolved
resolved --(ReopenWindow 内再次达到阈值)--> firing (Reopened=true)
```

- **聚合**：窗口按事件时间滑动，跨 metric/log/trace 统计同一 fingerprint 的
  error 事件。
- **可解释**：每次迁移（`Transition`）携带 `Reason`（如
  `"3 error events in 1m0s window (threshold 3)"`）和 `TriggerIDs`（作为证据的
  事件 ID 列表）。
- **防抖动**：水位线只前进不后退，迟到的旧事件参与窗口计数但不会把已恢复的
  状态拉回；迁移键 `rule|fingerprint|to|triggerEventID` 幂等，按相同顺序重放
  必然得到相同迁移序列（重启恢复不产生重复告警）。

## 背压与恢复语义（internal/sink, internal/pipeline）

- 下游分发走**有界队列**（`QueueCapacity`）。队列满时 `Ingest` 等待并在 ctx 超时后
  返回可区分的 `sink.ErrQueueFull`——背压向上游传导，内存不无界增长。
- 下游失败按指数退避重试（默认无限重试直至确认）；确认后回调写 WAL
  （`RecordNotified`）。
- **恢复语义**：重启时按接收序重放 WAL 重建告警状态机；已确认的迁移键跳过，
  未确认的补发。因此已确认接收的数据**不丢失、不重复计入**最终结论。

## 测试覆盖

| 场景 | 测试 |
|---|---|
| 字段校验失败原因可区分 | `internal/model` |
| 重复投递去重、乱序查询确定性、WAL 重启恢复 | `internal/store` |
| 触发/抑制/恢复/重开、乱序防抖动、重放确定性、跨信号关联 | `internal/alert` |
| 重复投递不计入、并发接入、重启不重复通知、下游失败恢复、背压有界 | `internal/pipeline` |

测试日志（`go test -v`）会打印关键输入、事件 ID、迁移键与判定依据。
