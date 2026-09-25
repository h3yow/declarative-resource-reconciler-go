# declarative-resource-reconciler-go

一个**可嵌入进程运行**的声明式资源协调器（reconciler）示例实现。全部状态与
“外部副作用”均在本地进程内模拟，不依赖真实集群、外部服务或网络凭据。

## 目标与模型

协调器围绕三个事实源工作：

| 事实源 | 载体 | 含义 |
| --- | --- | --- |
| 期望状态 Desired | 调用方提交的 `api.DesiredState`（带单调递增 `Generation`） | “我希望资源是什么样” |
| 观测状态 Observed | `store.Store` 中的 `api.Resource`（带乐观并发版本 `ResourceVersion`） | “现在实际是什么样” |
| 外部副作用 | `bus.Bus` 中幂等的 effect（`effect://<id>`） | 协调产生的对外影响，删除时必须释放 |

每次触发（Trigger/Delete）都会在**该资源唯一的 worker 协程**上执行若干次
reconcile pass，使观测状态向期望状态收敛，并把每一轮的判定写入可解释的
`Decision` 日志。

## 包结构

- `pkg/api`：领域词汇表 —— Resource / DesiredState / Condition / Decision、
  瞬时与永久错误分类、稳定的 Reason/Action 常量、保留未知字段的 JSON 编解码。
- `pkg/store`：并发安全的内存存储，JSON 序列化模拟真实 API 边界；
  `UpdateFunc` 使用乐观并发（CAS），读改写期间若被外部修改则返回
  `ConflictError`（按瞬时错误重试）。
- `pkg/bus`：模拟外部系统的副作用总线。`Ensure` 幂等（同一 id 永远只有一个
  effect，重复调用只是“认领”），`Cleanup` 幂等（不存在即视为已清理）；
  通过 `FailRule` 确定性注入瞬时/永久失败。
- `pkg/reconciler`：协调器本体与每资源串行队列。

## 协调语义

1. **串行化（同一资源）**：每个 id 只有一个 worker 协程。并发 Trigger 通过
   每 id 的计数 + 容量为 1 的唤醒通道排队：每个触发至少被处理一次（不丢），
   等待中的同 id 触发合流（不并发互相覆盖）。不同 id 互不阻塞。
2. **幂等收敛**：
   - 观测状态的 `SpecHash`/`Generation` 已等于期望、且 effect 引用已锚定 →
     `Action=Skip, Reason=ObservedMatchesDesired`，**不产生写入**；
   - 外部 effect 以 id 为幂等键，重复协调不会创建重复 effect；
   - Ready 条件的 `(Status, Reason, Message)` 元组不变时只增加计数、不落盘，
     避免重复触发造成写放大。创建与更新的区别记录在 Decision 日志中。
3. **代际顺序**：期望注册表只接受 `Generation` 不小于当前值的请求；更旧的
   触发仍会排队处理（不丢请求），但不会覆盖更新的期望。最终资源代际单调
   不减，收敛到最高代。
4. **漂移与重建**（外部变更）：读取观测状态后比对：
   - 关键字段 `spec.image` 缺失 → 以 `MissingKeyFieldRebuilt` 重建并记录
     `Ready=False`，不创建 effect；
   - `specHash` 为空（旧版本/外部写入）或哈希、代际与期望不一致 →
     以 `ExternalDriftRebuilt` 重建期望载荷并重新幂等 Ensure effect；
   - 资源被外部整体删除 → 下次触发走 Create 路径重建；
   - CAS 提交时发现又被外部修改 → 返回冲突，按瞬时错误重读再收敛。
5. **前向/后向兼容**：`Resource` 反序列化时把所有未知字段原样保存在
   `Extra`，提交时保留并在 JSON 顶层回写，旧版本/未知字段既不会导致崩溃，
   也不会被静默丢弃；发现未知字段会打印 `compat` 日志，可通过
   `Reconciler.CompatFields(id)` 线程安全地观测。损坏的 JSON 被当作瞬时错误
   持续重试，worker 不会 panic，外部修复（`Put`）后自动收敛。

## 重试分类与退避

| 情况 | 分类 | 行为 |
| --- | --- | --- |
| effect Ensure / Cleanup 暂时不可用 | `TransientError` | `Action=Retry`，记录 `Ready=False/RetryingTransientFailure`（删除时为 `Deleting=False/CleanupTransientFailure`），按指数退避重新入队：`Initial=10ms × 2^n`，封顶 `Max=2s`；请求保留在队列中，不丢弃 |
| 外部策略永久拒绝、关键字段缺失 | `PermanentError` | `Action=Fail`，记录稳定且可区分的 `Ready=False/PermanentFailure`（或 `MissingKeyFieldRebuilt`），**停止无效重试**；直到新的期望（更高 Generation）到达才允许再试 |
| 存储读改写冲突、损坏 JSON | 瞬时 | 重读/等修复后重试 |

退避期间到来的新触发会提前唤醒 worker；重试归属于原触发，不额外消耗触发计数。

## 删除规则（finalizer 语义）

1. `Delete` 给资源打上删除标记与 `Deleting` 条件，资源**不会立即消失**。
2. 若资源持有 `effect-cleanup` finalizer（说明存在外部 effect），先调用
   `bus.Cleanup`：
   - 清理瞬时失败 → 资源保留、退避重试（可观测 `CleanupTransientFailure`）；
   - 清理成功（effect 确认消失）→ **此时才**移除资源（`Deleted`）。
3. 从未成功创建 effect 的资源（例如关键字段一直缺失）没有 finalizer，
   删除时直接移除，理由区分为 `DeletedBeforeReady`，且不调用 Cleanup。
4. 删除进行中收到更高 Generation 的新 Trigger 会取消删除并重新收敛。

## 可解释性

每个 pass 产出一条 `Decision{Action, Reason, Attempt, Detail, Duration}`：
- 日志中打印触发输入、重试次数（attempt 从 1 递增）与判定依据；
- 测试通过 `Reconciler.Decisions(id)` / `DecisionsText(w, id)` 断言
  Action/Reason 可区分，并在 `t.Logf` 中输出触发输入、重试次数和判定依据。

Reason 词汇表（稳定、互不混淆）：
`Created`、`Reconciled`、`ObservedMatchesDesired`、
`RetryingTransientFailure`、`PermanentFailure`、`ExternalDriftRebuilt`、
`MissingKeyFieldRebuilt`、`CleanupInProgress`、`CleanupTransientFailure`、
`Deleted`、`DeletedBeforeReady`。

## 本地验证

无需任何外部依赖，仅需 Go 工具链：

```bash
go build ./...
go vet ./...
go test ./... -race -count=1        # 竞态检测下的完整验证
go test ./... -race -count=5        # 多次重复，验证时序稳定性
go test ./pkg/reconciler/ -v        # 查看每个场景的触发输入/重试次数/判定日志
```

测试覆盖：

| 测试 | 场景与断言 |
| --- | --- |
| `TestIdempotentConvergence` / `TestSkipWhenSatisfied` | 重复相同触发：副作用次数不增、资源版本不变、出现 Skip |
| `TestConcurrentTriggersSerializesAndConverges` | 40 goroutine 并发打同一 id + 读写并发：最终代际=最高值、代际不回退、effect 恰好 1 个 |
| `TestConcurrentDistinctIDs` | 20 个不同 id 并行收敛，互不阻塞 |
| `TestTransientFailureRetriedWithBackoff` | 注入 3 次瞬时失败：Ensure 调用恰为 4 次、attempt=[1,2,3] 后恢复 Ready |
| `TestPermanentFailureStopsRetrying` | 永久失败后调用计数不再增长、条件稳定可区分；新代际输入后可恢复 |
| `TestDeleteWaitsForCleanup` | 清理前 3 次瞬时失败期间资源保留，Cleanup 4 次后才移除，effect 确认消失 |
| `TestDeleteNeverReadyResource` / `TestDeleteCancelledByNewTrigger` | 无 finalizer 的删除理由区分；删除被新触发取消 |
| `TestExternalSpecDriftRebuilt` / `TestExternalKeyFieldCleared` / `TestExternalDeletionRecreated` | 外部改字段/清关键字段/删资源：以可区分原因重建 |
| `TestUnknownAndLegacyFieldsPreserved` | 未知与旧版本字段跨协调往返保留、打 compat 日志 |
| `TestMalformedJSONDoesNotPanic` | 存储损坏：按瞬时重试不崩溃，外部修复后收敛 |

## 嵌入式用法

```go
st := store.New()
eb := bus.New()
rec := reconciler.New(st, eb, logger, api.DefaultBackoff())
rec.Start(ctx)
defer rec.Stop()

rec.Trigger(api.DesiredState{ID: "w-1", Generation: 1,
    Spec: api.WidgetSpec{Image: "img:v1", Replicas: 2}})
rec.Delete(api.DeleteRequest{ID: "w-1", Generation: 2})
```
