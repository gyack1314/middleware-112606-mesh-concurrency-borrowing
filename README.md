# 服务网格的并发配额回收与分层公平准入

tenant 的 guarantee 表示该租户在线需求的同时执行名额；连续等待只在仍可兑现的同兼容范围里约束放行。endpoint 的 skill 为实例支持的工作类型。

## 输入消息

每个 JSONL 文件是一段独立输入记录。每行包括整数秒点 at、消息名称 kind 与对象 record。at 代表中间件收到消息的时刻，同一秒内按行序提交。业务时间和对应回执引用写在 record 中。

- **tenant**　name/class/guarantee，class 为 live 或 batch。
- **endpoint**　name/epoch/skill/cap，skill 为可处理类型列表。
- **request**　id/tenant/skill/deadline/preemptible。
- **sent**　以 permit 从 endpoint/epoch 派发 id。
- **accept**　permit 的实际接纳回报。
- **stop**　尝试请求停止 permit。
- **result**　permit 从 endpoint/epoch 返回 running、stopped、done、unknown。
- **tick**　检查秒点与有资格的工作。
- **inspect**　查询 tenant 或 endpoint 的限额和等待。
- **restart**　准入进程重启。

## 示例

- borrow-recall-without-phantom-slot.jsonl
- compatible-choice-and-lowered-cap.jsonl

## 读取演练资料

运行环境为 Python 3.11，只使用标准库。在根目录运行以下命令可核对文件格式及读取接口。

    python3 scenario_reader.py examples
    python3 -m unittest discover -v

资料读取器会按文件统计各类参与方消息的数量。演练记录只描述各参与方交给系统的输入。

## 准入服务入口

核心实现只使用 Python 3.11 标准库，入口为 `admission_service.py`；也可以从 `admission` 包或 `traffic_admission.py` 导入同一组 API。

```python
from admission_service import AdmissionService, LocalExecutionSide

svc = AdmissionService(fairness_delay=3)
executor = LocalExecutionSide(svc)

svc.upsert_tenant("train", tier="live", guarantee=1, max_wait=10, at=0)
svc.upsert_tenant("review", tier="batch", guarantee=0, at=0)
svc.upsert_endpoint("gpu-a", skills=["render", "analyze"], cap=1, epoch=1, at=0)

decision = svc.submit_now(
    id="review-1", tenant="review", skill="render",
    deadline=20, preemptible=True, at=1,
)
executor.adopt(decision.permit_id, at=2)

svc.submit_request(
    id="train-1", tenant="train", skill="render",
    deadline=30, preemptible=False, at=3, schedule=False,
)
plan = svc.tick(at=4)          # 可能返回 dispatches / stops / blocked_for_fairness
executor.confirm_stopped(plan.stops[0]["permit"], at=5)
svc.tick(at=6)

print(svc.inspect_tenant("train", at=7))
print(svc.inspect_endpoint("gpu-a"))
print(svc.continuations(at=7))
```

### 接入方法

- `upsert_tenant(name, tier="live"|"batch", guarantee=N, max_wait=秒)`：加载或变更租户。在线类型也接受 `cls="live"` / `"online"`。
- `upsert_endpoint(name, skills=[...], cap=N, epoch=E)`：加载或更新物理实例；降额直接用更小的 `cap` 再注册一次。
- `submit_request(...)` / `submit_now(...)`：提交并立即跑一次准入，返回是否已发放 permit。若要严格复刻 JSONL 的 `request` 后等待 `tick`，用 `enqueue_request(...)` 或 `handle_message(request_row)`。需要指定实例时可用 `admit_to(request_id, endpoint, at=...)`，它仍会做技能兼容和全局位置安全检查。
- `tick(at=...) -> TickResult`：返回本秒新发放的 `dispatches`、要求借用方停止的 `stops`、到期请求和公平性拦截。
- `mark_sent(...)` / `accept(permit)`：登记派发与本地执行端“已采用”回执。
- `request_stop(permit)`：向借用位置发出停止请求；重复调用不会产生第二张停止单。
- `report_result(permit, outcome="done"|"stopped"|"running"|"unknown")`：接收执行端完成、停止确认、运行中心跳或结果未明报告。
- `inspect_tenant(name)` / `inspect_endpoint(name)` / `continuations()`：查询保障未生效原因、实际阻碍和现在可继续发放的请求。
- `handle_message(row)`：直接处理上面的 JSONL 消息；`replay_jsonl(path)` 回放整个演练文件。

本地执行端适配器 `LocalExecutionSide` 给出了 `adopt()`、`stop_requested()`、`complete()`、`confirm_stopped()` 和 `report_unknown()` 的回调写法。

### 状态与安全边界

- `sent` 起即占用一个真实副本并发名额；`accept` 只把同一 permit 标记为执行端已采用，不重复占额。查询同时保留 `admitted_at` 与 `accepted_at`。
- 只有 `done` 和 `stopped` 是明确结束，才核销物理名额。`unknown`/断连不释放名额，且同一未明请求不能换 permit 向同一实例重复发放；其它新实例和未受影响名额仍可服务。
- 降额或保障配置变小不撤销已经发出/接纳的执行事实；新额度只约束今后的调度和未兑现保障。
- 不可暂停调用不会被假设能停止；查询会给出具体 permit、request、endpoint/epoch 和 `nonpreemptible_occupant` 阻碍。可暂停借用必须等到 `stopped` 或 `done` 才腾位。
- 位置选择使用兼容技能的容量匹配，避免把只能由灵活实例服务的在线请求堵死。例如 render 优先走 render-only，保留 flex 给 analyze。
- 已连续等待超过 `fairness_delay` 的在线请求形成同兼容范围屏障：更晚的同范围流量不能抢位；能使用独立/不相交兼容资源的请求仍会放行。
- 到期前未获准的 queued 请求变为 `expired` 且不再调度；已经派发/接纳的请求即使调用方不再等待，也继续保留 permit 并追踪到远端明确结束。

常见保障原因码包括：`remote_unknown`、`waiting_stop_confirmation`、`nonpreemptible_occupant`、`endpoint_at_zero_capacity`、`endpoint_disconnected`、`no_compatible_instance`、`placement_conflict`、`waiting_for_older_request`、`ready_to_admit`。视图中同时提供 `reason_zh`、`blockers`、`usable_endpoints`、`admittable_now` 和 `continuable_requests`。

### 回放与测试

```bash
python3 admission_service.py examples
python3 -m admission examples --quiet-responses
python3 -m unittest discover -v
```

CLI 输出每个 JSONL 的处理响应、错误和所有 `inspect` 视图。配套准入测试覆盖两个随仓演练、借用收回、未明占用、降额、全局兼容位置、老化公平性、到期/调用方离开、配置变更和本地执行端回执。

