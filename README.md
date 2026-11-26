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
