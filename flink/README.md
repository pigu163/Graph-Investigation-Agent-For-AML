# Flink 组件说明

本目录包含本项目的 Flink 运行环境、计算 SQL 和提交脚本。当前 Compose 配置把 Flink 拆成三个组件：

```text
你在终端运行 docker compose run flink-submit
                         │
                         │ 提交 SQL
                         ▼
                 flink-jobmanager
                 协调任务 + Web UI
                         │
                         │ 分配计算任务
                         ▼
                 flink-taskmanager
                 实际执行计算
                    │          │
                    │          └── 写入 PostgreSQL
                    └───────────── 读取 Kafka
```

## 三个组件分别做什么

### `flink-jobmanager`

JobManager 是 Flink 集群的协调者。它负责接收 SQL 作业、生成执行计划、将任务分配给 TaskManager、监控作业状态、管理 checkpoint，并提供 Web UI 和 REST API。

它的 Web UI 映射到宿主机的 `http://localhost:8081`。当前 Compose 中只有一个 JobManager。它主要负责安排和管理任务；真正处理 Kafka 记录、执行窗口聚合和调用 JDBC Sink 的算子运行在 TaskManager 中。

### `flink-taskmanager`

TaskManager 是实际的计算工作进程。它负责执行 JobManager 分配的算子，例如：

- 从 Kafka 读取 `aml-transactions`。
- 解析 JSON 和事件时间。
- 在一小时窗口中去重交易。
- 将一笔交易转换成付款账户和收款账户的两个观察记录。
- 计算收付款笔数、金额、净流入、Fan-in 和 Fan-out。
- 通过 JDBC 将结果写入 PostgreSQL。

当前配置为一个 TaskManager、两个 task slots、默认并行度一。小规模本地演示足够；以后增加 TaskManager 或 slot，才能并行处理更多计算任务。

### `flink-submit`

`flink-submit` 是一个临时的 SQL Client 容器，不是长期运行的计算节点。它负责读取 SQL、替换运行模式占位符、连接 JobManager、提交作业，然后退出。`--rm` 会删除提交客户端容器，但已经提交到集群的作业不会因此消失。

Compose 给它设置了 `profiles: [etl]`，所以它不会随着普通的 `docker compose up` 自动启动。

## 一次运行发生了什么

```text
Kafka topic aml-transactions
            │
            ▼
  Flink Kafka Source（TaskManager）
            │
            ▼
      一小时事件时间窗口
            │
            ▼
    账户/币种维度的聚合
            │
            ▼
   Flink JDBC Sink（TaskManager）
            │
            ▼
 PostgreSQL account_window_features
```

运行 `docker compose run --rm flink-submit` 时，Submit 容器读取 `submit.sh`，默认使用持续的 `unbounded`/`streaming` 模式，将 `account_features.sql` 提交给 JobManager。JobManager 再把执行计划安排到 TaskManager，之后 Submit 容器可以退出，而计算任务继续运行。

## 两种提交模式

### 持续运行模式

```bash
docker compose run --rm flink-submit
```

作业会持续等待新的 Kafka 交易，适合实时监控和持续回放。在 UI 的 **Running Jobs** 中可以看到 `aml-account-features-v1`。

### 有界测试模式

```bash
docker compose run --rm flink-submit latest-offset
```

作业读取启动时 Kafka 中已有的消息，使用 batch runtime，处理完成后进入 `FINISHED`。它适合验证历史样本，因为有限输入可以关闭最后一个窗口。

`latest-offset` 是 `submit.sh` 的参数，不是 Kafka topic 名称。脚本会把它写入 SQL 的 `scan.bounded.mode`，并把运行模式改成 `batch`。

## 代码文件之间的关系

### `Dockerfile`

基于官方 Flink 镜像创建 `aml-flink:local`，加入 Kafka SQL Connector、Flink JDBC Connector 和 PostgreSQL JDBC Driver，使 Flink 能读 Kafka、写 PostgreSQL。它还把本目录复制到镜像里的 `/opt/flink/aml`。

### `account_features.sql`

这是 Flink 的计算定义，不是 PostgreSQL 查询脚本。它定义 Kafka 输入表、事件时间和 watermark、一小时窗口去重、账户收付款观察、窗口聚合，以及 PostgreSQL JDBC 输出。

### `schema.sql`

这是 PostgreSQL 的建表脚本，创建 `account_window_features`，定义数据库实际保存的列、主键和数值类型。执行方式：

```bash
docker compose exec -T postgres psql \
  -U aml_user -d aml_db < flink/schema.sql
```

### `submit.sh`

这是提交入口。它只负责选择运行模式、替换 SQL 占位符和调用 Flink SQL Client，不负责实现 AML 计算逻辑。

## Kafka 和 PostgreSQL 地址

Flink 容器位于 Docker Compose 网络中，所以使用服务名访问其他容器：

```text
Kafka：      kafka:29092
PostgreSQL： postgres:5432
```

浏览器和宿主机程序访问映射到电脑的端口：

```text
Kafka：      localhost:9092
PostgreSQL： localhost:5432
Flink UI：   localhost:8081
```

SQL 里必须写 `kafka:29092` 和 `postgres:5432`。在 Flink 容器里，`localhost` 指的是 Flink 自己。

## 如何查看运行状态

打开 [Flink Web UI](http://localhost:8081)，可以看到 Running Jobs、Completed Jobs、Task Managers，以及各算子的记录数、watermark 和 checkpoint。

命令行也可以查看：

```bash
curl http://localhost:8081/overview
curl http://localhost:8081/jobs/overview
docker compose logs -f flink-jobmanager
docker compose logs -f flink-taskmanager
```

## 常见操作

启动 Flink 集群和 UI：

```bash
docker compose up -d --build --wait flink-jobmanager flink-taskmanager
```

提交持续运行的作业：

```bash
docker compose run --rm flink-submit
```

修改 `account_features.sql` 或 `Dockerfile` 后重新构建并提交：

```bash
docker compose build flink-jobmanager
docker compose run --rm flink-submit
```

Flink 的 checkpoint 和状态使用项目内的 `volume/flink/` 目录。开发重置时，应同时停止 Flink 作业、删除消费进度、清空 PostgreSQL 特征表和清除 Flink 状态；项目提供的 `scripts/reset_streaming.py --yes` 会执行这一整套操作。

## 最重要的理解

`flink-submit` 是“提交者”，`flink-jobmanager` 是“协调者和 UI”，`flink-taskmanager` 是“执行者”。提交 SQL 的动作很短，但 SQL 代表的计算作业可以在 TaskManager 中持续运行。Kafka 保存输入消息，Flink 负责计算，PostgreSQL 保存计算结果。
