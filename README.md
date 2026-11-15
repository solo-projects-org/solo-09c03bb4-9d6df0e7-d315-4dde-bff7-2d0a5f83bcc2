# 震例目录镜像采集（逻辑复制 · wal2json · SQLite）

地震台网数据员把**持续修订中的震例目录**安全地交给重定位分析：网页一键开始建立镜像，
之后持续接收源库的**新增、更正、删除**；初次取数不停写源库；镜像与续采位置持久保存后
才向源库确认复制进度；重启后已建镜像续采、未建完重做且不供数。

## 组成

```
┌────────────┐  逻辑复制(wal2json)  ┌─────────────────────┐        ┌──────────┐
│ PostgreSQL │ ───────────────────▶ │ app: FastAPI        │ ─────▶ │ 浏览器    │
│ seismic_   │  CREATE_REPLICATION_│  · 初始装载(快照)    │  HTTP  │ 状态/查询 │
│ events     │  SLOT 导出一致性快照 │  · 流式续采          │        │ CSV 下载  │
│ (持续被修订)│ ◀── send_feedback ── │  · SQLite 镜像+位置  │        └──────────┘
└─────▲──────┘   确认复制进度        └─────────┬───────────┘
      │                                        │ 持久化(卷 appdata)
      │ 不断新增/更正/删除                        ▼
└─────┴────────┐                        ┌──────────────┐
│  simulator   │                        │ mirror.db    │
│ (源库扰动器)  │                        │ events+meta  │
└──────────────┘                        └──────────────┘
```

- **db**：PostgreSQL 16 + wal2json 插件，`wal_level=logical`；初始化脚本写入
  **30000 条历史震例**（震例号 / UTC 发震时刻 / 纬度 / 经度 / 震级）。
- **app**：FastAPI + psycopg2 + SQLite。后台线程做初始装载与逻辑复制续采，
  网页提供采集状态、震例查询、当前目录 CSV 下载、一致性校验。
- **simulator**：源库扰动器，持续对震例表做新增/更正/删除（含一笔事务改多行的
  “台网重新测定”），模拟目录的持续修订。

## 快速开始

```bash
docker compose up --build
# 打开 http://localhost:8000 ，点击「开始建立镜像」
```

状态页每 3 秒自动刷新，依次经历：`初始装载中 → 追平变更中 → 已就绪（供数中）`。
装载期间 simulator 仍在写源库（约每秒一笔事务），这些变更全部经复制槽在追平阶段补齐。

## 需求实现对照

| 要求 | 实现 |
| --- | --- |
| 初次取数不停写源库 | 初始装载在 `CREATE_REPLICATION_SLOT` 导出的**一致性快照**里做 REPEATABLE READ 只读扫描，源库照常读写 |
| 完整初始镜像接上后续变更，不漏不重 | 快照与槽位 `consistent_point` 严格对齐：快照之前的都在装载里，之后的都从槽位流式到达 |
| 旧值不得盖回新值 | 单槽单消费者，变更严格按 LSN 顺序应用；wal2json 每条消息携带该事务提交时的整行新值 |
| 每笔源事务整笔可见 | wal2json `format-version 1`：一条消息=一笔源事务；其全部行变更在**同一个 SQLite 事务**里提交 |
| 镜像与续采位置持久保存后才确认 | 行变更与 `meta.last_applied_lsn` 同事务提交（WAL + `synchronous=FULL`），**提交后**才 `send_feedback` |
| 写入失败不前移 | 失败/崩溃则不确认；源库重发；镜像端 upsert/delete 幂等重放 |
| 重启后已建镜像续采 | 状态 `done` 持久在 SQLite：重启后直接从槽位已确认位置续采，不重新装载 |
| 未建完则重做且不供数 | 重启时状态为 `loading/catchup`：丢弃半成品重新建立；就绪前查询/CSV 一律 503 |
| 每次取数只读同一已提交状态 | 每个网页请求只开一个 SQLite 只读事务，计数、取页、整个 CSV 流都在同一快照里 |
| 只处理行变更 | 仅应用 insert/update/delete；truncate 等记录并忽略 |

## 演示场景

### 1. 装载中修订（源库不停写）

点击「开始建立镜像」后（默认每批 500 行、批间停 120ms，装载约 7 秒），立刻另开终端制造修订：

```bash
docker compose exec db psql -U postgres -d quakes -c \
  "UPDATE seismic_events SET magnitude=9.9 WHERE event_id='EQH-000001';"
docker compose exec db psql -U postgres -d quakes -c \
  "INSERT INTO seismic_events VALUES('EQDEMO-1', now(), 30.0, 100.0, 5.5);"
docker compose exec db psql -U postgres -d quakes -c \
  "DELETE FROM seismic_events WHERE event_id='EQH-000002';"
```

同时 simulator 每秒都在改库。状态变为「已就绪」后到「查询震例」查
`EQH-000001`（震级 9.9）、`EQDEMO-1`（存在）、`EQH-000002`（不存在）——
装载期间发生的修订一条不漏地接上了。

### 2. 一致性校验（不漏不重）

```bash
docker compose pause simulator        # 暂停源库扰动
# 等状态页「待确认延迟」变为 0，点「一致性校验」
# => 两侧各 N 行，SHA-256 相同
docker compose unpause simulator
```

### 3. 保存后尚未确认就重启

状态「已就绪」时点击「模拟崩溃：保存后未确认」。下一笔源事务**写入镜像并持久化之后、
向源库确认之前**，进程故意退出；Compose 自动重启 app。随后可在状态页看到：

- 事件日志记录“已保存但未确认，模拟进程崩溃”；
- 短时间内「镜像已保存位置」领先「源库已确认位置」（黄色提示条）；
- 源库把该事务**重发**，镜像幂等重放 —— 行数不变、内容不重不漏，两侧位置重新对齐。

### 4. 未建完则重做，且不供数

```bash
# 在「初始装载中」阶段：
docker compose restart app
```

app 重启后发现状态是 `loading`（未建完），自动丢弃半成品重新装载；
此期间访问 `/events` 或 `/events.csv` 返回 **503 镜像未就绪，暂不供数**。

### 5. 已建镜像续采

```bash
# 状态「已就绪」时：
docker compose restart app
```

事件日志显示“镜像已就绪，从复制槽的已确认位置续采”——**不重新装载**，
直接从上次确认位置继续，重启期间的源库变更全部补齐。

## 网页与接口

| 路径 | 说明 |
| --- | --- |
| `GET /` | 采集状态（3s 自刷新）：状态机、行数、已保存/已确认 LSN、复制槽延迟、事件日志、操作按钮 |
| `POST /mirror/start` | 开始建立镜像（idle 时） |
| `POST /mirror/rebuild` | 丢弃当前镜像重新建立 |
| `POST /demo/crash-after-save` | 武装“保存后未确认就崩溃”演示 |
| `GET /events` | 查询震例：震例号前缀、发震时刻区间、最小震级、经纬度范围、分页 |
| `GET /events.csv` | 下载当前目录 CSV（支持同样筛选；整个文件出自同一已提交状态） |
| `GET /verify` | 源库 vs 镜像全量比对（行数 + SHA-256 + 差异样例） |
| `GET /api/status` | 上述状态的 JSON 版 |
| `GET /healthz` | 存活探针 |

## 配置（compose.yaml 环境变量）

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `SLOT_NAME` | `quake_mirror` | 逻辑复制槽名 |
| `LOAD_BATCH` | `500` | 初始装载每批行数 |
| `LOAD_SLEEP_MS` | `120` | 批间停顿（放大演示窗口；生产设 `0`） |
| `TICK_MS` | `900` | simulator 扰动节奏基准 |

## 故障恢复矩阵

| 时刻 | 重启后行为 |
| --- | --- |
| 初始装载中 | 状态 `loading` → 重做装载，就绪前不供数 |
| 追平中 | 状态 `catchup` → 重做装载（快照已失效，必须重来），就绪前不供数 |
| 已就绪 | 状态 `done` → 从槽位已确认位置续采，不重新装载 |
| 保存后未确认就崩溃 | 源库重发未确认事务，镜像幂等重放，不重不漏 |
| 复制槽被删（源库重置） | 自动转为重新建立镜像 |
| 卷被清空（`down -v`） | 一切从零开始，等待点击「开始建立镜像」 |

## 本地测试

核心逻辑（SQLite 落库、wal2json 事务应用、LSN 换算、快照读一致性）有单元测试，
无需 Docker：

```bash
python3 -I tests/test_core.py
```

## 目录结构

```
compose.yaml            # 一键启动：db + app + simulator
db/                     # PostgreSQL 16 + wal2json，含 3 万条历史震例的初始化脚本
app/                    # FastAPI 网页 + 逻辑复制采集线程 + SQLite 镜像
  replicator.py         #   状态机：idle→loading→catchup→done，快照装载与流式续采
  store.py              #   SQLite 层：数据与续采位置同事务持久化
  changes.py            #   wal2json 行变更 -> 幂等 upsert/delete
  main.py               #   网页：状态 / 查询 / CSV / 校验
simulator/              # 源库扰动器：持续新增/更正/删除
tests/test_core.py      # 核心逻辑单元测试
```
