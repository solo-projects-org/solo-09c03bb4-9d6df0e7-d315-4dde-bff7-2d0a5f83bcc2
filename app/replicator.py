"""逻辑复制采集 worker（单线程状态机）。

状态机：
  idle --(网页点击开始)--> loading --(初始装载完成)--> catchup --(追平)--> done（持续流式续采）

重启恢复（状态持久化在 SQLite meta 表）：
  done              -> 不重新装载，从复制槽已确认位置续采；
  loading / catchup -> 镜像未建完：丢弃半成品重新建立，就绪前网页不供数。

不漏不重的关键：
  1. CREATE_REPLICATION_SLOT 会导出一个与槽位 consistent_point 严格对齐的
     一致性快照；初始装载在该快照里读，后续变更从该槽位流式接收 —— 两段严丝合缝。
  2. 每笔源事务的全部行变更 + 续采位置在同一个 SQLite 事务里提交，
     提交之后才向源库 send_feedback 确认；写入失败或进程崩溃则不确认，
     源库会重发，镜像端幂等重放（upsert/delete），旧值不会盖回新值。
"""
import json
import os
import queue
import threading
import time
import traceback

import psycopg2
from psycopg2.extras import LogicalReplicationConnection

from canonical import norm_ts
from changes import apply_transaction
from lsnutil import int_to_lsn, lsn_to_int
from store import Store, utcnow

WAL2JSON_OPTIONS = {
    "format-version": "1",   # 一条消息 = 一笔源事务
    "add-tables": "public.seismic_events",
    "include-xids": "1",
    "include-timestamp": "1",
}


class RebuildRequested(Exception):
    """装载/续采过程中收到「重新建立」指令。"""


def _bump(cur, key, delta=1):
    cur.execute("SELECT value FROM meta WHERE key=?", (key,))
    row = cur.fetchone()
    value = int(row[0]) if row and row[0] else 0
    Store.set_meta(cur, key, str(value + delta))


class Replicator(threading.Thread):
    def __init__(self, pg, slot, store: Store, load_batch=500, load_sleep_ms=0):
        super().__init__(daemon=True, name="replicator")
        self.pg = dict(pg)                 # psycopg2 连接关键字参数
        self.slot = slot
        self.store = store
        self.load_batch = load_batch
        self.load_sleep_ms = load_sleep_ms
        self.cmds = queue.Queue()
        self.crash_armed = threading.Event()   # 演示：保存后未确认就崩溃

    # ---------------- 外部指令 ----------------
    def request_start(self):
        self.cmds.put("start")

    def request_rebuild(self):
        self.cmds.put("rebuild")

    def arm_crash_after_save(self):
        self.crash_armed.set()

    # ---------------- 主循环 ----------------
    def run(self):
        while True:
            state = self.store.get_meta("load_state", "idle")
            try:
                if state == "idle":
                    cmd = self.cmds.get()        # 等待网页点击「开始建立镜像」
                    if cmd in ("start", "rebuild"):
                        with self.store.write_txn() as cur:
                            Store.set_meta(cur, "load_state", "loading")
                            self.store.log("收到开始指令，准备初始装载", cur)
                elif state in ("loading", "catchup"):
                    # 含「重启后发现未建完 -> 重做」路径
                    self._reset_for_build()
                    self._build_and_stream()
                elif state == "done":
                    self._stream()
                else:
                    with self.store.write_txn() as cur:
                        Store.set_meta(cur, "load_state", "idle")
                        self.store.log(f"未知状态 {state!r}，已重置为 idle", cur)
            except RebuildRequested:
                with self.store.write_txn() as cur:
                    Store.set_meta(cur, "load_state", "loading")
                    self.store.log("收到重建指令：丢弃当前进度，重新建立镜像", cur)
            except Exception as exc:             # 连接中断等：按持久化状态恢复
                self.store.log(f"复制中断：{exc!r}；5 秒后按当前状态恢复")
                traceback.print_exc()
                time.sleep(5)

    # ---------------- 初始装载 ----------------
    def _reset_for_build(self):
        with self.store.write_txn() as cur:
            cur.execute("DELETE FROM events")
            for key, value in (
                ("load_state", "loading"),
                ("loaded_rows", "0"),
                ("applied_txns", "0"),
                ("applied_changes", "0"),
                ("last_applied_lsn", ""),
                ("consistent_point", ""),
                ("target_lsn", ""),
                ("ready_at", ""),
            ):
                Store.set_meta(cur, key, value)
            self.store.log("清空镜像，开始（重新）初始装载；就绪前不供数", cur)

    def _build_and_stream(self):
        rep_conn = psycopg2.connect(connection_factory=LogicalReplicationConnection, **self.pg)
        try:
            rep_cur = rep_conn.cursor()
            self._drop_slot_if_exists()
            # 建槽即导出一致性快照：装载内容与槽位 consistent_point 严格对齐
            rep_cur.execute(f"CREATE_REPLICATION_SLOT {self.slot} LOGICAL wal2json")
            slot_name, consistent_point, snapshot_name, _plugin = rep_cur.fetchone()
            with self.store.write_txn() as cur:
                Store.set_meta(cur, "consistent_point", consistent_point)
                self.store.log(
                    f"已创建复制槽 {slot_name}（consistent_point={consistent_point}），"
                    f"并导出一致性快照 {snapshot_name}",
                    cur,
                )
            t0 = time.monotonic()
            loaded = self._initial_load(snapshot_name)
            target = self._current_wal_flush_lsn()
            with self.store.write_txn() as cur:
                Store.set_meta(cur, "load_state", "catchup")
                Store.set_meta(cur, "target_lsn", int_to_lsn(target))
                self.store.log(
                    f"初始装载完成：{loaded} 行，耗时 {time.monotonic() - t0:.1f}s；"
                    f"开始追平后续变更（目标 {int_to_lsn(target)}）",
                    cur,
                )
            # 从 consistent_point 起流式接收：装载期间的源库修改全部在这里补齐
            self._consume(rep_cur, slot_name, lsn_to_int(consistent_point), target)
        finally:
            try:
                rep_conn.close()
            except Exception:
                pass

    def _initial_load(self, snapshot_name):
        """在导出快照里全量读取源表；源库此时照常读写，互不影响。"""
        conn = psycopg2.connect(**self.pg)
        try:
            conn.set_session(isolation_level="REPEATABLE READ", readonly=True, autocommit=False)
            with conn.cursor() as cur:
                cur.execute(f"SET TRANSACTION SNAPSHOT '{snapshot_name}'")
            scan = conn.cursor(name="initial_load")
            scan.itersize = self.load_batch
            scan.execute(
                "SELECT event_id, origin_time, latitude, longitude, magnitude "
                "FROM seismic_events ORDER BY event_id"
            )
            loaded = 0
            while True:
                batch = scan.fetchmany(self.load_batch)
                if not batch:
                    break
                self._drain_rebuild_cmd()
                rows = [
                    (r[0], norm_ts(r[1]), float(r[2]), float(r[3]), float(r[4]))
                    for r in batch
                ]
                with self.store.write_txn() as cur:
                    cur.executemany(
                        "INSERT INTO events(event_id, origin_time, latitude, longitude, magnitude)"
                        " VALUES(?, ?, ?, ?, ?)",
                        rows,
                    )
                    loaded += len(batch)
                    Store.set_meta(cur, "loaded_rows", str(loaded))
                if self.load_sleep_ms:
                    time.sleep(self.load_sleep_ms / 1000.0)
            conn.commit()
            return loaded
        finally:
            conn.close()

    # ---------------- 流式续采 ----------------
    def _stream(self):
        """done 状态下的（重）入口：不重新装载，直接从槽位确认位置续采。"""
        if not self._slot_exists():
            with self.store.write_txn() as cur:
                Store.set_meta(cur, "load_state", "loading")
                self.store.log("复制槽不存在（源库可能已重置），无法续采，转为重新建立镜像", cur)
            return
        rep_conn = psycopg2.connect(connection_factory=LogicalReplicationConnection, **self.pg)
        try:
            rep_cur = rep_conn.cursor()
            self.store.log(f"镜像已就绪，从复制槽 {self.slot} 的已确认位置续采")
            self._consume(rep_cur, self.slot, 0, None)
        finally:
            try:
                rep_conn.close()
            except Exception:
                pass

    def _consume(self, rep_cur, slot_name, start_lsn, target_lsn):
        rep_cur.start_replication(
            slot_name=slot_name,
            start_lsn=start_lsn,           # 0 表示从槽位已确认位置开始
            options=WAL2JSON_OPTIONS,
            decode=True,
            status_interval=5,
        )
        rep_cur.consume_stream(lambda msg: self._on_message(rep_cur, msg, target_lsn))

    def _on_message(self, rep_cur, msg, target_lsn):
        self._drain_rebuild_cmd()
        if msg.payload:
            payload = json.loads(msg.payload)
            lsn = msg.data_start
            with self.store.write_txn() as cur:
                counts = apply_transaction(cur, payload)
                n_changes = counts["insert"] + counts["update"] + counts["delete"]
                _bump(cur, "applied_txns", 1)
                if n_changes:
                    _bump(cur, "applied_changes", n_changes)
                Store.set_meta(cur, "last_applied_lsn", int_to_lsn(lsn))
                row = cur.execute("SELECT value FROM meta WHERE key='load_state'").fetchone()
                state = row[0] if row else ""
                if n_changes:
                    self.store.log(
                        f"应用源事务 LSN={int_to_lsn(lsn)}：新增 {counts['insert']}、"
                        f"更正 {counts['update']}、删除 {counts['delete']}",
                        cur,
                    )
                if state == "catchup" and target_lsn is not None and lsn >= target_lsn:
                    Store.set_meta(cur, "load_state", "done")
                    Store.set_meta(cur, "ready_at", utcnow())
                    self.store.log(f"已追平至 {int_to_lsn(lsn)}，镜像就绪，开始供数", cur)
            # —— 顺序保证：上面的事务提交（数据+位置已持久）后，才向源库确认 ——
            if self.crash_armed.is_set():
                # 演示「保存后尚未确认就重启」：故意在 send_feedback 之前杀掉进程
                self.crash_armed.clear()
                self.store.log(
                    f"演示：事务 {int_to_lsn(lsn)} 已保存但未确认，模拟进程崩溃；"
                    f"重启后源库将重发该事务"
                )
                print(f"[replicator] crash-after-save at {int_to_lsn(lsn)}", flush=True)
                os._exit(1)
            rep_cur.send_feedback(flush_lsn=lsn)
        else:
            # keepalive：源库空闲时也可能借此完成追平判定
            if target_lsn is not None and self.store.get_meta("load_state") == "catchup":
                last = self.store.get_meta("last_applied_lsn", "")
                if last and lsn_to_int(last) >= target_lsn:
                    with self.store.write_txn() as cur:
                        Store.set_meta(cur, "load_state", "done")
                        Store.set_meta(cur, "ready_at", utcnow())
                        self.store.log("已追平（源库空闲），镜像就绪，开始供数", cur)
            if msg.reply_requested:
                last = self.store.get_meta("last_applied_lsn", "")
                rep_cur.send_feedback(flush_lsn=lsn_to_int(last) if last else 0, reply=True)

    # ---------------- 源库小查询 ----------------
    def _drop_slot_if_exists(self):
        for _ in range(20):
            conn = psycopg2.connect(**self.pg)
            conn.autocommit = True
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT active FROM pg_replication_slots WHERE slot_name=%s",
                        (self.slot,),
                    )
                    row = cur.fetchone()
                    if not row:
                        return
                    if not row[0]:
                        cur.execute("SELECT pg_drop_replication_slot(%s)", (self.slot,))
                        self.store.log(f"已删除遗留复制槽 {self.slot}")
                        return
            finally:
                conn.close()
            time.sleep(0.5)   # 等源库察觉旧 walsender 断开
        raise RuntimeError(f"复制槽 {self.slot} 仍被占用，无法删除")

    def _slot_exists(self):
        conn = psycopg2.connect(**self.pg)
        conn.autocommit = True
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT 1 FROM pg_replication_slots WHERE slot_name=%s", (self.slot,)
                )
                return cur.fetchone() is not None
        finally:
            conn.close()

    def _current_wal_flush_lsn(self):
        conn = psycopg2.connect(**self.pg)
        conn.autocommit = True
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT pg_current_wal_flush_lsn()::text")
                return lsn_to_int(cur.fetchone()[0])
        finally:
            conn.close()

    def _drain_rebuild_cmd(self):
        try:
            cmd = self.cmds.get_nowait()
        except queue.Empty:
            return
        if cmd == "rebuild":
            raise RebuildRequested()
        # 装载/续采期间的 start 无意义，忽略
