"""逻辑复制采集 worker（单线程状态机）。

状态机：
  idle --(网页点击开始)--> loading --(初始装载完成)--> catchup --(追平)--> done（持续流式续采）

重启恢复（状态持久化在 SQLite meta 表）：
  done              -> 先做复制槽连续性证明，通过才从槽位已确认位置续采；
  loading / catchup -> 镜像未建完：丢弃半成品重新建立，就绪前网页不供数。

同名换槽断档防护（核心）
========================
done 状态续采前，用镜像已保存位置 last_applied_lsn 核验同名槽的
confirmed_flush_lsn / restart_lsn（见 slotguard.py）：
  * 两个 LSN 都不越过保存点 => 证明槽仍覆盖保存点以来的 WAL，允许续采；
  * 槽缺失 / LSN 越界（典型：停机期间槽被删后同名重建）=> 无法证明，
    立即置 serving_hold 停止旧目录供数，并把状态转回 loading 自动重建镜像；
    重建失败或进程再次重启，loading+hold 都随卷保留，绝不发布旧值或半成品。

不漏不重的关键：
  1. CREATE_REPLICATION_SLOT 会导出一个与槽位 consistent_point 严格对齐的
     一致性快照；初始装载在该快照里读，后续变更从该槽位流式接收 —— 两段严丝合缝。
  2. 每笔源事务的全部行变更 + 续采位置在同一个 SQLite 事务里提交，
     提交之后才向源库 send_feedback 确认；写入失败或进程崩溃则不确认，
     源库会重发。重发事务由 LSN 守卫（is_already_applied）跳过，绝不回退目录。
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
from slotguard import SlotProofError, ensure_slot_continuity, is_already_applied
from store import Store, utcnow

WAL2JSON_OPTIONS = {
    "format-version": "1",   # 一条消息 = 一笔源事务
    "add-tables": "public.seismic_events",
    "include-xids": "1",
    "include-timestamp": "1",
}


class RebuildRequested(Exception):
    """装载/续采过程中收到「重新建立」指令。"""


class RebuildFailed(Exception):
    """自动重建镜像失败：保持停止供数，等待下一轮重试。"""


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
                    # 含「重启后发现未建完 -> 重做」与「断档后自动重建」路径：
                    # 状态只要不是 done，网页一律 503，绝不发布旧值或半成品
                    self._reset_for_build()
                    self._build_and_stream()
                elif state == "done":
                    self._resume_verified()
                else:
                    with self.store.write_txn() as cur:
                        Store.set_meta(cur, "load_state", "idle")
                        self.store.log(f"未知状态 {state!r}，已重置为 idle", cur)
            except RebuildRequested:
                with self.store.write_txn() as cur:
                    Store.set_meta(cur, "load_state", "loading")
                    Store.set_meta(cur, "serving_hold", "")
                    self.store.log("收到重建指令：丢弃当前进度，重新建立镜像", cur)
            except RebuildFailed as exc:
                # 重建失败：保持 loading（不供数），稍后重试；期间不发布任何旧值
                self.store.log(f"自动重建未完成：{exc!r}；保持停止供数，10 秒后重试")
                time.sleep(10)
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
            # 重建期间持续保持停止供数：旧镜像已清空，半成品绝不发布；
            # 即便进程此刻再次重启，loading 状态 + hold 都会随卷保留，重启仍不供数
            Store.set_meta(cur, "serving_hold", "镜像重建中，就绪前不供数")
            self.store.log("清空镜像，开始（重新）初始装载；就绪前不供数", cur)

    def _build_and_stream(self):
        try:
            self._build_and_stream_inner()
        except RebuildRequested:
            raise
        except Exception as exc:
            # 重建任何环节失败：状态留在 loading、hold 保留 => 不发布旧值/半成品，
            # 交由主循环按 RebuildFailed 退避后整轮重试
            raise RebuildFailed(str(exc)) from exc

    def _build_and_stream_inner(self):
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
                # 快照已包含 consistent_point 之前的全部已提交数据（_reset_for_build
                # 已清空 last_applied_lsn）：把它记为基线已保存位置，使追平判定在
                # 源库空闲（只有保活）时也能成立
                Store.set_meta(cur, "last_applied_lsn", consistent_point)
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

    # ---------------- 流式续采（带连续性证明） ----------------
    def _resume_verified(self):
        """done 状态下的（重）入口。

        续采前必须先证明同名复制槽仍覆盖镜像已保存位置以来的 WAL；
        无法证明（槽被删后同名重建、槽缺失、LSN 越界）时：
          1. 立刻置 serving_hold 停止旧目录供数（查询/CSV 返回 503）；
          2. 自动丢弃旧镜像并重建；重建失败或进程再次重启都保持不供数，
             绝不发布旧值与半成品。
        """
        saved_lsn = self.store.get_meta("last_applied_lsn", "")
        try:
            slot_row = self._fetch_slot_row()
        except psycopg2.Error as exc:
            # 连不上源库 => 无法证明 => 保守停止供数，待下轮重试，绝不含糊续供
            self._hold_serving(f"无法核验复制槽连续性（源库连接异常：{exc!r}），暂停供数并重试")
            raise

        try:
            reason = ensure_slot_continuity(saved_lsn, slot_row)
        except SlotProofError as exc:
            self._hold_serving(str(exc))
            with self.store.write_txn() as cur:
                Store.set_meta(cur, "load_state", "loading")
                self.store.log("连续性证明未通过：自动转为重新建立镜像；就绪前不供数", cur)
            # 回到主循环后 loading 分支执行 _reset_for_build + _build_and_stream
            return
        rep_conn = psycopg2.connect(connection_factory=LogicalReplicationConnection, **self.pg)
        try:
            rep_cur = rep_conn.cursor()
            self.store.log(f"连续性证明通过：{reason}")
            self._last_reverify = 0.0      # 复位周期性复核计时
            # 此前若因源库瞬时不可达而停供，证明通过即解除，恢复正常供数
            if self.store.get_meta("serving_hold", ""):
                with self.store.write_txn() as cur:
                    self._clear_hold(cur, "源库恢复且复制槽连续性证明通过，恢复供数")
            self._consume(rep_cur, self.slot, 0, None)
        finally:
            try:
                rep_conn.close()
            except Exception:
                pass

    def _hold_serving(self, reason):
        """置停止供数闩锁（幂等）；只有重建完成/续采健康后才允许清除。"""
        if self.store.get_meta("serving_hold", "") != reason:
            with self.store.write_txn() as cur:
                Store.set_meta(cur, "serving_hold", reason)
                self.store.log(f"停止供数：{reason}", cur)

    def _clear_hold(self, cur, reason):
        if self.store.get_meta("serving_hold", ""):
            Store.set_meta(cur, "serving_hold", "")
            self.store.log(reason, cur)

    _REVERIFY_INTERVAL = 10.0     # 流式续采期间的槽连续性复核周期（秒）

    def _reverify_or_abort(self, rep_cur):
        """续采中周期性复核槽连续性；断档则停供并抛 SlotProofError 中断本流。"""
        now = time.monotonic()
        if now - getattr(self, "_last_reverify", 0.0) < self._REVERIFY_INTERVAL:
            return
        self._last_reverify = now
        saved_lsn = self.store.get_meta("last_applied_lsn", "")
        try:
            slot_row = self._fetch_slot_row()
            ensure_slot_continuity(saved_lsn, slot_row)
        except SlotProofError as exc:
            self._hold_serving(str(exc))
            with self.store.write_txn() as cur:
                Store.set_meta(cur, "load_state", "loading")
                self.store.log("续采中复核发现槽覆盖断档：中断续采，自动重建镜像", cur)
            try:
                rep_cur.close()
            except Exception:
                pass
            raise
        except psycopg2.Error:
            # 瞬时查询失败不武断停供：连接本身的健康检查会兜底，下个周期再复核
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
        lsn = msg.data_start
        if msg.payload:
            payload = json.loads(msg.payload)
            saved_lsn = self.store.get_meta("last_applied_lsn", "")
            if is_already_applied(lsn, saved_lsn):
                # LSN 守卫：源库重发「已保存但未确认」的事务。镜像内容已包含该
                # 事务，跳过即可 —— 重放旧值不得把目录回退；仅向源库补确认。
                self.store.log(
                    f"跳过重发事务 LSN={int_to_lsn(lsn)}（不超过已保存位置 "
                    f"{saved_lsn}），目录不回退，仅补确认"
                )
                rep_cur.send_feedback(flush_lsn=lsn)
                return
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
                    # 重建（或首次建立）完整成功后才解除停止供数闩锁
                    self._clear_hold(cur, "镜像重建并追平完成，恢复供数")
                    self.store.log(f"已追平至 {int_to_lsn(lsn)}，镜像就绪，开始供数", cur)
            # —— 顺序保证：上面的事务提交（数据+位置已持久）后，才向源库确认。
            #    若写事务失败会在提交前抛异常，本行不会执行 => 不确认源库，源库重发 ——
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
            # keepalive：msg.lsn 是源库当前 WAL 位置。若它已达到追平目标，说明
            # 目标点之前的事务都已交付（源库空闲时靠这条路径完成追平判定）
            if target_lsn is not None and self.store.get_meta("load_state") == "catchup":
                if lsn >= target_lsn:
                    with self.store.write_txn() as cur:
                        row = cur.execute(
                            "SELECT value FROM meta WHERE key='last_applied_lsn'"
                        ).fetchone()
                        last_text = row[0] if row and row[0] else "0/0"
                        Store.set_meta(cur, "load_state", "done")
                        Store.set_meta(cur, "ready_at", utcnow())
                        if lsn_to_int(last_text) < lsn:
                            Store.set_meta(cur, "last_applied_lsn", int_to_lsn(lsn))
                        self._clear_hold(cur, "镜像重建并追平完成（源库空闲），恢复供数")
                        self.store.log("已追平（源库空闲保活），镜像就绪，开始供数", cur)
            # 续采路径的停止供数闩锁只可能在进流前置位；保活到达即证明连接健康，
            # 在同一写事务里解除，随后按源库要求补反馈
            if self.store.get_meta("serving_hold", "") and \
                    self.store.get_meta("load_state", "") == "done":
                with self.store.write_txn() as cur:
                    self._clear_hold(cur, "复制槽连续性已证明（源库空闲保活），恢复供数")
            elif target_lsn is None and self.store.get_meta("load_state", "") == "done":
                # 纵深防御：流式续采期间周期性复核同名槽是否仍连续（槽可能在连接
                # 存活时被删除并同名重建）。证明失败即停供并中断本流，主循环转重建。
                self._reverify_or_abort(rep_cur)
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

    def _fetch_slot_row(self):
        """取同名槽的连续性证明材料；不存在返回 None。

        行结构与 :func:`slotguard.verify_slot_continuity` 的 slot_row 对齐：
        ``(slot_name, active, confirmed_flush_lsn, restart_lsn)``
        """
        conn = psycopg2.connect(**self.pg)
        conn.autocommit = True
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT slot_name, active, confirmed_flush_lsn::text, restart_lsn::text "
                    "FROM pg_replication_slots WHERE slot_name=%s",
                    (self.slot,),
                )
                return cur.fetchone()
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
