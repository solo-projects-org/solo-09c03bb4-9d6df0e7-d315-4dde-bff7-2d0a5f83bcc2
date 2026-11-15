#!/usr/bin/env python3
"""同名换槽断档与正常续采的回归测试（仅标准库，本机可直接运行）：

  python3 -I tests/test_recovery.py

本环境没有 PostgreSQL，这里用一个最小的内存假实现扮演
psycopg2 / LogicalReplicationConnection / walsender：
  - 普通连接负责 pg_replication_slots 查询、建/删槽、初始装载快照扫描；
  - 逻辑复制连接负责 start_replication / consume_stream / send_feedback。
被测对象是真实的 Replicator + Store + slotguard，仅替换 psycopg2 模块，
因此覆盖的是完整的状态机/证明/停供/重建/确认路径，而非单个函数。
"""
import json
import os
import sys
import tempfile
import types
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "app"))

# ---- 在导入 replicator 之前注入假 psycopg2（replicator 顶层 import psycopg2） ----


class FakePgError(Exception):
    pass


class _BaseCursor:
    def __init__(self, fake):
        self.fake = fake
        self._rows = []
        self._idx = 0

    def execute(self, sql, params=None):
        self._rows = self.fake.execute(sql, params or ())
        self._idx = 0

    def executemany(self, sql, seq):
        for params in seq:
            self.fake.execute(sql, params)

    def fetchone(self):
        if self._idx < len(self._rows):
            row = self._rows[self._idx]
            self._idx += 1
            return row
        return None

    def fetchall(self):
        rows, self._rows = self._rows, []
        return rows

    def fetchmany(self, size):
        chunk = self._rows[self._idx:self._idx + size]
        self._idx += len(chunk)
        return chunk

    def close(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *a):
        self.close()


class _NormalCursor(_BaseCursor):
    pass


class _RepCursor(_BaseCursor):
    def __init__(self, fake):
        super().__init__(fake)
        self.repl_started = None

    def start_replication(self, slot_name, start_lsn, options=None, decode=True,
                          status_interval=0):
        self.repl_started = (slot_name, start_lsn)

    def consume_stream(self, cb):
        for msg in list(self.fake.stream):
            if getattr(self, "_closed", False):
                raise FakePgError("replication stream terminated")
            cb(msg)

    def close(self):
        self._closed = True

    def send_feedback(self, flush_lsn=0, reply=False):
        self.fake.feedback.append((flush_lsn, reply))


class _Conn:
    def __init__(self, fake, replication=False):
        self.fake = fake
        self.autocommit = False
        self._replication = replication

    def cursor(self, name=None):
        if name:                       # 命名游标：初始装载扫描
            return _NormalCursor(self.fake)
        return _RepCursor(self.fake) if self._replication else _NormalCursor(self.fake)

    def set_session(self, **kw):
        pass

    def commit(self):
        pass

    def rollback(self):
        pass

    def close(self):
        pass


class FakeMsg:
    def __init__(self, lsn, payload=None, reply_requested=False):
        self.data_start = lsn
        self.payload = None if payload is None else json.dumps(payload)
        self.reply_requested = reply_requested


def _change(kind, eid, mag=None):
    if kind == "delete":
        return {"kind": "delete", "schema": "public", "table": "seismic_events",
                "oldkeys": {"keynames": ["event_id"], "keytypes": ["text"],
                            "keyvalues": [eid]}}
    return {"kind": kind, "schema": "public", "table": "seismic_events",
            "columnnames": ["event_id", "origin_time", "latitude", "longitude", "magnitude"],
            "columntypes": ["text", "timestamptz", "numeric", "numeric", "numeric"],
            "columnvalues": [eid, "2026-11-14 08:00:00+00", 30.0, 100.0,
                             3.0 if mag is None else mag]}


def _txn(*changes):
    return {"change": list(changes)}


class FakePg:
    """内存假源库：目录表 + 复制槽目录 + 逻辑复制消息队列。"""

    def __init__(self):
        self.rows = {}                       # event_id -> magnitude（其余列固定）
        self.slots = {}                      # name -> dict(active, confirmed, restart)
        self.stream = []                     # 待投递 FakeMsg
        self.feedback = []
        self.next_lsn = 0x100
        self.wal_lsn = 0x100                 # pg_current_wal_flush_lsn 的模拟值
        self.fail_load = False
        self.raise_on_connect = None         # 异常类型，模拟源库不可达

    # 由假 psycopg2.connect 调用
    def connect(self, replication=False, **kw):
        if self.raise_on_connect is not None:
            raise self.raise_on_connect("simulated source outage")
        return _Conn(self, replication=replication)

    def _lsn_text(self, i):
        return f"{i >> 32:X}/{i & 0xFFFFFFFF:X}"

    def emit(self, payload):
        lsn = self.next_lsn
        self.next_lsn += 0x10
        self.wal_lsn = self.next_lsn
        self.stream.append(FakeMsg(lsn, payload))
        return lsn

    def emit_keepalive(self, reply_requested=False):
        # 保活携带当前 WAL 位置（与真实 keepalive 语义一致），随后位置前移
        self.stream.append(FakeMsg(self.wal_lsn, None, reply_requested))
        self.next_lsn += 0x10
        self.wal_lsn = self.next_lsn

    def execute(self, sql, params):
        s = sql.strip()
        if s.startswith("SELECT slot_name, active, confirmed_flush_lsn::text, restart_lsn::text"):
            slot = self.slots.get(params[0])
            return [(params[0], slot["active"], self._lsn_text(slot["confirmed"]),
                     self._lsn_text(slot["restart"]))] if slot else []
        if s.startswith("SELECT active FROM pg_replication_slots"):
            slot = self.slots.get(params[0])
            return [(slot["active"],)] if slot else []
        if s.startswith("SELECT 1 FROM pg_replication_slots"):
            return [(1,)] if params[0] in self.slots else []
        if s.startswith("SELECT pg_drop_replication_slot"):
            self.slots.pop(params[0], None)
            return []
        if s.startswith("CREATE_REPLICATION_SLOT"):
            # 语句形如 CREATE_REPLICATION_SLOT <name> LOGICAL wal2json
            name = s.split()[1]
            point = self.wal_lsn
            self.slots[name] = {"active": False, "confirmed": point, "restart": point}
            # 新槽只投递建槽之后的消息：旧队列作废；随后 WAL 前移，
            # 用一条携带新位置的保活让重建的追平判定（last >= target）成立
            self.stream.clear()
            self.next_lsn += 0x10
            self.wal_lsn = self.next_lsn
            self.stream.append(FakeMsg(self.wal_lsn, None, False))
            return [(name, self._lsn_text(point), f"snap-{point:x}", "wal2json")]
        if s.startswith("SELECT pg_current_wal_flush_lsn"):
            return [(self._lsn_text(self.wal_lsn),)]
        if s.startswith("SELECT pg_terminate_backend"):
            return []
        if s.startswith("SELECT event_id, origin_time, latitude, longitude, magnitude"):
            # 初始装载快照扫描
            if self.fail_load:
                raise FakePgError("initial load boom")
            return [(eid, "2026-11-14 08:00:00+00", 30.0, 100.0, mag)
                    for eid, mag in sorted(self.rows.items())]
        if s.startswith("SELECT count(*)"):
            return [(len(self.rows),)]
        if s.startswith("SET TRANSACTION SNAPSHOT"):
            return []
        if s.startswith("UPDATE seismic_events SET magnitude="):
            # 演示端点：UPDATE seismic_events SET magnitude=8.8 WHERE event_id='EQH-000001'
            mag = float(s.split("magnitude=", 1)[1].split()[0])
            eid = s.split("event_id=", 1)[1].strip().strip("'").rstrip(";").strip("'")
            if eid in self.rows:
                self.rows[eid] = mag
            return []
        raise AssertionError(f"unexpected SQL: {sql}")


def install_fake_psycopg(fake):
    pkg = types.ModuleType("psycopg2")
    extras = types.ModuleType("psycopg2.extras")

    def connect(*a, **kw):
        replication = kw.pop("connection_factory", None) is LogicalReplicationConnection
        return fake.connect(replication=replication, **kw)

    class LogicalReplicationConnection:  # 仅用作标记
        pass

    pkg.connect = connect
    pkg.Error = FakePgError
    extras.LogicalReplicationConnection = LogicalReplicationConnection
    pkg.extras = extras
    sys.modules["psycopg2"] = pkg
    sys.modules["psycopg2.extras"] = extras
    return pkg


# ---- 测试夹具 ----

class ReplicatorTestBase(unittest.TestCase):
    def setUp(self):
        self.fake = FakePg()
        install_fake_psycopg(self.fake)
        # 延迟到假模块就位后再导入
        import importlib
        import replicator as replicator_mod
        import store as store_mod
        self.replicator_mod = importlib.reload(replicator_mod)
        self.store_mod = importlib.reload(store_mod)
        self.tmp = tempfile.TemporaryDirectory()
        self.store = self.store_mod.Store(os.path.join(self.tmp.name, "mirror.db"))
        self.rep = self.replicator_mod.Replicator(
            {}, "quake_mirror", self.store, load_batch=100, load_sleep_ms=0
        )

    def tearDown(self):
        self.tmp.cleanup()

    # ---- 构造一个“已就绪、正在供数”的镜像 ----
    def given_ready_mirror(self, events=("EQ1", "EQ2"), mags=None):
        mags = mags or {}
        for eid in events:
            self.fake.rows[eid] = mags.get(eid, 3.0)
        with self.store.write_txn() as cur:
            cur.execute(
                "INSERT INTO events(event_id, origin_time, latitude, longitude, magnitude)"
                " VALUES(?, ?, ?, ?, ?)",
                ("__seed__", "2026-11-14T08:00:00.000000Z", 30.0, 100.0, 3.0),
            )
            cur.execute("DELETE FROM events")
            for eid in events:
                cur.execute(
                    "INSERT INTO events(event_id, origin_time, latitude, longitude, magnitude)"
                    " VALUES(?, ?, ?, ?, ?)",
                    (eid, "2026-11-14T08:00:00.000000Z", 30.0, 100.0, mags.get(eid, 3.0)),
                )
            self.store_mod.Store.set_meta(cur, "load_state", "done")
            self.store_mod.Store.set_meta(cur, "last_applied_lsn", "0/80")
            self.store_mod.Store.set_meta(cur, "serving_hold", "")
        # 源库上的“原配”复制槽：两个 LSN 都在镜像保存点 0/80 之前
        self.fake.slots["quake_mirror"] = {"active": False,
                                           "confirmed": 0x80, "restart": 0x70}

    def mirror_mag(self, eid):
        rows = self.store.read("SELECT magnitude FROM events WHERE event_id=?", (eid,))
        return rows[0][0] if rows else None

    def is_serving(self):
        return self.store.get_meta("load_state") == "done" and \
            not self.store.get_meta("serving_hold", "")


class SameNameSlotSwapTest(ReplicatorTestBase):
    """回归 1：停机期间同名换槽 => 必须停供并自动重建，绝不继续发布旧值。"""

    def swap_slot(self):
        # 源库把 EQ1 改成 8.8（断档变更，新槽不会投递），再同名重建槽
        self.fake.rows["EQ1"] = 8.8
        new_point = 0x500
        self.fake.slots["quake_mirror"] = {"active": False,
                                           "confirmed": new_point, "restart": new_point}

    def test_proof_fails_and_serving_stops_before_rebuild(self):
        self.given_ready_mirror()
        self.swap_slot()
        # 续采核验：证明失败，应置 hold、转 loading，并直接返回（不连逻辑复制）
        self.rep._resume_verified()
        self.assertFalse(self.is_serving(), "断档后必须立即停止旧目录供数")
        self.assertEqual(self.store.get_meta("load_state"), "loading")
        self.assertTrue(self.store.get_meta("serving_hold"))
        # 旧值仍然只是被“封存”，但已不可供数；查询通路看到的是 503 条件
        self.assertEqual(self.mirror_mag("EQ1"), 3.0)

    def test_full_auto_rebuild_recovers_new_value(self):
        self.given_ready_mirror()
        self.swap_slot()
        self.rep._resume_verified()                  # 证明失败 -> loading
        # 主循环 loading 分支：清空 + 重建（假源库当前 EQ1=8.8）
        self.rep._reset_for_build()
        self.rep._build_and_stream()
        self.assertEqual(self.store.get_meta("load_state"), "done")
        self.assertTrue(self.is_serving(), "重建追平后必须恢复供数")
        self.assertEqual(self.mirror_mag("EQ1"), 8.8, "新镜像必须包含断档期间的 8.8")
        self.assertEqual(self.mirror_mag("EQ2"), 3.0)

    def test_rebuild_failure_keeps_serving_closed(self):
        self.given_ready_mirror()
        self.swap_slot()
        self.rep._resume_verified()
        self.fake.fail_load = True
        with self.assertRaises(self.replicator_mod.RebuildFailed):
            self.rep._reset_for_build()
            self.rep._build_and_stream()
        # 失败后：状态停留 loading、hold 保留、旧值不发布、半成品不发布
        self.assertEqual(self.store.get_meta("load_state"), "loading")
        self.assertTrue(self.store.get_meta("serving_hold"))
        self.assertFalse(self.is_serving())

    def test_restart_during_rebuild_still_does_not_serve(self):
        self.given_ready_mirror()
        self.swap_slot()
        self.rep._resume_verified()
        self.rep._reset_for_build()                # 已清空旧镜像、置 loading+hold
        # 模拟“再次重启”：用同一磁盘文件新建 Store/Replicator（持久化恢复）
        del self.rep
        store2 = self.store_mod.Store(os.path.join(self.tmp.name, "mirror.db"))
        rep2 = self.replicator_mod.Replicator({}, "quake_mirror", store2)
        self.assertEqual(store2.get_meta("load_state"), "loading")
        self.assertTrue(store2.get_meta("serving_hold"))
        self.assertEqual(store2.read("SELECT count(*) FROM events")[0][0], 0)
        # 重启后完成重建
        rep2._build_and_stream()
        self.assertEqual(store2.get_meta("load_state"), "done")
        rows = store2.read("SELECT event_id, magnitude FROM events ORDER BY event_id")
        self.assertEqual(rows, [("EQ1", 8.8), ("EQ2", 3.0)])

    def test_missing_slot_treated_as_unprovable(self):
        self.given_ready_mirror()
        self.fake.slots.clear()                    # 槽直接没了
        self.rep._resume_verified()
        self.assertFalse(self.is_serving())
        self.assertEqual(self.store.get_meta("load_state"), "loading")

    def test_source_unreachable_holds_serving_and_retries(self):
        self.given_ready_mirror()
        self.fake.raise_on_connect = FakePgError
        with self.assertRaises(FakePgError):
            self.rep._resume_verified()
        self.assertFalse(self.is_serving(), "源库不可达、无法证明时不得继续供数")
        self.assertEqual(self.store.get_meta("load_state"), "done")  # 状态未变，仅闩锁停供
        # 源库恢复、槽仍连续 => 证明通过，闩锁解除，恢复供数且不重建
        self.fake.raise_on_connect = None
        self.fake.emit_keepalive(reply_requested=True)
        self.rep._resume_verified()
        self.assertTrue(self.is_serving(), "源库恢复后应自动解除停供")
        self.assertEqual(self.store.get_meta("load_state"), "done")
        self.assertEqual(self.store.get_meta("loaded_rows"), None)  # 未触发重建


class NormalResumeTest(ReplicatorTestBase):
    """回归 2：正常重启（槽未动）直接续采，不重新装载。"""

    def test_proof_passes_and_streams_from_slot(self):
        self.given_ready_mirror(mags={"EQ1": 3.0})
        # 停机期间源库新增一笔 EQ3（LSN > 0x80），原槽仍覆盖该 WAL
        lsn = self.fake.emit(_txn(_change("insert", "EQ3", mag=5.5)))
        self.rep._resume_verified()
        self.assertTrue(self.is_serving())
        self.assertEqual(self.store.get_meta("load_state"), "done")
        self.assertEqual(self.mirror_mag("EQ3"), 5.5)
        # 续采位置推进，且已向源库确认
        self.assertEqual(self.store.get_meta("last_applied_lsn"),
                         f"{lsn >> 32:X}/{lsn & 0xFFFFFFFF:X}")
        self.assertTrue(self.fake.feedback)
        # 没有重建：装载行数统计保持 0（重建才会重置/累加）
        self.assertEqual(self.store.get_meta("loaded_rows"), None)

    def test_redelivered_transaction_does_not_roll_back_catalog(self):
        # 镜像已保存到 0/90：EQ1 已是 8.8；源库重发 0/90 之前的旧事务（EQ1=3.0）
        self.given_ready_mirror(mags={"EQ1": 8.8})
        with self.store.write_txn() as cur:
            self.store_mod.Store.set_meta(cur, "last_applied_lsn", "0/90")
        old = FakeMsg(0x90, _txn(_change("update", "EQ1", mag=3.0)))
        new_lsn = self.fake.emit(_txn(_change("update", "EQ1", mag=9.1)))  # 之后的新事务
        self.fake.stream = [old] + self.fake.stream
        self.rep._resume_verified()
        self.assertEqual(self.mirror_mag("EQ1"), 9.1, "重发的旧值不得回退目录")
        # 旧事务也被补确认（反馈里出现过 0x90）
        self.assertIn(0x90, [f[0] for f in self.fake.feedback])

    def test_keepalive_clears_nothing_without_hold(self):
        self.given_ready_mirror()
        self.fake.emit_keepalive(reply_requested=True)
        self.rep._resume_verified()
        self.assertTrue(self.is_serving())
        self.assertEqual(self.mirror_mag("EQ1"), 3.0)

    def test_midstream_swap_detected_on_keepalive_reverify(self):
        """连接存活期间槽被同名重建：保活复核应捕获断档并中断转重建。"""
        self.given_ready_mirror()
        self.rep._REVERIFY_INTERVAL = -1.0    # 每个保活都强制复核
        # 第一条保活：槽仍连续，正常
        self.fake.emit_keepalive()
        # 第二条保活前，槽被同名重建到保存点之后
        self.fake.slots["quake_mirror"] = {"active": False,
                                           "confirmed": 0x900, "restart": 0x900}
        self.fake.emit_keepalive()
        # 流被中断（异常类型取决于驱动），但持久化的安全状态必须落盘
        try:
            self.rep._resume_verified()
        except Exception:
            pass
        self.assertFalse(self.is_serving())
        self.assertEqual(self.store.get_meta("load_state"), "loading")
        self.assertTrue(self.store.get_meta("serving_hold"))

    def test_write_failure_does_not_acknowledge_source(self):
        """落库失败 => 不 send_feedback；数据与位置都不前移，源库可重发。"""
        self.given_ready_mirror()
        self.fake.emit(_txn(_change("insert", "EQ9", mag=7.7)))
        saved_before = self.store.get_meta("last_applied_lsn")

        # 让 SQLite 写事务在应用变更时失败：临时破坏 events 表
        with self.store.write_txn() as cur:
            cur.execute("DROP TABLE events")
        with self.assertRaises(Exception):
            self.rep._resume_verified()
        # 没有任何反馈发往源库，已保存位置保持不变
        self.assertEqual(self.fake.feedback, [])
        self.assertEqual(self.store.get_meta("last_applied_lsn"), saved_before)


class FreshProcessRestartTest(unittest.TestCase):
    """用独立子进程验证持久化：换个 Python 进程打开同一个镜像文件，
    状态机仍按磁盘上的 load_state / serving_hold 恢复（模拟容器重启）。"""

    def test_hold_survives_fresh_process(self):
        import subprocess
        tmp = tempfile.TemporaryDirectory()
        db_path = os.path.join(tmp.name, "mirror.db")
        worker = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                              "_restart_worker.py")
        # 第一次：构造 ready 镜像并制造“断档待重建”的停供现场
        r1 = subprocess.run(
            [sys.executable, "-I", worker, db_path, "arm"],
            capture_output=True, text=True, cwd=os.path.dirname(worker),
        )
        self.assertEqual(r1.returncode, 0, r1.stderr)
        # 全新进程：应看到 loading + hold（旧行仍在但被闩锁封存、绝不供数）
        r2 = subprocess.run(
            [sys.executable, "-I", worker, db_path, "inspect"],
            capture_output=True, text=True, cwd=os.path.dirname(worker),
        )
        self.assertEqual(r2.returncode, 0, r2.stderr)
        self.assertEqual(r2.stdout.strip(), "loading|held|1")
        tmp.cleanup()

class SlotGuardPureTest(unittest.TestCase):
    """slotguard 纯逻辑判定的边界用例。"""

    def setUp(self):
        from slotguard import verify_slot_continuity, is_already_applied
        self.verify = verify_slot_continuity
        self.guard = is_already_applied

    def row(self, confirmed, restart):
        return ("quake_mirror", False, confirmed, restart)

    def test_empty_saved_lsn_always_ok(self):
        ok, _ = self.verify("", self.row("0/9999", "0/9999"))
        self.assertTrue(ok)
        ok, _ = self.verify(None, None)
        self.assertTrue(ok)

    def test_continuous_slot_passes(self):
        ok, _ = self.verify("0/100", self.row("0/100", "0/F0"))
        self.assertTrue(ok)

    def test_recreated_slot_fails(self):
        ok, reason = self.verify("0/100", self.row("0/500", "0/500"))
        self.assertFalse(ok)
        self.assertIn("断档", reason)

    def test_only_restart_ahead_fails(self):
        ok, _ = self.verify("0/100", self.row("0/100", "0/200"))
        self.assertFalse(ok)

    def test_missing_or_incomplete_slot_fails(self):
        ok, _ = self.verify("0/100", None)
        self.assertFalse(ok)
        ok, _ = self.verify("0/100", ("s", False, None, "0/10"))
        self.assertFalse(ok)

    def test_lsn_guard(self):
        self.assertTrue(self.guard("0/100", "0/100"))
        self.assertTrue(self.guard("0/90", "0/100"))
        self.assertFalse(self.guard("0/101", "0/100"))
        self.assertFalse(self.guard("0/100", ""))


if __name__ == "__main__":
    unittest.main(verbosity=2)
