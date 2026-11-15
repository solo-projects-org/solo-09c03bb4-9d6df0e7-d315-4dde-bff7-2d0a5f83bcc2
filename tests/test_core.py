#!/usr/bin/env python3
"""核心逻辑单元测试（仅标准库，本机可直接运行）：

  python3 -I tests/test_core.py

覆盖：LSN 换算、时间规范化、wal2json 事务应用的幂等与原子性、
     读事务快照一致性（每次取数只读同一已提交状态）。
"""
import os
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from decimal import Decimal

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "app"))

from canonical import canonical_line, norm_ts            # noqa: E402
from changes import apply_transaction                    # noqa: E402
from lsnutil import int_to_lsn, lsn_to_int               # noqa: E402
from store import Store                                  # noqa: E402

TS = "2026-11-14 08:00:00.123456+00"


def change(kind, eid, ts=TS, lat=34.567, lon=100.123, mag=4.5):
    if kind == "delete":
        return {
            "kind": "delete", "schema": "public", "table": "seismic_events",
            "oldkeys": {"keynames": ["event_id"], "keytypes": ["text"], "keyvalues": [eid]},
        }
    return {
        "kind": kind, "schema": "public", "table": "seismic_events",
        "columnnames": ["event_id", "origin_time", "latitude", "longitude", "magnitude"],
        "columntypes": ["text", "timestamp with time zone", "numeric", "numeric", "numeric"],
        "columnvalues": [eid, ts, lat, lon, mag],
    }


def change_rename(eid_old, eid_new):
    ch = change("update", eid_new)
    ch["oldkeys"] = {"keynames": ["event_id"], "keytypes": ["text"], "keyvalues": [eid_old]}
    return ch


class LsnTest(unittest.TestCase):
    def test_roundtrip(self):
        for s in ("0/0", "0/1B3C4D0", "1B3/4A0", "FF/FFFFFFFF", "2/9A08C800"):
            self.assertEqual(int_to_lsn(lsn_to_int(s)), s)
        self.assertLess(lsn_to_int("0/10"), lsn_to_int("1/0"))


class NormTsTest(unittest.TestCase):
    def test_load_path_equals_replication_path(self):
        from_load = norm_ts(datetime(2026, 11, 14, 8, 0, 0, 123456, tzinfo=timezone.utc))
        from_wal = norm_ts("2026-11-14 08:00:00.123456+00")
        self.assertEqual(from_load, from_wal)
        self.assertEqual(from_wal, "2026-11-14T08:00:00.123456Z")

    def test_other_offsets_normalized_to_utc(self):
        self.assertEqual(
            norm_ts("2026-11-14 16:00:00+08:00"), "2026-11-14T08:00:00.000000Z"
        )

    def test_canonical_line_decimal_float_equal(self):
        a = canonical_line("EQ1", "T", Decimal("34.500"), Decimal("100.100"), Decimal("4.0"))
        b = canonical_line("EQ1", "T", 34.5, 100.1, 4.0)
        self.assertEqual(a, b)
        self.assertEqual(a, "EQ1|T|34.500|100.100|4.0")


class StoreTestBase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.store = Store(os.path.join(self.tmp.name, "mirror.db"))

    def tearDown(self):
        self.tmp.cleanup()

    def apply(self, payload, lsn):
        with self.store.write_txn() as cur:
            counts = apply_transaction(cur, payload)
            Store.set_meta(cur, "last_applied_lsn", lsn)
            return counts

    def rows(self):
        return self.store.read(
            "SELECT event_id, origin_time, latitude, longitude, magnitude FROM events ORDER BY event_id"
        )


class ApplyTest(StoreTestBase):
    def test_insert_update_delete_flow(self):
        self.apply({"change": [change("insert", "A"), change("insert", "B", mag=6.1)]}, "0/10")
        self.assertEqual([r[0] for r in self.rows()], ["A", "B"])

        self.apply({"change": [change("update", "A", mag=5.5)]}, "0/20")
        row_a = [r for r in self.rows() if r[0] == "A"][0]
        self.assertEqual(row_a[4], 5.5)
        self.assertEqual(row_a[1], "2026-11-14T08:00:00.123456Z")   # 时间已规范化

        self.apply({"change": [change("delete", "B")]}, "0/30")
        self.assertEqual([r[0] for r in self.rows()], ["A"])
        self.assertEqual(self.store.get_meta("last_applied_lsn"), "0/30")

    def test_replay_is_idempotent(self):
        """保存后未确认 -> 源库重发 -> 幂等重放，最终状态不变、不重。"""
        txns = [
            ({"change": [change("insert", "A"), change("insert", "B")]}, "0/10"),
            ({"change": [change("update", "A", mag=7.2)]}, "0/20"),
            ({"change": [change("delete", "B")]}, "0/30"),
        ]
        for payload, lsn in txns:
            self.apply(payload, lsn)
        before = self.rows()
        for payload, lsn in txns:          # 模拟崩溃重启后整段重发
            self.apply(payload, lsn)
        self.assertEqual(self.rows(), before)

    def test_old_value_never_overwrites_new(self):
        """按 LSN 顺序应用：旧事务重放只会被后续新事务再次覆盖为新值。"""
        self.apply({"change": [change("insert", "A", mag=3.0)]}, "0/10")
        self.apply({"change": [change("update", "A", mag=8.8)]}, "0/20")
        # 崩溃重放：旧 insert 又来一遍，随后新 update 也重放，最终仍是新值
        self.apply({"change": [change("insert", "A", mag=3.0)]}, "0/10")
        self.apply({"change": [change("update", "A", mag=8.8)]}, "0/20")
        self.assertEqual(self.rows()[0][4], 8.8)

    def test_primary_key_rename(self):
        self.apply({"change": [change("insert", "OLD")]}, "0/10")
        self.apply({"change": [change_rename("OLD", "NEW")]}, "0/20")
        self.assertEqual([r[0] for r in self.rows()], ["NEW"])

    def test_truncate_ignored(self):
        self.apply({"change": [change("insert", "A")]}, "0/10")
        counts = self.apply({"change": [{"kind": "truncate", "schema": "public", "table": "seismic_events"}]}, "0/20")
        self.assertEqual(counts["other"], 1)
        self.assertEqual(len(self.rows()), 1)

    def test_transaction_atomicity_on_failure(self):
        """一笔源事务应用一半失败 -> 整体回滚，数据与位置都不前移。"""
        with self.assertRaises(RuntimeError):
            with self.store.write_txn() as cur:
                apply_transaction(cur, {"change": [change("insert", "A"), change("insert", "B")]})
                Store.set_meta(cur, "last_applied_lsn", "0/10")
                raise RuntimeError("模拟写入失败")
        self.assertEqual(self.rows(), [])
        self.assertIsNone(self.store.get_meta("last_applied_lsn"))

    def test_whole_transaction_visible_together(self):
        """一笔多行事务提交后整笔可见；读事务只看到提交前或提交后。"""
        with self.store.write_txn() as cur:
            apply_transaction(cur, {"change": [change("insert", f"E{i:02d}") for i in range(20)]})
            Store.set_meta(cur, "last_applied_lsn", "0/10")
        self.assertEqual(len(self.rows()), 20)


class SnapshotReadTest(StoreTestBase):
    def test_read_txn_sees_one_committed_state(self):
        self.apply({"change": [change("insert", "A")]}, "0/10")
        txn = self.store.read_txn()
        try:
            n1 = txn.execute("SELECT count(*) FROM events").fetchone()[0]
            # 读事务打开期间，写者提交了新事务
            self.apply({"change": [change("insert", "B")]}, "0/20")
            n2 = txn.execute("SELECT count(*) FROM events").fetchone()[0]
        finally:
            txn.close()
        self.assertEqual((n1, n2), (1, 1))          # 同一已提交状态
        self.assertEqual(self.store.read("SELECT count(*) FROM events")[0][0], 2)  # 新读见新态


if __name__ == "__main__":
    unittest.main(verbosity=2)
