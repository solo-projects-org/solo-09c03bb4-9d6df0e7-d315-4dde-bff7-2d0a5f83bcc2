#!/usr/bin/env python3
"""FreshProcessRestartTest 的子进程替身：在独立解释器里打开同一镜像文件。

用法:
  python -I _restart_worker.py <mirror.db> arm       # 造一个 ready 后断档停供现场
  python -I _restart_worker.py <mirror.db> inspect   # 新进程只读检查恢复状态

不依赖 psycopg2/fastapi：psycopg2 由 test_recovery 的内存假实现顶替。
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "app"))

import test_recovery as tr  # noqa: E402


def main():
    db_path, mode = sys.argv[1], sys.argv[2]
    fake = tr.FakePg()
    tr.install_fake_psycopg(fake)

    import replicator as replicator_mod  # noqa: E402
    from store import Store  # noqa: E402

    store = Store(db_path)
    rep = replicator_mod.Replicator({}, "quake_mirror", store)

    if mode == "arm":
        # ready 镜像（EQ1=3.0），原槽在保存点 0/80 之前
        with store.write_txn() as cur:
            cur.execute(
                "INSERT INTO events(event_id, origin_time, latitude, longitude, magnitude)"
                " VALUES(?, ?, ?, ?, ?)",
                ("EQ1", "2026-11-14T08:00:00.000000Z", 30.0, 100.0, 3.0),
            )
            Store.set_meta(cur, "load_state", "done")
            Store.set_meta(cur, "last_applied_lsn", "0/80")
        fake.rows["EQ1"] = 3.0
        fake.slots["quake_mirror"] = {"active": False, "confirmed": 0x80, "restart": 0x70}
        # 同名换槽：新槽出生在 0x500，覆盖不到保存点之后
        fake.slots["quake_mirror"] = {"active": False, "confirmed": 0x500, "restart": 0x500}
        rep._resume_verified()      # 证明失败 => hold + loading
        assert store.get_meta("load_state") == "loading"
        assert store.get_meta("serving_hold")
        return

    if mode == "inspect":
        state = store.get_meta("load_state", "idle")
        held = "held" if store.get_meta("serving_hold", "") else "open"
        n = store.read("SELECT count(*) FROM events")[0][0]
        print(f"{state}|{held}|{n}")
        return

    raise SystemExit(f"unknown mode {mode}")


if __name__ == "__main__":
    main()
