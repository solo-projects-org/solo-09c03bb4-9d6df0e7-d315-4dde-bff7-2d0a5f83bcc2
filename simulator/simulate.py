#!/usr/bin/env python3
"""源库扰动器：持续对震例表做新增 / 更正 / 删除，模拟目录的持续修订。

- 每个动作都是一笔独立事务（更正动作经常一笔事务改多行，
  用来演示镜像端“每笔源事务整笔可见”）；
- 周期性做一次“台网重新测定”：一笔事务批量修订十余条震级。
"""
import os
import random
import time
from datetime import datetime, timedelta, timezone

import psycopg2

PG = dict(
    host=os.environ.get("PGHOST", "db"),
    port=int(os.environ.get("PGPORT", "5432")),
    dbname=os.environ.get("PGDATABASE", "quakes"),
    user=os.environ.get("PGUSER", "postgres"),
    password=os.environ.get("PGPASSWORD", "postgres"),
)
TICK = float(os.environ.get("TICK_MS", "900")) / 1000.0

_seq = 0


def connect():
    while True:
        try:
            conn = psycopg2.connect(**PG)
            conn.autocommit = False
            return conn
        except Exception as exc:
            print(f"[simulator] 等待源库: {exc}", flush=True)
            time.sleep(2)


def rand_lat():
    return round(random.uniform(18, 54), 3)


def rand_lon():
    return round(random.uniform(73, 135), 3)


def rand_mag():
    return round(random.uniform(2.5, 9.0), 1)


def one_tick(cur):
    """执行一笔事务，返回描述文本。"""
    global _seq
    r = random.random()
    if r < 0.40:
        n = random.randint(1, 3)
        rows = []
        for _ in range(n):
            _seq += 1
            eid = "EQN-" + datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S") + f"-{_seq:05d}"
            rows.append((
                eid,
                datetime.now(timezone.utc) - timedelta(seconds=random.randint(0, 3600)),
                rand_lat(), rand_lon(), rand_mag(),
            ))
        cur.executemany(
            "INSERT INTO seismic_events(event_id, origin_time, latitude, longitude, magnitude)"
            " VALUES(%s, %s, %s, %s, %s)",
            rows,
        )
        return f"新增 {n} 条：{', '.join(x[0] for x in rows)}"
    if r < 0.85:
        n = random.randint(1, 5)
        cur.execute("SELECT event_id FROM seismic_events ORDER BY random() LIMIT %s", (n,))
        ids = [x[0] for x in cur.fetchall()]
        for eid in ids:
            cur.execute(
                "UPDATE seismic_events SET magnitude=%s, latitude=%s, longitude=%s WHERE event_id=%s",
                (rand_mag(), rand_lat(), rand_lon(), eid),
            )
        return f"更正 {len(ids)} 条（同一事务）：{', '.join(ids)}"
    n = random.randint(1, 2)
    cur.execute("SELECT event_id FROM seismic_events ORDER BY random() LIMIT %s", (n,))
    ids = [x[0] for x in cur.fetchall()]
    if ids:
        cur.execute("DELETE FROM seismic_events WHERE event_id = ANY(%s)", (ids,))
    return f"删除 {len(ids)} 条：{', '.join(ids)}"


def recalibration(cur):
    """台网重新测定：一笔事务批量修订多条震级。"""
    cur.execute("SELECT event_id FROM seismic_events ORDER BY random() LIMIT 15")
    ids = [x[0] for x in cur.fetchall()]
    for eid in ids:
        cur.execute("UPDATE seismic_events SET magnitude=%s WHERE event_id=%s", (rand_mag(), eid))
    return f"台网重新测定：一笔事务批量修订 {len(ids)} 条震级"


def main():
    conn = connect()
    cur = conn.cursor()
    while True:  # 等震例表就绪
        try:
            cur.execute("SELECT count(*) FROM seismic_events")
            n = cur.fetchone()[0]
            conn.commit()
            print(f"[simulator] 源库就绪，现有 {n} 条历史震例", flush=True)
            break
        except psycopg2.Error as exc:
            conn.rollback()
            print(f"[simulator] 等待震例表: {exc}", flush=True)
            time.sleep(2)

    i = 0
    while True:
        i += 1
        try:
            msg = recalibration(cur) if i % 50 == 0 else one_tick(cur)
            conn.commit()
            print(f"[simulator] #{i} {msg}", flush=True)
        except psycopg2.Error as exc:
            conn.rollback()
            print(f"[simulator] 错误（已回滚）: {exc}", flush=True)
            try:
                conn.close()
            except Exception:
                pass
            conn = connect()
            cur = conn.cursor()
        time.sleep(TICK * random.uniform(0.5, 1.5))


if __name__ == "__main__":
    main()
