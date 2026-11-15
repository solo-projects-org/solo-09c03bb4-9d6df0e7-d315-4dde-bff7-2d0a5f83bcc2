"""SQLite 镜像存储层。

要点：
- WAL 模式 + synchronous=FULL：写事务提交即持久（进程被杀、容器重启不丢已提交数据）；
- 镜像数据与续采位置（meta.last_applied_lsn）在同一个事务里落库，二者永远一致；
- 网页读请求各自使用独立只读事务 => 每次取数只读同一已提交状态，
  绝不会看到某笔源事务应用了一半的中间态。
"""
import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime, timezone

SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    event_id    TEXT PRIMARY KEY,
    origin_time TEXT NOT NULL,      -- ISO-8601 UTC，如 2026-11-14T08:00:00.123456Z
    latitude    REAL NOT NULL,
    longitude   REAL NOT NULL,
    magnitude   REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS events_origin_time_idx ON events(origin_time);

CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT
);

CREATE TABLE IF NOT EXISTS event_log (
    id  INTEGER PRIMARY KEY AUTOINCREMENT,
    ts  TEXT NOT NULL,
    msg TEXT NOT NULL
);
"""

LOG_KEEP = 100


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


class Store:
    def __init__(self, path: str):
        self._path = path
        self._lock = threading.Lock()          # 单写者（采集线程），日志等偶发写也走同一把锁
        self._w = sqlite3.connect(path, isolation_level=None, check_same_thread=False)
        self._w.execute("PRAGMA journal_mode=WAL")
        self._w.execute("PRAGMA synchronous=FULL")
        self._w.execute("PRAGMA busy_timeout=10000")
        self._w.executescript(SCHEMA)

    # ---------- 写 ----------
    @contextmanager
    def write_txn(self):
        """一个写事务：镜像变更与位置推进必须放在同一个事务里提交。"""
        with self._lock:
            cur = self._w.cursor()
            cur.execute("BEGIN IMMEDIATE")
            try:
                yield cur
                cur.execute("COMMIT")
            except BaseException:
                cur.execute("ROLLBACK")
                raise
            finally:
                cur.close()

    # ---------- 读 ----------
    def read(self, sql, params=()):
        rows = self.read_txn()
        try:
            return rows.execute(sql, params).fetchall()
        finally:
            rows.close()

    def read_txn(self):
        """返回一个已开启只读事务的连接：多次查询看到同一已提交快照。用毕 close()。"""
        conn = sqlite3.connect(self._path)
        conn.execute("PRAGMA busy_timeout=10000")
        conn.execute("BEGIN")
        return conn

    # ---------- meta ----------
    def get_meta(self, key, default=None):
        rows = self.read("SELECT value FROM meta WHERE key=?", (key,))
        return rows[0][0] if rows else default

    def get_all_meta(self):
        return dict(self.read("SELECT key, value FROM meta"))

    @staticmethod
    def set_meta(cur, key, value):
        cur.execute(
            "INSERT INTO meta(key, value) VALUES(?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, str(value)),
        )

    # ---------- 事件日志 ----------
    def log(self, msg, cur=None):
        """cur 为空时自成一个小事务；否则并入调用方的事务（与数据同生共死）。"""
        if cur is None:
            with self.write_txn() as c:
                self.log(msg, c)
            return
        cur.execute("INSERT INTO event_log(ts, msg) VALUES(?, ?)", (utcnow(), msg))
        cur.execute(
            "DELETE FROM event_log WHERE id NOT IN "
            "(SELECT id FROM event_log ORDER BY id DESC LIMIT ?)",
            (LOG_KEEP,),
        )

    def recent_log(self, limit=20):
        return self.read(
            "SELECT ts, msg FROM event_log ORDER BY id DESC LIMIT ?", (limit,)
        )
