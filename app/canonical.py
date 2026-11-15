"""两条数据通路（初始装载的 psycopg2 类型 / 逻辑复制的 wal2json 文本）的统一规范化。

镜像里只保存规范化后的值，保证：
- 初始装载与后续变更写出的内容完全一致（校验时两侧哈希才能相同）；
- origin_time 统一为 ISO-8601 UTC 文本，字典序即时间序，可直接比较与筛选。
"""
from datetime import datetime, timezone


def norm_ts(value) -> str:
    """timestamptz -> '2026-11-14T08:00:00.123456Z'。

    初始装载收到 datetime；wal2json 收到文本 '2026-11-14 08:00:00.123456+00'。
    """
    if isinstance(value, datetime):
        dt = value
    else:
        s = str(value).strip()
        if s.endswith("+00"):
            s = s[:-3] + "+00:00"
        dt = datetime.fromisoformat(s.replace(" ", "T", 1))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def canonical_line(event_id, origin_time, latitude, longitude, magnitude) -> str:
    """一致性校验用的规范行：数值按表定义的小数位格式化，消除 Decimal/float 差异。"""
    return (
        f"{event_id}|{origin_time}"
        f"|{float(latitude):.3f}|{float(longitude):.3f}|{float(magnitude):.1f}"
    )
