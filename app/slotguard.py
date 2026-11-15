"""复制槽连续性证明（同名换槽断档防护）。

故障场景
========
应用停机期间，源库上的复制槽被删除、随后又用**同一个槽名**重建（例如 DBA
清槽后重建）。重建槽的 confirmed_flush_lsn / restart_lsn 从源库**当前** WAL
位置起算，槽出生之前、镜像上次已保存位置之后的全部变更永久丢失。若应用只凭
“槽名还在”就续采，会一边显示“已就绪”，一边持续供应停机前的旧目录
（EQ1 的 3.0 永远收不到已提交的 8.8）。

证明规则（只有通过证明才允许续采供数）
======================================
设镜像已保存位置为 ``saved_lsn``（meta.last_applied_lsn）：

* 镜像从未保存过任何位置（saved_lsn 为空）——无旧目录可保护，允许；
* 源库查不到同名槽——无法证明，拒绝；
* 槽的 ``confirmed_flush_lsn`` 或 ``restart_lsn`` 为空——槽未就绪，拒绝；
* 槽的两个 LSN 都 **<= saved_lsn**——该槽仍覆盖镜像保存点以来的 WAL，
  重放区间与已保存位置连续，**证明通过**（重发的事务由 LSN 守卫幂等跳过，
  不会回退目录）；
* 任一 LSN **> saved_lsn**——槽在镜像保存点之后才出生，中间存在不可补的
  WAL 断档，**证明失败**：必须停止旧目录供数并自动重建镜像。

本模块只做纯逻辑判断，不依赖 psycopg2，便于在无数据库的环境下单测。
"""
from lsnutil import lsn_to_int


class SlotProofError(Exception):
    """复制槽无法证明覆盖镜像保存点以来的变更（断档/缺槽/信息不全）。"""


def _as_int(lsn):
    if lsn is None:
        return None
    s = str(lsn).strip()
    if not s:
        return None
    return lsn_to_int(s)


def verify_slot_continuity(saved_lsn, slot_row):
    """证明同名槽仍覆盖镜像已保存位置以来的 WAL。

    参数
    ----
    saved_lsn:
        镜像 meta.last_applied_lsn 的文本（如 ``'0/16B3C40'``），空串/None
        表示镜像尚未保存过任何事务。
    slot_row:
        ``(slot_name, active, confirmed_flush_lsn, restart_lsn)`` 四元组；
        None 表示源库上不存在同名槽。LSN 均为文本（pg 系统目录输出）。

    返回
    ----
    ``(ok: bool, reason: str)``。ok=True 时 reason 为证明通过的说明；
    ok=False 时 reason 为拒绝原因（供事件日志/网页展示）。本函数不抛异常，
    便于调用方直接落日志；需要异常语义时用 :func:`ensure_slot_continuity`。
    """
    saved = _as_int(saved_lsn)
    if saved is None:
        return True, "镜像尚无已保存位置，无需连续性证明"
    if slot_row is None:
        return False, "源库不存在同名复制槽，无法证明覆盖保存点以来的变更"
    _name, _active, confirmed_text, restart_text = slot_row
    confirmed = _as_int(confirmed_text)
    restart = _as_int(restart_text)
    if confirmed is None or restart is None:
        return False, "复制槽 LSN 信息不完整（槽可能刚重建尚未就绪），无法证明连续性"
    if confirmed <= saved and restart <= saved:
        return (
            True,
            f"槽位 confirmed_flush_lsn={confirmed_text}、restart_lsn={restart_text} "
            f"均不越过镜像保存位置 {saved_lsn}，WAL 覆盖连续，允许续采",
        )
    return (
        False,
        f"复制槽覆盖断档：槽位 confirmed_flush_lsn={confirmed_text}、"
        f"restart_lsn={restart_text} 已越过镜像保存位置 {saved_lsn}；"
        f"该槽（疑似删除后同名重建）丢失了保存点之后的变更，必须重建镜像",
    )


def ensure_slot_continuity(saved_lsn, slot_row):
    """同 :func:`verify_slot_continuity`，证明失败时抛 :class:`SlotProofError`。"""
    ok, reason = verify_slot_continuity(saved_lsn, slot_row)
    if not ok:
        raise SlotProofError(reason)
    return reason


def is_already_applied(msg_lsn, saved_lsn):
    """LSN 守卫：消息 LSN 是否已落在镜像保存位置（含相等）。

    用于“保存后未确认即崩溃”后的源库重发：重发事务的 LSN 不大于已保存位置，
    直接跳过，保证重送事务不会把目录回退到旧值。
    入参可为 int 或 LSN 文本；saved_lsn 为空表示没有已保存位置。
    """
    saved = _as_int(saved_lsn)
    if saved is None:
        return False
    msg = msg_lsn if isinstance(msg_lsn, int) else _as_int(msg_lsn)
    return msg is not None and msg <= saved
