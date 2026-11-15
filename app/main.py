"""震例目录镜像采集网页：采集状态、震例查询、当前目录 CSV 下载、一致性校验。

读一致性：每个请求只开一个只读事务，整次取数只读同一已提交状态；
镜像未就绪（idle/loading/catchup）时一律 503，不供数。
"""
import hashlib
import html
import os
import time
import urllib.parse
from contextlib import asynccontextmanager

import psycopg2
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, RedirectResponse, Response, StreamingResponse

from canonical import canonical_line, norm_ts
from lsnutil import lsn_to_int
from replicator import Replicator
from store import Store

PG = dict(
    host=os.environ.get("PGHOST", "db"),
    port=int(os.environ.get("PGPORT", "5432")),
    dbname=os.environ.get("PGDATABASE", "quakes"),
    user=os.environ.get("PGUSER", "postgres"),
    password=os.environ.get("PGPASSWORD", "postgres"),
)
SLOT = os.environ.get("SLOT_NAME", "quake_mirror")
MIRROR_DB = os.environ.get("MIRROR_DB", "/data/mirror.db")
LOAD_BATCH = int(os.environ.get("LOAD_BATCH", "500"))
LOAD_SLEEP_MS = int(os.environ.get("LOAD_SLEEP_MS", "120"))

store = Store(MIRROR_DB)
replicator = Replicator(PG, SLOT, store, load_batch=LOAD_BATCH, load_sleep_ms=LOAD_SLEEP_MS)


@asynccontextmanager
async def lifespan(_app):
    replicator.start()
    yield


app = FastAPI(title="震例目录镜像采集", lifespan=lifespan)

STATE_LABELS = {
    "idle": "未建立",
    "loading": "初始装载中",
    "catchup": "追平变更中",
    "done": "已就绪（供数中）",
}
FILTER_KEYS = ("q", "t_from", "t_to", "min_mag", "lat_min", "lat_max", "lon_min", "lon_max")
PAGE_SIZE = 50

CSS = """
body{font-family:-apple-system,'Segoe UI','Microsoft YaHei',sans-serif;margin:0;background:#f4f6f8;color:#222}
header{background:#1f3a5f;color:#fff;padding:14px 24px}
header h1{margin:0;font-size:20px}
header span{font-size:13px;font-weight:normal;opacity:.8}
main{max-width:1080px;margin:20px auto;padding:0 16px}
.card{background:#fff;border-radius:8px;padding:16px 20px;margin-bottom:16px;box-shadow:0 1px 3px rgba(0,0,0,.08)}
table{border-collapse:collapse;width:100%}
th,td{border:1px solid #e3e7ec;padding:6px 10px;font-size:14px;text-align:left}
th{background:#f0f3f7}
.badge{display:inline-block;padding:3px 12px;border-radius:12px;font-size:13px;color:#fff}
.b-idle{background:#888}.b-loading{background:#e6a23c}.b-catchup{background:#409eff}.b-done{background:#2ea44f}
.warn{background:#fff8e1;border:1px solid #f0c36d;border-radius:6px;padding:10px 14px;margin-bottom:16px;font-size:14px}
.ok{background:#e9f7ef;border:1px solid #7dcea0;border-radius:6px;padding:10px 14px;margin-bottom:16px;font-size:14px}
.err{background:#fdedec;border:1px solid #e6b0aa;border-radius:6px;padding:10px 14px;margin-bottom:16px;font-size:14px}
button,.btn{background:#1f6feb;color:#fff;border:0;border-radius:6px;padding:8px 14px;font-size:14px;
           cursor:pointer;text-decoration:none;display:inline-block;margin:2px 6px 2px 0}
button.danger,.btn.danger{background:#c0392b}
.muted{color:#777;font-size:13px}
.mono{font-family:ui-monospace,Menlo,Consolas,monospace}
input{padding:6px 8px;border:1px solid #ccc;border-radius:4px;font-size:14px;margin:2px 4px 2px 0}
label{font-size:13px;color:#555}
form.inline{display:inline}
.log td:first-child{white-space:nowrap;color:#666;font-family:ui-monospace,Menlo,Consolas,monospace;font-size:12px}
h2{font-size:16px;margin:6px 0 12px}
"""


def esc(s):
    return html.escape(str(s))


def page(title, body, refresh=False):
    meta = '<meta http-equiv="refresh" content="3">' if refresh else ""
    return (
        "<!doctype html><html lang='zh'><head><meta charset='utf-8'>"
        f"<title>{esc(title)}</title>{meta}<style>{CSS}</style></head><body>"
        "<header><h1>震例目录镜像采集 <span>PostgreSQL 逻辑复制 · wal2json · SQLite 镜像</span></h1></header>"
        f"<main>{body}</main></body></html>"
    )


def serving():
    return store.get_meta("load_state", "idle") == "done"


def not_ready():
    state = store.get_meta("load_state", "idle")
    body = (
        "<div class='card'><h2>镜像未就绪，暂不供数</h2>"
        f"<p>当前状态：<span class='badge b-{esc(state)}'>{esc(STATE_LABELS.get(state, state))}</span></p>"
        "<p class='muted'>按策略：镜像未建完（或重启后未建完需重做）时不提供查询与下载，"
        "以免把不完整的目录交给重定位分析。</p>"
        "<p><a class='btn' href='/'>返回采集状态</a></p></div>"
    )
    return HTMLResponse(page("镜像未就绪", body), status_code=503)


# ---------------- 状态汇总 ----------------

def collect_status():
    meta = store.get_all_meta()
    state = meta.get("load_state", "idle")
    st = {
        "state": state,
        "state_label": STATE_LABELS.get(state, state),
        "serving": state == "done",
        "loaded_rows": int(meta.get("loaded_rows") or 0),
        "applied_txns": int(meta.get("applied_txns") or 0),
        "applied_changes": int(meta.get("applied_changes") or 0),
        "last_applied_lsn": meta.get("last_applied_lsn") or "",
        "consistent_point": meta.get("consistent_point") or "",
        "target_lsn": meta.get("target_lsn") or "",
        "ready_at": meta.get("ready_at") or "",
        "mirror_rows": store.read("SELECT count(*) FROM events")[0][0],
        "crash_armed": replicator.crash_armed.is_set(),
        "source_ok": False,
        "source_rows": None,
        "slot": None,
    }
    try:
        conn = psycopg2.connect(connect_timeout=3, **PG)
        conn.autocommit = True
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT count(*) FROM seismic_events")
                st["source_rows"] = cur.fetchone()[0]
                cur.execute(
                    "SELECT slot_name, active, confirmed_flush_lsn::text, "
                    "pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)::bigint "
                    "FROM pg_replication_slots WHERE slot_name=%s",
                    (SLOT,),
                )
                row = cur.fetchone()
                if row:
                    st["slot"] = {
                        "name": row[0],
                        "active": bool(row[1]),
                        "confirmed_flush_lsn": row[2],
                        "lag_bytes": int(row[3]),
                    }
            st["source_ok"] = True
        finally:
            conn.close()
    except Exception as exc:
        st["source_error"] = repr(exc)
    st["unconfirmed_ahead"] = bool(
        st["slot"]
        and st["last_applied_lsn"]
        and state == "done"
        and lsn_to_int(st["last_applied_lsn"]) > lsn_to_int(st["slot"]["confirmed_flush_lsn"])
    )
    st["log"] = store.recent_log(15)
    return st


# ---------------- 路由：状态页 ----------------

@app.get("/", response_class=HTMLResponse)
def dashboard():
    st = collect_status()
    state = st["state"]

    rows = [
        ("镜像状态", f"<span class='badge b-{esc(state)}'>{esc(st['state_label'])}</span>"),
        ("是否供数", "是" if st["serving"] else "否（未就绪不供数）"),
        ("初始装载行数", st["loaded_rows"]),
        ("镜像当前行数", st["mirror_rows"]),
        ("源库当前行数（实时）", st["source_rows"] if st["source_ok"] else "源库不可用"),
        ("已应用源事务数", st["applied_txns"]),
        ("已应用行变更数", st["applied_changes"]),
        ("镜像已保存位置 last_applied_lsn", f"<span class='mono'>{esc(st['last_applied_lsn'] or '—')}</span>"),
    ]
    if st["slot"]:
        slot = st["slot"]
        rows.append((
            "源库已确认位置 confirmed_flush_lsn",
            f"<span class='mono'>{esc(slot['confirmed_flush_lsn'])}</span>",
        ))
        rows.append((
            "复制槽",
            f"<span class='mono'>{esc(slot['name'])}</span> · "
            f"{'活跃' if slot['active'] else '空闲'} · 待确认延迟 {slot['lag_bytes']} 字节",
        ))
    else:
        rows.append(("复制槽", "（未创建）" if st["source_ok"] else "源库不可用"))
    if st["consistent_point"]:
        rows.append(("槽位一致点 consistent_point", f"<span class='mono'>{esc(st['consistent_point'])}</span>"))
    if st["target_lsn"]:
        rows.append(("追平目标 target_lsn", f"<span class='mono'>{esc(st['target_lsn'])}</span>"))
    if st["ready_at"]:
        rows.append(("就绪时间", esc(st["ready_at"])))

    table = "".join(f"<tr><th style='width:34%'>{k}</th><td>{v}</td></tr>" for k, v in rows)

    banner = ""
    if st["unconfirmed_ahead"]:
        banner = (
            "<div class='warn'>⚠ 镜像已保存的位置领先源库已确认的位置 —— 即"
            "「保存后尚未确认就重启」的现场：源库会把未确认的事务重发过来，"
            "镜像端幂等重放，不重不漏。</div>"
        )
    if not st["source_ok"]:
        banner += f"<div class='err'>源库连接异常：{esc(st.get('source_error', ''))}</div>"

    actions = []
    if state == "idle":
        actions.append(
            "<form class='inline' method='post' action='/mirror/start'>"
            "<button type='submit'>开始建立镜像</button></form>"
        )
    else:
        actions.append(
            "<form class='inline' method='post' action='/mirror/rebuild' "
            "onsubmit='return confirm(\"确定丢弃当前镜像并重新建立？\")'>"
            "<button type='submit' class='danger'>重新建立镜像</button></form>"
        )
    if state == "done":
        if st["crash_armed"]:
            actions.append(
                "<span class='warn' style='display:inline-block;margin:2px 6px 2px 0;padding:7px 12px'>"
                "已武装：下一笔源事务保存后将不确认并崩溃</span>"
            )
        else:
            actions.append(
                "<form class='inline' method='post' action='/demo/crash-after-save'>"
                "<button type='submit' class='danger'>模拟崩溃：保存后未确认</button></form>"
            )
        actions.append("<a class='btn' href='/events'>查询震例</a>")
        actions.append("<a class='btn' href='/events.csv'>下载当前目录 CSV</a>")
        actions.append("<a class='btn' href='/verify'>一致性校验</a>")
    else:
        actions.append("<span class='muted'>镜像就绪后开放查询、CSV 下载与一致性校验</span>")

    log_rows = "".join(
        f"<tr><td>{esc(ts)}</td><td>{esc(msg)}</td></tr>" for ts, msg in st["log"]
    ) or "<tr><td colspan='2' class='muted'>暂无事件</td></tr>"

    body = (
        f"{banner}"
        f"<div class='card'><h2>采集状态</h2><table>{table}</table>"
        f"<p style='margin-top:14px'>{''.join(actions)}</p></div>"
        f"<div class='card'><h2>最近事件</h2><table class='log'>"
        f"<tr><th style='width:26%'>时间</th><th>事件</th></tr>{log_rows}</table></div>"
    )
    return page("采集状态", body, refresh=True)


@app.get("/api/status")
def api_status():
    st = collect_status()
    st["log"] = [list(x) for x in st["log"]]
    return st


@app.post("/mirror/start")
def mirror_start():
    if store.get_meta("load_state", "idle") == "idle":
        replicator.request_start()
    return RedirectResponse("/", status_code=303)


@app.post("/mirror/rebuild")
def mirror_rebuild():
    replicator.request_rebuild()
    return RedirectResponse("/", status_code=303)


@app.post("/demo/crash-after-save")
def crash_after_save():
    if serving():
        replicator.arm_crash_after_save()
        store.log("已武装崩溃演示：下一笔源事务写入镜像并持久化后，不向源库确认即退出进程")
    return RedirectResponse("/", status_code=303)


# ---------------- 路由：查询与下载 ----------------

def _norm_bound(s, is_end):
    s = s.strip()
    if len(s) == 10 and s[4] == "-" and s[7] == "-":   # 仅日期
        return s + ("T23:59:59.999999Z" if is_end else "T00:00:00.000000Z")
    return s


def build_where(f):
    where, params = [], []
    if f["q"]:
        where.append("event_id LIKE ?")
        params.append(f["q"] + "%")
    if f["t_from"]:
        where.append("origin_time >= ?")
        params.append(_norm_bound(f["t_from"], False))
    if f["t_to"]:
        where.append("origin_time <= ?")
        params.append(_norm_bound(f["t_to"], True))
    if f["min_mag"]:
        where.append("magnitude >= ?")
        params.append(float(f["min_mag"]))
    if f["lat_min"]:
        where.append("latitude >= ?")
        params.append(float(f["lat_min"]))
    if f["lat_max"]:
        where.append("latitude <= ?")
        params.append(float(f["lat_max"]))
    if f["lon_min"]:
        where.append("longitude >= ?")
        params.append(float(f["lon_min"]))
    if f["lon_max"]:
        where.append("longitude <= ?")
        params.append(float(f["lon_max"]))
    return (" WHERE " + " AND ".join(where)) if where else "", params


def get_filters(request):
    qp = request.query_params
    return {k: (qp.get(k) or "").strip() for k in FILTER_KEYS}


@app.get("/events", response_class=HTMLResponse)
def events(request: Request):
    if not serving():
        return not_ready()
    f = get_filters(request)
    try:
        page_no = max(1, int(request.query_params.get("page", "1")))
        where_sql, params = build_where(f)
    except ValueError:
        return HTMLResponse(
            page("参数错误", "<div class='card'><p>筛选参数格式不正确。</p>"
                 "<p><a class='btn' href='/events'>返回查询</a></p></div>"),
            status_code=400,
        )

    # 计数与取页在同一个只读事务里：整次取数只读同一已提交状态
    txn = store.read_txn()
    try:
        total = txn.execute(f"SELECT count(*) FROM events{where_sql}", params).fetchone()[0]
        pages = max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE)
        page_no = min(page_no, pages)
        rows = txn.execute(
            "SELECT event_id, origin_time, latitude, longitude, magnitude FROM events"
            f"{where_sql} ORDER BY event_id LIMIT ? OFFSET ?",
            params + [PAGE_SIZE, (page_no - 1) * PAGE_SIZE],
        ).fetchall()
    finally:
        txn.close()

    body_rows = "".join(
        f"<tr><td class='mono'>{esc(r[0])}</td><td class='mono'>{esc(r[1])}</td>"
        f"<td>{r[2]:.3f}</td><td>{r[3]:.3f}</td><td>{r[4]:.1f}</td></tr>"
        for r in rows
    ) or "<tr><td colspan='5' class='muted'>无匹配震例</td></tr>"

    inputs = (
        f"<form method='get' action='/events'>"
        f"<label>震例号前缀</label><input name='q' value='{esc(f['q'])}' size='10'>"
        f"<label>发震时刻自</label><input name='t_from' value='{esc(f['t_from'])}' size='10' placeholder='2024-01-01'>"
        f"<label>至</label><input name='t_to' value='{esc(f['t_to'])}' size='10' placeholder='2024-12-31'>"
        f"<label>最小震级</label><input name='min_mag' value='{esc(f['min_mag'])}' size='4'>"
        f"<label>纬度</label><input name='lat_min' value='{esc(f['lat_min'])}' size='4'>"
        f"<label>~</label><input name='lat_max' value='{esc(f['lat_max'])}' size='4'>"
        f"<label>经度</label><input name='lon_min' value='{esc(f['lon_min'])}' size='4'>"
        f"<label>~</label><input name='lon_max' value='{esc(f['lon_max'])}' size='4'>"
        f"<button type='submit'>查询</button></form>"
    )

    qs = dict(f)
    qs_csv = urllib.parse.urlencode({k: v for k, v in qs.items() if v})
    prev_q = urllib.parse.urlencode({**{k: v for k, v in qs.items() if v}, "page": page_no - 1})
    next_q = urllib.parse.urlencode({**{k: v for k, v in qs.items() if v}, "page": page_no + 1})
    pager = []
    if page_no > 1:
        pager.append(f"<a class='btn' href='/events?{prev_q}'>上一页</a>")
    pager.append(f"<span class='muted'>第 {page_no} / {pages} 页 · 共 {total} 条（同一已提交状态）</span>")
    if page_no < pages:
        pager.append(f"<a class='btn' href='/events?{next_q}'>下一页</a>")
    pager.append(f"<a class='btn' href='/events.csv?{qs_csv}'>按当前条件下载 CSV</a>")
    pager.append("<a class='btn' href='/'>返回采集状态</a>")

    body = (
        f"<div class='card'><h2>查询震例</h2>{inputs}</div>"
        f"<div class='card'><table><tr><th>震例号</th><th>UTC 发震时刻</th>"
        f"<th>纬度</th><th>经度</th><th>震级</th></tr>{body_rows}</table>"
        f"<p style='margin-top:12px'>{''.join(pager)}</p></div>"
    )
    return page("查询震例", body)


@app.get("/events.csv")
def events_csv(request: Request):
    if not serving():
        return Response("镜像未就绪，暂不供数\n", status_code=503, media_type="text/plain; charset=utf-8")
    try:
        where_sql, params = build_where(get_filters(request))
    except ValueError:
        return Response("筛选参数格式不正确\n", status_code=400, media_type="text/plain; charset=utf-8")

    def gen():
        yield "event_id,origin_time_utc,latitude,longitude,magnitude\n"
        txn = store.read_txn()          # 整个 CSV 出自同一已提交状态
        try:
            cur = txn.execute(
                "SELECT event_id, origin_time, latitude, longitude, magnitude FROM events"
                f"{where_sql} ORDER BY event_id",
                params,
            )
            while True:
                rows = cur.fetchmany(1000)
                if not rows:
                    break
                for r in rows:
                    yield f"{r[0]},{r[1]},{r[2]:.3f},{r[3]:.3f},{r[4]:.1f}\n"
        finally:
            txn.close()

    return StreamingResponse(
        gen(),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": "attachment; filename=quake_catalog.csv"},
    )


# ---------------- 路由：一致性校验 ----------------

@app.get("/verify", response_class=HTMLResponse)
def verify():
    if not serving():
        return not_ready()

    # 先等镜像追到源库当前 WAL 末尾（最多约 5 秒），减少在途变更造成的误判
    note = ""
    try:
        conn = psycopg2.connect(connect_timeout=3, **PG)
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("SELECT pg_current_wal_flush_lsn()::text")
            target = lsn_to_int(cur.fetchone()[0])
        conn.close()
        for _ in range(20):
            last = store.get_meta("last_applied_lsn", "")
            if last and lsn_to_int(last) >= target:
                break
            time.sleep(0.25)
    except Exception as exc:
        note = f"<div class='warn'>等待追平时源库不可用：{esc(repr(exc))}（仍继续比对）</div>"

    # 源库侧：一个 REPEATABLE READ 快照里取全量
    conn = psycopg2.connect(**PG)
    src_lines = []
    try:
        conn.set_session(isolation_level="REPEATABLE READ", readonly=True, autocommit=False)
        scan = conn.cursor(name="verify_scan")
        scan.itersize = 2000
        scan.execute(
            "SELECT event_id, origin_time, latitude, longitude, magnitude "
            "FROM seismic_events ORDER BY event_id"
        )
        for r in scan:
            src_lines.append(canonical_line(r[0], norm_ts(r[1]), r[2], r[3], r[4]))
        conn.commit()
    finally:
        conn.close()

    # 镜像侧：一个只读事务里取全量
    mir_lines = []
    txn = store.read_txn()
    try:
        for r in txn.execute(
            "SELECT event_id, origin_time, latitude, longitude, magnitude FROM events ORDER BY event_id"
        ):
            mir_lines.append(canonical_line(r[0], r[1], r[2], r[3], r[4]))
    finally:
        txn.close()

    def digest(lines):
        h = hashlib.sha256()
        for ln in lines:
            h.update(ln.encode("utf-8"))
            h.update(b"\n")
        return h.hexdigest()

    src_hash, mir_hash = digest(src_lines), digest(mir_lines)
    equal = src_lines == mir_lines

    if equal:
        result = (
            f"<div class='ok'>✔ 两侧完全一致：各 {len(src_lines)} 行，"
            f"SHA-256 = <span class='mono'>{src_hash[:16]}…</span></div>"
        )
    else:
        src_map = {ln.split("|", 1)[0]: ln for ln in src_lines}
        mir_map = {ln.split("|", 1)[0]: ln for ln in mir_lines}
        diffs = []
        for eid in sorted(set(src_map) | set(mir_map)):
            if src_map.get(eid) != mir_map.get(eid):
                diffs.append(
                    f"<tr><td class='mono'>{esc(eid)}</td>"
                    f"<td class='mono'>{esc(src_map.get(eid, '（无）'))}</td>"
                    f"<td class='mono'>{esc(mir_map.get(eid, '（无）'))}</td></tr>"
                )
            if len(diffs) >= 10:
                break
        result = (
            f"<div class='err'>✘ 两侧不一致：源库 {len(src_lines)} 行"
            f"（SHA-256 <span class='mono'>{src_hash[:16]}…</span>），"
            f"镜像 {len(mir_lines)} 行（SHA-256 <span class='mono'>{mir_hash[:16]}…</span>）。"
            f"若源库仍在写入，在途变更会造成短暂差异 —— 请先 "
            f"<span class='mono'>docker compose pause simulator</span>，"
            f"待状态页「待确认延迟」为 0 后再校验。</div>"
            f"<div class='card'><h2>差异样例（前 10 条）</h2>"
            f"<table><tr><th>震例号</th><th>源库</th><th>镜像</th></tr>{''.join(diffs)}</table></div>"
        )

    body = (
        f"{note}{result}"
        "<div class='card'><p><a class='btn' href='/'>返回采集状态</a>"
        "<a class='btn' href='/verify'>重新校验</a></p></div>"
    )
    return page("一致性校验", body)


@app.get("/healthz")
def healthz():
    return {"ok": True}
