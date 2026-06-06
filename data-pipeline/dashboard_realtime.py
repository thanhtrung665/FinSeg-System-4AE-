import sys
import os
import json
import time
import logging
import warnings
from datetime import datetime, timezone
from pathlib import Path

import streamlit as st
import pandas as pd
import plotly.graph_objects as go

# ── Suppress noise warnings truoc khi import bat ky thu gi ───────────────────
warnings.filterwarnings("ignore")
logging.getLogger("vnstock").setLevel(logging.ERROR)
logging.getLogger("realtime_pipeline.crawlers.facebook_crawler").setLevel(logging.ERROR)
logging.getLogger("kafka").setLevel(logging.ERROR)
logging.getLogger("httpx").setLevel(logging.ERROR)
logging.getLogger("sentence_transformers").setLevel(logging.ERROR)
os.environ.setdefault("VNSTOCK_SHOW_ADS", "0")
os.environ.setdefault("VNSTOCK_DISABLE_NOTIFICATION", "1")
logging.basicConfig(level=logging.ERROR)

# ── Path ──────────────────────────────────────────────────────────────────────
_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(_ROOT))

# ── Page config (PHAI o dau tien sau import) ──────────────────────────────────
st.set_page_config(
    page_title="FinSent Realtime",
    page_icon="🔴",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── CSS inject dung st.html() (Streamlit 1.45+) ───────────────────────────────
_CSS = """
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Be+Vietnam+Pro:wght@400;500;600;700;800&display=swap" rel="stylesheet">
<style>
html,body,[class*="css"],[class*="st-"],div,p,span,h1,h2,h3,h4,h5,h6,label,button,input,textarea,select{font-family:'Be Vietnam Pro','Segoe UI',sans-serif!important}
.kpi{padding:18px 16px;border-radius:14px;text-align:center;margin-bottom:10px;font-weight:700}
.kpi-label{font-size:.72rem;color:#999;text-transform:uppercase;letter-spacing:.7px;margin-bottom:6px}
.kpi-value{font-size:2.5rem;font-weight:800;line-height:1}
.kpi-sub{font-size:.72rem;color:#888;margin-top:4px}
.kpi-blue{background:rgba(79,195,247,.1);border:1px solid #4fc3f7}
.kpi-green{background:rgba(0,200,83,.1);border:1px solid #00c853}
.kpi-yellow{background:rgba(255,193,7,.1);border:1px solid #ffc107}
.kpi-red{background:rgba(255,82,82,.1);border:1px solid #ff5252}
.action-card{padding:12px 10px;border-radius:10px;text-align:center;font-weight:700;font-size:.9rem}
.badge{display:inline-block;padding:4px 14px;border-radius:20px;font-size:.78rem;font-weight:700;text-transform:uppercase}
.b-normal{background:#4fc3f7;color:#000}
.b-risk_low{background:#ffc107;color:#000}
.b-risk_high{background:#ff5252;color:#fff}
@keyframes blink{0%,100%{opacity:1}50%{opacity:.2}}
.live-dot{display:inline-block;width:9px;height:9px;border-radius:50%;background:#ff5252;animation:blink 1.2s infinite;margin-right:5px}
.src-tag{display:inline-block;padding:2px 8px;border-radius:6px;font-size:.7rem;font-weight:600;margin:2px;background:rgba(79,195,247,.15);color:#4fc3f7;border:1px solid rgba(79,195,247,.3)}
</style>
"""
st.html(_CSS)


# ══════════════════════════════════════════════════════════════════════════════
# CONSTANTS
# ══════════════════════════════════════════════════════════════════════════════
_VMSI_FILE     = _ROOT / "live_vmsi.json"
_CYCLE_SECS    = 1800
_TICKER_PRESETS = ["SHB","VCB","TCB","MBB","VPB","BID","CTG","HDB","LPB","VNINDEX"]

_FALLBACK = {
    "vmsi_value": 50.0, "status": "normal", "ticker": "",
    "risk_warning": "Chua co du lieu. Nhap ma co phieu va bam Phan tich ngay.",
    "macro_summary": "",
    "component_scores": {"s_social":0.0,"s_macro":0.0,"s_nhnn":0,"s_news":0.0,"vmsi_raw":50.0},
    "processing_metadata": {"processing_time_seconds":0,"social_messages_processed":0},
    "timestamp": datetime.now(timezone.utc).isoformat(),
}

_STATUS_MAP = {
    "normal":    ("Can bang",    "#4fc3f7", "b-normal"),
    "risk_low":  ("Than trong",  "#ffc107", "b-risk_low"),
    "risk_high": ("Rui ro cao",  "#ff5252", "b-risk_high"),
}


# ══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════════════════════
def load_vmsi() -> dict:
    try:
        with open(_VMSI_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return _FALLBACK.copy()


def vmsi_color(v: float) -> str:
    if v <= 25 or v >= 81: return "#ff5252"
    if v <= 40 or v >= 61: return "#ffc107"
    return "#4fc3f7"


def to_pct(v: float) -> float:
    return round((float(v) + 1) * 50, 1)


def _push_history(val: float):
    if "rt_hist_y" not in st.session_state:
        st.session_state.rt_hist_y = []
        st.session_state.rt_hist_x = []
    st.session_state.rt_hist_y.append(val)
    st.session_state.rt_hist_x.append(datetime.now().strftime("%H:%M:%S"))
    if len(st.session_state.rt_hist_y) > 60:
        st.session_state.rt_hist_y = st.session_state.rt_hist_y[-60:]
        st.session_state.rt_hist_x = st.session_state.rt_hist_x[-60:]


# ══════════════════════════════════════════════════════════════════════════════
# CACHE
# ══════════════════════════════════════════════════════════════════════════════
@st.cache_resource(show_spinner="Dang tai Realtime Engine (~60s lan dau)...")
def get_engine(ticker: str):
    from realtime_pipeline.vmsi_realtime import RealtimeVMSIEngine
    return RealtimeVMSIEngine(ticker=ticker)


@st.cache_resource(show_spinner="Dang tai Chatbot AI...")
def get_chatbot():
    from multi_agent_system.agents.chatbot_agent import FinancialChatbot
    return FinancialChatbot()


# ══════════════════════════════════════════════════════════════════════════════
# SIDEBAR
# ══════════════════════════════════════════════════════════════════════════════
with st.sidebar:
    st.markdown("**FinSent** `Realtime`")
    st.caption("Phan tich tam ly thi truong theo thoi gian thuc")
    st.divider()

    st.markdown("**Chon ma co phieu**")
    preset = st.selectbox(
        "Preset",
        ["(Chon nhanh)"] + _TICKER_PRESETS,
        label_visibility="collapsed",
    )
    custom = st.text_input(
        "Hoac nhap ma bat ky",
        placeholder="VD: FPT, HPG, MSN...",
        max_chars=10,
    ).strip().upper()

    if custom:
        ticker_input = custom
    elif preset and preset != "(Chon nhanh)":
        ticker_input = preset
    else:
        ticker_input = st.session_state.get("last_ticker", "SHB")

    st.session_state["last_ticker"] = ticker_input
    st.divider()

    run_now = st.button(
        f"Phan tich {ticker_input} ngay",
        type="primary",
        use_container_width=True,
    )
    st.divider()

    # Auto scheduler
    st.markdown("**Auto Scheduler (30 phut / chu ky)**")
    auto_on = st.toggle("Bat Auto Scheduler", value=False)
    if auto_on:
        st.info("Scheduler chay moi 30 phut, tu dong cap nhat live_vmsi.json.")
        if "bg_sched" not in st.session_state:
            from realtime_pipeline.scheduler import RealtimeScheduler
            sched = RealtimeScheduler(ticker=ticker_input, interval_seconds=_CYCLE_SECS)
            bg = sched.start_background()
            st.session_state["bg_sched"]  = bg
            st.session_state["sched_obj"] = sched
    else:
        if "bg_sched" in st.session_state and st.session_state["bg_sched"]:
            try:
                st.session_state["bg_sched"].shutdown(wait=False)
            except Exception:
                pass
            del st.session_state["bg_sched"]

    st.divider()

    # System status
    st.markdown("**Trang thai he thong**")
    try:
        from kafka import KafkaConsumer as _KC
        from data_pipeline_ingestion.config import settings as _cfg
        _t = _KC(bootstrap_servers=[_cfg.KAFKA_BROKER],
                 request_timeout_ms=2000, connections_max_idle_ms=2000)
        _t.close()
        st.success("Kafka")
    except Exception:
        st.error("Kafka Offline")

    try:
        from data_pipeline_ingestion.config import settings as _cfg2
        _cfg2.get_chroma_client()
        st.success("ChromaDB Cloud")
    except Exception:
        st.error("ChromaDB Offline")

    _fb_ready = bool(os.getenv("FB_EMAIL", ""))
    st.success("Facebook (credentials)") if _fb_ready else st.warning("Facebook (stub mode)")

    if _VMSI_FILE.exists():
        mt = datetime.fromtimestamp(_VMSI_FILE.stat().st_mtime).strftime("%H:%M:%S")
        st.info(f"live_vmsi.json | {mt}")
    else:
        st.warning("live_vmsi.json chua co")

    st.divider()
    st.markdown("**Nguon du lieu**")
    st.html(
        "<span class='src-tag'>CafeF</span>"
        "<span class='src-tag'>Vietstock</span>"
        "<span class='src-tag'>ChinhPhu</span>"
        "<span class='src-tag'>NHNN</span>"
        "<span class='src-tag'>Facebook</span>"
        "<span class='src-tag'>vnstock</span>"
    )
    st.divider()
    st.caption("FinSent-Agent Realtime v1.0 | 4AE")


# ══════════════════════════════════════════════════════════════════════════════
# CHAY CHU KY
# ══════════════════════════════════════════════════════════════════════════════
if run_now:
    prog = st.progress(0, text=f"Dang phan tich {ticker_input}...")
    with st.spinner(""):
        try:
            engine = get_engine(ticker_input)
            if engine.ticker != ticker_input:
                engine.ticker = ticker_input
                engine._producer = None
                engine._mac = None

            prog.progress(10, "Crawl tin tuc + mang xa hoi...")
            social_n = engine._collect_social()

            prog.progress(35, "Lay du lieu gia co phieu...")
            mkt_sent = engine._collect_market()

            prog.progress(55, "Query ChromaDB + tinh VMSI...")
            mac_result = engine._run_mac_cycle()

            prog.progress(80, "Lam giau voi market sentiment...")
            result = engine._enrich_with_market(mac_result, mkt_sent)

            prog.progress(100, "Hoan tat!")
            time.sleep(0.3)
            prog.empty()

            if "rt_log" not in st.session_state:
                st.session_state.rt_log = []
            st.session_state.rt_log.append({
                "Gio":    datetime.now().strftime("%H:%M:%S"),
                "Ticker": ticker_input,
                "VMSI":   result.get("vmsi_value", 50),
                "Status": result.get("status", "normal"),
                "Social": social_n,
            })
            st.session_state.rt_log = st.session_state.rt_log[-20:]
            st.toast(f"Phan tich {ticker_input} hoan tat | VMSI={result.get('vmsi_value')}", icon="✅")

        except Exception as e:
            prog.empty()
            st.error(f"Loi phan tich: {e}")


# ══════════════════════════════════════════════════════════════════════════════
# DOC DU LIEU
# ══════════════════════════════════════════════════════════════════════════════
data         = load_vmsi()
vmsi_v       = float(data.get("vmsi_value", 50))
status       = data.get("status", "normal")
scores       = data.get("component_scores", {})
meta         = data.get("processing_metadata", {})
ticker_shown = data.get("ticker", ticker_input) or ticker_input
_push_history(vmsi_v)

ts_raw = data.get("timestamp", "")
try:
    ts_str = datetime.fromisoformat(ts_raw).strftime("%d/%m %H:%M UTC")
except Exception:
    ts_str = datetime.now().strftime("%d/%m %H:%M")

status_label, status_color, badge_cls = _STATUS_MAP.get(
    status, ("Khong xac dinh", "#aaa", "b-normal")
)
color = vmsi_color(vmsi_v)


# ══════════════════════════════════════════════════════════════════════════════
# HEADER
# ══════════════════════════════════════════════════════════════════════════════
hc1, hc2, hc3 = st.columns([6, 3, 3])
with hc1:
    st.html(
        f"<div style='display:flex;align-items:center;gap:10px;padding:4px 0'>"
        f"<span style='font-size:1.5rem;font-weight:800;color:#fff'>FinSent</span>"
        f"<span style='font-size:.85rem;background:#2e3b4e;padding:3px 10px;"
        f"border-radius:8px;color:#4fc3f7;font-weight:600'>REALTIME</span>"
        f"<span style='font-size:1.1rem;font-weight:700;color:{color}'>{ticker_shown}</span>"
        f"</div>"
    )
with hc2:
    st.html(
        f"<div style='text-align:right;padding-top:8px;color:#888;font-size:.8rem'>"
        f"<span class='live-dot'></span>LIVE | {ts_str}</div>"
    )
with hc3:
    next_str = "--"
    if "bg_sched" in st.session_state:
        from datetime import timedelta
        next_str = (datetime.now() + timedelta(seconds=_CYCLE_SECS)).strftime("%H:%M")
    st.html(
        f"<div style='text-align:right;padding-top:8px;color:#888;font-size:.8rem'>"
        f"Chu ky tiep: {next_str}</div>"
    )

st.divider()


# ══════════════════════════════════════════════════════════════════════════════
# KPI CARDS
# ══════════════════════════════════════════════════════════════════════════════
k1, k2, k3, k4, k5 = st.columns(5)

with k1:
    st.html(
        f"<div class='kpi kpi-blue'>"
        f"<div class='kpi-label'>Chi so VMSI</div>"
        f"<div class='kpi-value' style='color:{color}'>{vmsi_v:.1f}</div>"
        f"<div class='kpi-sub'>/ 100</div></div>"
    )
with k2:
    st.html(
        f"<div class='kpi kpi-yellow'>"
        f"<div class='kpi-label'>Trang thai</div>"
        f"<div style='margin:8px 0'>"
        f"<span class='badge {badge_cls}'>{status_label}</span>"
        f"</div></div>"
    )
with k3:
    s_nhnn_v = scores.get("s_nhnn", 0)
    nhnn_lbl = {-1: "That chat", 0: "Trung lap", 1: "Noi long"}.get(s_nhnn_v, "-")
    nhnn_ico = {-1: "📉", 0: "➡️", 1: "📈"}.get(s_nhnn_v, "")
    st.html(
        f"<div class='kpi kpi-green'>"
        f"<div class='kpi-label'>Chinh sach NHNN</div>"
        f"<div style='font-size:1rem;font-weight:700;margin-top:8px'>{nhnn_ico} {nhnn_lbl}</div>"
        f"</div>"
    )
with k4:
    mkt_s    = float(data.get("market_sentiment", scores.get("s_news", 0.0)))
    mkt_c    = "#00c853" if mkt_s > 0.05 else ("#ff5252" if mkt_s < -0.05 else "#aaa")
    mkt_lbl  = "Tang" if mkt_s > 0.05 else ("Giam" if mkt_s < -0.05 else "Di ngang")
    st.html(
        f"<div class='kpi kpi-blue'>"
        f"<div class='kpi-label'>Thi truong ({ticker_shown})</div>"
        f"<div class='kpi-value' style='font-size:1.4rem;color:{mkt_c}'>{mkt_lbl}</div>"
        f"<div class='kpi-sub'>{mkt_s:+.3f}</div></div>"
    )
with k5:
    msgs   = meta.get("social_messages_processed", 0)
    proc_t = meta.get("processing_time_seconds", 0)
    st.html(
        f"<div class='kpi kpi-blue'>"
        f"<div class='kpi-label'>Hieu suat</div>"
        f"<div style='font-size:1.1rem;font-weight:700;margin-top:6px'>{msgs} msgs</div>"
        f"<div class='kpi-sub'>{proc_t:.2f}s / chu ky</div></div>"
    )


# ══════════════════════════════════════════════════════════════════════════════
# BIEU DO VMSI + RADAR
# ══════════════════════════════════════════════════════════════════════════════
col_chart, col_radar = st.columns([6, 4], gap="large")

with col_chart:
    st.markdown("##### Lich su VMSI — phien hien tai")
    hx = st.session_state.get("rt_hist_x", [ts_str])
    hy = st.session_state.get("rt_hist_y", [vmsi_v])

    fig = go.Figure()
    fig.add_hrect(y0=0,  y1=25,  fillcolor="rgba(255,82,82,.07)",  line_width=0,
                  annotation_text="Hoang loan", annotation_position="bottom right",
                  annotation_font=dict(color="#ff5252", size=9))
    fig.add_hrect(y0=75, y1=100, fillcolor="rgba(0,200,83,.07)",   line_width=0,
                  annotation_text="Tham lam",   annotation_position="top right",
                  annotation_font=dict(color="#00c853", size=9))
    fig.add_hrect(y0=40, y1=61,  fillcolor="rgba(79,195,247,.03)", line_width=0)
    fig.add_trace(go.Scatter(
        x=hx, y=hy, mode="lines+markers", name="VMSI",
        line=dict(color="#4fc3f7", width=2.5),
        marker=dict(size=5, color="#4fc3f7"),
        fill="tozeroy", fillcolor="rgba(79,195,247,.07)",
    ))
    fig.add_hline(
        y=vmsi_v, line_dash="dot", line_color=color,
        annotation_text=f"  {vmsi_v:.1f}",
        annotation_position="right",
        annotation_font=dict(size=11, color=color),
    )
    fig.update_layout(
        plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
        height=300, margin=dict(l=10, r=55, t=10, b=30),
        yaxis=dict(range=[0, 100], gridcolor="#2a2a2a", tickfont=dict(size=10)),
        xaxis=dict(gridcolor="#2a2a2a", tickfont=dict(size=9)),
        showlegend=False,
        font=dict(family="Be Vietnam Pro, Segoe UI, sans-serif"),
    )
    st.plotly_chart(fig, use_container_width=True)

with col_radar:
    st.markdown("##### Component Scores")
    s_soc = float(scores.get("s_social", 0))
    s_mac = float(scores.get("s_macro",  0))
    s_nhn = float(scores.get("s_nhnn",   0))
    s_nws = float(scores.get("s_news",   0))

    cats = ["Social", "Macro", "NHNN", "News", "VMSI"]
    vals = [to_pct(s_soc), to_pct(s_mac), to_pct(s_nhn), to_pct(s_nws), vmsi_v]

    fig_r = go.Figure(go.Scatterpolar(
        r=vals + [vals[0]], theta=cats + [cats[0]],
        fill="toself", fillcolor="rgba(79,195,247,.12)",
        line=dict(color="#4fc3f7", width=2),
        marker=dict(size=5),
    ))
    fig_r.update_layout(
        polar=dict(
            radialaxis=dict(visible=True, range=[0, 100],
                            tickfont=dict(size=9), gridcolor="#333"),
            angularaxis=dict(tickfont=dict(size=10)),
            bgcolor="rgba(0,0,0,0)",
        ),
        paper_bgcolor="rgba(0,0,0,0)",
        height=300, margin=dict(l=20, r=20, t=10, b=20),
        showlegend=False,
        font=dict(family="Be Vietnam Pro, Segoe UI, sans-serif"),
    )
    st.plotly_chart(fig_r, use_container_width=True)


# ══════════════════════════════════════════════════════════════════════════════
# HANG 3: CANH BAO + BANG DIEM | CHATBOT
# ══════════════════════════════════════════════════════════════════════════════
st.divider()
left, right = st.columns([5, 5], gap="large")

with left:
    st.markdown("##### Khuyen nghi hanh dong")
    ac1, ac2, ac3 = st.columns(3)
    ac1.html(
        "<div class='action-card' style='background:rgba(0,200,83,.1);"
        "border:1px solid #00c853;color:#00c853'>"
        "MUA VAO<br><small style='font-weight:400'>VMSI &lt; 25</small></div>"
    )
    ac2.html(
        "<div class='action-card' style='background:rgba(255,193,7,.1);"
        "border:1px solid #ffc107;color:#ffc107'>"
        "NAM GIU<br><small style='font-weight:400'>VMSI 25-75</small></div>"
    )
    ac3.html(
        "<div class='action-card' style='background:rgba(255,82,82,.1);"
        "border:1px solid #ff5252;color:#ff5252'>"
        "GIAM MARGIN<br><small style='font-weight:400'>VMSI &gt; 75</small></div>"
    )

    risk_w = data.get("risk_warning", "")
    if risk_w:
        st.markdown("##### Canh bao rui ro")
        kws_red    = ["DO", "FOMO", "Hoang", "hoang"]
        kws_yellow = ["than trong", "e de", "than", "chuan bi"]
        if any(k in risk_w for k in kws_red):
            st.error(risk_w)
        elif any(k in risk_w.lower() for k in kws_yellow):
            st.warning(risk_w)
        else:
            st.info(risk_w)

    msumm = data.get("macro_summary", "")
    if msumm and "fallback" not in msumm.lower():
        st.markdown("##### Phan tich vi mo NHNN")
        st.info(msumm)

    st.markdown("##### Bang diem thanh phan")
    df_s = pd.DataFrame({
        "Thanh phan": [
            "S_Social (MXH + Bao)", "S_Macro (Vi mo)",
            "S_NHNN (Chinh sach)",  "S_News (Gia CP)", "VMSI Raw",
        ],
        "Gia tri": [
            f"{s_soc:+.4f}", f"{s_mac:+.4f}",
            f"{s_nhn:+.0f}", f"{s_nws:+.4f}",
            f"{scores.get('vmsi_raw', vmsi_v):.2f}/100",
        ],
        "Dai": ["[-1,1]", "[-1,1]", "{-1,0,1}", "[-1,1]", "[0,100]"],
    })
    st.dataframe(df_s, hide_index=True, use_container_width=True)

    log = st.session_state.get("rt_log", [])
    if log:
        st.markdown("##### Lich su phan tich (session)")
        st.dataframe(pd.DataFrame(log[::-1]), hide_index=True, use_container_width=True)


with right:
    st.markdown("##### Chatbot tu van tai chinh AI")

    qa1, qa2, qa3, qa4 = st.columns(4)
    with qa1:
        if st.button("VMSI?",       use_container_width=True):
            st.session_state["_q"] = f"VMSI {ticker_shown} dang o muc nao va co y nghia gi?"
    with qa2:
        if st.button("Khuyen nghi", use_container_width=True):
            st.session_state["_q"] = f"Khuyen nghi dau tu cho {ticker_shown} dua tren VMSI?"
    with qa3:
        if st.button("Tin tuc",     use_container_width=True):
            st.session_state["_q"] = f"Tin tuc thi truong anh huong den {ticker_shown}?"
    with qa4:
        if st.button("NHNN",        use_container_width=True):
            st.session_state["_q"] = "Chinh sach NHNN hien tai tac dong the nao den nganh ngan hang?"

    _init_msg = (
        f"Xin chao! Toi la chuyen gia tu van AI cua FinSent-Agent.\n\n"
        f"Dang theo doi: **{ticker_shown}** "
        f"— VMSI = **{vmsi_v:.1f}/100** ({status_label}).\n\n"
        f"Toi co the giup gi cho ban?"
    )
    if "rt_msgs" not in st.session_state:
        st.session_state.rt_msgs = [{"role": "assistant", "content": _init_msg}]
    elif st.session_state.rt_msgs and st.session_state.rt_msgs[0].get("content") != _init_msg:
        st.session_state.rt_msgs[0]["content"] = _init_msg

    chat_box = st.container(height=340)
    with chat_box:
        for m in st.session_state.rt_msgs:
            with st.chat_message(m["role"]):
                st.markdown(m["content"])

    q_auto  = st.session_state.pop("_q", None)
    q_user  = st.chat_input(f"Hoi ve {ticker_shown}, VMSI, rui ro...")
    q_final = q_auto or q_user

    if q_final:
        st.session_state.rt_msgs.append({"role": "user", "content": q_final})
        with chat_box:
            with st.chat_message("user"):
                st.markdown(q_final)
        with chat_box:
            with st.chat_message("assistant"):
                with st.spinner("Dang phan tich..."):
                    try:
                        bot  = get_chatbot()
                        resp = bot.generate_advice(
                            user_query=q_final,
                            vmsi_context={**data, "ticker": ticker_shown},
                        )
                    except Exception as e:
                        resp = (
                            f"Loi chatbot: {e}\n\n"
                            f"VMSI **{vmsi_v:.1f}** ({status_label}): {risk_w}"
                        )
                st.markdown(resp)
        st.session_state.rt_msgs.append({"role": "assistant", "content": resp})
        st.rerun()

    if len(st.session_state.rt_msgs) > 1:
        if st.button("Xoa lich su chat", use_container_width=True):
            st.session_state.rt_msgs = [{"role": "assistant", "content": _init_msg}]
            st.rerun()
