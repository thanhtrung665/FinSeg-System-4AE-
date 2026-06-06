# -*- coding: utf-8 -*-
"""
dashboard.py - FinSent-Agent Streamlit Dashboard
Ket noi voi: live_vmsi.json, MACSystem, FinancialChatbot
"""

import sys
import os
import json
import time
import logging
from pathlib import Path
from datetime import datetime, timezone

import streamlit as st
import pandas as pd
import plotly.graph_objects as go

# ── Path setup ────────────────────────────────────────────────────────────────
_PIPELINE_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(_PIPELINE_ROOT))

logging.basicConfig(level=logging.WARNING)

# ── Page config (phai o dau tien) ─────────────────────────────────────────────
st.set_page_config(
    page_title="FinSent-Agent Dashboard",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── CSS + Google Fonts (Be Vietnam Pro ho tro tieng Viet tot nhat) ────────────
st.markdown("""
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Be+Vietnam+Pro:wght@400;600;700;800&display=swap" rel="stylesheet">
<style>
html, body, [class*="css"], [class*="st-"], .stMarkdown, .stText,
div, p, span, h1, h2, h3, h4, h5, h6, label, button, input, textarea {
    font-family: 'Be Vietnam Pro', 'Segoe UI', Arial, sans-serif !important;
}
.metric-card {
    padding: 16px 20px; border-radius: 12px;
    text-align: center; margin-bottom: 12px; font-weight: 600;
}
.card-green  { background: rgba(0,200,83,.12);   border: 1px solid #00c853; color: #00c853; }
.card-yellow { background: rgba(255,193,7,.12);  border: 1px solid #ffc107; color: #ffc107; }
.card-red    { background: rgba(255,82,82,.12);  border: 1px solid #ff5252; color: #ff5252; }
.card-blue   { background: rgba(79,195,247,.12); border: 1px solid #4fc3f7; color: #4fc3f7; }
.badge {
    display: inline-block; padding: 3px 12px; border-radius: 20px;
    font-size: 0.8rem; font-weight: 700; text-transform: uppercase;
}
.badge-normal    { background:#4fc3f7; color:#000; }
.badge-risk_low  { background:#ffc107; color:#000; }
.badge-risk_high { background:#ff5252; color:#fff; }
</style>
""", unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
# HELPER: Doc live_vmsi.json
# ══════════════════════════════════════════════════════════════════════════════
_VMSI_FILE = _PIPELINE_ROOT / "live_vmsi.json"

_FALLBACK_DATA = {
    "vmsi_value": 50.0,
    "status": "normal",
    "risk_warning": "Chua co du lieu — chay MAC System de bat dau.",
    "macro_summary": "",
    "component_scores": {
        "s_social": 0.0, "s_macro": 0.0, "s_nhnn": 0,
        "s_news": 0.0, "vmsi_raw": 50.0,
    },
    "processing_metadata": {"processing_time_seconds": 0, "social_messages_processed": 0},
    "timestamp": datetime.now(timezone.utc).isoformat(),
}

def load_vmsi_data() -> dict:
    try:
        with open(_VMSI_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return _FALLBACK_DATA.copy()


# ══════════════════════════════════════════════════════════════════════════════
# HELPER: VMSI history ring buffer (max 60 diem)
# ══════════════════════════════════════════════════════════════════════════════
def _update_history(vmsi_val: float):
    if "vmsi_history" not in st.session_state:
        st.session_state.vmsi_history = []
        st.session_state.time_history = []
    st.session_state.vmsi_history.append(vmsi_val)
    st.session_state.time_history.append(datetime.now().strftime("%H:%M:%S"))
    if len(st.session_state.vmsi_history) > 60:
        st.session_state.vmsi_history = st.session_state.vmsi_history[-60:]
        st.session_state.time_history  = st.session_state.time_history[-60:]


# ══════════════════════════════════════════════════════════════════════════════
# CACHE: Load MACSystem va Chatbot 1 lan duy nhat
# ══════════════════════════════════════════════════════════════════════════════
@st.cache_resource(show_spinner="Dang khoi tao Multi-Agent System (~60s lan dau)...")
def get_mac_system():
    from multi_agent_system.agents.mac_orchestrator import MACSystem
    return MACSystem()


@st.cache_resource(show_spinner="Dang tai Chatbot AI...")
def get_chatbot():
    from multi_agent_system.agents.chatbot_agent import FinancialChatbot
    return FinancialChatbot()


@st.cache_resource(show_spinner="Dang khoi tao Realtime Engine...")
def get_realtime_engine(ticker: str):
    """Load RealtimeVMSIEngine cho ticker cu the."""
    from realtime_pipeline.vmsi_realtime import RealtimeVMSIEngine
    return RealtimeVMSIEngine(ticker=ticker)


@st.cache_resource
def get_scheduler():
    """Background scheduler - chi khoi tao 1 lan."""
    from realtime_pipeline.scheduler import RealtimeScheduler
    return RealtimeScheduler(ticker="SHB", interval_seconds=1800)


# ══════════════════════════════════════════════════════════════════════════════
# HELPER: Mau sac va nhan trang thai
# ══════════════════════════════════════════════════════════════════════════════
def vmsi_color(v: float) -> str:
    if v <= 25 or v >= 75: return "#ff5252"
    if v <= 45 or v >= 60: return "#ffc107"
    return "#4fc3f7"

_STATUS_LABELS = {
    "normal":    "✅ Cân bằng",
    "risk_low":  "⚠️ Thận trọng",
    "risk_high": "🚨 Rủi ro cao",
}

def status_label(s: str) -> str:
    return _STATUS_LABELS.get(s, s)


# ══════════════════════════════════════════════════════════════════════════════
# SIDEBAR
# ══════════════════════════════════════════════════════════════════════════════
with st.sidebar:
    st.markdown("## ⚙️ Điều khiển")
    st.divider()

    ticker_ctx = st.selectbox(
        "Ticker Context",
        ["SHB", "VCB", "TCB", "MBB", "VPB", "BID", "CTG", "VNINDEX", "SCB"],
        help="Nhap ten hoac ma co phieu de phan tich",
    )

    # Input tu do
    custom_ticker = st.text_input(
        "Hoac nhap ma co phieu:", placeholder="VD: HPG, FPT, MSN..."
    ).strip().upper()
    if custom_ticker:
        ticker_ctx = custom_ticker

    st.divider()

    # Mode: Demo (MAC cu) hoac Realtime (crawl that)
    pipeline_mode = st.radio(
        "Che do pipeline",
        ["Demo (du lieu goc)", "Realtime (crawl that)"],
        index=0,
        help="Realtime se crawl CafeF, Vietstock, Facebook va vnstock",
    )
    is_realtime = pipeline_mode.startswith("Realtime")

    run_cycle = st.button(
        "▶ Chạy phân tích ngay",
        type="primary",
        use_container_width=True,
    )

    st.divider()

    auto_refresh   = st.toggle("Bật auto refresh", value=False)
    refresh_secs   = st.slider(
        "Chu kỳ (giây)", 10, 120, 30,
        disabled=not auto_refresh,
    )

    st.divider()

    st.markdown("**Trạng thái hệ thống**")

    # Kafka
    try:
        from kafka import KafkaConsumer as _KC
        from data_pipeline_ingestion.config import settings as _cfg
        _tc = _KC(bootstrap_servers=[_cfg.KAFKA_BROKER], request_timeout_ms=3000, connections_max_idle_ms=3000)
        _tc.close()
        st.success("🟢 Kafka — Online")
    except Exception:
        st.error("🔴 Kafka — Offline")

    # ChromaDB
    try:
        from data_pipeline_ingestion.config import settings as _cfg2
        _cfg2.get_chroma_client()
        st.success("🟢 ChromaDB Cloud — Online")
    except Exception:
        st.error("🔴 ChromaDB — Offline")

    # live_vmsi.json
    if _VMSI_FILE.exists():
        mtime = datetime.fromtimestamp(_VMSI_FILE.stat().st_mtime).strftime("%H:%M:%S")
        st.info(f"📄 live_vmsi.json\nCập nhật: {mtime}")
    else:
        st.warning("⚠️ live_vmsi.json chưa có")

    st.divider()
    st.caption("FinSent-Agent v1.0 · 4AE Team")


# ══════════════════════════════════════════════════════════════════════════════
# CHAY CHU KY PHAN TICH
# ══════════════════════════════════════════════════════════════════════════════
if run_cycle:
    with st.spinner(f"Đang phân tích [{ticker_ctx}]..."):
        try:
            mac = get_mac_system()
            res = mac.execute_sequential_workflow(ticker_context=ticker_ctx)
            if "error" not in res:
                t = res.get("processing_metadata", {}).get("processing_time_seconds", 0)
                st.toast(f"✅ Hoàn tất | VMSI={res.get('vmsi_value')} | {t:.2f}s", icon="✅")
            else:
                st.toast(f"⚠️ Lỗi: {res.get('details', '')}", icon="⚠️")
        except Exception as e:
            st.toast(f"❌ Lỗi khởi động MAC: {e}", icon="❌")


# ══════════════════════════════════════════════════════════════════════════════
# DOC DU LIEU & UPDATE HISTORY
# ══════════════════════════════════════════════════════════════════════════════
vmsi_data = load_vmsi_data()
vmsi_val  = float(vmsi_data.get("vmsi_value", 50))
status    = vmsi_data.get("status", "normal")
scores    = vmsi_data.get("component_scores", {})
metadata  = vmsi_data.get("processing_metadata", {})
_update_history(vmsi_val)


# ══════════════════════════════════════════════════════════════════════════════
# HEADER
# ══════════════════════════════════════════════════════════════════════════════
c_h1, c_h2 = st.columns([7, 3])
with c_h1:
    st.markdown(
        "### 📈 FinSent-Agent "
        "<span style='font-size:.55em; background:#2e3b4e; padding:3px 10px; "
        "border-radius:10px; color:#4fc3f7'>VMSI Dashboard</span>",
        unsafe_allow_html=True,
    )
with c_h2:
    ts = vmsi_data.get("timestamp", "")
    try:
        ts_fmt = datetime.fromisoformat(ts).strftime("%d/%m/%Y %H:%M UTC")
    except Exception:
        ts_fmt = datetime.now().strftime("%d/%m/%Y %H:%M")
    st.markdown(
        f"<div style='text-align:right;color:#aaa;padding-top:10px'>"
        f"🔄 {ts_fmt}</div>",
        unsafe_allow_html=True,
    )

st.divider()


# ══════════════════════════════════════════════════════════════════════════════
# HANG 1: KPI CARDS
# ══════════════════════════════════════════════════════════════════════════════
k1, k2, k3, k4 = st.columns(4)
color = vmsi_color(vmsi_val)

with k1:
    st.markdown(
        f"<div class='metric-card card-blue'>"
        f"<div style='font-size:.8rem;color:#aaa'>CHỈ SỐ VMSI</div>"
        f"<div style='font-size:2.4rem;font-weight:800;color:{color}'>{vmsi_val:.1f}</div>"
        f"<div style='font-size:.75rem;color:#aaa'>/ 100</div>"
        f"</div>",
        unsafe_allow_html=True,
    )
with k2:
    badge_cls = f"badge-{status}"
    st.markdown(
        f"<div class='metric-card card-yellow'>"
        f"<div style='font-size:.8rem;color:#aaa'>TRẠNG THÁI</div>"
        f"<div style='margin-top:8px'>"
        f"<span class='badge {badge_cls}'>{status_label(status)}</span>"
        f"</div></div>",
        unsafe_allow_html=True,
    )
with k3:
    s_nhnn = scores.get("s_nhnn", 0)
    nhnn_icon = {-1: "📉 Thắt chặt", 0: "➡️ Trung lập", 1: "📈 Nới lỏng"}.get(s_nhnn, "—")
    st.markdown(
        f"<div class='metric-card card-green'>"
        f"<div style='font-size:.8rem;color:#aaa'>CHÍNH SÁCH NHNN</div>"
        f"<div style='font-size:1.3rem;font-weight:700;margin-top:6px'>{nhnn_icon}</div>"
        f"</div>",
        unsafe_allow_html=True,
    )
with k4:
    msgs   = metadata.get("social_messages_processed", 0)
    proc_t = metadata.get("processing_time_seconds", 0)
    st.markdown(
        f"<div class='metric-card card-blue'>"
        f"<div style='font-size:.8rem;color:#aaa'>HIỆU SUẤT</div>"
        f"<div style='font-size:1.2rem;font-weight:700;margin-top:4px'>{msgs} msgs</div>"
        f"<div style='font-size:.75rem;color:#aaa'>{proc_t:.3f}s / chu kỳ</div>"
        f"</div>",
        unsafe_allow_html=True,
    )


# ══════════════════════════════════════════════════════════════════════════════
# HANG 2: BIEU DO VMSI + RADAR
# ══════════════════════════════════════════════════════════════════════════════
chart_col, radar_col = st.columns([6, 4], gap="large")

with chart_col:
    st.markdown("##### 📈 Lịch sử VMSI (phiên hiện tại)")
    hist_x = st.session_state.get("time_history", ["now"])
    hist_y = st.session_state.get("vmsi_history", [vmsi_val])

    fig_line = go.Figure()
    fig_line.add_hrect(y0=0,  y1=25,  fillcolor="rgba(255,82,82,.07)",   line_width=0,
                       annotation_text="Hoảng loạn",  annotation_position="bottom right",
                       annotation_font_color="#ff5252", annotation_font_size=10)
    fig_line.add_hrect(y0=75, y1=100, fillcolor="rgba(0,200,83,.07)",    line_width=0,
                       annotation_text="Tham lam",    annotation_position="top right",
                       annotation_font_color="#00c853", annotation_font_size=10)
    fig_line.add_hrect(y0=25, y1=75,  fillcolor="rgba(79,195,247,.03)",  line_width=0)
    fig_line.add_trace(go.Scatter(
        x=hist_x, y=hist_y,
        mode='lines+markers',
        line=dict(color='#4fc3f7', width=2.5),
        marker=dict(size=5),
        fill='tozeroy',
        fillcolor='rgba(79,195,247,.08)',
    ))
    fig_line.add_hline(
        y=vmsi_val, line_dash="dot", line_color=vmsi_color(vmsi_val),
        annotation_text=f"  {vmsi_val:.1f}", annotation_position="right",
        annotation_font_size=11,
    )
    fig_line.update_layout(
        plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)',
        height=300, margin=dict(l=10, r=50, t=10, b=30),
        yaxis=dict(range=[0, 100], gridcolor='#333', tickfont=dict(size=10)),
        xaxis=dict(gridcolor='#333', tickfont=dict(size=9)),
        showlegend=False,
        font=dict(family="Be Vietnam Pro, Segoe UI, Arial, sans-serif"),
    )
    st.plotly_chart(fig_line, use_container_width=True)

with radar_col:
    st.markdown("##### 🕸️ Component Scores")

    s_social = scores.get("s_social", 0.0)
    s_macro  = scores.get("s_macro",  0.0)
    s_nhnn_f = float(scores.get("s_nhnn", 0))
    s_news   = scores.get("s_news",   0.0)

    def to_pct(v): return round((float(v) + 1) * 50, 1)

    cats = ["Social", "Macro", "NHNN", "News", "VMSI"]
    vals = [to_pct(s_social), to_pct(s_macro), to_pct(s_nhnn_f), to_pct(s_news), vmsi_val]

    fig_radar = go.Figure(go.Scatterpolar(
        r=vals + [vals[0]],
        theta=cats + [cats[0]],
        fill='toself',
        fillcolor='rgba(79,195,247,.15)',
        line=dict(color='#4fc3f7', width=2),
        marker=dict(size=5),
    ))
    fig_radar.update_layout(
        polar=dict(
            radialaxis=dict(visible=True, range=[0, 100], tickfont=dict(size=9), gridcolor='#444'),
            angularaxis=dict(tickfont=dict(size=11)),
            bgcolor='rgba(0,0,0,0)',
        ),
        paper_bgcolor='rgba(0,0,0,0)',
        height=300,
        margin=dict(l=20, r=20, t=20, b=20),
        showlegend=False,
        font=dict(family="Be Vietnam Pro, Segoe UI, Arial, sans-serif"),
    )
    st.plotly_chart(fig_radar, use_container_width=True)


# ══════════════════════════════════════════════════════════════════════════════
# HANG 3: CANH BAO & CHATBOT
# ══════════════════════════════════════════════════════════════════════════════
st.divider()
left_col, right_col = st.columns([5, 5], gap="large")


# ── LEFT: Khuyen nghi & Canh bao ─────────────────────────────────────────────
with left_col:
    st.markdown("##### 🎯 Khuyến nghị hành động")
    a1, a2, a3 = st.columns(3)
    a1.markdown(
        "<div class='metric-card card-green'><b>MUA VÀO</b><br>"
        "<small>VMSI &lt; 25</small></div>",
        unsafe_allow_html=True,
    )
    a2.markdown(
        "<div class='metric-card card-yellow'><b>NẮM GIỮ</b><br>"
        "<small>VMSI 25–75</small></div>",
        unsafe_allow_html=True,
    )
    a3.markdown(
        "<div class='metric-card card-red'><b>GIẢM MARGIN</b><br>"
        "<small>VMSI &gt; 75</small></div>",
        unsafe_allow_html=True,
    )

    # Risk warning
    risk_warning = vmsi_data.get("risk_warning", "")
    if risk_warning:
        st.markdown("##### ⚠️ Cảnh báo rủi ro")
        if any(kw in risk_warning for kw in ["ĐỎ", "FOMO", "Hoảng"]):
            st.error(risk_warning)
        elif any(kw in risk_warning.lower() for kw in ["e dè", "thận trọng"]):
            st.warning(risk_warning)
        else:
            st.info(risk_warning)

    # Macro summary
    macro_summary = vmsi_data.get("macro_summary", "")
    fallback_msg  = "Khong lay duoc bao cao vi mo (fallback)."
    if macro_summary and macro_summary != fallback_msg:
        st.markdown("##### 📋 Phân tích vĩ mô NHNN")
        st.info(macro_summary)

    # Bang diem thanh phan
    st.markdown("##### 📊 Chi tiết điểm thành phần")
    df_scores = pd.DataFrame({
        "Thành phần": [
            "S_Social (Mạng xã hội)",
            "S_Macro (Vĩ mô)",
            "S_NHNN (Chính sách)",
            "S_News (Tin tức)",
            "VMSI Raw",
        ],
        "Giá trị": [
            f"{s_social:+.4f}",
            f"{s_macro:+.4f}",
            f"{s_nhnn_f:+.0f}",
            f"{s_news:+.4f}",
            f"{scores.get('vmsi_raw', vmsi_val):.2f} / 100",
        ],
        "Dải": ["[-1, 1]", "[-1, 1]", "{-1, 0, 1}", "[-1, 1]", "[0, 100]"],
    })
    st.dataframe(df_scores, hide_index=True, use_container_width=True)


# ── RIGHT: Chatbot AI ─────────────────────────────────────────────────────────
with right_col:
    st.markdown("##### 💬 Chatbot tư vấn tài chính AI")

    # Quick actions
    q1, q2, q3, q4 = st.columns(4)
    with q1:
        if st.button("📊 VMSI?", use_container_width=True):
            st.session_state["_quick"] = "VMSI la gi va hien tai dang o muc nao?"
    with q2:
        if st.button("🎯 Khuyen nghi", use_container_width=True):
            st.session_state["_quick"] = "Cho toi khuyen nghi dau tu cu the dua tren VMSI hien tai."
    with q3:
        if st.button("⚠️ Rui ro", use_container_width=True):
            st.session_state["_quick"] = "Phan tich rui ro thi truong hien tai giup toi."
    with q4:
        if st.button("🏦 NHNN", use_container_width=True):
            st.session_state["_quick"] = "Chinh sach NHNN anh huong the nao den thi truong?"

    # Khoi tao lich su chat
    if "messages" not in st.session_state:
        st.session_state.messages = [{
            "role": "assistant",
            "content": (
                f"👋 Xin chào! Tôi là chuyên gia tư vấn AI của **FinSent-Agent**.\n\n"
                f"📊 VMSI hiện tại: **{vmsi_val:.1f}/100** — {status_label(status)}.\n\n"
                f"Tôi có thể giúp gì cho bạn hôm nay?"
            ),
        }]

    # Hien thi lich su chat
    chat_box = st.container(height=330)
    with chat_box:
        for msg in st.session_state.messages:
            with st.chat_message(msg["role"]):
                st.markdown(msg["content"])

    # Xu ly quick action
    quick_prompt = st.session_state.pop("_quick", None)
    user_input   = st.chat_input("Hỏi về chiến lược đầu tư, VMSI, rủi ro...")
    final_prompt = quick_prompt or user_input

    if final_prompt:
        st.session_state.messages.append({"role": "user", "content": final_prompt})
        with chat_box:
            with st.chat_message("user"):
                st.markdown(final_prompt)

        with chat_box:
            with st.chat_message("assistant"):
                with st.spinner("Đang phân tích..."):
                    try:
                        bot      = get_chatbot()
                        response = bot.generate_advice(
                            user_query=final_prompt,
                            vmsi_context=vmsi_data,
                        )
                    except Exception as e:
                        response = (
                            f"⚠️ Lỗi chatbot: {e}\n\n"
                            f"VMSI **{vmsi_val:.1f}** — {status_label(status)}: "
                            f"{vmsi_data.get('risk_warning', '')}"
                        )
                st.markdown(response)

        st.session_state.messages.append({"role": "assistant", "content": response})
        st.rerun()

    # Nut xoa lich su
    if len(st.session_state.messages) > 1:
        if st.button("🗑️ Xóa lịch sử chat", use_container_width=True):
            st.session_state.messages = [{
                "role": "assistant",
                "content": f"🔄 Đã xóa. VMSI: **{vmsi_val:.1f}** — {status_label(status)}.",
            }]
            st.rerun()


# ══════════════════════════════════════════════════════════════════════════════
# AUTO REFRESH
# ══════════════════════════════════════════════════════════════════════════════
if auto_refresh:
    with st.empty():
        st.caption(f"⏱ Auto-refresh sau {refresh_secs}s...")
    time.sleep(refresh_secs)
    st.rerun()
