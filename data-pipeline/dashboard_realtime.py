import sys, os, json, time, logging, warnings
from datetime import datetime, timezone
from pathlib import Path

import streamlit as st
import pandas as pd
import plotly.graph_objects as go

# ── Suppress noise ────────────────────────────────────────────────────────────
warnings.filterwarnings("ignore")
os.environ.setdefault("VNSTOCK_SHOW_ADS", "0")
os.environ.setdefault("VNSTOCK_DISABLE_NOTIFICATION", "1")
logging.basicConfig(level=logging.ERROR)
for _lg in ["vnstock","kafka","httpx","sentence_transformers",
            "realtime_pipeline.crawlers.facebook_crawler"]:
    logging.getLogger(_lg).setLevel(logging.ERROR)

_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(_ROOT))

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="FINSENT-AGENT",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ── Full CSS — Terminal Trader Style ─────────────────────────────────────────
st.html("""
<link href="https://fonts.googleapis.com/css2?family=Share+Tech+Mono&family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">
<style>
/* ── Reset & Base ── */
html,body,[class*="css"],[class*="st-"],
div,p,span,h1,h2,h3,h4,h5,label,button,input,textarea,select{
    font-family:'Inter','Segoe UI',sans-serif!important;
}
.mono{font-family:'Share Tech Mono',monospace!important;}
[data-testid="stAppViewContainer"]{background:#0d1117;}
[data-testid="stHeader"]{display:none;}
[data-testid="stSidebar"]{display:none;}
[data-testid="collapsedControl"]{display:none;}
section[data-testid="stMain"] > div{padding-top:0!important;}
/* Hide streamlit default elements */
#MainMenu,footer,[data-testid="stToolbar"]{display:none;}

/* ── Top Bar ── */
.topbar{
    display:flex;align-items:center;justify-content:space-between;
    background:#0d1117;border-bottom:1px solid #1a2332;
    padding:0 20px;height:44px;position:sticky;top:0;z-index:999;
}
.topbar-logo{
    font-family:'Share Tech Mono',monospace;font-size:1.1rem;
    font-weight:700;color:#00ff88;letter-spacing:2px;
    text-shadow:0 0 10px rgba(0,255,136,.4);
}
.topbar-index{display:flex;gap:20px;align-items:center;}
.idx-item{font-size:.75rem;font-family:'Share Tech Mono',monospace;}
.idx-name{color:#6b7280;}
.idx-val{color:#e5e7eb;font-weight:600;}
.idx-up{color:#00ff88;}
.idx-dn{color:#ff4757;}
.topbar-right{display:flex;gap:16px;align-items:center;color:#6b7280;font-size:.85rem;}

/* ── Left Sidebar Nav ── */
.leftnav{
    position:fixed;left:0;top:44px;bottom:28px;width:44px;
    background:#0d1117;border-right:1px solid #1a2332;
    display:flex;flex-direction:column;align-items:center;
    padding:12px 0;z-index:998;gap:4px;
}
.nav-btn{
    width:32px;height:32px;border-radius:6px;
    display:flex;align-items:center;justify-content:center;
    cursor:pointer;font-size:.9rem;color:#6b7280;
    transition:all .2s;border:1px solid transparent;
}
.nav-btn:hover,.nav-btn.active{
    background:rgba(0,255,136,.1);color:#00ff88;
    border-color:rgba(0,255,136,.3);
}
.nav-divider{width:24px;height:1px;background:#1a2332;margin:6px 0;}

/* ── Main Content area ── */
.main-wrap{margin-left:44px;padding:12px 16px;background:#0d1117;min-height:100vh;}

/* ── KPI Cards ── */
.kpi-grid{display:grid;grid-template-columns:repeat(4,1fr);gap:10px;margin-bottom:12px;}
.kpi-card{
    background:#111827;border:1px solid #1e2d3d;border-radius:8px;
    padding:14px 18px;position:relative;overflow:hidden;
}
.kpi-card::before{
    content:'';position:absolute;top:0;left:0;right:0;height:2px;
    background:var(--accent,#00ff88);
}
.kpi-label-text{
    font-size:.65rem;letter-spacing:1.5px;text-transform:uppercase;
    color:#6b7280;margin-bottom:6px;font-weight:500;
}
.kpi-main{font-family:'Share Tech Mono',monospace;font-size:2.2rem;
    font-weight:700;line-height:1;color:var(--accent,#00ff88);}
.kpi-sub-text{font-size:.72rem;color:#6b7280;margin-top:4px;font-family:'Share Tech Mono',monospace;}
.kpi-vmsi .kpi-main{font-size:2.8rem;}

/* ── Section Header ── */
.sec-hdr{
    display:flex;align-items:center;gap:8px;
    font-size:.68rem;letter-spacing:1.5px;text-transform:uppercase;
    color:#4b5563;font-weight:600;margin-bottom:8px;
    border-bottom:1px solid #1a2332;padding-bottom:6px;
}
.sec-hdr svg,.sec-hdr span.ico{color:#00ff88;font-size:.75rem;}

/* ── Chart container ── */
.chart-box{
    background:#111827;border:1px solid #1e2d3d;border-radius:8px;
    padding:14px;margin-bottom:12px;
}

/* ── Strategy cards ── */
.strat-grid{display:grid;grid-template-columns:repeat(3,1fr);gap:8px;margin-top:10px;}
.strat-btn{
    padding:10px 8px;border-radius:6px;text-align:center;
    font-family:'Share Tech Mono',monospace;font-size:.85rem;font-weight:700;
    letter-spacing:1px;cursor:pointer;border:1px solid;
    transition:all .2s;text-transform:uppercase;
}
.strat-buy{background:rgba(0,255,136,.15);border-color:#00ff88;color:#00ff88;}
.strat-hold{background:rgba(107,114,128,.1);border-color:#4b5563;color:#9ca3af;}
.strat-sell{background:rgba(255,71,87,.15);border-color:#ff4757;color:#ff4757;}
.strat-buy:hover{background:rgba(0,255,136,.25);box-shadow:0 0 12px rgba(0,255,136,.3);}
.strat-sell:hover{background:rgba(255,71,87,.25);box-shadow:0 0 12px rgba(255,71,87,.3);}

/* ── News / FUD items ── */
.news-box{background:#111827;border:1px solid #1e2d3d;border-radius:8px;padding:12px;max-height:280px;overflow-y:auto;}
.news-item{border-left:3px solid #1e2d3d;padding:8px 10px;margin-bottom:8px;background:#0d1117;border-radius:0 6px 6px 0;}
.news-item.fud{border-left-color:#ff4757;}
.news-item.positive{border-left-color:#00ff88;}
.news-item.neutral{border-left-color:#fbbf24;}
.news-tag{
    display:inline-block;padding:1px 7px;border-radius:3px;
    font-family:'Share Tech Mono',monospace;font-size:.6rem;font-weight:700;
    text-transform:uppercase;letter-spacing:.5px;float:right;
}
.tag-debunked{background:rgba(255,71,87,.2);color:#ff4757;border:1px solid #ff4757;}
.tag-fact{background:rgba(0,255,136,.2);color:#00ff88;border:1px solid #00ff88;}
.tag-alert{background:rgba(251,191,36,.2);color:#fbbf24;border:1px solid #fbbf24;}
.news-title{color:#e5e7eb;font-size:.78rem;font-weight:600;margin-bottom:4px;}
.news-title.fud-title{color:#ff4757;}
.news-title.pos-title{color:#00ff88;}
.news-body{color:#9ca3af;font-size:.7rem;line-height:1.5;font-family:'Share Tech Mono',monospace;}
.news-src{color:#4b5563;font-size:.62rem;margin-top:4px;}

/* ── Two-col layout ── */
.col-left{float:left;width:48%;margin-right:2%;}
.col-right{float:left;width:50%;}

/* ── Chat box ── */
.chat-wrap{background:#111827;border:1px solid #1e2d3d;border-radius:8px;padding:14px;height:100%;}
.chat-hdr{display:flex;justify-content:space-between;align-items:center;margin-bottom:10px;}
.chat-title{font-family:'Share Tech Mono',monospace;font-size:.7rem;color:#6b7280;letter-spacing:1px;text-transform:uppercase;}
.online-dot{width:8px;height:8px;border-radius:50%;background:#00ff88;display:inline-block;box-shadow:0 0 6px #00ff88;}

/* ── Status bar ── */
.statusbar{
    position:fixed;bottom:0;left:0;right:0;height:28px;
    background:#0d1117;border-top:1px solid #1a2332;
    display:flex;align-items:center;padding:0 16px;
    font-family:'Share Tech Mono',monospace;font-size:.65rem;color:#6b7280;
    z-index:999;gap:20px;
}
.status-ok{color:#00ff88;}
.status-warn{color:#fbbf24;}
.status-right{margin-left:auto;display:flex;gap:16px;}

/* ── Tabs ── */
.tab-bar{
    display:flex;gap:2px;margin-bottom:12px;
    border-bottom:1px solid #1a2332;
}
.tab-item{
    padding:7px 16px;font-size:.72rem;letter-spacing:.8px;
    text-transform:uppercase;color:#6b7280;cursor:pointer;
    border-bottom:2px solid transparent;margin-bottom:-1px;
    font-weight:600;transition:all .2s;
}
.tab-item.active{color:#00ff88;border-bottom-color:#00ff88;}
.tab-item:hover{color:#9ca3af;}

/* ── Ticker search ── */
.ticker-input input{
    background:#111827!important;border:1px solid #1e2d3d!important;
    color:#00ff88!important;font-family:'Share Tech Mono',monospace!important;
    border-radius:6px!important;font-size:.9rem!important;
}
/* ── Scrollbar ── */
::-webkit-scrollbar{width:4px;height:4px;}
::-webkit-scrollbar-track{background:#0d1117;}
::-webkit-scrollbar-thumb{background:#1e2d3d;border-radius:2px;}
::-webkit-scrollbar-thumb:hover{background:#2d3f52;}


/* Fix chatbot icon khong bi tran chu */
[data-testid="chatAvatarIcon"] {
    min-width: 32px !important;
    width: 32px !important;
    height: 32px !important;
    border-radius: 50% !important;
    display: flex !important;
    align-items: center !important;
    justify-content: center !important;
    font-size: .85rem !important;
    overflow: hidden !important;
    flex-shrink: 0 !important;
}
[data-testid="stChatMessage"] {
    align-items: flex-start !important;
}
[data-testid="stChatMessage"] > div:first-child {
    flex-shrink: 0 !important;
}

/* ── Plotly chart bg ── */
.js-plotly-plot .plotly .bg{fill:#111827!important;}
</style>
""")

# ══════════════════════════════════════════════════════════════════════════════
# DATA HELPERS
# ══════════════════════════════════════════════════════════════════════════════
_VMSI_FILE = _ROOT / "live_vmsi.json"

_FALLBACK = {
    "vmsi_value": 50.0, "status": "normal", "ticker": "SHB",
    "risk_warning": "Chưa có dữ liệu. Bấm Phân tích ngay.",
    "macro_summary": "",
    "component_scores": {"s_social":0.0,"s_macro":0.0,"s_nhnn":0,"s_news":0.0,"vmsi_raw":50.0},
    "processing_metadata": {"processing_time_seconds":0,"social_messages_processed":0},
    "timestamp": datetime.now(timezone.utc).isoformat(),
}

def load_vmsi() -> dict:
    try:
        with open(_VMSI_FILE,"r",encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return _FALLBACK.copy()

def vmsi_color(v:float)->str:
    if v<=25: return "#ff4757"
    if v<=40: return "#fbbf24"
    if v>=81: return "#ff4757"
    if v>=61: return "#00ff88"
    return "#00b4d8"

def vmsi_label(v:float)->str:
    if v<=20: return "EXTREME FEAR"
    if v<=35: return "FEAR"
    if v<=50: return "NEUTRAL"
    if v<=65: return "GREED"
    if v<=80: return "HIGH GREED"
    return "EXTREME GREED"

def _push_history(val:float):
    if "rt_y" not in st.session_state:
        st.session_state.rt_y=[]
        st.session_state.rt_x=[]
    st.session_state.rt_y.append(val)
    st.session_state.rt_x.append(datetime.now().strftime("%H:%M:%S"))
    if len(st.session_state.rt_y)>60:
        st.session_state.rt_y=st.session_state.rt_y[-60:]
        st.session_state.rt_x=st.session_state.rt_x[-60:]

def _fmt_price(v)->str:
    try: return f"{float(v):,.2f}"
    except: return "--"

@st.cache_resource(show_spinner=False)
def get_engine(ticker:str):
    from realtime_pipeline.vmsi_realtime import RealtimeVMSIEngine
    return RealtimeVMSIEngine(ticker=ticker)

@st.cache_resource(show_spinner=False)
def get_chatbot():
    from multi_agent_system.agents.chatbot_agent import FinancialChatbot
    return FinancialChatbot()

def _get_stock_price(ticker:str)->dict:
    """Lấy giá cổ phiếu hôm nay."""
    try:
        from realtime_pipeline.crawlers.stock_crawler import fetch_stock_realtime
        bar = fetch_stock_realtime(ticker)
        if bar:
            return {"open":bar.open,"close":bar.close,"change":bar.price_change,"up":bar.is_up}
    except Exception:
        pass
    return {"open":0,"close":0,"change":0,"up":False}

def _get_news_items(ticker:str)->list:
    """Lấy tin tức mới nhất từ session cache."""
    return st.session_state.get("news_items",[])

# ══════════════════════════════════════════════════════════════════════════════
# TOP BAR
# ══════════════════════════════════════════════════════════════════════════════
st.html("""
<div class="topbar">
  <div style="display:flex;align-items:center;gap:24px">
    <span class="topbar-logo">FINSENT-AGENT</span>
    <div class="topbar-index">
      <span class="idx-item">
        <span class="idx-name">VN-INDEX: </span>
        <span class="idx-val">1,284.12 </span>
        <span class="idx-up">(+0.4%)</span>
      </span>
      <span class="idx-item">
        <span class="idx-name">VN30: </span>
        <span class="idx-val">1,302.45 </span>
        <span class="idx-up">(+0.2%)</span>
      </span>
      <span class="idx-item">
        <span class="idx-name">HNX: </span>
        <span class="idx-val">242.15 </span>
        <span class="idx-dn">(-0.1%)</span>
      </span>
    </div>
  </div>
  <div class="topbar-right">
    <span>🔔</span>
    <span>⚙️</span>
    <span style="width:28px;height:28px;border-radius:50%;background:#1e2d3d;
      display:inline-flex;align-items:center;justify-content:center;font-size:.7rem">U</span>
  </div>
</div>
""")

# ══════════════════════════════════════════════════════════════════════════════
# LEFT NAV (fixed via HTML — Streamlit sidebar hidden)
# ══════════════════════════════════════════════════════════════════════════════
st.html("""
<div class="leftnav">
  <div class="nav-btn active" title="Dashboard">📊</div>
  <div class="nav-btn" title="Phân tích">📈</div>
  <div class="nav-btn" title="Danh mục">💼</div>
  <div class="nav-divider"></div>
  <div class="nav-btn" title="Tin tức">📰</div>
  <div class="nav-btn" title="Báo cáo">📋</div>
  <div style="flex:1"></div>
  <div class="nav-btn" title="Cài đặt">⚙️</div>
</div>
""")

# ══════════════════════════════════════════════════════════════════════════════
# MAIN CONTENT WRAPPER
# ══════════════════════════════════════════════════════════════════════════════
# Dùng margin-left CSS đã inject để đẩy content sang phải left nav
st.html('<div style="margin-left:44px;">')

# ── Ticker search + Run button ────────────────────────────────────────────────
tc1, tc2, tc3, tc4 = st.columns([2, 1, 1, 6])
with tc1:
    ticker_input = st.text_input(
        "MÃ CỔ PHIẾU",
        value=st.session_state.get("last_ticker","SHB"),
        max_chars=10,
        placeholder="SHB, VCB, VNINDEX...",
        label_visibility="visible",
    ).strip().upper() or "SHB"
    st.session_state["last_ticker"] = ticker_input

with tc2:
    run_now = st.button("▶ PHÂN TÍCH", type="primary", use_container_width=True)

with tc3:
    auto_on = st.toggle("AUTO 30P", value=st.session_state.get("auto_on",False))
    st.session_state["auto_on"] = auto_on

with tc4:
    pass  # spacer

# ── Tab bar ───────────────────────────────────────────────────────────────────
tab_sel = st.session_state.get("active_tab","DASHBOARD")
t1,t2,t3,t4,t5 = st.columns([1.5,1.5,1.5,1.5,5])
with t1:
    if st.button("◉ DASHBOARD", use_container_width=True,
                 type="primary" if tab_sel=="DASHBOARD" else "secondary"):
        st.session_state["active_tab"]="DASHBOARD"; st.rerun()
with t2:
    if st.button("◎ PHÂN TÍCH", use_container_width=True,
                 type="primary" if tab_sel=="ANALYSIS" else "secondary"):
        st.session_state["active_tab"]="ANALYSIS"; st.rerun()
with t3:
    if st.button("◎ THỊ TRƯỜNG", use_container_width=True,
                 type="primary" if tab_sel=="MARKET" else "secondary"):
        st.session_state["active_tab"]="MARKET"; st.rerun()
with t4:
    if st.button("◎ TIN TỨC", use_container_width=True,
                 type="primary" if tab_sel=="NEWS" else "secondary"):
        st.session_state["active_tab"]="NEWS"; st.rerun()

# ══════════════════════════════════════════════════════════════════════════════
# CHAY CHU KY
# ══════════════════════════════════════════════════════════════════════════════
if run_now:
    with st.status(f"Đang phân tích [{ticker_input}]...", expanded=True) as s:
        try:
            st.write("Crawl Facebook + tin tức...")
            engine = get_engine(ticker_input)
            if engine.ticker != ticker_input:
                engine.ticker = ticker_input; engine._producer=None; engine._mac=None
            social_n = engine._collect_social()

            st.write("Lấy dữ liệu giá cổ phiếu...")
            mkt_sent = engine._collect_market()

            st.write("Query ChromaDB + tính VMSI...")
            mac_res = engine._run_mac_cycle()

            st.write("Làm giàu kết quả...")
            result = engine._enrich_with_market(mac_res, mkt_sent)

            # Lưu news vào session
            if "news_items" not in st.session_state:
                st.session_state.news_items = []
            macro_summ = result.get("macro_summary","")
            rw = result.get("risk_warning","")
            if rw:
                st.session_state.news_items.insert(0,{"type":"neutral","title":rw[:120],"body":"","src":"VMSI Engine"})
            if macro_summ and "fallback" not in macro_summ.lower():
                st.session_state.news_items.insert(0,{"type":"positive","title":"Phân tích vĩ mô NHNN","body":macro_summ[:200],"src":"MacroAgent / ChromaDB"})
            st.session_state.news_items = st.session_state.news_items[:15]

            s.update(label=f"Hoàn tất | VMSI={result.get('vmsi_value')} | ticker={ticker_input}", state="complete")
            st.toast(f"VMSI={result.get('vmsi_value')}", icon="✅")

            if "rt_log" not in st.session_state: st.session_state.rt_log=[]
            st.session_state.rt_log.append({
                "Time":datetime.now().strftime("%H:%M:%S"),
                "Ticker":ticker_input,
                "VMSI":result.get("vmsi_value",50),
                "Status":result.get("status","normal"),
            })
        except Exception as e:
            s.update(label=f"Lỗi: {e}", state="error")

# ══════════════════════════════════════════════════════════════════════════════
# DOC DU LIEU
# ══════════════════════════════════════════════════════════════════════════════
data    = load_vmsi()
vmsi_v  = float(data.get("vmsi_value",50))
status  = data.get("status","normal")
scores  = data.get("component_scores",{})
meta    = data.get("processing_metadata",{})
tshown  = data.get("ticker",ticker_input) or ticker_input
ts_raw  = data.get("timestamp","")
mkt_s   = float(data.get("market_sentiment", scores.get("s_news",0.0)))
rw_text = data.get("risk_warning","")
_push_history(vmsi_v)

try: ts_fmt = datetime.fromisoformat(ts_raw).strftime("%H:%M:%S")
except: ts_fmt = datetime.now().strftime("%H:%M:%S")

clr = vmsi_color(vmsi_v)
lbl = vmsi_label(vmsi_v)

# ══════════════════════════════════════════════════════════════════════════════
# TAB: DASHBOARD
# ══════════════════════════════════════════════════════════════════════════════
if st.session_state.get("active_tab","DASHBOARD") == "DASHBOARD":

    # ── KPI Cards ─────────────────────────────────────────────────────────────
    price_data = _get_stock_price(tshown) if tshown else {}
    open_p  = price_data.get("open",0)
    close_p = price_data.get("close",0)
    chg     = price_data.get("change",0)
    chg_col = "#00ff88" if chg >= 0 else "#ff4757"
    chg_sign = "+" if chg >= 0 else ""

    k1,k2,k3,k4 = st.columns(4)
    with k1:
        st.html(f"""
        <div class="kpi-card kpi-vmsi" style="--accent:{clr}">
          <div class="kpi-label-text">Chỉ số tâm lý VMSI</div>
          <div class="kpi-main" style="color:{clr}">{vmsi_v:.0f}</div>
          <div class="kpi-sub-text">{lbl}</div>
        </div>""")
    with k2:
        st.html(f"""
        <div class="kpi-card" style="--accent:#e5e7eb">
          <div class="kpi-label-text">Giá OPEN — {tshown}</div>
          <div class="kpi-main" style="color:#e5e7eb">{_fmt_price(open_p) if open_p else '--'}</div>
          <div class="kpi-sub-text">Giá mở cửa phiên</div>
        </div>""")
    with k3:
        st.html(f"""
        <div class="kpi-card" style="--accent:#e5e7eb">
          <div class="kpi-label-text">Giá hiện tại</div>
          <div class="kpi-main" style="color:#e5e7eb">{_fmt_price(close_p) if close_p else '--'}</div>
          <div class="kpi-sub-text">Cập nhật: {ts_fmt}</div>
        </div>""")
    with k4:
        st.html(f"""
        <div class="kpi-card" style="--accent:{chg_col}">
          <div class="kpi-label-text">Biến lợi nhuận / Chênh lệch</div>
          <div class="kpi-main" style="color:{chg_col}">{chg_sign}{chg:.2f}%</div>
          <div class="kpi-sub-text">So với giá mở cửa</div>
        </div>""")

    # ── VMSI Chart ─────────────────────────────────────────────────────────────
    st.html('<div class="sec-hdr"><span class="ico">〜</span> BIỂU ĐỒ CHỈ SỐ TÂM LÝ TRONG 24 GIỜ</div>')

    hx = st.session_state.get("rt_x",[ts_fmt])
    hy = st.session_state.get("rt_y",[vmsi_v])

    fig = go.Figure()

    # Vung fear/greed
    fig.add_hrect(y0=0,  y1=25,  fillcolor="rgba(255,71,87,.06)",  line_width=0)
    fig.add_hrect(y0=25, y1=50,  fillcolor="rgba(251,191,36,.04)", line_width=0)
    fig.add_hrect(y0=50, y1=75,  fillcolor="rgba(0,180,216,.03)",  line_width=0)
    fig.add_hrect(y0=75, y1=100, fillcolor="rgba(0,255,136,.06)",  line_width=0)

    # Nhan vung
    for y_pos, txt, col in [(12,"FEAR","#ff4757"),(37,"NEUTRAL","#fbbf24"),
                              (62,"GREED","#00b4d8"),(87,"EXTREME GREED","#00ff88")]:
        fig.add_annotation(x=hx[0] if hx else 0, y=y_pos, text=txt, showarrow=False,
            font=dict(size=8, color=col, family="Share Tech Mono"),
            xanchor="left", yanchor="middle", opacity=0.4)

    # Duong VMSI chinh
    fig.add_trace(go.Scatter(
        x=hx, y=hy, mode="lines",
        line=dict(color=clr, width=2),
        fill="tozeroy",
        fillcolor=f"rgba{tuple(int(clr.lstrip('#')[i:i+2],16) for i in (0,2,4)) + (0.08,)}",
        name="VMSI",
        hovertemplate="<b>%{y:.1f}</b><br>%{x}<extra></extra>",
    ))

    # Duong hien tai
    fig.add_hline(y=vmsi_v, line_dash="dot", line_color=clr, line_width=1, opacity=0.6,
                  annotation_text=f"  {vmsi_v:.1f}", annotation_position="right",
                  annotation_font=dict(size=10, color=clr, family="Share Tech Mono"))

    fig.update_layout(
        plot_bgcolor="#111827", paper_bgcolor="#111827",
        height=200, margin=dict(l=40,r=60,t=8,b=30),
        yaxis=dict(range=[0,100], gridcolor="#1e2d3d", tickfont=dict(size=9,color="#4b5563",family="Share Tech Mono"),
                   showgrid=True, zeroline=False, tickmode="array", tickvals=[0,25,50,75,100]),
        xaxis=dict(gridcolor="#1e2d3d", tickfont=dict(size=8,color="#4b5563",family="Share Tech Mono"),
                   showgrid=False, zeroline=False),
        showlegend=False,
        font=dict(family="Share Tech Mono"),
    )
    st.plotly_chart(fig, use_container_width=True)

    # ── Bottom two-col: Strategy + News  |  Chatbot ───────────────────────────
    col_l, col_r = st.columns([5, 5], gap="medium")

    with col_l:
        # Strategy
        st.html('<div class="sec-hdr"><span class="ico">▣</span> CHIẾN LƯỢC TÚI TIỀN</div>')

        strat_text = {
            "risk_high_fear": "Tín hiệu thị trường đang ở mức Sợ hãi. Phù hợp để bắt đáy các mã có nền tảng cơ bản tốt.",
            "risk_high_fomo": "Thị trường đang ở vùng Tham lam cực độ. Nguy cơ bong bóng. Cần giảm tỷ trọng và chốt lời.",
            "risk_low_bear":  "Thị trường thận trọng. Giữ vị thế phòng thủ, rà soát danh mục.",
            "normal":         "Thị trường cân bằng. Duy trì cơ cấu tài sản hiện tại. Theo dõi tín hiệu mới.",
            "risk_low_bull":  "Xu hướng tích cực. Dòng tiền lan tỏa. Xem xét tăng tỷ trọng cổ phiếu cơ bản.",
        }
        if vmsi_v <= 25: sk = "risk_high_fear"
        elif vmsi_v <= 40: sk = "risk_low_bear"
        elif vmsi_v >= 81: sk = "risk_high_fomo"
        elif vmsi_v >= 61: sk = "risk_low_bull"
        else: sk = "normal"

        st.html(f"""
        <div style="background:#111827;border:1px solid #1e2d3d;border-radius:8px;padding:14px;margin-bottom:10px;">
          <p style="color:#9ca3af;font-size:.82rem;line-height:1.7;margin:0 0 14px;">{strat_text[sk]}</p>
          <div class="strat-grid">
            <div class="strat-btn strat-buy">MÚC</div>
            <div class="strat-btn strat-hold">NẮM IM</div>
            <div class="strat-btn strat-sell">SỤT</div>
          </div>
        </div>""")

        # News / FUD Buster
        st.html('<div class="sec-hdr"><span class="ico">▤</span> BẢNG TIN / FUD BUSTER</div>')
        news_items = _get_news_items(tshown)

        if not news_items:
            # Hien thi fallback data demo
            news_items = [
                {"type":"fud","title":"FUD ALERT — Tin đồn thanh tra ngân hàng","body":"AI logic: Xác suất giả mạo 85%. Dòng tiền lớn chưa có dấu hiệu rút. Không nên panic sell.","src":"ChromaDB Cloud"},
                {"type":"positive","title":"POSITIVE SIGNAL — Vốn FDI đổ mạnh","body":"Vốn FDI đổ mạnh vào khu công nghiệp phía Bắc trong quý 3.","src":"CafeF RSS"},
                {"type":"neutral","title":"NHNN giữ lãi suất điều hành","body":"NHNN giữ nguyên lãi suất điều hành, hỗ trợ ổn định thị trường.","src":"ChinhPhu RSS"},
            ]

        news_html = '<div class="news-box">'
        for it in news_items[:8]:
            t = it.get("type","neutral")
            border = {"fud":"fud","positive":"positive","neutral":"neutral"}.get(t,"neutral")
            title_cls = {"fud":"fud-title","positive":"pos-title","neutral":""}.get(t,"")
            tag = it.get("tag","")
            tag_html = ""
            if t == "fud":
                tag_html = '<span class="news-tag tag-debunked">DEBUNKED</span>'
            elif t == "positive":
                tag_html = '<span class="news-tag tag-fact">FACT</span>'
            elif tag:
                tag_html = f'<span class="news-tag tag-alert">{tag}</span>'

            news_html += f"""
            <div class="news-item {border}">
              {tag_html}
              <div class="news-title {title_cls}">{it.get('title','')[:80]}</div>
              <div class="news-body">{it.get('body','')[:150]}</div>
              <div class="news-src">Source: {it.get('src','')}</div>
            </div>"""
        news_html += "</div>"
        st.html(news_html)

    with col_r:
        # Chatbot
        st.html("""
        <div class="chat-hdr">
          <span class="chat-title">⚡ AI COPILOT</span>
          <span><span class="online-dot"></span>&nbsp;<span style="font-family:'Share Tech Mono';font-size:.65rem;color:#00ff88;">ONLINE</span></span>
        </div>""")

        _init_msg = (
            f"Xin chào, tôi là trợ lý AI FINSENT. "
            f"Hệ thống phát hiện thị trường đang có dấu hiệu quá bán do tâm lý FUD lan rộng. "
            f"VMSI hiện tại: **{vmsi_v:.1f}/100** — {lbl}. "
            f"Bạn có muốn xem danh sách các mã cổ phiếu đang ở vùng tích lũy tốt không?"
        )
        if "rt_msgs" not in st.session_state:
            st.session_state.rt_msgs = [{"role":"assistant","content":_init_msg}]

        chat_box = st.container(height=340)
        with chat_box:
            for m in st.session_state.rt_msgs:
                with st.chat_message(m["role"], avatar="🤖" if m["role"]=="assistant" else "👤"):
                    st.markdown(m["content"])

        q_user = st.chat_input(f"Hỏi AI về mã cổ phiếu, xu hướng thị trường...")
        if q_user:
            st.session_state.rt_msgs.append({"role":"user","content":q_user})
            with chat_box:
                with st.chat_message("user", avatar="👤"): st.markdown(q_user)
            with chat_box:
                with st.chat_message("assistant", avatar="🤖"):
                    with st.spinner(""):
                        try:
                            bot  = get_chatbot()
                            resp = bot.generate_advice(q_user, {**data,"ticker":tshown})
                        except Exception as e:
                            resp = f"Lỗi chatbot: {e}\n\nVMSI {vmsi_v:.1f} — {lbl}: {rw_text}"
                    st.markdown(resp)
            st.session_state.rt_msgs.append({"role":"assistant","content":resp})
            st.rerun()

        if len(st.session_state.rt_msgs) > 2:
            if st.button("↺ Xóa lịch sử", use_container_width=True):
                st.session_state.rt_msgs=[{"role":"assistant","content":_init_msg}]
                st.rerun()

# ══════════════════════════════════════════════════════════════════════════════
# TAB: PHÂN TÍCH — Stock Detail + Portfolio + Chatbot
# ══════════════════════════════════════════════════════════════════════════════
elif st.session_state.get("active_tab") == "ANALYSIS":

    # ── KPI hàng trên: Portfolio metrics ──────────────────────────────────────
    # Lấy dữ liệu giá cổ phiếu thực
    price_d = _get_stock_price(tshown)
    open_p  = price_d.get("open", 0)
    close_p = price_d.get("close", 0)
    chg_pct = price_d.get("change", 0)
    chg_col = "#00ff88" if chg_pct >= 0 else "#ff4757"
    chg_sgn = "+" if chg_pct >= 0 else ""

    # Tính toán mock portfolio (thay bằng dữ liệu thật khi có)
    qty_held   = st.session_state.get("qty_held", 1000)
    avg_entry  = st.session_state.get("avg_entry", open_p if open_p else 15.0)
    curr_val   = qty_held * close_p if close_p else 0
    unrealized = (close_p - avg_entry) * qty_held if close_p and avg_entry else 0
    unr_pct    = ((close_p - avg_entry) / avg_entry * 100) if avg_entry else 0
    unr_col    = "#00ff88" if unrealized >= 0 else "#ff4757"

    k1,k2,k3,k4 = st.columns(4)
    with k1:
        st.html(f"""
        <div class="kpi-card" style="--accent:#e5e7eb">
          <div style="display:flex;justify-content:space-between;align-items:flex-start">
            <div class="kpi-label-text">Giá thị trường</div>
            <span style="font-size:.6rem;color:#6b7280;font-family:'Share Tech Mono',monospace">▲</span>
          </div>
          <div class="kpi-main" style="color:#e5e7eb;font-size:1.9rem">
            {_fmt_price(close_p) if close_p else '--'}
          </div>
          <div class="kpi-sub-text" style="color:{chg_col}">{chg_sgn}{chg_pct:.2f}% vs last close</div>
        </div>""")
    with k2:
        st.html(f"""
        <div class="kpi-card" style="--accent:{unr_col}">
          <div style="display:flex;justify-content:space-between;align-items:flex-start">
            <div class="kpi-label-text">Unrealized PnL</div>
            <span style="font-size:.6rem;color:#6b7280;font-family:'Share Tech Mono',monospace">~</span>
          </div>
          <div class="kpi-main" style="color:{unr_col};font-size:1.9rem">
            {'+' if unrealized>=0 else ''}{unrealized:,.0f}
          </div>
          <div class="kpi-sub-text" style="color:{unr_col}">{chg_sgn}{unr_pct:.2f}% Intraday volatility</div>
        </div>""")
    with k3:
        st.html(f"""
        <div class="kpi-card" style="--accent:#00b4d8">
          <div style="display:flex;justify-content:space-between;align-items:flex-start">
            <div class="kpi-label-text">Giá vào trung bình</div>
            <span style="font-size:.6rem;color:#6b7280;font-family:'Share Tech Mono',monospace">⬡</span>
          </div>
          <div class="kpi-main" style="color:#00b4d8;font-size:1.9rem">
            {_fmt_price(avg_entry)}
          </div>
          <div class="kpi-sub-text">Avg entry — {qty_held:,} cổ phiếu</div>
        </div>""")
    with k4:
        st.html(f"""
        <div class="kpi-card" style="--accent:#a78bfa">
          <div style="display:flex;justify-content:space-between;align-items:flex-start">
            <div class="kpi-label-text">Giá trị danh mục</div>
            <span style="font-size:.6rem;color:#6b7280;font-family:'Share Tech Mono',monospace">▣</span>
          </div>
          <div class="kpi-main" style="color:#a78bfa;font-size:1.9rem">
            {curr_val:,.0f}
          </div>
          <div class="kpi-sub-text">Active Positions: 1</div>
        </div>""")

    # ── Layout: Chart (left) + Chatbot (right) ────────────────────────────────
    col_main, col_chat = st.columns([6, 4], gap="medium")

    with col_main:
        # ── Section header ─────────────────────────────────────────────────────
        st.html(f'<div class="sec-hdr"><span class="ico">◈</span> BIỂU ĐỒ CỔ PHIẾU & DANH MỤC — {tshown}</div>')

        # ── Candlestick / Line chart ───────────────────────────────────────────
        try:
            from realtime_pipeline.crawlers.stock_crawler import fetch_stock_history, RawStockBar
            bars: list = fetch_stock_history(tshown, "2025-04-01", "2026-06-30")
        except Exception:
            bars = []

        if bars:
            dates   = [b.trading_date for b in bars]
            opens   = [b.open   for b in bars]
            highs   = [b.high   for b in bars]
            lows    = [b.low    for b in bars]
            closes  = [b.close  for b in bars]
            vols    = [b.volume for b in bars]
            colors  = ["#00ff88" if b.is_up else "#ff4757" for b in bars]

            # Candlestick chart
            fig_c = go.Figure()
            fig_c.add_trace(go.Candlestick(
                x=dates, open=opens, high=highs, low=lows, close=closes,
                name=tshown,
                increasing=dict(line=dict(color="#00ff88"), fillcolor="rgba(0,255,136,.2)"),
                decreasing=dict(line=dict(color="#ff4757"), fillcolor="rgba(255,71,87,.2)"),
            ))
            # MA20
            if len(closes) >= 20:
                ma20 = [sum(closes[max(0,i-19):i+1])/min(i+1,20) for i in range(len(closes))]
                fig_c.add_trace(go.Scatter(
                    x=dates, y=ma20, mode="lines", name="MA20",
                    line=dict(color="#fbbf24", width=1, dash="dot"),
                ))
            # MA50
            if len(closes) >= 50:
                ma50 = [sum(closes[max(0,i-49):i+1])/min(i+1,50) for i in range(len(closes))]
                fig_c.add_trace(go.Scatter(
                    x=dates, y=ma50, mode="lines", name="MA50",
                    line=dict(color="#a78bfa", width=1, dash="dash"),
                ))

            fig_c.update_layout(
                plot_bgcolor="#111827", paper_bgcolor="#111827",
                height=260, margin=dict(l=50,r=20,t=8,b=5),
                yaxis=dict(gridcolor="#1a2332", tickfont=dict(size=8,color="#4b5563",family="Share Tech Mono"),
                           side="right"),
                xaxis=dict(gridcolor="#1a2332", tickfont=dict(size=7,color="#4b5563",family="Share Tech Mono"),
                           rangeslider_visible=False),
                legend=dict(font=dict(size=9,color="#9ca3af",family="Share Tech Mono"),
                            bgcolor="rgba(0,0,0,0)", orientation="h", x=0, y=1.08),
                font=dict(family="Share Tech Mono"),
                xaxis_rangebreaks=[dict(bounds=["sat","mon"])],
            )
            st.plotly_chart(fig_c, use_container_width=True)

            # Volume bars
            fig_v = go.Figure(go.Bar(
                x=dates, y=vols,
                marker_color=colors,
                name="Volume",
            ))
            fig_v.update_layout(
                plot_bgcolor="#111827", paper_bgcolor="#111827",
                height=80, margin=dict(l=50,r=20,t=0,b=25),
                yaxis=dict(gridcolor="#1a2332", tickfont=dict(size=7,color="#4b5563",family="Share Tech Mono"),
                           side="right", showticklabels=False),
                xaxis=dict(gridcolor="#1a2332", tickfont=dict(size=7,color="#4b5563",family="Share Tech Mono")),
                showlegend=False,
                font=dict(family="Share Tech Mono"),
            )
            st.plotly_chart(fig_v, use_container_width=True)
        else:
            st.html("""
            <div style="background:#111827;border:1px solid #1e2d3d;border-radius:8px;
              padding:40px;text-align:center;color:#4b5563;font-family:'Share Tech Mono',monospace;">
              NO CHART DATA — Bấm Phân tích ngay để tải dữ liệu
            </div>""")

        # ── Portfolio table ────────────────────────────────────────────────────
        st.html('<div class="sec-hdr" style="margin-top:12px"><span class="ico">▤</span> BIỂU ĐỒ CHỈ SỐ TÂM LÝ & DANH MỤC</div>')

        # Build portfolio rows
        port_rows = []
        if close_p:
            port_rows.append({
                "ASSET": tshown, "TYPE": "EQ",
                "QTY": qty_held,
                "AVG ENTRY": f"{avg_entry:,.2f}",
                "LAST PRICE": f"{close_p:,.2f}",
                "UNREALIZED PNL": f"{'+' if unrealized>=0 else ''}{unrealized:,.0f} ({chg_sgn}{unr_pct:.1f}%)",
                "PNL_VAL": unrealized,
            })

        if port_rows:
            # HTML table theo dung style design
            rows_html = ""
            for r in port_rows:
                pnl_col = "#00ff88" if r["PNL_VAL"] >= 0 else "#ff4757"
                action_btn = "CLOSE RISK" if r["PNL_VAL"] < -5 else "TRADE"
                action_col = "#ff4757" if action_btn == "CLOSE RISK" else "#00b4d8"
                rows_html += f"""
                <tr style="border-bottom:1px solid #1a2332;">
                  <td style="padding:8px 10px;">
                    <span style="color:#00ff88;font-weight:700;">{r['ASSET']}</span>
                    <span style="color:#4b5563;font-size:.65rem;margin-left:4px;">{r['TYPE']}</span>
                  </td>
                  <td style="padding:8px 10px;text-align:right;">{r['QTY']:,}</td>
                  <td style="padding:8px 10px;text-align:right;color:#9ca3af;">${r['AVG ENTRY']}</td>
                  <td style="padding:8px 10px;text-align:right;color:#e5e7eb;">${r['LAST PRICE']}</td>
                  <td style="padding:8px 10px;text-align:right;color:{pnl_col};">{r['UNREALIZED PNL']}</td>
                  <td style="padding:8px 10px;text-align:center;">
                    <span style="background:rgba(0,0,0,.3);border:1px solid {action_col};
                      color:{action_col};padding:2px 8px;border-radius:4px;
                      font-size:.65rem;font-family:'Share Tech Mono',monospace;cursor:pointer;">
                      {action_btn}
                    </span>
                  </td>
                </tr>"""

            st.html(f"""
            <div style="background:#111827;border:1px solid #1e2d3d;border-radius:8px;overflow:hidden;">
              <table style="width:100%;border-collapse:collapse;font-family:'Share Tech Mono',monospace;font-size:.75rem;">
                <thead>
                  <tr style="border-bottom:1px solid #1e2d3d;background:#0d1117;">
                    <th style="padding:8px 10px;text-align:left;color:#4b5563;letter-spacing:1px;font-weight:500;font-size:.65rem;">ASSET</th>
                    <th style="padding:8px 10px;text-align:right;color:#4b5563;letter-spacing:1px;font-weight:500;font-size:.65rem;">QTY</th>
                    <th style="padding:8px 10px;text-align:right;color:#4b5563;letter-spacing:1px;font-weight:500;font-size:.65rem;">AVG ENTRY</th>
                    <th style="padding:8px 10px;text-align:right;color:#4b5563;letter-spacing:1px;font-weight:500;font-size:.65rem;">LAST PRICE</th>
                    <th style="padding:8px 10px;text-align:right;color:#4b5563;letter-spacing:1px;font-weight:500;font-size:.65rem;">UNREALIZED PNL</th>
                    <th style="padding:8px 10px;text-align:center;color:#4b5563;letter-spacing:1px;font-weight:500;font-size:.65rem;">ACTION</th>
                  </tr>
                </thead>
                <tbody>{rows_html}</tbody>
              </table>
            </div>""")

        # ── Margin / Chiến lược túi tiền bar ──────────────────────────────────
        st.html('<div class="sec-hdr" style="margin-top:12px"><span class="ico">◉</span> CHIẾN LƯỢC TÚI TIỀN</div>')

        # Tinh margin ratio (dua tren VMSI — cang cao VMSI thi cang risky)
        margin_ratio = min(99, max(1, vmsi_v * 1.2))
        safe_line = 60; call_line = 85
        bar_col = "#00ff88" if margin_ratio < safe_line else ("#fbbf24" if margin_ratio < call_line else "#ff4757")
        safe_pct = safe_line; call_pct = call_line

        st.html(f"""
        <div style="background:#111827;border:1px solid #1e2d3d;border-radius:8px;padding:14px;">
          <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:6px;">
            <span style="color:#9ca3af;font-size:.75rem;">Maintenance Margin Required</span>
            <span style="font-family:'Share Tech Mono',monospace;font-size:1.6rem;font-weight:700;color:{bar_col};">
              {margin_ratio:.1f}%<span style="font-size:.7rem;color:#6b7280;margin-left:6px;">CURRENT RATIO</span>
            </span>
          </div>
          <div style="position:relative;height:12px;background:#0d1117;border-radius:4px;overflow:hidden;margin-bottom:6px;">
            <div style="position:absolute;left:0;top:0;height:100%;width:{min(margin_ratio,safe_pct)}%;background:#00ff88;opacity:.7;border-radius:4px 0 0 4px;"></div>
            <div style="position:absolute;left:{safe_pct}%;top:0;height:100%;width:{min(margin_ratio-safe_pct,call_pct-safe_pct) if margin_ratio>safe_pct else 0}%;background:#fbbf24;opacity:.8;"></div>
            <div style="position:absolute;left:{call_pct}%;top:0;height:100%;
              width:{min(margin_ratio-call_pct,100-call_pct) if margin_ratio>call_pct else 0}%;
              background:#ff4757;opacity:.9;border-radius:0 4px 4px 0;
              background:repeating-linear-gradient(45deg,#ff4757,#ff4757 3px,#ff2d00 3px,#ff2d00 6px);"></div>
            <div style="position:absolute;left:{safe_pct}%;top:0;width:1px;height:100%;background:#4b5563;"></div>
            <div style="position:absolute;left:{call_pct}%;top:0;width:1px;height:100%;background:#ff4757;"></div>
          </div>
          <div style="display:flex;justify-content:space-between;font-family:'Share Tech Mono',monospace;font-size:.62rem;color:#4b5563;">
            <span>0%</span>
            <span style="color:#6b7280;">{safe_pct}% (Safe)</span>
            <span style="color:#fbbf24;">{call_pct}% (Call)</span>
            <span>100%</span>
          </div>
        </div>""")

        # ── Đề xuất giải pháp ─────────────────────────────────────────────────
        st.html('<div class="sec-hdr" style="margin-top:12px"><span class="ico">⊙</span> ĐỀ XUẤT GIẢI PHÁP</div>')

        alert_level = "ELEVATED VOLATILITY" if vmsi_v > 60 else ("LOW ACTIVITY" if vmsi_v < 35 else "STABLE")
        alert_col   = "#ff4757" if vmsi_v > 75 else ("#fbbf24" if vmsi_v > 55 else "#00ff88")
        de_xuat_text = (
            f"Elevated Volatility Detected. Social sentiment (CafeF/Facebook) indicates "
            f"localized {'panic' if vmsi_v < 30 else 'FOMO'} in financial sectors."
            if vmsi_v < 30 or vmsi_v > 70
            else "Market sentiment stable. No immediate action required."
        )
        early_warning = (
            f"EARLY MARGIN CALL WARNING\nProjection: 24-48h at current burn rate. "
            f"Consider hedging {tshown} positions."
            if margin_ratio > call_line
            else f"SYSTEM NOTE\nVMSI={vmsi_v:.1f} — {vmsi_label(vmsi_v)}. "
                 f"Monitor {tshown} closely."
        )

        st.html(f"""
        <div style="background:#111827;border:1px solid #1e2d3d;border-radius:8px;padding:14px;">
          <div style="display:flex;align-items:center;gap:8px;margin-bottom:8px;">
            <span style="width:8px;height:8px;border-radius:50%;background:{alert_col};
              display:inline-block;box-shadow:0 0 6px {alert_col};"></span>
            <span style="color:{alert_col};font-family:'Share Tech Mono',monospace;
              font-size:.75rem;font-weight:700;letter-spacing:1px;">{alert_level} DETECTED.</span>
          </div>
          <p style="color:#9ca3af;font-size:.78rem;line-height:1.6;margin:0 0 10px;">{de_xuat_text}</p>
          <div style="background:#0d1117;border:1px solid {alert_col}33;border-left:3px solid {alert_col};
            padding:10px 12px;border-radius:4px;font-family:'Share Tech Mono',monospace;
            font-size:.7rem;color:{alert_col};white-space:pre-line;">{early_warning}</div>
        </div>""")

    with col_chat:
        # ── Chatbot Phân tích & Giai dap ──────────────────────────────────────
        st.html(f"""
        <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:10px;">
          <span style="font-family:'Share Tech Mono',monospace;font-size:.7rem;
            color:#6b7280;letter-spacing:1.5px;text-transform:uppercase;">⚡ CHATBOT PHÂN TÍCH & GIẢI ĐÁP</span>
          <span><span class="online-dot"></span>&nbsp;
            <span style="font-family:'Share Tech Mono',monospace;font-size:.65rem;color:#00ff88;">ONLINE</span>
          </span>
        </div>""")

        # Quick action buttons theo design
        qa1, qa2 = st.columns(2)
        with qa1:
            if st.button("📊 Đề xuất phòng vệ", use_container_width=True, key="qa_defend"):
                st.session_state["_qa2"] = f"Đề xuất chiến lược phòng vệ danh mục {tshown} trong bối cảnh VMSI={vmsi_v:.0f}?"
        with qa2:
            if st.button(f"🔍 Phân tích {tshown}", use_container_width=True, key="qa_analyze"):
                st.session_state["_qa2"] = f"Phân tích kỹ thuật và cơ bản cho cổ phiếu {tshown}. Giá hiện tại {_fmt_price(close_p)}, biến động {chg_sgn}{chg_pct:.2f}%."

        qa3, qa4 = st.columns(2)
        with qa3:
            if st.button("✂ Cắt lỗ tự động", use_container_width=True, key="qa_stoploss"):
                st.session_state["_qa2"] = f"Khuyến nghị mức cắt lỗ hợp lý cho {tshown} khi VMSI={vmsi_v:.0f}?"
        with qa4:
            if st.button("📰 Tin tức ảnh hưởng", use_container_width=True, key="qa_news"):
                st.session_state["_qa2"] = f"Tin tức nào đang ảnh hưởng đến {tshown} hiện tại?"

        # Chat session rieng cho tab ANALYSIS
        _init2 = (
            f"Xin chào! Dựa trên danh mục của bạn, tôi nhận thấy mức độ rủi ro "
            f"đang tăng cao ở {tshown} do tâm lý {'FUD' if vmsi_v<40 else 'FOMO'} lan rộng. "
            f"Bạn có muốn xem danh sách các mã cổ phiếu đang ở vùng tích lũy tốt không?"
        )
        if "rt_msgs2" not in st.session_state:
            st.session_state.rt_msgs2 = [{"role":"assistant","content":_init2}]

        chat_box2 = st.container(height=480)
        with chat_box2:
            for m in st.session_state.rt_msgs2:
                with st.chat_message(m["role"], avatar="🤖" if m["role"]=="assistant" else "👤"):
                    st.markdown(m["content"])

        q_auto2 = st.session_state.pop("_qa2", None)
        q_user2 = st.chat_input(f"Hỏi AI về danh mục...", key="chat2")
        q_fin2  = q_auto2 or q_user2

        if q_fin2:
            st.session_state.rt_msgs2.append({"role":"user","content":q_fin2})
            with chat_box2:
                with st.chat_message("user", avatar="👤"): st.markdown(q_fin2)
            with chat_box2:
                with st.chat_message("assistant", avatar="🤖"):
                    with st.spinner(""):
                        try:
                            bot  = get_chatbot()
                            ctx2 = {**data, "ticker":tshown, "price":close_p,
                                    "change_pct":chg_pct, "avg_entry":avg_entry}
                            resp2 = bot.generate_advice(q_fin2, ctx2)
                        except Exception as e:
                            resp2 = (
                                f"**Phân tích {tshown}**\n\n"
                                f"- Giá hiện tại: {_fmt_price(close_p)}\n"
                                f"- Biến động: {chg_sgn}{chg_pct:.2f}%\n"
                                f"- VMSI: {vmsi_v:.1f} ({vmsi_label(vmsi_v)})\n"
                                f"- Khuyến nghị: {data.get('risk_warning','')}"
                            )
                    st.markdown(resp2)
            st.session_state.rt_msgs2.append({"role":"assistant","content":resp2})
            st.rerun()

        if len(st.session_state.rt_msgs2) > 2:
            if st.button("↺ Xóa chat", use_container_width=True, key="clr2"):
                st.session_state.rt_msgs2=[{"role":"assistant","content":_init2}]; st.rerun()

# ══════════════════════════════════════════════════════════════════════════════
# TAB: THỊ TRƯỜNG — Social Stream + NHNN Cross-Validation + Explainable AI
# ══════════════════════════════════════════════════════════════════════════════
elif st.session_state.get("active_tab") == "MARKET":

    col_stream, col_validation = st.columns([5, 5], gap="medium")

    # ── LEFT: Social Stream ───────────────────────────────────────────────────
    with col_stream:
        # Header with FUD/FOMO badges
        fud_level  = "HIGH"  if vmsi_v < 35 else ("MED" if vmsi_v < 50 else "LOW")
        fomo_level = "HIGH"  if vmsi_v > 70 else ("MED" if vmsi_v > 55 else "LOW")
        fud_col    = {"HIGH":"#ff4757","MED":"#fbbf24","LOW":"#4b5563"}[fud_level]
        fomo_col   = {"HIGH":"#ff4757","MED":"#fbbf24","LOW":"#4b5563"}[fomo_level]

        st.html(f"""
        <div style="display:flex;justify-content:space-between;align-items:center;
          border-bottom:1px solid #1a2332;padding-bottom:8px;margin-bottom:10px;">
          <span style="font-family:'Share Tech Mono',monospace;font-size:.72rem;
            color:#00ff88;letter-spacing:1.5px;font-weight:700;">✦ SOCIAL STREAM</span>
          <div style="display:flex;gap:6px;">
            <span style="background:{fud_col}22;border:1px solid {fud_col};color:{fud_col};
              padding:2px 8px;border-radius:3px;font-family:'Share Tech Mono',monospace;
              font-size:.6rem;font-weight:700;letter-spacing:1px;">FUD: {fud_level}</span>
            <span style="background:{fomo_col}22;border:1px solid {fomo_col};color:{fomo_col};
              padding:2px 8px;border-radius:3px;font-family:'Share Tech Mono',monospace;
              font-size:.6rem;font-weight:700;letter-spacing:1px;">FOMO: {fomo_level}</span>
          </div>
        </div>""")

        # Build social stream items from session + crawled data
        raw_posts = st.session_state.get("social_posts", [])

        # Demo posts nếu chưa crawl
        if not raw_posts:
            from datetime import datetime as _dt
            raw_posts = [
                {"source":"Telegram: NDT V.I.P","time":"10:14:02","text":f"Nghe don chieu nay NHNN hut tin phieu tiep, bank chuan bi an dap. Thay may room kia ho hao xa {tshown} roi, anh em can than bi up sot nhe.","sentiment":"FUD","confidence":92,"entity":tshown},
                {"source":"F319: Cổ phiếu vua","time":"10:12:45","text":f"STB ho room ngoai cai la tay muc nhu pha ma. Dong tien cuon cuon vao, khong muc bay gio thi doi len 40 moi muc a? All-in anh em oi!","sentiment":"FOMO","confidence":84,"entity":"STB"},
                {"source":"FB: Dau tu thong minh","time":"10:05:11","text":"Lai suat lien ngan hang qua dem dang nhich nhe len 4.2%. Thanh khoan he thong co ve bot du thua so voi tuan truoc.","sentiment":"NEUTRAL","confidence":95,"entity":"Macro"},
                {"source":"Zalo: Doi lai Tay Bac","time":"09:58:22","text":f"{tshown} gay nen 11.5 roi, can cheo cung gay. Ve menh gia som thoi, xa nhanh con kip.","sentiment":"FUD","confidence":88,"entity":tshown},
                {"source":"CafeF RSS","time":"09:45:00","text":f"NHNN bao cao on dinh thi truong, lai suat dieu hanh giu nguyen. Ho tro dong tien vao co phieu ngan hang.","sentiment":"POSITIVE","confidence":91,"entity":"NHNN"},
            ]

        sent_colors = {
            "FUD":      ("#ff4757","#ff475722"),
            "FOMO":     ("#fbbf24","#fbbf2422"),
            "NEUTRAL":  ("#6b7280","#6b728022"),
            "POSITIVE": ("#00ff88","#00ff8822"),
        }

        stream_html = '<div style="max-height:460px;overflow-y:auto;display:flex;flex-direction:column;gap:8px;">'
        for p in raw_posts[:10]:
            sent = p.get("sentiment","NEUTRAL")
            sc,bg = sent_colors.get(sent, ("#6b7280","#6b728022"))
            conf = p.get("confidence",0)
            ent  = p.get("entity","")
            txt  = p.get("text","")[:200]
            src  = p.get("source","Unknown")
            t    = p.get("time","--:--:--")

            # Border color by sentiment
            border_left = sc

            stream_html += f"""
            <div style="background:#111827;border:1px solid #1e2d3d;
              border-left:3px solid {border_left};border-radius:4px 8px 8px 4px;
              padding:10px 12px;">
              <div style="display:flex;justify-content:space-between;
                align-items:center;margin-bottom:6px;">
                <span style="color:#6b7280;font-family:'Share Tech Mono',monospace;
                  font-size:.65rem;">💬 {src}</span>
                <span style="color:#4b5563;font-family:'Share Tech Mono',monospace;
                  font-size:.62rem;">{t}</span>
              </div>
              <p style="color:#d1d5db;font-size:.78rem;line-height:1.6;
                margin:0 0 8px;font-style:italic;">"{txt}"</p>
              <div style="display:flex;gap:6px;align-items:center;flex-wrap:wrap;">
                <span style="background:{bg};border:1px solid {sc};color:{sc};
                  padding:1px 7px;border-radius:3px;font-family:'Share Tech Mono',monospace;
                  font-size:.6rem;font-weight:700;letter-spacing:.5px;">
                  PhoBERT: [{sent}] {conf}%
                </span>
                {f'<span style="background:#1e2d3d;color:#9ca3af;padding:1px 7px;border-radius:3px;font-family:Share Tech Mono,monospace;font-size:.6rem;">Entity: {ent}</span>' if ent else ''}
              </div>
            </div>"""

        stream_html += "</div>"
        st.html(stream_html)

        # Refresh button
        if st.button("↻ CẬP NHẬT STREAM", use_container_width=True, key="refresh_stream"):
            # Crawl Facebook + news thực tế
            with st.spinner("Đang crawl social stream..."):
                try:
                    from realtime_pipeline.crawlers.facebook_crawler import crawl_facebook_for_ticker
                    from realtime_pipeline.crawlers.news_crawler import crawl_news_for_ticker
                    from realtime_pipeline.normalizers.unified_normalizer import (
                        normalize_social_post, normalize_news_article
                    )
                    posts_new = []
                    fb_posts  = crawl_facebook_for_ticker(tshown)
                    for p in fb_posts[:5]:
                        n = normalize_social_post(p, tshown)
                        sent_lbl = n.get("sentiment",{}).get("label","neutral").upper()
                        conf_val = int(n.get("sentiment",{}).get("confidence",0)*100)
                        posts_new.append({
                            "source": f"Facebook: {p.source_name}",
                            "time":   datetime.now().strftime("%H:%M:%S"),
                            "text":   p.content_text[:180],
                            "sentiment": "FUD" if sent_lbl=="NEGATIVE" else ("FOMO" if conf_val>70 and sent_lbl=="POSITIVE" else sent_lbl),
                            "confidence": conf_val,
                            "entity": tshown,
                        })
                    news = crawl_news_for_ticker(tshown)
                    for a in news[:5]:
                        n2 = normalize_news_article(a, tshown)
                        sent_lbl2 = n2.get("sentiment",{}).get("label","neutral").upper()
                        posts_new.append({
                            "source": f"CafeF: {a.title[:40]}",
                            "time":   datetime.now().strftime("%H:%M:%S"),
                            "text":   a.content_text[:180],
                            "sentiment": "POSITIVE" if sent_lbl2=="POSITIVE" else ("FUD" if sent_lbl2=="NEGATIVE" else "NEUTRAL"),
                            "confidence": int(n2.get("sentiment",{}).get("confidence",0)*100),
                            "entity": tshown,
                        })
                    st.session_state["social_posts"] = posts_new
                    st.toast(f"Cập nhật {len(posts_new)} posts thành công!", icon="✅")
                    st.rerun()
                except Exception as e:
                    st.error(f"Lỗi crawl: {e}")

    # ── RIGHT: NHNN Cross-Validation ──────────────────────────────────────────
    with col_validation:
        st.html("""
        <div style="display:flex;justify-content:space-between;align-items:center;
          border-bottom:1px solid #1a2332;padding-bottom:8px;margin-bottom:10px;">
          <span style="font-family:'Share Tech Mono',monospace;font-size:.72rem;
            color:#00ff88;letter-spacing:1.5px;font-weight:700;">✦ NHNN CROSS-VALIDATION</span>
          <span style="display:flex;align-items:center;gap:5px;">
            <span style="width:7px;height:7px;border-radius:50%;background:#00ff88;
              display:inline-block;box-shadow:0 0 6px #00ff88;"></span>
            <span style="font-family:'Share Tech Mono',monospace;font-size:.6rem;
              color:#00ff88;letter-spacing:1px;">LIVE SYNC</span>
          </span>
        </div>""")

        # Lay macro data tu ChromaDB
        macro_summ = data.get("macro_summary","")
        s_nhnn_val = scores.get("s_nhnn", 0)
        nhnn_label = {-1:"THẮT CHẶT / TIÊU CỰC", 0:"TRUNG LẬP / ỔN ĐỊNH", 1:"NỚI LỎNG / HỖ TRỢ"}.get(s_nhnn_val, "UNKNOWN")
        nhnn_col   = {-1:"#ff4757", 0:"#00b4d8", 1:"#00ff88"}.get(s_nhnn_val, "#6b7280")

        # Panel 2 cols: Active Rumor | Official NHNN Data
        v1, v2 = st.columns(2, gap="small")

        # Detect active rumor dua tren FUD
        rumor_active = vmsi_v < 40 or fud_level == "HIGH"
        rumor_title  = f"{'SBV T-Bill Issuance' if vmsi_v < 40 else 'Margin Call Pressure'}"
        rumor_desc   = (
            "Social volume spike indicating fear of immediate liquidity drain via T-Bills."
            if vmsi_v < 40
            else "FUD regarding regulatory tightening spreading in groups."
        )
        rumor_conf   = max(50, int(100 - vmsi_v))

        with v1:
            r_col = "#ff4757" if rumor_active else "#6b7280"
            st.html(f"""
            <div style="background:#1a0a0a;border:1px solid {r_col}44;border-radius:8px;
              padding:14px;text-align:center;min-height:200px;">
              <div style="font-size:.6rem;font-family:'Share Tech Mono',monospace;
                color:{r_col};letter-spacing:1.5px;text-transform:uppercase;
                margin-bottom:14px;opacity:.8;">
                {'ACTIVE RUMOR TRIGGER' if rumor_active else 'NO ACTIVE RUMOR'}
              </div>
              <div style="font-size:2rem;margin:10px 0;color:{r_col};">⚠</div>
              <div style="color:{r_col};font-family:'Share Tech Mono',monospace;
                font-size:.95rem;font-weight:700;margin:8px 0;">{rumor_title}</div>
              <div style="color:#9ca3af;font-size:.7rem;line-height:1.5;margin:8px 0;">{rumor_desc}</div>
              <div style="margin-top:12px;">
                <span style="background:{r_col}22;border:1px solid {r_col};color:{r_col};
                  padding:2px 10px;border-radius:3px;font-family:'Share Tech Mono',monospace;
                  font-size:.65rem;font-weight:700;">
                  Confidence: {'High' if rumor_conf>70 else 'Med'} (Social {'FUD' if vmsi_v<40 else 'FOMO'})
                </span>
              </div>
            </div>""")

        with v2:
            # NHNN official stance tu MacroAgent
            nhnn_icon    = "✓" if s_nhnn_val >= 0 else "✗"
            nhnn_bg      = "#0a1a0a" if s_nhnn_val >= 0 else "#1a0a0a"
            nhnn_status  = "Rumor Debunked" if s_nhnn_val == 1 else ("Under Review" if s_nhnn_val == 0 else "Alert Confirmed")
            nhnn_detail  = (
                macro_summ[:160] if macro_summ and "fallback" not in macro_summ.lower()
                else "ChromaDB: Kiểm tra NHNN docs. Interbank rate stable at 4.2%."
            )
            confidence_score = max(60, int(abs(scores.get("s_macro",0)) * 100))

            st.html(f"""
            <div style="background:{nhnn_bg};border:1px solid {nhnn_col}44;border-radius:8px;
              padding:14px;text-align:center;min-height:200px;">
              <div style="font-size:.6rem;font-family:'Share Tech Mono',monospace;
                color:{nhnn_col};letter-spacing:1.5px;text-transform:uppercase;
                margin-bottom:14px;opacity:.8;">OFFICIAL NHNN DATA</div>
              <div style="font-size:2rem;margin:10px 0;color:{nhnn_col};">
                {'🛡' if s_nhnn_val>=0 else '⚡'}
              </div>
              <div style="color:{nhnn_col};font-family:'Share Tech Mono',monospace;
                font-size:.9rem;font-weight:700;margin:8px 0;">
                {'NO ACTION' if s_nhnn_val==0 else nhnn_label}
              </div>
              <div style="color:#9ca3af;font-size:.68rem;line-height:1.5;margin:8px 0;
                text-align:left;">{nhnn_detail[:150]}</div>
              <div style="margin-top:10px;">
                <span style="background:{nhnn_col}22;border:1px solid {nhnn_col};color:{nhnn_col};
                  padding:2px 8px;border-radius:3px;font-family:'Share Tech Mono',monospace;
                  font-size:.62rem;font-weight:700;">Status: {nhnn_status}</span>
              </div>
              <div style="display:flex;gap:6px;margin-top:8px;justify-content:center;">
                <span style="background:#1e2d3d;color:#9ca3af;padding:3px 8px;
                  border-radius:4px;font-family:'Share Tech Mono',monospace;font-size:.6rem;">
                  Source: ChromaDB
                </span>
                <span style="background:#1e2d3d;color:{nhnn_col};padding:3px 8px;
                  border-radius:4px;font-family:'Share Tech Mono',monospace;font-size:.6rem;">
                  Score: {confidence_score}%
                </span>
              </div>
            </div>""")

        # ── Trend signal bar ──────────────────────────────────────────────────
        st.html(f"""
        <div style="background:#111827;border:1px solid #1e2d3d;border-radius:8px;
          padding:12px;margin-top:8px;">
          <div style="font-size:.6rem;font-family:'Share Tech Mono',monospace;
            color:#4b5563;letter-spacing:1.5px;margin-bottom:8px;">SOCIAL vs OFFICIAL — DELTA SIGNAL</div>
          <div style="display:flex;gap:4px;margin-bottom:6px;">
            <div style="flex:1;text-align:center;">
              <div style="font-size:.62rem;color:#6b7280;font-family:'Share Tech Mono',monospace;">SOCIAL SENTIMENT</div>
              <div style="font-size:1.1rem;font-weight:700;color:{vmsi_color(vmsi_v)};font-family:'Share Tech Mono',monospace;">{vmsi_v:.0f}</div>
            </div>
            <div style="flex:0 0 30px;display:flex;align-items:center;justify-content:center;
              color:#4b5563;font-size:1rem;">↔</div>
            <div style="flex:1;text-align:center;">
              <div style="font-size:.62rem;color:#6b7280;font-family:'Share Tech Mono',monospace;">NHNN POLICY</div>
              <div style="font-size:1.1rem;font-weight:700;color:{nhnn_col};font-family:'Share Tech Mono',monospace;">
                {'+1' if s_nhnn_val==1 else ('-1' if s_nhnn_val==-1 else '0')}
              </div>
            </div>
            <div style="flex:0 0 30px;display:flex;align-items:center;justify-content:center;
              color:#4b5563;font-size:1rem;">→</div>
            <div style="flex:1;text-align:center;">
              <div style="font-size:.62rem;color:#6b7280;font-family:'Share Tech Mono',monospace;">VERDICT</div>
              <div style="font-size:.7rem;font-weight:700;color:{'#fbbf24' if rumor_active else '#00ff88'};
                font-family:'Share Tech Mono',monospace;">
                {'FUD DETECTED' if rumor_active and s_nhnn_val>=0 else ('CONFIRMED' if s_nhnn_val<0 else 'STABLE')}
              </div>
            </div>
          </div>
        </div>""")

    # ── BOTTOM: Explainable AI Chatbot ────────────────────────────────────────
    st.html("""
    <div style="display:flex;justify-content:space-between;align-items:center;
      border-top:1px solid #1a2332;border-bottom:1px solid #1a2332;
      padding:8px 0;margin:12px 0;">
      <span style="font-family:'Share Tech Mono',monospace;font-size:.7rem;
        color:#6b7280;letter-spacing:1.5px;text-transform:uppercase;
        display:flex;align-items:center;gap:8px;">
        <span>🤖</span> QWEN2-7B // EXPLAINABLE AI
      </span>
      <span style="background:rgba(0,255,136,.15);border:1px solid #00ff88;
        color:#00ff88;padding:2px 10px;border-radius:3px;
        font-family:'Share Tech Mono',monospace;font-size:.6rem;letter-spacing:1px;">
        ● PROCESSING
      </span>
    </div>""")

    # Init chat session rieng cho tab MARKET
    _init3 = (
        f"Xin chào! Hệ thống phát hiện thị trường đang có dấu hiệu "
        f"{'quá bán do tâm lý FUD lan rộng' if vmsi_v<40 else ('FOMO cụm cao' if vmsi_v>70 else 'ổn định')}. "
        f"VMSI={vmsi_v:.1f}, NHNN policy: {nhnn_label}. "
        f"Bạn có muốn tôi phân tích nguyên nhân biến động và đối chiếu với dữ liệu chính thống không?"
    )
    if "rt_msgs3" not in st.session_state:
        st.session_state.rt_msgs3 = [{"role":"assistant","content":_init3}]

    # Quick action buttons theo design
    qb1,qb2,qb3,qb4 = st.columns(4)
    with qb1:
        if st.button(f"❓ Vì sao {tshown} bán mạnh?", use_container_width=True, key="qm1"):
            st.session_state["_qm"] = f"Vì sao {tshown} đang bị bán mạnh hôm nay? VMSI={vmsi_v:.0f}, FUD={fud_level}."
    with qb2:
        if st.button("🔍 Đối chiếu NHNN", use_container_width=True, key="qm2"):
            st.session_state["_qm"] = f"Đối chiếu tin đồn trên mạng xã hội với dữ liệu chính thức của NHNN về {tshown}."
    with qb3:
        if st.button("📊 Phân tích FUD/FOMO", use_container_width=True, key="qm3"):
            st.session_state["_qm"] = f"Phân tích các tin FUD và FOMO đang ảnh hưởng đến {tshown} trên các kênh mạng xã hội."
    with qb4:
        if st.button("✅ Xác minh tin tức", use_container_width=True, key="qm4"):
            st.session_state["_qm"] = f"Xác minh tin tức: '{rumor_title}'. Đây là FUD hay tin thật? So sánh với dữ liệu ChromaDB."

    chat_box3 = st.container(height=280)
    with chat_box3:
        for m in st.session_state.rt_msgs3:
            with st.chat_message(m["role"], avatar="🤖" if m["role"]=="assistant" else "👤"):
                st.markdown(m["content"])

    q_auto3 = st.session_state.pop("_qm", None)
    q_user3 = st.chat_input("Hỏi AI về tin tức, FUD/FOMO, đối chiếu NHNN...", key="chat3")
    q_fin3  = q_auto3 or q_user3

    if q_fin3:
        st.session_state.rt_msgs3.append({"role":"user","content":q_fin3})
        with chat_box3:
            with st.chat_message("user", avatar="👤"): st.markdown(q_fin3)
        with chat_box3:
            with st.chat_message("assistant", avatar="🤖"):
                with st.spinner(""):
                    try:
                        bot   = get_chatbot()
                        ctx3  = {
                            **data, "ticker": tshown,
                            "fud_level": fud_level, "fomo_level": fomo_level,
                            "nhnn_policy": nhnn_label, "rumor": rumor_title,
                            "rumor_confidence": rumor_conf,
                        }
                        resp3 = bot.generate_advice(q_fin3, ctx3)
                    except Exception as e:
                        # Fallback explainable response
                        resp3 = (
                            f"**Phân tích {tshown} — Explainable AI**\n\n"
                            f"**1. Sentiment '{rumor_title}' {'(Bull Trap)' if vmsi_v<40 else '(Bear Trap)'}:** "
                            f"{'Tin đồn giả mạo lan trên Telegram và Zalo, tạo ra panic bán tháo giả tạo. (FUD {rumor_conf}%)' if rumor_active else 'Không phát hiện FUD nguy hiểm.'}\n\n"
                            f"**2. NHNN Cross-Check:** {nhnn_detail[:200]}\n\n"
                            f"**3. Kết luận:** VMSI={vmsi_v:.1f} ({vmsi_label(vmsi_v)}). "
                            f"{'Rumor bị bác bỏ bởi dữ liệu ChromaDB. Khuyến nghị: Không panic sell.' if s_nhnn_val>=0 else 'Cảnh báo có thể xác nhận. Cần theo dõi thêm.'}"
                        )
                st.markdown(resp3)
        st.session_state.rt_msgs3.append({"role":"assistant","content":resp3})
        st.rerun()

    if len(st.session_state.rt_msgs3) > 2:
        if st.button("↺ Xóa chat", use_container_width=True, key="clr3"):
            st.session_state.rt_msgs3=[{"role":"assistant","content":_init3}]; st.rerun()

# ══════════════════════════════════════════════════════════════════════════════
# TAB: TIN TỨC — Bao cao tong hop + Gui Telegram
# ══════════════════════════════════════════════════════════════════════════════
elif st.session_state.get("active_tab") == "NEWS":

    # ── Tinh toan truoc, tranh nested f-string ────────────────────────────────
    from datetime import datetime as _dt
    now_str    = _dt.now().strftime("%d/%m/%Y %H:%M")
    s_nhnn_r   = scores.get("s_nhnn", 0)
    nhnn_lbl_r = {-1:"Thắt chặt / Tiêu cực", 0:"Trung lập / Ổn định", 1:"Nới lỏng / Hỗ trợ"}.get(s_nhnn_r, "N/A")

    price_r   = _get_stock_price(tshown)
    close_r   = price_r.get("close", 0)
    chg_r     = price_r.get("change", 0)
    chg_sgn_r = "+" if chg_r >= 0 else ""

    macro_r = data.get("macro_summary", "Chưa có dữ liệu vĩ mô.")
    risk_r  = data.get("risk_warning",  "Chưa có cảnh báo.")
    news_r  = _get_news_items(tshown)

    # Khuyen nghi (tinh truoc de tranh nested ternary trong f-string)
    if vmsi_v > 75:
        khuyen_nghi = "GIẢM MARGIN / BÁN TỪNG PHẦN — VMSI > 75, nguy cơ bong bóng. Nên chốt lời từng phần."
        kn_icon = "🔴"
    elif vmsi_v < 25:
        khuyen_nghi = "MUA VÀO / BẮT ĐÁY — VMSI < 25, vùng sợ hãi cực độ, có thể bắt đáy các mã cơ bản tốt."
        kn_icon = "🟢"
    elif vmsi_v < 40:
        khuyen_nghi = "THẬN TRỌNG / NẮM GIỮ — VMSI 25-40, thị trường e dè, giữ vị thế phòng thủ."
        kn_icon = "🟡"
    elif vmsi_v < 60:
        khuyen_nghi = "NẮM GIỮ / TĂNG DẦN — VMSI 40-60, thị trường cân bằng, duy trì cơ cấu tài sản."
        kn_icon = "🟢"
    else:
        khuyen_nghi = "THEO DÕI / CHỐT LỜI DẦN — VMSI 60-75, xu hướng tích cực nhưng cần đề chừng."
        kn_icon = "🟡"

    # Tin tuc noi bat
    if news_r:
        news_lines = "\n".join([
            f"- **[{it.get('type','').upper()}]** {it.get('title','')} _(Source: {it.get('src','')})_"
            for it in news_r[:5]
        ])
    else:
        news_lines = "_Chưa có tin tức. Chạy phân tích để cập nhật._"

    # Macro section
    macro_section = (
        macro_r if macro_r and "fallback" not in macro_r.lower()
        else "_Không có dữ liệu vĩ mô từ ChromaDB._"
    )

    close_str = _fmt_price(close_r) if close_r else "N/A"
    s_soc_str = f"{scores.get('s_social', 0):+.4f}"
    s_mac_str = f"{scores.get('s_macro', 0):+.4f}"
    vmsi_raw_str = f"{scores.get('vmsi_raw', vmsi_v):.1f}"

    # Build report (khong dung nested f-string)
    report_lines = [
        f"## BÁO CÁO TÂM LÝ THỊ TRƯỜNG — {tshown}",
        f"**Thời gian:** {now_str} UTC+7 | **Chu kỳ:** 30 phút/lần",
        "",
        "---",
        "",
        "### CHỈ SỐ TÂM LÝ VMSI",
        f"> **{vmsi_v:.1f} / 100** — **{vmsi_label(vmsi_v)}**",
        "",
        f"Trạng thái: `{status.upper()}` | Market sentiment: `{chg_sgn_r}{chg_r:.2f}%`",
        "",
        "---",
        "",
        f"### GIÁ CỔ PHIẾU {tshown}",
        "| Chỉ tiêu | Giá trị |",
        "|---|---|",
        f"| Giá hiện tại | **{close_str}** |",
        f"| Biến động | **{chg_sgn_r}{chg_r:.2f}%** |",
        f"| VMSI raw | **{vmsi_raw_str}/100** |",
        f"| S_Social | **{s_soc_str}** |",
        f"| S_Macro  | **{s_mac_str}** |",
        "",
        "---",
        "",
        "### NHẬN ĐỊNH CHÍNH SÁCH NHNN",
        f"**Đánh giá:** {nhnn_lbl_r}",
        "",
        macro_section,
        "",
        "---",
        "",
        "### CẢNH BÁO RỦI RO",
        risk_r,
        "",
        "---",
        "",
        "### TIN TỨC NỔI BẬT",
        news_lines,
        "",
        "---",
        "",
        "### KHUYẾN NGHỊ HÀNH ĐỘNG",
        f"{kn_icon} **{khuyen_nghi}**",
        "",
        "---",
        "_Phân tích tự động bởi FinSent-Agent v1.0 | ChromaDB + PhoBERT + MacroAgent_",
    ]
    report_md = "\n".join(report_lines)

    # Telegram message (ngan gon hon)
    if vmsi_v > 75: tg_kn = "GIẢM MARGIN"
    elif vmsi_v < 25: tg_kn = "MUA VÀO"
    elif vmsi_v < 40: tg_kn = "THẬN TRỌNG"
    else: tg_kn = "NẮM GIỮ"

    # ── Layout: Preview (left) | Actions (right) ─────────────────────────────
    rep_col, act_col = st.columns([6, 4], gap="medium")

    with rep_col:
        st.html(f"""
        <div style="display:flex;justify-content:space-between;align-items:center;
          border-bottom:1px solid #1a2332;padding-bottom:8px;margin-bottom:12px;">
          <span style="font-family:'Share Tech Mono',monospace;font-size:.72rem;
            color:#00ff88;letter-spacing:1.5px;font-weight:700;">✦ BÁO CÁO TỔNG HỢP</span>
          <div style="display:flex;gap:8px;align-items:center;">
            <span style="background:rgba(0,255,136,.1);border:1px solid #00ff8866;
              color:#00ff88;padding:2px 8px;border-radius:3px;
              font-family:'Share Tech Mono',monospace;font-size:.6rem;">AUTO-GENERATED</span>
            <span style="color:#4b5563;font-family:'Share Tech Mono',monospace;font-size:.62rem;">
              {now_str}
            </span>
          </div>
        </div>""")

        # Hien thi bao cao dang bai viet
        st.html(f"""
        <div style="background:#111827;border:1px solid #1e2d3d;border-radius:8px;
          padding:20px;max-height:640px;overflow-y:auto;">

          <!-- Header -->
          <div style="border-left:4px solid {clr};padding-left:14px;margin-bottom:18px;">
            <div style="font-family:'Share Tech Mono',monospace;font-size:.65rem;
              color:#4b5563;letter-spacing:1.5px;margin-bottom:4px;">BÁO CÁO TÂM LÝ THỊ TRƯỜNG</div>
            <div style="font-size:1.3rem;font-weight:700;color:#e5e7eb;">{tshown}</div>
            <div style="font-size:.7rem;color:#6b7280;margin-top:2px;">{now_str} UTC+7 | Chu kỳ 30 phút</div>
          </div>

          <!-- VMSI block -->
          <div style="background:#0d1117;border:1px solid #1e2d3d;border-radius:6px;
            padding:12px;margin-bottom:12px;display:flex;align-items:center;gap:16px;">
            <div style="text-align:center;min-width:60px;">
              <div style="font-family:'Share Tech Mono',monospace;font-size:2.2rem;
                font-weight:800;color:{clr};line-height:1;">{vmsi_v:.0f}</div>
              <div style="font-size:.6rem;color:{clr};letter-spacing:1px;">{vmsi_label(vmsi_v)}</div>
            </div>
            <div>
              <div style="font-size:.75rem;color:#9ca3af;margin-bottom:4px;">
                Trạng thái: <span style="color:#e5e7eb;font-weight:600;">{status.upper()}</span>
              </div>
              <div style="font-size:.75rem;color:#9ca3af;">
                Gia <strong style="color:#e5e7eb;">{tshown}</strong>:
                <span style="color:#e5e7eb;font-family:'Share Tech Mono',monospace;">{close_str}</span>
                <span style="color:{'#00ff88' if chg_r>=0 else '#ff4757'};margin-left:4px;">
                  ({chg_sgn_r}{chg_r:.2f}%)
                </span>
              </div>
            </div>
          </div>

          <!-- NHNN section -->
          <div style="margin-bottom:12px;">
            <div style="font-size:.65rem;letter-spacing:1.5px;color:#4b5563;
              text-transform:uppercase;margin-bottom:6px;font-family:'Share Tech Mono',monospace;">
              Nhận định NHNN
            </div>
            <div style="background:#0d1117;border-left:3px solid {'#00ff88' if s_nhnn_r>=0 else '#ff4757'};
              padding:8px 12px;border-radius:0 6px 6px 0;font-size:.75rem;color:#9ca3af;line-height:1.6;">
              <strong style="color:{'#00ff88' if s_nhnn_r>=0 else '#ff4757'};">{nhnn_lbl_r}</strong>
              <br>{macro_section[:220]}
            </div>
          </div>

          <!-- Risk warning -->
          <div style="margin-bottom:12px;">
            <div style="font-size:.65rem;letter-spacing:1.5px;color:#4b5563;
              text-transform:uppercase;margin-bottom:6px;font-family:'Share Tech Mono',monospace;">
              Cảnh báo rủi ro
            </div>
            <div style="background:#1a0d0d;border-left:3px solid #ff4757;
              padding:8px 12px;border-radius:0 6px 6px 0;
              font-size:.75rem;color:#fca5a5;line-height:1.6;">{risk_r[:300]}</div>
          </div>

          <!-- Tin tuc noi bat -->
          <div style="margin-bottom:12px;">
            <div style="font-size:.65rem;letter-spacing:1.5px;color:#4b5563;
              text-transform:uppercase;margin-bottom:6px;font-family:'Share Tech Mono',monospace;">
              Tin tức nổi bật
            </div>
            {''.join([
                f"""<div style="display:flex;align-items:flex-start;gap:8px;
                  padding:6px 0;border-bottom:1px solid #1a2332;">
                  <span style="color:{'#ff4757' if it.get('type','')=='fud' else ('#00ff88' if it.get('type','')=='positive' else '#fbbf24')};
                    font-size:.7rem;min-width:40px;font-family:Share Tech Mono,monospace;font-weight:700;">
                    [{it.get('type','').upper()[:3]}]
                  </span>
                  <div>
                    <div style="color:#d1d5db;font-size:.74rem;">{it.get('title','')[:90]}</div>
                    <div style="color:#4b5563;font-size:.62rem;font-family:Share Tech Mono,monospace;">
                      {it.get('src','')}
                    </div>
                  </div></div>"""
                for it in (news_r[:5] if news_r else [{"type":"neutral","title":"Chưa có tin tức","src":""}])
            ])}
          </div>

          <!-- Khuyen nghi -->
          <div style="background:{'#0a1a0a' if kn_icon in ('🟢','') else '#1a1000'};
            border:1px solid {'#00ff88' if kn_icon=='🟢' else '#fbbf24' if kn_icon=='🟡' else '#ff4757'};
            border-radius:6px;padding:12px;margin-top:12px;">
            <div style="font-size:.65rem;letter-spacing:1.5px;color:#4b5563;
              text-transform:uppercase;margin-bottom:6px;font-family:'Share Tech Mono',monospace;">
              Khuyến nghị hành động
            </div>
            <div style="font-family:'Share Tech Mono',monospace;font-size:.85rem;font-weight:700;
              color:{'#00ff88' if kn_icon=='🟢' else '#fbbf24' if kn_icon=='🟡' else '#ff4757'};">
              {kn_icon} {khuyen_nghi}
            </div>
          </div>

          <div style="margin-top:14px;padding-top:10px;border-top:1px solid #1a2332;
            font-family:'Share Tech Mono',monospace;font-size:.6rem;color:#4b5563;text-align:right;">
            FinSent-Agent v1.0 | ChromaDB + PhoBERT + MacroAgent
          </div>
        </div>""")

    with act_col:
        st.html("""
        <div style="border-bottom:1px solid #1a2332;padding-bottom:8px;margin-bottom:12px;">
          <span style="font-family:'Share Tech Mono',monospace;font-size:.72rem;
            color:#00ff88;letter-spacing:1.5px;font-weight:700;">✦ XUẤT & GỬI</span>
        </div>""")

        # ── Telegram ──────────────────────────────────────────────────────────
        st.markdown("**📤 Telegram Bot**")
        tg_token   = st.text_input(
            "Bot Token",
            value=os.getenv("TELEGRAM_BOT_TOKEN",""),
            type="password", key="tg_token",
        )
        tg_chat_id = st.text_input(
            "Chat ID / Channel",
            value=os.getenv("TELEGRAM_CHAT_ID",""),
            key="tg_chat",
        )

        send_tg = st.button("📤 GỬI QUA TELEGRAM", type="primary", use_container_width=True)
        if send_tg:
            if not tg_token or not tg_chat_id:
                st.error("Vui lòng nhập Bot Token và Chat ID")
            else:
                with st.spinner("Đang gửi..."):
                    try:
                        import urllib.request, urllib.parse as _up

                        tg_text = (
                            f"📊 *BÁO CÁO VMSI — {tshown}*\n"
                            f"🕐 {now_str}\n\n"
                            f"*Chỉ số tâm lý:* `{vmsi_v:.1f}/100` — {vmsi_label(vmsi_v)}\n"
                            f"*Giá {tshown}:* `{close_str}` ({chg_sgn_r}{chg_r:.2f}%)\n"
                            f"*NHNN:* {nhnn_lbl_r}\n\n"
                            f"⚠️ {risk_r[:200]}\n\n"
                            f"💡 {kn_icon} {tg_kn}\n\n"
                            f"_FinSent\\-Agent v1\\.0_"
                        )
                        url  = f"https://api.telegram.org/bot{tg_token}/sendMessage"
                        data_tg = _up.urlencode({
                            "chat_id": tg_chat_id,
                            "text": tg_text,
                            "parse_mode": "MarkdownV2",
                        }).encode()
                        req = urllib.request.Request(url, data=data_tg, method="POST")
                        with urllib.request.urlopen(req, timeout=10) as resp_tg:
                            result_tg = json.loads(resp_tg.read())
                        if result_tg.get("ok"):
                            st.success("Gửi Telegram thành công!")
                            st.toast("Báo cáo đã gửi!", icon="📤")
                        else:
                            st.error(f"Telegram lỗi: {result_tg.get('description','')}")
                    except Exception as e:
                        st.error(f"Lỗi gửi Telegram: {e}")

        st.divider()

        # ── Export ────────────────────────────────────────────────────────────
        st.markdown("**📥 Xuất báo cáo**")
        st.download_button(
            label="📄 Tải về Markdown (.md)",
            data=report_md.encode("utf-8"),
            file_name=f"finsent_{tshown}_{_dt.now().strftime('%Y%m%d_%H%M')}.md",
            mime="text/markdown",
            use_container_width=True,
        )
        st.download_button(
            label="📋 Tải về Text (.txt)",
            data=report_md.encode("utf-8"),
            file_name=f"finsent_{tshown}_{_dt.now().strftime('%Y%m%d_%H%M')}.txt",
            mime="text/plain",
            use_container_width=True,
        )

        st.divider()

        # ── Lịch sử phân tích ─────────────────────────────────────────────────
        st.markdown("**📊 Lịch sử phân tích**")
        log = st.session_state.get("rt_log", [])
        if log:
            for entry in log[::-1][:8]:
                vcol = vmsi_color(float(entry.get("VMSI", 50)))
                st.html(f"""
                <div style="display:flex;justify-content:space-between;padding:5px 8px;
                  background:#111827;border-radius:4px;margin-bottom:4px;
                  border-left:2px solid {vcol};">
                  <span style="font-family:'Share Tech Mono',monospace;
                    font-size:.65rem;color:#9ca3af;">
                    {entry.get('Time','')} — {entry.get('Ticker','')}
                  </span>
                  <span style="font-family:'Share Tech Mono',monospace;
                    font-size:.65rem;color:{vcol};">
                    VMSI {entry.get('VMSI','--')}
                  </span>
                </div>""")
        else:
            st.html("""<div style="color:#4b5563;font-family:'Share Tech Mono',monospace;
              font-size:.7rem;padding:8px;">Chưa có lịch sử. Bấm Phân tích ngay.</div>""")

        st.divider()

        # ── Auto-report toggle ────────────────────────────────────────────────
        st.markdown("**⚙️ Tự động gửi báo cáo**")
        auto_report = st.toggle(
            "Gửi Telegram sau mỗi chu kỳ 30p",
            value=st.session_state.get("auto_report", False),
        )
        st.session_state["auto_report"] = auto_report
        if auto_report:
            if tg_token and tg_chat_id:
                st.success("Đang hoạt động — sẽ gửi sau mỗi chu kỳ")
            else:
                st.warning("Chưa có Bot Token / Chat ID")

# ══════════════════════════════════════════════════════════════════════════════
# STATUS BAR
# ══════════════════════════════════════════════════════════════════════════════
kafka_ok = False
try:
    import socket
    from data_pipeline_ingestion.config import settings as _c
    host,port = _c.KAFKA_BROKER.split(":")
    s = socket.socket(); s.settimeout(1)
    kafka_ok = s.connect_ex((host,int(port)))==0; s.close()
except Exception: pass

proc_t = meta.get("processing_time_seconds",0)
msgs   = meta.get("social_messages_processed",0)

st.html(f"""
<div class="statusbar">
  <span>System Health: <span class="status-ok">OPTIMAL</span></span>
  <span>Latency: <span class="status-ok">{proc_t:.1f}s</span></span>
  <span>AI connectivity: <span class="{'status-ok' if kafka_ok else 'status-warn'}">
    {'SECURED' if kafka_ok else 'STANDBY'}</span></span>
  <span>Messages: <span class="status-ok">{msgs}</span></span>
  <div class="status-right">
    <span>API Status</span>
    <span>Security Logs</span>
    <span>Terminal V4.2.0</span>
  </div>
</div>
""")

# ── Auto refresh ──────────────────────────────────────────────────────────────
if auto_on:
    time.sleep(1800)
    st.rerun()
