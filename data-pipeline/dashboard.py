import streamlit as st
import pandas as pd
import json
import plotly.graph_objects as go
from datetime import datetime
import time

# ==========================================
# 1. CẤU HÌNH TRANG (Phải ở dòng đầu tiên)
# ==========================================
st.set_page_config(
    page_title="FinSent-Agent Dashboard",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# Thêm CSS custom để tinh chỉnh màu sắc cho giống thiết kế
st.markdown("""
    <style>
    .metric-box {
        padding: 15px; border-radius: 10px; text-align: center; margin-bottom: 15px;
    }
    .buy-box { background-color: rgba(0, 200, 83, 0.1); border: 1px solid #00c853; color: #00c853; }
    .hold-box { background-color: rgba(255, 193, 7, 0.1); border: 1px solid #ffc107; color: #ffc107; }
    .sell-box { background-color: rgba(255, 82, 82, 0.1); border: 1px solid #ff5252; color: #ff5252; }
    </style>
""", unsafe_allow_html=True)

# ==========================================
# 2. HÀM ĐỌC DỮ LIỆU REAL-TIME
# ==========================================
def load_vmsi_data():
    try:
        with open("live_vmsi.json", "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        # Fallback data nếu chưa chạy backend
        return {"vmsi_value": 50, "status": "normal", "risk_warning": "Đang chờ dữ liệu..."}

vmsi_data = load_vmsi_data()

# ==========================================
# 3. HEADER
# ==========================================
col_header1, col_header2 = st.columns([8, 2])
with col_header1:
    st.markdown("### 🅵 FinSent-Agent <span style='font-size:0.5em; background-color:#2e3b4e; padding:2px 8px; border-radius:10px; color:#4fc3f7'>SHB Edition</span>", unsafe_allow_html=True)
with col_header2:
    st.markdown(f"<div style='text-align: right; color: #ff5252;'>🔴 LIVE {datetime.now().strftime('%H:%M:%S')}</div>", unsafe_allow_html=True)

st.divider()

# ==========================================
# 4. BIỂU ĐỒ VMSI (NỬA TRÊN)
# ==========================================
st.markdown("##### 📈 Biểu đồ VMSI — Trục X: Thời gian (đơn vị 1H)")

# Giả lập dữ liệu chuỗi thời gian cho biểu đồ
time_labels = [f"{i}:00" for i in range(20, 24)] + [f"0{i}:00" for i in range(0, 10)] + [f"{i}:00" for i in range(10, 20)]
vmsi_values = [45, 48, 50, 55, 62, 65, 68, 70, 70, 68, 65, 60, 58, 55, 54, 53, 56, 58, 60, 62, 63, 65, vmsi_data['vmsi_value'], None]

fig = go.Figure()

# Vùng Sợ hãi (0-25)
fig.add_hrect(y0=0, y1=25, line_width=0, fillcolor="rgba(255, 82, 82, 0.1)", annotation_text="Sợ hãi", annotation_position="bottom right")
# Vùng Tham lam (75-100)
fig.add_hrect(y0=75, y1=100, line_width=0, fillcolor="rgba(0, 200, 83, 0.1)", annotation_text="Tham lam", annotation_position="top right")

# Đường VMSI thực tế
fig.add_trace(go.Scatter(x=time_labels, y=vmsi_values, mode='lines+markers', name='VMSI Thực tế', line=dict(color='#4fc3f7', width=3)))

fig.update_layout(
    plot_bgcolor='rgba(0,0,0,0)',
    paper_bgcolor='rgba(0,0,0,0)',
    height=350,
    margin=dict(l=20, r=20, t=20, b=20),
    yaxis=dict(range=[0, 100])
)
st.plotly_chart(fig, use_container_width=True)

# ==========================================
# 5. KHU VỰC BÊN DƯỚI (CHIA 2 CỘT)
# ==========================================
col_left, col_right = st.columns([5, 5], gap="large")

with col_left:
    # --- BLOCK 1: KHUYẾN NGHỊ HÀNH ĐỘNG ---
    st.markdown("##### 🎯 KHUYẾN NGHỊ HÀNH ĐỘNG")
    c1, c2, c3 = st.columns(3)
    with c1: st.markdown("<div class='metric-box buy-box'><b>MUA VÀO</b><br><small>Khi VMSI < 25 (Vùng sợ hãi cực độ)</small></div>", unsafe_allow_html=True)
    with c2: st.markdown("<div class='metric-box hold-box'><b>NẮM GIỮ</b><br><small>Khi VMSI 25-75 (Vùng trung lập)</small></div>", unsafe_allow_html=True)
    with c3: st.markdown("<div class='metric-box sell-box'><b>GIẢM MARGIN</b><br><small>Khi VMSI > 75 (Vùng tham lam cực độ)</small></div>", unsafe_allow_html=True)

    # --- BLOCK 2: CẢNH BÁO SỚM ---
    st.markdown("##### ⚠️ CẢNH BÁO SỚM 24-48H")
    st.info(f"🔮 **Dự báo:** Hệ thống nhận định tâm lý đang ở mức {vmsi_data['vmsi_value']}\n\n"
            f"📉 **Phân tích:** {vmsi_data['risk_warning']}\n\n"
            f"💡 **Hành động:** Chú ý các nhịp điều chỉnh của thị trường.")

    # --- BLOCK 3: BẢNG CẢNH BÁO ---
    st.markdown("##### 🚨 BẢNG CẢNH BÁO")
    df_alerts = pd.DataFrame({
        "Thời gian": ["14:30", "14:15", "13:45"],
        "Nội dung": ["VMSI vượt ngưỡng 75 (Tham lam)", "Phát hiện lệch pha với VN-Index", "Tin đồn NHNN trên Telegram"],
        "Mức độ": ["🟠 CAM", "🟡 VÀNG", "🟡 VÀNG"],
        "Hành động": ["Xem", "Xem", "Xem"]
    })
    st.dataframe(df_alerts, hide_index=True, use_container_width=True)

with col_right:
    # --- BLOCK 4: CHATBOT TƯ VẤN ---
    st.markdown("##### 💬 Chatbot tư vấn VMSI")
    
    # Quick actions
    qa1, qa2, qa3 = st.columns(3)
    if qa1.button("📊 VMSI là gì?"): st.session_state.chat_input = "VMSI là gì?"
    if qa2.button("🎯 Khuyến nghị?"): st.session_state.chat_input = "Cho tôi khuyến nghị hiện tại."
    if qa3.button("🔍 Phân tích?"): st.session_state.chat_input = "Phân tích rủi ro giúp tôi."

    # Khởi tạo lịch sử chat
    if "messages" not in st.session_state:
        st.session_state.messages = [{"role": "assistant", "content": "👋 Chào bạn! Tôi là chuyên gia tư vấn của FinSent. Tôi đang theo dõi VMSI ở mức " + str(vmsi_data['vmsi_value']) + ". Tôi có thể giúp gì cho chiến lược của bạn hôm nay?"}]

    # Hiển thị tin nhắn (có giới hạn chiều cao bằng CSS container)
    chat_container = st.container(height=350)
    with chat_container:
        for msg in st.session_state.messages:
            st.chat_message(msg["role"]).write(msg["content"])

    # Ô nhập liệu
    if prompt := st.chat_input("Nhập tin nhắn..."):
        st.session_state.messages.append({"role": "user", "content": prompt})
        st.chat_message("user").write(prompt)
        
        # GIẢ LẬP GỌI API LLM TẠI ĐÂY (Sẽ thay bằng code gọi API thật)
        with st.chat_message("assistant"):
            with st.spinner("Đang phân tích..."):
                time.sleep(1) # Giả lập độ trễ mạng
                response = f"Dựa trên mức VMSI {vmsi_data['vmsi_value']} hiện tại, hệ thống ghi nhận trạng thái {vmsi_data['status']}. Lời khuyên của tôi cho câu hỏi '{prompt}' là bạn nên theo sát các diễn biến vĩ mô tiếp theo."
                st.write(response)
        st.session_state.messages.append({"role": "assistant", "content": response})