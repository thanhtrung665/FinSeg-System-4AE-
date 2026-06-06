"""
fix_vietnamese.py
Thay the tieng Viet khong dau → co dau trong dashboard_realtime.py
Va fix icon chatbot bi tran chu
"""
import re
from pathlib import Path

FILE = Path("dashboard_realtime.py")
src  = FILE.read_text(encoding="utf-8")

# ── 1. Cac cum tu tieng Viet khong dau → co dau ──────────────
REPLACEMENTS = [
    # Tieu de / labels
    ("CHI SO TAM LY VMSI",       "CHỈ SỐ TÂM LÝ VMSI"),
    ("Chi so tam ly VMSI",        "Chỉ số tâm lý VMSI"),
    ("Chi so tam ly",             "Chỉ số tâm lý"),
    ("GIA OPEN",                  "GIÁ OPEN"),
    ("GIA HIEN TAI",              "GIÁ HIỆN TẠI"),
    ("Gia hien tai",              "Giá hiện tại"),
    ("Gia OPEN",                  "Giá OPEN"),
    ("Gia mo cua phien",          "Giá mở cửa phiên"),
    ("Cap nhat:",                 "Cập nhật:"),
    ("BIEN LOI NHUAN",            "BIÊN LỢI NHUẬN"),
    ("Bien loi nhuan",            "Biên lợi nhuận"),
    ("So voi gia mo cua",         "So với giá mở cửa"),
    ("CHIEN LUOC TUI TIEN",       "CHIẾN LƯỢC TÚI TIỀN"),
    ("Chien luoc tui tien",       "Chiến lược túi tiền"),
    ("BANG TIN / FUD BUSTER",     "BẢNG TIN / FUD BUSTER"),
    ("Phan tich ngay",            "Phân tích ngay"),
    ("PHAN TICH",                 "PHÂN TÍCH"),
    ("THI TRUONG",                "THỊ TRƯỜNG"),
    ("TIN TUC",                   "TIN TỨC"),
    ("DASHBOARD",                 "DASHBOARD"),
    # Chatbot headers
    ("AI COPILOT",                "AI COPILOT"),
    ("ONLINE",                    "ONLINE"),
    ("CHATBOT PHAN TICH",         "CHATBOT PHÂN TÍCH"),
    ("Chatbot Phan tich",         "Chatbot Phân tích"),
    ("QWEN2-7B // EXPLAINABLE AI","QWEN2-7B // EXPLAINABLE AI"),
    # Buttons
    ("PHAN TICH",                 "PHÂN TÍCH"),
    ("AUTO 30P",                  "AUTO 30P"),
    ("GUI QUA TELEGRAM",          "GỬI QUA TELEGRAM"),
    ("Tai ve Markdown",           "Tải về Markdown"),
    ("Tai ve Text",               "Tải về Text"),
    ("Xoa lich su",               "Xóa lịch sử"),
    ("Xoa chat",                  "Xóa chat"),
    ("CAP NHAT STREAM",           "CẬP NHẬT STREAM"),
    # Section headers
    ("SOCIAL STREAM",             "SOCIAL STREAM"),
    ("NHNN CROSS-VALIDATION",     "NHNN CROSS-VALIDATION"),
    ("BAO CAO TONG HOP",          "BÁO CÁO TỔNG HỢP"),
    ("XUAT & GUI",                "XUẤT & GỬI"),
    ("Lich su phan tich",         "Lịch sử phân tích"),
    ("Tu dong gui bao cao",       "Tự động gửi báo cáo"),
    ("Gui Telegram sau moi chu ky","Gửi Telegram sau mỗi chu kỳ"),
    ("Active — se gui sau moi chu ky","Active — sẽ gửi sau mỗi chu kỳ"),
    ("Chua co Bot Token",         "Chưa có Bot Token"),
    # KPI analysis tab
    ("Gia thi truong",            "Giá thị trường"),
    ("vs last close",             "vs last close"),
    ("Unrealized PnL",            "Unrealized PnL"),
    ("Intraday volatility",       "Intraday volatility"),
    ("Gia vao trung binh",        "Giá vào trung bình"),
    ("Avg entry",                 "Avg entry"),
    ("Gia tri danh muc",          "Giá trị danh mục"),
    ("Active Positions",          "Active Positions"),
    ("BIEU DO CO PHIEU",          "BIỂU ĐỒ CỔ PHIẾU"),
    ("Bieu do chi so",            "Biểu đồ chỉ số"),
    ("CHIEN LUOC TUI TIEN",       "CHIẾN LƯỢC TÚI TIỀN"),
    ("Maintenance Margin Required","Maintenance Margin Required"),
    ("DE XUAT GIAI PHAP",         "ĐỀ XUẤT GIẢI PHÁP"),
    ("De xuat giai phap",         "Đề xuất giải pháp"),
    ("EARLY MARGIN CALL WARNING", "EARLY MARGIN CALL WARNING"),
    # Quick action buttons
    ("De xuat phong ve",          "Đề xuất phòng vệ"),
    ("Cat lo tu dong",            "Cắt lỗ tự động"),
    ("Tin tuc anh huong",         "Tin tức ảnh hưởng"),
    ("Vi sao",                    "Vì sao"),
    ("Doi chieu NHNN",            "Đối chiếu NHNN"),
    ("Phan tich FUD/FOMO",        "Phân tích FUD/FOMO"),
    ("Xac minh tin tuc",          "Xác minh tin tức"),
    # Status / indicators
    ("Trang thai:",               "Trạng thái:"),
    ("Trang thai he thong",       "Trạng thái hệ thống"),
    ("System Health:",            "System Health:"),
    ("OPTIMAL",                   "OPTIMAL"),
    ("SECURED",                   "SECURED"),
    ("STANDBY",                   "STANDBY"),
    # Messages - chatbot init
    ("Chao ban, toi la tro ly AI FINSENT. He thong phat hien thi truong dang co dau hieu qua ban do tam ly FUD lan rong. VMSI hien tai:",
     "Xin chào! Tôi là trợ lý AI FINSENT. Hệ thống phát hiện thị trường đang có dấu hiệu quá bán do tâm lý FUD lan rộng. VMSI hiện tại:"),
    ("Ban co muon xem danh sach cac ma co phieu dang o vung tich luy tot khong?",
     "Bạn có muốn xem danh sách các mã cổ phiếu đang ở vùng tích lũy tốt không?"),
    ("Hoi AI ve ma co phieu, xu huong thi truong...",
     "Hỏi AI về mã cổ phiếu, xu hướng thị trường..."),
    ("Hoi AI ve danh muc...",
     "Hỏi AI về danh mục..."),
    ("Hoi AI ve tin tuc, FUD/FOMO, doi chieu NHNN...",
     "Hỏi AI về tin tức, FUD/FOMO, đối chiếu NHNN..."),
    # Status bar
    ("Dang phan tich",            "Đang phân tích"),
    ("Hoan tat",                  "Hoàn tất"),
    ("Chua co du lieu. Bam Phan tich ngay.",
     "Chưa có dữ liệu. Bấm Phân tích ngay."),
    # Misc
    ("Bot Token",                 "Bot Token"),
    ("Chat ID / Channel",         "Chat ID / Channel"),
    ("Telegram Bot",              "Telegram Bot"),
    ("Auto-GENERATED",            "Auto-GENERATED"),
    ("Chu ky 30 phut",            "Chu kỳ 30 phút"),
    ("Bam Phan tich ngay de tai du lieu",
     "Bấm Phân tích ngay để tải dữ liệu"),
]

for old, new in REPLACEMENTS:
    src = src.replace(old, new)

# ── 2. Fix icon chatbot bi tran chu ──────────────────────────
# Van de: chat_message dung icon "art_" hoac text qua dai
# Fix: dung avatar ngan gon hon
src = src.replace(
    'with st.chat_message(m["role"]):',
    'with st.chat_message(m["role"], avatar="🤖" if m["role"]=="assistant" else "👤"):'
)

# ── 3. Them CSS fix icon chatbot khong bi tran ────────────────
# Inject them CSS sau dong <style> dau tien trong _CSS
extra_css = """
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
"""
src = src.replace(
    "/* ── Plotly chart bg ── */",
    extra_css + "\n/* ── Plotly chart bg ── */"
)

# ── 4. Ghi file ───────────────────────────────────────────────
FILE.write_bytes(src.encode("utf-8"))
print(f"Done. File size: {len(src):,} chars")

# Verify syntax
import ast
ast.parse(src)
print("Syntax OK")
