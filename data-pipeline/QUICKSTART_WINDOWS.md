# FinSent-Agent — Quick Start cho Windows

## 🎯 Chạy Dashboard trên Windows (Không cần Kafka)

Dashboard có thể chạy **hoàn toàn độc lập** trên Windows mà không cần Kafka. Dashboard sẽ đọc dữ liệu từ file `live_vmsi.json` thay vì từ Kafka.

---

## 🚀 Cách 1: Chạy Dashboard Realtime (KHUYẾN NGHỊ)

### Bước 1: Mở Command Prompt hoặc PowerShell

```cmd
cd C:\Users\asus\Downloads\FinSeg-System-4AE-\data-pipeline
```

### Bước 2: Kích hoạt virtual environment (nếu có)

```cmd
.venv\Scripts\activate
```

### Bước 3: Chạy Dashboard Realtime

```cmd
streamlit run dashboard_realtime.py
```

Dashboard sẽ tự động mở tại: **http://localhost:8501**

---

## 🎨 Cách 2: Chạy Dashboard Demo

Dashboard Demo sử dụng dữ liệu mẫu từ CSV files:

```cmd
streamlit run dashboard.py
```

Dashboard sẽ mở tại: **http://localhost:8501**

---

## ✨ Các chức năng hoạt động KHÔNG CẦN Kafka

### ✅ Hoạt động bình thường:
- ✅ Xem VMSI score từ file `live_vmsi.json`
- ✅ Xem biểu đồ và metrics
- ✅ Xem tin tức (từ cache)
- ✅ Chat với AI Chatbot (nếu có HuggingFace API key)
- ✅ Xem danh mục và phân tích

### ⚠️ Cần chạy lệnh thủ công:
- ⚠️ Nút "▶ PHÂN TÍCH" - Sẽ crawl dữ liệu mới nhưng **không push vào Kafka**
- ⚠️ Nút "↻ CẬP NHẬT STREAM" - Crawl social media/news mới

---

## 🔧 Nếu muốn cào dữ liệu và tính VMSI mới

### Option A: Chạy 1 chu kỳ thủ công (không cần Kafka)

```cmd
cd realtime_pipeline
python scheduler.py --ticker SHB --once
```

Lệnh này sẽ:
1. Crawl Facebook + tin tức + NHNN + giá cổ phiếu
2. Tính VMSI score
3. **Lưu vào `live_vmsi.json`** (Dashboard sẽ tự động đọc)
4. **KHÔNG push vào Kafka** (vì Kafka chưa chạy)

### Option B: Chạy scheduler tự động mỗi 30 phút

```cmd
cd realtime_pipeline
python scheduler.py --ticker SHB
```

Scheduler sẽ chạy liên tục, cập nhật VMSI mỗi 30 phút.

---

## 📊 Kiểm tra kết quả

Sau khi chạy scheduler, kiểm tra file VMSI:

```cmd
type live_vmsi.json
```

Hoặc dùng Python:

```cmd
python -c "import json; print(json.load(open('live_vmsi.json')))"
```

---

## 🐛 Troubleshooting

### ❌ Lỗi: `RequestTimedOutError: Request timed out after 30000 ms`

**Nguyên nhân:** Dashboard cố kết nối Kafka nhưng Kafka chưa chạy.

**Giải pháp:** 
- ✅ **Đã fix tự động!** Dashboard bây giờ hoạt động mà không cần Kafka
- Dashboard sẽ hiện warning nhưng vẫn hoạt động bình thường
- Để tắt warning hoàn toàn, chỉ cần **không click nút "▶ PHÂN TÍCH"**

### ❌ Lỗi: `use_container_width` deprecation warning

**Nguyên nhân:** Streamlit phiên bản mới không dùng `use_container_width` nữa.

**Giải pháp:**
- ✅ **Đã fix!** Tất cả `use_container_width=True` đã được thay bằng `width='stretch'`
- Restart dashboard để áp dụng thay đổi

### ❌ Lỗi: `No module named 'feedparser'`

**Giải pháp:**

```cmd
pip install feedparser beautifulsoup4 lxml requests
```

### ❌ Dashboard trống không có dữ liệu

**Nguyên nhân:** File `live_vmsi.json` chưa được tạo hoặc chưa có dữ liệu.

**Giải pháp:**

```cmd
# Chạy 1 chu kỳ để sinh dữ liệu
cd realtime_pipeline
python scheduler.py --ticker SHB --once
```

---

## 🎯 Workflow đầy đủ trên Windows

### 1️⃣ Lần đầu tiên setup

```cmd
# Cài dependencies
pip install -r requirements.txt
pip install -r realtime_pipeline\requirements_realtime.txt

# Cấu hình .env (BẮT BUỘC)
copy .env.example .env
notepad .env
```

**Điền vào .env:**
```env
CHROMADB_API_KEY=ck-xxxxxxxxxxxx
CHROMADB_TENANT=xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
CHROMADB_DATABASE=fin-sent-database
HUGGINGFACE_API_KEY=hf_xxxxxxxxxxxx
```

### 2️⃣ Chạy 1 chu kỳ test

```cmd
cd realtime_pipeline
python scheduler.py --ticker SHB --once
```

Output mong đợi:
```
[INFO] Crawling Facebook posts...
[INFO] Crawling news articles...
[INFO] Getting stock prices...
[INFO] Calculating VMSI...
[INFO] VMSI = 65.3 / 100
[INFO] Saved to live_vmsi.json
```

### 3️⃣ Mở Dashboard

```cmd
cd ..
streamlit run dashboard_realtime.py
```

### 4️⃣ Xem kết quả

Mở browser: **http://localhost:8501**

---

## 🚀 Nâng cao: Chạy với Kafka (Optional)

Nếu muốn chạy đầy đủ với Kafka streaming:

### 1. Cài Docker Desktop

Download tại: https://www.docker.com/products/docker-desktop

### 2. Khởi động Kafka

```cmd
docker-compose -f docker-compose.kafka.yml up -d
```

### 3. Verify Kafka

```cmd
docker ps | findstr kafka
```

### 4. Chạy đầy đủ hệ thống

```cmd
# Terminal 1: Scheduler
cd realtime_pipeline
python scheduler.py --ticker SHB

# Terminal 2: Vector Worker
python run_vector_worker.py

# Terminal 3: Dashboard
cd ..
streamlit run dashboard_realtime.py
```

---

## 📖 Tài liệu khác

- **QUICKSTART.md** - Hướng dẫn đầy đủ (Linux/Mac)
- **SETUP_GPU_SERVER.md** - Hướng dẫn deploy lên GPU server
- **TROUBLESHOOTING.md** - Xử lý lỗi chi tiết
- **README_INTEGRATION.md** - Chi tiết kiến trúc hệ thống

---

## ✅ Tóm tắt

**Để chạy dashboard trên Windows:**

```cmd
# Bước 1: Cấu hình .env
copy .env.example .env
notepad .env

# Bước 2: Cài dependencies
pip install -r requirements.txt
pip install -r realtime_pipeline\requirements_realtime.txt

# Bước 3: Tạo dữ liệu VMSI
cd realtime_pipeline
python scheduler.py --ticker SHB --once
cd ..

# Bước 4: Mở dashboard
streamlit run dashboard_realtime.py
```

**Truy cập:** http://localhost:8501

**Kafka:** KHÔNG BẮT BUỘC - Dashboard hoạt động độc lập!

---

Made with ❤️ by FinSent-Agent Team
