# Troubleshooting Guide

## ❌ Lỗi: "No module named 'feedparser'"

### Nguyên nhân
Streamlit đang cache old code hoặc sử dụng Python environment sai.

### Giải pháp

#### 1. Kiểm tra dependencies
```bash
python check_dependencies.py
```

Nếu thiếu package, cài đặt:
```bash
pip install feedparser beautifulsoup4 lxml requests
pip install -r requirements.txt
pip install -r realtime_pipeline/requirements_realtime.txt
```

#### 2. Clear Streamlit cache và restart

**Windows:**
```bash
# Dừng Streamlit (Ctrl+C trong terminal)
# Xóa cache
rd /s /q %USERPROFILE%\.streamlit\cache

# Khởi động lại
streamlit run dashboard_realtime.py --server.port 8502
```

**Linux/Mac:**
```bash
# Dừng Streamlit (Ctrl+C)
# Xóa cache
rm -rf ~/.streamlit/cache

# Khởi động lại
streamlit run dashboard_realtime.py --server.port 8502
```

#### 3. Force reload với flag
```bash
streamlit run dashboard_realtime.py --server.port 8502 --server.runOnSave true
```

#### 4. Kiểm tra Python environment
```bash
# Kiểm tra Python đang dùng
python --version
which python  # Linux/Mac
where python  # Windows

# Kiểm tra feedparser trong Python hiện tại
python -c "import feedparser; print('OK:', feedparser.__version__)"
```

Nếu OK nhưng Streamlit vẫn lỗi, có thể Streamlit dùng Python khác:
```bash
# Xem Python mà Streamlit dùng
streamlit --version

# Cài lại feedparser cho đúng environment
python -m pip install --upgrade feedparser
```

---

## ❌ Lỗi: Import module khác

### Giải pháp chung

1. **Check all dependencies:**
   ```bash
   python check_dependencies.py
   ```

2. **Reinstall tất cả:**
   ```bash
   pip install --upgrade -r requirements.txt
   pip install --upgrade -r realtime_pipeline/requirements_realtime.txt
   ```

3. **Clear cache và restart:**
   ```bash
   # Windows
   rd /s /q %USERPROFILE%\.streamlit\cache
   
   # Linux/Mac
   rm -rf ~/.streamlit/cache
   ```

---

## ❌ Lỗi: Kafka connection failed

### Giải pháp

1. **Kiểm tra Kafka đang chạy:**
   ```bash
   docker ps | grep kafka
   ```

2. **Nếu chưa chạy:**
   ```bash
   docker-compose -f docker-compose.kafka.yml up -d
   ```

3. **Kiểm tra port 9092:**
   ```bash
   # Windows
   netstat -an | findstr 9092
   
   # Linux/Mac
   nc -zv localhost 9092
   ```

---

## ❌ Lỗi: ChromaDB authentication failed

### Giải pháp

1. **Kiểm tra .env file:**
   ```bash
   cat .env | grep CHROMADB  # Linux/Mac
   type .env | findstr CHROMADB  # Windows
   ```

2. **Đảm bảo có đầy đủ:**
   ```
   CHROMADB_API_KEY=ck-xxxxxxxxxxxx
   CHROMADB_TENANT=xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
   CHROMADB_DATABASE=fin-sent-database
   CHROMADB_COLLECTION=macro_policies
   ```

3. **Test connection:**
   ```bash
   python -c "from realtime_pipeline.config import get_chroma_client; client = get_chroma_client(); print('ChromaDB OK')"
   ```

---

## ❌ Lỗi: Dashboard không hiển thị data

### Giải pháp

1. **Kiểm tra live_vmsi.json:**
   ```bash
   # Xem file có tồn tại không
   ls -la live_vmsi.json  # Linux/Mac
   dir live_vmsi.json  # Windows
   
   # Xem nội dung
   cat live_vmsi.json  # Linux/Mac
   type live_vmsi.json  # Windows
   ```

2. **Chạy 1 chu kỳ để tạo data:**
   ```bash
   python realtime_pipeline/scheduler.py --ticker SHB --once
   ```

3. **Refresh dashboard:**
   - Bấm F5 trong browser
   - Hoặc bấm nút "PHÂN TÍCH" trong dashboard

---

## ❌ Lỗi: Permission denied khi chạy script

### Windows
```bash
# Chạy PowerShell as Administrator
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
```

### Linux/Mac
```bash
# Thêm quyền execute
chmod +x realtime_pipeline/manage_processes.sh
chmod +x realtime_pipeline/run_vector_worker.py
```

---

## 🔧 Debug mode

### Enable verbose logging

1. **Sửa đầu file bạn muốn debug:**
   ```python
   import logging
   logging.basicConfig(level=logging.DEBUG)
   ```

2. **Hoặc set environment variable:**
   ```bash
   # Windows
   set LOG_LEVEL=DEBUG
   
   # Linux/Mac
   export LOG_LEVEL=DEBUG
   ```

3. **Xem logs realtime:**
   ```bash
   tail -f logs/scheduler.log  # Linux/Mac
   Get-Content logs\scheduler.log -Wait  # Windows PowerShell
   ```

---

## 📞 Vẫn gặp lỗi?

1. **Chạy full verify:**
   ```bash
   python realtime_pipeline/verify_quick.py
   ```

2. **Kiểm tra system info:**
   ```bash
   python --version
   pip --version
   docker --version
   streamlit --version
   ```

3. **Gửi log khi report bug:**
   ```bash
   # Lưu output vào file
   python realtime_pipeline/verify_quick.py > verify_output.txt 2>&1
   python check_dependencies.py > dependencies.txt 2>&1
   ```

4. **Check GitHub Issues:**
   - [https://github.com/your-repo/issues](https://github.com/your-repo/issues)

---

**Last Updated:** 2026-06-06  
**Version:** v2.0.0
