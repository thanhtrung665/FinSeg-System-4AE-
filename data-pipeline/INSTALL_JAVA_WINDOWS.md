# Cài Java trên Windows - Hướng dẫn nhanh

## 🎯 Kafka cần Java để chạy

Kafka yêu cầu Java 11 trở lên.

---

## ⚡ Cách 1: Download Java từ Adoptium (Khuyến nghị)

### Bước 1: Download

**Link:** https://adoptium.net/temurin/releases/

**Chọn:**
- Operating System: **Windows**
- Architecture: **x64**
- Package Type: **JDK**
- Version: **17** (Khuyến nghị) hoặc **11**

**File download:** `OpenJDK17U-jdk_x64_windows_hotspot_xxx.msi`

### Bước 2: Cài đặt

1. Double-click file `.msi` đã download
2. Click **Next** → **Next** → **Install**
3. **Quan trọng:** Tick vào ô:
   - ☑ **Add to PATH**
   - ☑ **Set JAVA_HOME variable**

### Bước 3: Verify

Mở **Command Prompt** mới và chạy:
```cmd
java -version
```

**Output mong đợi:**
```
openjdk version "17.0.10" 2024-01-16
OpenJDK Runtime Environment Temurin-17.0.10+7 (build 17.0.10+7)
OpenJDK 64-Bit Server VM Temurin-17.0.10+7 (build 17.0.10+7, mixed mode, sharing)
```

---

## 🔧 Cách 2: Cài Java bằng Chocolatey

Nếu bạn có Chocolatey package manager:

```cmd
choco install temurin17
```

Verify:
```cmd
java -version
```

---

## 🔧 Cách 3: Cài Java bằng WinGet (Windows 11)

```cmd
winget install EclipseAdoptium.Temurin.17.JDK
```

Verify:
```cmd
java -version
```

---

## ⚙️ Nếu Java đã cài nhưng command không hoạt động

### Set JAVA_HOME manually:

1. **Tìm đường dẫn Java:**
   ```cmd
   dir "C:\Program Files\Eclipse Adoptium" /s /b | findstr jdk
   ```
   
   Ví dụ output: `C:\Program Files\Eclipse Adoptium\jdk-17.0.10+7`

2. **Set environment variables:**
   ```cmd
   setx JAVA_HOME "C:\Program Files\Eclipse Adoptium\jdk-17.0.10+7"
   setx PATH "%PATH%;%JAVA_HOME%\bin"
   ```

3. **QUAN TRỌNG:** Đóng và mở lại Command Prompt

4. **Verify:**
   ```cmd
   java -version
   echo %JAVA_HOME%
   ```

---

## ✅ Sau khi cài Java xong

Quay lại setup Kafka:

```cmd
cd C:\Users\asus\Downloads\FinSeg-System-4AE-\data-pipeline
powershell -ExecutionPolicy Bypass -File download_kafka.ps1
```

---

## 🐛 Troubleshooting

### ❌ "java is not recognized as an internal or external command"

**Giải pháp:**

1. Kiểm tra Java đã cài chưa:
   ```cmd
   dir "C:\Program Files\Eclipse Adoptium"
   ```

2. Nếu đã cài, set PATH:
   ```cmd
   setx PATH "%PATH%;C:\Program Files\Eclipse Adoptium\jdk-17.0.10+7\bin"
   ```

3. Đóng và mở lại Command Prompt

4. Test lại:
   ```cmd
   java -version
   ```

### ❌ "JAVA_HOME is not set"

```cmd
setx JAVA_HOME "C:\Program Files\Eclipse Adoptium\jdk-17.0.10+7"
```

Đóng và mở lại Command Prompt, test:
```cmd
echo %JAVA_HOME%
```

---

## 📊 Next Steps

Sau khi Java đã cài xong:

1. **Download Kafka:**
   ```cmd
   powershell -ExecutionPolicy Bypass -File download_kafka.ps1
   ```

2. **Khởi động Kafka:**
   ```cmd
   run_all_kafka.bat
   ```

3. **Test Kafka:**
   ```cmd
   python test_kafka_connection.py
   ```

4. **Enable Kafka trong dashboard:**
   - Sửa `dashboard_realtime.py`: `kafka_enabled=True`

5. **Chạy hệ thống:**
   ```cmd
   # Terminal 1: Kafka
   run_all_kafka.bat
   
   # Terminal 2: Scheduler
   python realtime_pipeline\scheduler.py --ticker SHB
   
   # Terminal 3: Vector Worker
   python realtime_pipeline\run_vector_worker.py
   
   # Terminal 4: Dashboard
   streamlit run dashboard_realtime.py
   ```

---

Made with ❤️ by FinSent-Agent Team
