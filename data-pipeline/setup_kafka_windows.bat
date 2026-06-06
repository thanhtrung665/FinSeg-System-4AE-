@echo off
REM ============================================================================
REM Setup Kafka Native tren Windows (Khong dung Docker)
REM ============================================================================

echo ========================================================
echo   FINSENT-AGENT KAFKA SETUP FOR WINDOWS
echo ========================================================
echo.

REM Kiem tra Java
echo [1/7] Kiem tra Java...
java -version >nul 2>&1
if %errorlevel% neq 0 (
    echo [ERROR] Java chua duoc cai dat!
    echo.
    echo Download Java tai: https://adoptium.net/temurin/releases/
    echo Chon: Windows x64, JDK 11 hoac 17
    echo.
    echo Sau khi cai Java, chay lai script nay.
    pause
    exit /b 1
)
echo [OK] Java da duoc cai dat
java -version
echo.

REM Kiem tra Kafka da download chua
echo [2/7] Kiem tra Kafka binary...
if not exist "kafka" (
    echo [INFO] Kafka chua duoc download. Dang download...
    echo.
    echo Download Kafka binary tu: https://kafka.apache.org/downloads
    echo Chon: kafka_2.13-3.6.0.tgz (hoac moi hon)
    echo.
    echo Giai nen vao thu muc: %cd%\kafka
    echo.
    echo Sau khi giai nen xong, chay lai script nay.
    pause
    exit /b 1
)
echo [OK] Kafka binary da ton tai
echo.

REM Tao thu muc data
echo [3/7] Tao thu muc data...
if not exist "kafka\data\zookeeper" mkdir kafka\data\zookeeper
if not exist "kafka\data\kafka-logs" mkdir kafka\data\kafka-logs
echo [OK] Tao thu muc data thanh cong
echo.

REM Cau hinh Zookeeper
echo [4/7] Cau hinh Zookeeper...
(
echo dataDir=%cd%\kafka\data\zookeeper
echo clientPort=2181
echo maxClientCnxns=0
echo admin.enableServer=false
) > kafka\config\zookeeper.properties
echo [OK] Zookeeper config: kafka\config\zookeeper.properties
echo.

REM Cau hinh Kafka Server
echo [5/7] Cau hinh Kafka Server...
(
echo broker.id=0
echo listeners=PLAINTEXT://localhost:9092
echo advertised.listeners=PLAINTEXT://localhost:9092
echo num.network.threads=3
echo num.io.threads=8
echo socket.send.buffer.bytes=102400
echo socket.receive.buffer.bytes=102400
echo socket.request.max.bytes=104857600
echo log.dirs=%cd%\kafka\data\kafka-logs
echo num.partitions=3
echo num.recovery.threads.per.data.dir=1
echo offsets.topic.replication.factor=1
echo transaction.state.log.replication.factor=1
echo transaction.state.log.min.isr=1
echo log.retention.hours=168
echo log.segment.bytes=1073741824
echo log.retention.check.interval.ms=300000
echo zookeeper.connect=localhost:2181
echo zookeeper.connection.timeout.ms=18000
echo group.initial.rebalance.delay.ms=0
) > kafka\config\server.properties
echo [OK] Kafka config: kafka\config\server.properties
echo.

echo [6/7] Tao scripts khoi dong...

REM Script khoi dong Zookeeper
(
echo @echo off
echo title Zookeeper Server
echo cd /d %%~dp0kafka
echo echo Starting Zookeeper...
echo .\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties
) > start_zookeeper.bat
echo [OK] Created: start_zookeeper.bat

REM Script khoi dong Kafka
(
echo @echo off
echo title Kafka Server
echo cd /d %%~dp0kafka
echo timeout /t 10 /nobreak
echo echo Starting Kafka Server...
echo .\bin\windows\kafka-server-start.bat .\config\server.properties
) > start_kafka.bat
echo [OK] Created: start_kafka.bat

REM Script tao topics
(
echo @echo off
echo cd /d %%~dp0kafka
echo echo Waiting for Kafka to start...
echo timeout /t 15 /nobreak
echo.
echo echo Creating Kafka topics...
echo.
echo echo [1/3] Creating topic: fb_mock_data
echo .\bin\windows\kafka-topics.bat --create --topic fb_mock_data --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
echo.
echo echo [2/3] Creating topic: realtime_market
echo .\bin\windows\kafka-topics.bat --create --topic realtime_market --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
echo.
echo echo [3/3] Creating topic: realtime_policy
echo .\bin\windows\kafka-topics.bat --create --topic realtime_policy --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
echo.
echo echo Listing all topics:
echo .\bin\windows\kafka-topics.bat --list --bootstrap-server localhost:9092
echo.
echo echo Done!
echo pause
) > create_topics.bat
echo [OK] Created: create_topics.bat

REM Script stop Kafka
(
echo @echo off
echo echo Stopping Kafka...
echo taskkill /FI "WindowTitle eq Kafka*" /T /F
echo taskkill /FI "WindowTitle eq Zookeeper*" /T /F
echo echo Done!
echo pause
) > stop_kafka.bat
echo [OK] Created: stop_kafka.bat

echo.
echo [7/7] Setup hoan tat!
echo.
echo ========================================================
echo   HUONG DAN KHOI DONG KAFKA
echo ========================================================
echo.
echo Buoc 1: Mo Command Prompt (Terminal 1)
echo         Chay: start_zookeeper.bat
echo.
echo Buoc 2: Mo Command Prompt moi (Terminal 2)
echo         Chay: start_kafka.bat
echo.
echo Buoc 3: Mo Command Prompt moi (Terminal 3)
echo         Chay: create_topics.bat
echo.
echo Sau khi xong, Kafka se chay o:
echo   - Zookeeper: localhost:2181
echo   - Kafka:     localhost:9092
echo.
echo De dung Kafka:
echo   Chay: stop_kafka.bat
echo.
echo ========================================================
pause
