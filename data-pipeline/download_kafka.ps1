# ============================================================================
# Download va cai dat Kafka tu dong tren Windows
# ============================================================================

Write-Host "========================================================" -ForegroundColor Cyan
Write-Host "  FINSENT-AGENT - AUTO DOWNLOAD KAFKA FOR WINDOWS" -ForegroundColor Cyan
Write-Host "========================================================" -ForegroundColor Cyan
Write-Host ""

# Kiem tra Java
Write-Host "[1/5] Kiem tra Java..." -ForegroundColor Yellow
try {
    $javaVersion = java -version 2>&1 | Select-String "version"
    Write-Host "[OK] Java da duoc cai dat: $javaVersion" -ForegroundColor Green
} catch {
    Write-Host "[ERROR] Java chua duoc cai dat!" -ForegroundColor Red
    Write-Host ""
    Write-Host "Download Java tai: https://adoptium.net/temurin/releases/" -ForegroundColor Yellow
    Write-Host "Chon: Windows x64, JDK 11 hoac 17" -ForegroundColor Yellow
    Write-Host ""
    exit 1
}

# Kiem tra Kafka da co chua
Write-Host ""
Write-Host "[2/5] Kiem tra Kafka binary..." -ForegroundColor Yellow
if (Test-Path "kafka") {
    Write-Host "[INFO] Kafka da ton tai. Skip download." -ForegroundColor Green
} else {
    Write-Host "[INFO] Dang download Kafka..." -ForegroundColor Yellow
    
    # Kafka version
    $kafkaVersion = "3.6.0"
    $scalaVersion = "2.13"
    $kafkaName = "kafka_$scalaVersion-$kafkaVersion"
    $kafkaUrl = "https://downloads.apache.org/kafka/$kafkaVersion/$kafkaName.tgz"
    $kafkaFile = "$kafkaName.tgz"
    
    Write-Host "Downloading: $kafkaUrl" -ForegroundColor Cyan
    
    try {
        # Download
        Invoke-WebRequest -Uri $kafkaUrl -OutFile $kafkaFile
        Write-Host "[OK] Downloaded: $kafkaFile" -ForegroundColor Green
        
        # Giai nen
        Write-Host ""
        Write-Host "[3/5] Giai nen Kafka..." -ForegroundColor Yellow
        
        # Dung tar (Windows 10+ co san)
        tar -xzf $kafkaFile
        
        # Doi ten thu muc
        Rename-Item -Path $kafkaName -NewName "kafka"
        
        # Xoa file tar
        Remove-Item $kafkaFile
        
        Write-Host "[OK] Giai nen thanh cong: kafka\" -ForegroundColor Green
        
    } catch {
        Write-Host "[ERROR] Khong the download Kafka: $_" -ForegroundColor Red
        Write-Host ""
        Write-Host "Vui long download thu cong tai:" -ForegroundColor Yellow
        Write-Host "https://kafka.apache.org/downloads" -ForegroundColor Yellow
        Write-Host "Giai nen vao thu muc: $PWD\kafka" -ForegroundColor Yellow
        exit 1
    }
}

# Tao thu muc data
Write-Host ""
Write-Host "[4/5] Tao thu muc data..." -ForegroundColor Yellow
New-Item -ItemType Directory -Force -Path "kafka\data\zookeeper" | Out-Null
New-Item -ItemType Directory -Force -Path "kafka\data\kafka-logs" | Out-Null
Write-Host "[OK] Tao thu muc data thanh cong" -ForegroundColor Green

# Cau hinh
Write-Host ""
Write-Host "[5/5] Tao cau hinh..." -ForegroundColor Yellow

$currentDir = (Get-Location).Path -replace '\\', '/'

# Zookeeper config
$zkConfig = @"
dataDir=$currentDir/kafka/data/zookeeper
clientPort=2181
maxClientCnxns=0
admin.enableServer=false
"@
$zkConfig | Out-File -FilePath "kafka\config\zookeeper.properties" -Encoding utf8

# Kafka config
$kafkaConfig = @"
broker.id=0
listeners=PLAINTEXT://localhost:9092
advertised.listeners=PLAINTEXT://localhost:9092
num.network.threads=3
num.io.threads=8
socket.send.buffer.bytes=102400
socket.receive.buffer.bytes=102400
socket.request.max.bytes=104857600
log.dirs=$currentDir/kafka/data/kafka-logs
num.partitions=3
num.recovery.threads.per.data.dir=1
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
log.retention.hours=168
log.segment.bytes=1073741824
log.retention.check.interval.ms=300000
zookeeper.connect=localhost:2181
zookeeper.connection.timeout.ms=18000
group.initial.rebalance.delay.ms=0
"@
$kafkaConfig | Out-File -FilePath "kafka\config\server.properties" -Encoding utf8

Write-Host "[OK] Cau hinh hoan tat" -ForegroundColor Green

# Tao scripts
Write-Host ""
Write-Host "Tao scripts khoi dong..." -ForegroundColor Yellow

# start_zookeeper.bat
$zkScript = @"
@echo off
title Zookeeper Server
cd /d %~dp0kafka
echo Starting Zookeeper...
.\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties
"@
$zkScript | Out-File -FilePath "start_zookeeper.bat" -Encoding ascii

# start_kafka.bat
$kafkaScript = @"
@echo off
title Kafka Server
cd /d %~dp0kafka
timeout /t 10 /nobreak
echo Starting Kafka Server...
.\bin\windows\kafka-server-start.bat .\config\server.properties
"@
$kafkaScript | Out-File -FilePath "start_kafka.bat" -Encoding ascii

# create_topics.bat
$topicsScript = @"
@echo off
cd /d %~dp0kafka
echo Waiting for Kafka to start...
timeout /t 15 /nobreak

echo Creating Kafka topics...
echo.

echo [1/3] Creating topic: fb_mock_data
.\bin\windows\kafka-topics.bat --create --topic fb_mock_data --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1 --if-not-exists

echo [2/3] Creating topic: realtime_market
.\bin\windows\kafka-topics.bat --create --topic realtime_market --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1 --if-not-exists

echo [3/3] Creating topic: realtime_policy
.\bin\windows\kafka-topics.bat --create --topic realtime_policy --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1 --if-not-exists

echo.
echo Listing all topics:
.\bin\windows\kafka-topics.bat --list --bootstrap-server localhost:9092
echo.
echo Done!
pause
"@
$topicsScript | Out-File -FilePath "create_topics.bat" -Encoding ascii

# stop_kafka.bat
$stopScript = @"
@echo off
echo Stopping Kafka...
taskkill /FI "WindowTitle eq Kafka*" /T /F 2>nul
taskkill /FI "WindowTitle eq Zookeeper*" /T /F 2>nul
echo Done!
pause
"@
$stopScript | Out-File -FilePath "stop_kafka.bat" -Encoding ascii

# run_all_kafka.bat - Script chay tat ca trong 1 lenh
$runAllScript = @"
@echo off
echo ========================================================
echo   STARTING KAFKA FULL SYSTEM
echo ========================================================
echo.

echo [1/3] Starting Zookeeper...
start "Zookeeper Server" cmd /c start_zookeeper.bat

echo [2/3] Waiting 10 seconds...
timeout /t 10 /nobreak

echo [3/3] Starting Kafka Server...
start "Kafka Server" cmd /c start_kafka.bat

echo.
echo Waiting 15 seconds for Kafka to initialize...
timeout /t 15 /nobreak

echo.
echo Creating topics...
call create_topics.bat

echo.
echo ========================================================
echo   KAFKA IS READY!
echo ========================================================
echo.
echo Zookeeper: localhost:2181
echo Kafka:     localhost:9092
echo.
echo To stop: Chay stop_kafka.bat
echo.
pause
"@
$runAllScript | Out-File -FilePath "run_all_kafka.bat" -Encoding ascii

Write-Host "[OK] Created: start_zookeeper.bat" -ForegroundColor Green
Write-Host "[OK] Created: start_kafka.bat" -ForegroundColor Green
Write-Host "[OK] Created: create_topics.bat" -ForegroundColor Green
Write-Host "[OK] Created: stop_kafka.bat" -ForegroundColor Green
Write-Host "[OK] Created: run_all_kafka.bat" -ForegroundColor Green

Write-Host ""
Write-Host "========================================================" -ForegroundColor Cyan
Write-Host "  SETUP HOAN TAT!" -ForegroundColor Green
Write-Host "========================================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "CACH 1: Chay tung buoc (de debug):" -ForegroundColor Yellow
Write-Host "  1. Chay: start_zookeeper.bat" -ForegroundColor White
Write-Host "  2. Chay: start_kafka.bat" -ForegroundColor White
Write-Host "  3. Chay: create_topics.bat" -ForegroundColor White
Write-Host ""
Write-Host "CACH 2: Chay tat ca mot lenh (de nhanh):" -ForegroundColor Yellow
Write-Host "  Chay: run_all_kafka.bat" -ForegroundColor White
Write-Host ""
Write-Host "De dung Kafka:" -ForegroundColor Yellow
Write-Host "  Chay: stop_kafka.bat" -ForegroundColor White
Write-Host ""
Write-Host "========================================================" -ForegroundColor Cyan
