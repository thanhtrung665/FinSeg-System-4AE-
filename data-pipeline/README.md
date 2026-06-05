# FinSent-Agent: Local Data Pipeline Infrastructure

Hệ thống Data Pipeline này cung cấp hạ tầng streaming cục bộ bằng **Apache Kafka** chạy trên Docker Desktop, kết hợp với Vector Database được quản lý trên **ChromaDB Cloud**. 

Mục tiêu: Đảm bảo độ trễ thấp cho dữ liệu time-series và tính toàn vẹn dữ liệu chuẩn bị cho các Multi-Agent AI (PhoBERT/LLM).

## Yêu cầu hệ thống (Prerequisites)
- **Docker Desktop** (Phiên bản ≥ 4.x)
- Minimum RAM: **8GB** (Docker cần cấu hình ít nhất 4GB RAM)
- Disk space: **20GB** free

## Khởi chạy nhanh (Quick Start)

1. Sao chép cấu hình môi trường:
   ```bash
   cp .env.example .env