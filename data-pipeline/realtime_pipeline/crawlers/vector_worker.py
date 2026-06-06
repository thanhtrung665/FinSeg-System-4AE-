# -*- coding: utf-8 -*-
"""
realtime_pipeline/vector_worker.py

Tiến trình Consumer chuyên dụng cho Vector Database.
Lắng nghe Kafka các topic: 'news_data' và 'policy_data'.
Thực hiện Chunking -> Embedding -> Ingest lên ChromaDB Cloud.
"""

import os
import json
import logging
import time
from datetime import datetime
from kafka import KafkaConsumer
import chromadb
from sentence_transformers import SentenceTransformer

logger = logging.getLogger("Vector-Worker")

class RealtimeVectorIngestor:
    def __init__(self):
        logger.info("Khởi tạo Vector Ingestor...")
        
        # 1. Khởi tạo ChromaDB Cloud Client
        try:
            self.chroma_client = chromadb.HttpClient(
                host="api.trychroma.com", # Hoặc endpoint cloud cụ thể của bạn nếu có
                headers={"x-chroma-token": os.getenv("CHROMADB_API_KEY")},
                tenant=os.getenv("CHROMADB_TENANT"),
                database=os.getenv("CHROMADB_DATABASE")
            )
            self.collection = self.chroma_client.get_or_create_collection(
                name=os.getenv("CHROMADB_COLLECTION", "macro_policies")
            )
            logger.info("✅ Kết nối ChromaDB Cloud thành công!")
        except Exception as e:
            logger.error(f"❌ Lỗi kết nối ChromaDB: {e}")
            self.collection = None

        # 2. Khởi tạo Embedding Model (HuggingFace)
        hf_token = os.getenv("HUGGINGFACE_API_KEY")
        logger.info("Đang tải mô hình keepitreal/vietnamese-sbert...")
        try:
            self.embedder = SentenceTransformer(
                'keepitreal/vietnamese-sbert', 
                token=hf_token
            )
            logger.info("✅ Tải Embedding Model thành công!")
        except Exception as e:
            logger.error(f"❌ Lỗi tải model: {e}")
            self.embedder = None

    def chunk_text(self, text: str, chunk_size: int = 1500, overlap: int = 200) -> list:
        """Cắt văn bản thành các đoạn nhỏ có phần chồng lấn (overlap) để giữ ngữ cảnh"""
        if not text:
            return []
        chunks = []
        start = 0
        while start < len(text):
            end = start + chunk_size
            chunks.append(text[start:end])
            start += chunk_size - overlap # Lùi lại một chút để tạo overlap
        return chunks

    def process_message(self, data: dict, topic: str):
        """Xử lý 1 gói tin từ Kafka và đẩy lên ChromaDB"""
        if not self.collection or not self.embedder:
            logger.warning("Chưa sẵn sàng kết nối DB/Model. Bỏ qua tin.")
            return

        try:
            # Xác định các trường dữ liệu tùy theo nguồn (Báo chí hay NHNN)
            doc_id = data.get("article_id") or data.get("doc_id")
            text_content = data.get("content_text", "")
            title = data.get("title", "")
            ticker = data.get("ticker_context", "MACRO")
            source = data.get("source", "unknown")
            published_at = data.get("published_at", "")

            if not text_content or len(text_content) < 50:
                return

            # Cắt chunk
            chunks = self.chunk_text(text_content)
            
            for i, chunk in enumerate(chunks):
                chunk_id = f"{doc_id}_chunk_{i}"
                
                # Tạo vector
                vector = self.embedder.encode(chunk).tolist()
                
                # Metadata (Giới hạn các kiểu dữ liệu cơ bản: str, int, float)
                metadata = {
                    "source": str(source),
                    "title": str(title)[:200], # Giới hạn độ dài title
                    "ticker_context": str(ticker),
                    "published_at": str(published_at),
                    "topic": str(topic),
                    "chunk_index": i
                }

                # Đẩy lên ChromaDB
                self.collection.upsert(
                    ids=[chunk_id],
                    embeddings=[vector],
                    metadatas=[metadata],
                    documents=[chunk]
                )
            
            logger.info(f"Đã nạp thành công '{title[:30]}...' ({len(chunks)} chunks) vào Vector DB.")

        except Exception as e:
            logger.error(f"Lỗi khi Ingest vector: {e}")

    def run_consumer(self):
        """Vòng lặp lắng nghe Kafka liên tục"""
        try:
            consumer = KafkaConsumer(
                'news_data', 'policy_data', # Lắng nghe đồng thời 2 topic
                bootstrap_servers=[os.getenv('KAFKA_BROKER', 'localhost:9092')],
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                auto_offset_reset='latest', # Chỉ lấy tin mới nhất lúc realtime
                group_id='vector_ingestion_group'
            )
            logger.info("🎧 Vector Worker đang lắng nghe Kafka...")
            
            for message in consumer:
                topic = message.topic
                data = message.value
                self.process_message(data, topic)
                
        except Exception as e:
            logger.error(f"Lỗi Kafka Consumer trong Vector Worker: {e}")