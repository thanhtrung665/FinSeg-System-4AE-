import os
import glob
import logging
from datetime import datetime, timezone
import pdfplumber
from sentence_transformers import SentenceTransformer
from config import settings

# Cấu hình Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class NHNNIngestor:
    def __init__(self):
        logger.info("Khởi tạo NHNN Ingestor...")
        
        # Kết nối ChromaDB Cloud thông qua cấu hình tập trung
        self.chroma_client = settings.get_chroma_client()
        self.collection = self.chroma_client.get_or_create_collection(
            name=settings.CHROMADB_COLLECTION,
            metadata={"hnsw:space": "cosine"}
        )
        
        # Khởi tạo mô hình Embedding (Tích hợp HuggingFace API Token)
        # Khuyến nghị dùng model đa ngữ hỗ trợ tiếng Việt cho MVP
        logger.info("Đang tải mô hình Embedding (kết nối HuggingFace)...")
        self.embedder = SentenceTransformer(
            'keepitreal/vietnamese-sbert', 
            token=settings.HUGGINGFACE_API_KEY
        )
        logger.info("Khởi tạo thành công!")

    def extract_text_from_pdf(self, pdf_path: str) -> str:
        """Trích xuất văn bản thô từ file PDF"""
        text_content = ""
        try:
            with pdfplumber.open(pdf_path) as pdf:
                for page in pdf.pages:
                    extracted = page.extract_text()
                    if extracted:
                        text_content += extracted + "\n"
        except Exception as e:
            logger.error(f"Lỗi khi đọc file {pdf_path}: {e}")
        return text_content.strip()

    def process_and_ingest(self, directory_path: str, ticker_context: str, default_date: str):
        """Xử lý hàng loạt PDF trong thư mục và đẩy lên Vector DB"""
        if not os.path.exists(directory_path):
            logger.error(f"Không tìm thấy thư mục: {directory_path}")
            return

        pdf_files = glob.glob(os.path.join(directory_path, "*.pdf"))
        if not pdf_files:
            logger.warning(f"Không có file PDF nào trong {directory_path}")
            return

        for idx, file_path in enumerate(pdf_files):
            file_name = os.path.basename(file_path)
            logger.info(f"Đang xử lý: {file_name}")
            
            # 1. Trích xuất text
            content = self.extract_text_from_pdf(file_path)
            if not content:
                logger.warning(f"File {file_name} rỗng hoặc không thể đọc text.")
                continue

            # Để MVP chạy nhanh, ta chunk (cắt) văn bản theo độ dài cố định nếu quá dài
            # Ở bản thực tế, ta sẽ dùng LangChain TextSplitter
            chunk_size = 2000
            chunks = [content[i:i+chunk_size] for i in range(0, len(content), chunk_size)]

            for chunk_id, chunk_text in enumerate(chunks):
                # 2. Tạo Vector
                vector = self.embedder.encode(chunk_text).tolist()
                
                # 3. Chuẩn bị Metadata (Bắt buộc dùng Standard JSON format dictionary)
                metadata = {
                    "source_file": file_name,
                    "ticker_context": ticker_context,
                    "publish_date": default_date,
                    "document_type": "policy_regulation" if ticker_context == "SHB" else "news_article",
                    "chunk_index": chunk_id
                }
                
                doc_id = f"{ticker_context}_{file_name}_{chunk_id}"

                # 4. Đẩy lên ChromaDB Cloud
                try:
                    self.collection.add(
                        documents=[chunk_text],
                        embeddings=[vector],
                        metadatas=[metadata],
                        ids=[doc_id]
                    )
                except Exception as e:
                    logger.error(f"Lỗi khi đẩy {doc_id} lên ChromaDB: {e}")
                    
            logger.info(f"Đã ingest xong {file_name} ({len(chunks)} chunks).")

if __name__ == "__main__":
    ingestor = NHNNIngestor()
    
    # 1. Ingest dữ liệu Khủng hoảng SCB (Q4/2022)
    logger.info("--- BẮT ĐẦU INGEST DỮ LIỆU SCB ---")
    ingestor.process_and_ingest(
        directory_path=settings.NHNN_DOCS_PATH,
        ticker_context="SCB",
        default_date="2022-10-15T00:00:00Z" # Chuẩn ISO 8601
    )
    
    # 2. Ingest dữ liệu Baseline SHB (2025-2026)
    logger.info("--- BẮT ĐẦU INGEST DỮ LIỆU SHB ---")
    ingestor.process_and_ingest(
        directory_path=settings.SHB_DOCS_PATH,
        ticker_context="SHB",
        default_date="2026-01-15T00:00:00Z"
    )