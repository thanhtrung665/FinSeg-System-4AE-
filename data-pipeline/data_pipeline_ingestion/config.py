import os
import chromadb
from dotenv import load_dotenv
from pydantic import BaseModel, Field

# Tải biến môi trường từ file .env
load_dotenv()

class PipelineConfig(BaseModel):
    """Lớp cấu hình tập trung cho toàn bộ Data Pipeline"""
    
    # Kafka Settings
    KAFKA_BROKER: str = Field(default_factory=lambda: os.getenv("KAFKA_BROKER_URL", "localhost:9092"))
    
    # ChromaDB Settings
    CHROMADB_MODE: str = Field(default_factory=lambda: os.getenv("CHROMADB_MODE", "cloud"))
    CHROMADB_API_KEY: str = Field(default_factory=lambda: os.getenv("CHROMADB_API_KEY"))
    CHROMADB_TENANT: str = Field(default_factory=lambda: os.getenv("CHROMADB_TENANT"))
    CHROMADB_DATABASE: str = Field(default_factory=lambda: os.getenv("CHROMADB_DATABASE", "fin-sent-database"))
    CHROMADB_COLLECTION: str = Field(default_factory=lambda: os.getenv("CHROMADB_COLLECTION", "macro_policies"))
    
    # HuggingFace
    HUGGINGFACE_API_KEY: str = Field(default_factory=lambda: os.getenv("HUGGINGFACE_API_KEY"))
    
    # Data Paths
    SHB_DATASET_PATH: str = Field(default_factory=lambda: os.getenv("SHB_DATASET_PATH"))
    SCB_DATASET_PATH: str = Field(default_factory=lambda: os.getenv("SCB_DATASET_PATH"))
    
    @classmethod
    def get_chroma_client(cls):
        """Khởi tạo kết nối Vector DB một cách an toàn"""
        config = cls()
        
        if config.CHROMADB_MODE.lower() == 'cloud':
            if not config.CHROMADB_API_KEY:
                raise ValueError("Lỗi: Thiếu CHROMADB_API_KEY cho chế độ Cloud.")
                
            print("Đang kết nối tới ChromaDB Cloud...")
            return chromadb.CloudClient(
                tenant=config.CHROMADB_TENANT,
                database=config.CHROMADB_DATABASE,
                api_key=config.CHROMADB_API_KEY
            )
        else:
            print("Đang kết nối tới ChromaDB Local (Fallback)...")
            # Logic fallback cho local docker nếu cần
            return chromadb.HttpClient(host='localhost', port=8000)

# Khởi tạo instance global để các file khác import
settings = PipelineConfig()