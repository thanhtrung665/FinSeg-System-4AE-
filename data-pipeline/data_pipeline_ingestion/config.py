import os
import chromadb
from pathlib import Path
from dotenv import load_dotenv
from pydantic import BaseModel, Field

# Tìm file .env từ thư mục data-pipeline/ bất kể script được gọi từ đâu
_PIPELINE_ROOT = Path(__file__).resolve().parent.parent
_ENV_FILE = _PIPELINE_ROOT / ".env"
load_dotenv(dotenv_path=str(_ENV_FILE))


class PipelineConfig(BaseModel):
    """Lớp cấu hình tập trung cho toàn bộ Data Pipeline."""

    # Kafka Settings
    KAFKA_BROKER: str = Field(default_factory=lambda: os.getenv("KAFKA_BROKER_URL", "localhost:9092"))

    # ChromaDB Settings
    CHROMADB_MODE: str = Field(default_factory=lambda: os.getenv("CHROMADB_MODE", "cloud"))
    CHROMADB_API_KEY: str = Field(default_factory=lambda: os.getenv("CHROMADB_API_KEY", ""))
    CHROMADB_TENANT: str = Field(default_factory=lambda: os.getenv("CHROMADB_TENANT", ""))
    CHROMADB_DATABASE: str = Field(default_factory=lambda: os.getenv("CHROMADB_DATABASE", "fin-sent-database"))
    CHROMADB_COLLECTION: str = Field(default_factory=lambda: os.getenv("CHROMADB_COLLECTION", "macro_policies"))

    # HuggingFace
    HUGGINGFACE_API_KEY: str = Field(default_factory=lambda: os.getenv("HUGGINGFACE_API_KEY", ""))

    # Data Paths — default trỏ về data-pipeline/data/ theo absolute path
    SHB_DATASET_PATH: str = Field(
        default_factory=lambda: os.getenv(
            "SHB_DATASET_PATH",
            str(_PIPELINE_ROOT / "data" / "facebook_mock_shb.csv")
        )
    )
    SCB_DATASET_PATH: str = Field(
        default_factory=lambda: os.getenv(
            "SCB_DATASET_PATH",
            str(_PIPELINE_ROOT / "data" / "facebook_mock_SCB (1).csv")
        )
    )
    NHNN_DOCS_PATH: str = Field(
        default_factory=lambda: os.getenv(
            "NHNN_DOCS_PATH",
            str(_PIPELINE_ROOT / "data" / "nhnn_docs_SCB")
        )
    )
    SHB_DOCS_PATH: str = Field(
        default_factory=lambda: os.getenv(
            "SHB_DOCS_PATH",
            str(_PIPELINE_ROOT / "data" / "shb")
        )
    )

    # Replay & Logging
    REPLAY_RATE_LIMIT: str = Field(default_factory=lambda: os.getenv("REPLAY_RATE_LIMIT", "1"))
    LOG_LEVEL: str = Field(default_factory=lambda: os.getenv("LOG_LEVEL", "INFO"))

    @classmethod
    def get_chroma_client(cls):
        """Khởi tạo kết nối Vector DB một cách an toàn."""
        mode = os.getenv("CHROMADB_MODE", "cloud").lower()

        if mode == "cloud":
            api_key = os.getenv("CHROMADB_API_KEY", "")
            tenant = os.getenv("CHROMADB_TENANT", "")
            database = os.getenv("CHROMADB_DATABASE", "fin-sent-database")

            if not api_key:
                raise ValueError("Lỗi: Thiếu CHROMADB_API_KEY cho chế độ Cloud.")

            return chromadb.CloudClient(
                tenant=tenant,
                database=database,
                api_key=api_key,
            )
        else:
            return chromadb.HttpClient(host="localhost", port=8000)


# Singleton instance — các module khác chỉ cần: from data_pipeline_ingestion.config import settings
settings = PipelineConfig()
