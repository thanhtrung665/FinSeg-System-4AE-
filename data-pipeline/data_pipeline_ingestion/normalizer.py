import re
from datetime import datetime, timezone
from typing import Dict, Any
from pydantic import BaseModel, field_validator


class FacebookMockData(BaseModel):
    """Schema chuẩn hóa Standard JSON cho dữ liệu Facebook — Pydantic v2 compatible."""
    comment_id: str
    ticker: str
    content_text: str
    created_at: str
    likes: int
    shares: int
    comments: int
    timestamp_ingested: str
    normalized: bool = True
    source_dataset: str

    @field_validator('likes', 'shares', 'comments', mode='before')
    @classmethod
    def parse_to_int(cls, v: Any) -> int:
        """Tự động ép kiểu các trường đếm số về integer, nếu rỗng hoặc lỗi thì trả về 0."""
        try:
            clean_v = str(v).strip()
            return int(clean_v) if clean_v else 0
        except (ValueError, TypeError):
            return 0


def normalize_csv_row(row: Dict[str, Any], dataset_name: str) -> dict:
    """
    Chuẩn hóa dòng dữ liệu CSV thành Standard JSON:
    1. Đưa toàn bộ header về lowercase.
    2. Loại bỏ các cột rác (Unnamed, cột rỗng).
    3. Trích xuất đầy đủ văn bản bình luận phục vụ AI.
    """
    cleaned_row: Dict[str, str] = {}

    for key, value in row.items():
        # Bỏ qua các key rỗng (do dư dấu phẩy) hoặc các cột Unnamed
        if not key or re.match(r'^Unnamed:\s*\d+$', str(key), re.IGNORECASE):
            continue

        clean_key = str(key).strip().lower()
        clean_value = str(value).strip() if value is not None else ""
        cleaned_row[clean_key] = clean_value

    normalized_obj = FacebookMockData(
        comment_id=cleaned_row.get("comment_id", ""),
        ticker=cleaned_row.get("ticker", ""),
        content_text=cleaned_row.get("content_text", ""),
        created_at=cleaned_row.get("created_at", ""),
        likes=cleaned_row.get("likes", 0),
        shares=cleaned_row.get("shares", 0),
        comments=cleaned_row.get("comments", 0),
        timestamp_ingested=datetime.now(timezone.utc).isoformat(),
        source_dataset=dataset_name,
    )

    # Pydantic v2: dùng model_dump() thay vì .dict()
    return normalized_obj.model_dump()
