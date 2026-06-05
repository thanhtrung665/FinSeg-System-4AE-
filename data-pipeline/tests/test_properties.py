import json
import pytest
from hypothesis import given, strategies as st
from datetime import datetime, timezone

# Import các module cốt lõi từ pipeline của chúng ta
from data_pipeline_ingestion.normalizer import normalize_csv_row

# =====================================================================
# PROPERTY 1: Schema Normalization Correctness
# Mọi dòng CSV dù lộn xộn cỡ nào cũng phải bị ép về đúng chuẩn Standard JSON.
# =====================================================================

# Khởi tạo một "cỗ máy sinh dữ liệu rác" giả lập CSV từ Facebook
# Cố tình tạo các Header viết hoa, viết thường lộn xộn và các cột Unnamed: N
csv_row_strategy = st.fixed_dictionaries({
    "Ticker": st.text(min_size=1, max_size=5),
    "Shares": st.integers(min_value=-100, max_value=1000000), # Cố tình cho số âm để test validator
    "Comments": st.one_of(st.integers(min_value=-50, max_value=50000), st.just("")),
    "content_text": st.text(),
    "Unnamed: 7": st.just("rác"),
    "Unnamed: 25": st.just("nhiễu")
})

@given(row=csv_row_strategy)
def test_normalization_removes_unnamed_and_lowercases(row):
    """
    Xác minh: Lớp chuẩn hóa phải xóa sạch cột Unnamed và đưa key về lowercase.
    Đồng thời các số âm hoặc rỗng phải bị ép về 0.
    """
    result = normalize_csv_row(row, "facebook_mock_shb.csv")
    
    # 1. Không còn cột Unnamed nào lọt qua được
    assert not any(k.lower().startswith("unnamed") for k in result.keys())
    
    # 2. Các key bắt buộc phải tồn tại và ở dạng lowercase
    assert "ticker" in result
    assert "shares" in result
    assert "comments" in result
    assert "content_text" in result
    
    # 3. Giá trị âm hoặc rỗng phải được validator ép về >= 0
    assert result["shares"] >= 0
    assert result["comments"] >= 0

# =====================================================================
# PROPERTY 2 & 3: Standard JSON Enforcement & JSONL Rejection
# Bảo vệ Vector Database khỏi các định dạng lậu.
# =====================================================================

def ingest_to_chromadb_mock(data_string: str):
    """Hàm giả lập chốt chặn đầu vào của ChromaDB"""
    try:
        # Thử parse data, nếu có nhiều JSON object cách nhau bằng \n -> JSONL (Bị cấm)
        lines = data_string.strip().split('\n')
        if len(lines) > 1:
            raise ValueError("Tuyệt đối không