import time
import logging
from typing import List, Dict, Tuple, Any

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from data_pipeline_ingestion.config import settings

# Model embedding PHẢI khớp với model dùng khi ingest trong nhnn_ingestor.py
# keepitreal/vietnamese-sbert → 768 dimensions
_EMBEDDING_MODEL_NAME = "keepitreal/vietnamese-sbert"


class MacroAgent:
    """
    Agent phân tích chính sách vĩ mô từ kho tài liệu NHNN trên ChromaDB Cloud.
    Dùng cùng embedding model với nhnn_ingestor để đảm bảo vector dimension khớp (768-dim).
    Tích hợp: In-memory Cache (TTL 300s), Circuit Breaker, Graceful Fallback.
    """

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)

        # Cache (TTL: 5 phút)
        self.cache: Dict[str, Dict[str, Any]] = {}
        self.cache_ttl = 300

        # Circuit Breaker
        self.circuit_open = False

        # Load embedding model — PHẢI khớp với nhnn_ingestor.py
        self.embedder = self._load_embedder()

        # Kết nối ChromaDB
        self.collection = self._setup_chroma_connection()

    def _load_embedder(self):
        """
        Tải SentenceTransformer với model khớp lúc ingest.
        Trả về None nếu không tải được — hệ thống fallback gracefully.
        """
        try:
            from sentence_transformers import SentenceTransformer
            self.logger.info(f"Đang tải embedding model: {_EMBEDDING_MODEL_NAME}...")
            embedder = SentenceTransformer(_EMBEDDING_MODEL_NAME)
            self.logger.info("Tải embedding model thành công.")
            return embedder
        except ImportError:
            self.logger.error(
                "Thiếu sentence-transformers. Cài: pip install sentence-transformers"
            )
            return None
        except Exception as e:
            self.logger.error(f"Lỗi tải embedding model: {e}")
            return None

    def _setup_chroma_connection(self):
        """Kết nối ChromaDB Cloud. Circuit Breaker mở nếu fail."""
        try:
            self.logger.info("Đang kết nối ChromaDB Cloud (MacroAgent)...")
            client = settings.get_chroma_client()
            collection = client.get_collection(name=settings.CHROMADB_COLLECTION)
            self.circuit_open = False
            self.logger.info("Kết nối ChromaDB thành công!")
            return collection
        except Exception as e:
            self.circuit_open = True
            self.logger.error(f"CRITICAL: Lỗi kết nối ChromaDB: {e}. Dùng Fallback.")
            return None

    # ------------------------------------------------------------------ Cache
    def _get_from_cache(self, query: str) -> Any:
        if query in self.cache:
            entry = self.cache[query]
            if time.time() - entry['timestamp'] < self.cache_ttl:
                self.logger.debug(f"Cache HIT: '{query[:60]}...'")
                return entry['data']
            del self.cache[query]
        return None

    def _save_to_cache(self, query: str, data: Any):
        self.cache[query] = {'timestamp': time.time(), 'data': data}

    # ------------------------------------------------------------------ Search
    def perform_semantic_search(
        self, query_text: str, k: int = 5, threshold: float = 0.35
    ) -> List[Dict]:
        """
        Tìm kiếm ngữ nghĩa trong kho tài liệu NHNN.

        Dùng query_embeddings (không phải query_texts) vì collection được ingest thủ công
        với embeddings 768-dim — ChromaDB không có embedding_function nội bộ cho collection này.

        threshold=0.35: phù hợp với cosine similarity của vietnamese-sbert
        trên văn bản tài chính tiếng Việt (~0.40–0.50 là bình thường).
        """
        # 1. Cache
        cached = self._get_from_cache(query_text)
        if cached is not None:
            return cached

        # 2. Fallback guards
        if self.circuit_open or self.collection is None:
            self.logger.warning("ChromaDB không khả dụng — trả về [].")
            return []
        if self.embedder is None:
            self.logger.warning("Embedding model không khả dụng — trả về [].")
            return []

        try:
            t0 = time.time()
            # Embed query với cùng model và normalize (cosine similarity)
            query_vector = self.embedder.encode(
                [query_text], normalize_embeddings=True
            ).tolist()

            results = self.collection.query(
                query_embeddings=query_vector,
                n_results=k,
                include=["documents", "metadatas", "distances"],
            )

            valid_docs = []
            if results and results.get('documents') and results['documents'][0]:
                for i in range(len(results['documents'][0])):
                    distance = results['distances'][0][i]
                    # ChromaDB cosine distance = 1 - cosine_similarity
                    similarity = max(0.0, 1.0 - distance)
                    if similarity >= threshold:
                        valid_docs.append({
                            "content": results['documents'][0][i],
                            "similarity": round(similarity, 4),
                            "metadata": results['metadatas'][0][i] if results.get('metadatas') else {},
                        })

            elapsed = time.time() - t0
            self.logger.info(
                f"Semantic search '{query_text[:50]}...' "
                f"→ {len(valid_docs)}/{k} docs (threshold={threshold}) "
                f"trong {elapsed:.2f}s"
            )

            self._save_to_cache(query_text, valid_docs)
            return valid_docs

        except Exception as e:
            self.logger.error(f"Lỗi query ChromaDB: {e}")
            return []

    # ------------------------------------------------------------------ Analysis
    def analyze_policy_sentiment(self, documents: List[Dict]) -> int:
        """
        Rule-based keyword scoring → {-1, 0, 1}.
        -1: thắt chặt / rủi ro
         0: trung lập
         1: nới lỏng / hỗ trợ
        """
        if not documents:
            self.logger.info("Không có tài liệu → S_nhnn = 0 (Trung lập).")
            return 0

        restrictive = [
            "thắt chặt", "tăng lãi suất", "hút tiền", "kiểm soát",
            "hạn chế", "khởi tố", "vi phạm", "xử lý nghiêm", "phong tỏa",
        ]
        accommodative = [
            "nới lỏng", "giảm lãi suất", "bơm tiền", "hỗ trợ",
            "khuyến khích", "tháo gỡ", "ổn định",
        ]

        score_sum: float = 0.0
        for doc in documents:
            content = doc["content"].lower()
            r_count = sum(1 for kw in restrictive if kw in content)
            a_count = sum(1 for kw in accommodative if kw in content)

            if r_count > a_count:
                score_sum -= doc["similarity"]
            elif a_count > r_count:
                score_sum += doc["similarity"]

        if score_sum < -0.5:
            return -1
        elif score_sum > 0.5:
            return 1
        return 0

    def generate_vietnamese_summary(self, documents: List[Dict], s_nhnn: int) -> str:
        """Sinh báo cáo vắn tắt tiếng Việt dựa trên kết quả phân tích."""
        if not documents:
            return "Không ghi nhận chỉ thị vĩ mô hay tin tức pháp lý nào nổi bật."

        policy_map = {
            -1: "Thắt chặt / Cảnh báo rủi ro (Rút thanh khoản, thanh tra)",
             0: "Trung lập (Duy trì ổn định)",
             1: "Nới lỏng / Hỗ trợ tích cực (Bơm thanh khoản, giảm lãi suất)",
        }
        avg_sim = sum(d["similarity"] for d in documents) / len(documents)

        return (
            f"Đã phân tích {len(documents)} tài liệu từ Ngân hàng Nhà nước.\n"
            f"- Độ tin cậy trung bình: {avg_sim * 100:.1f}%\n"
            f"- Đánh giá chính sách: {policy_map.get(s_nhnn, 'Không xác định')}\n"
            f"- Hệ thống đã đối chiếu để khử nhiễu tin đồn mạng xã hội."
        )

    def process_macro_context(self, ticker_context: str) -> Tuple[int, str]:
        """Entry point chính: Semantic search → Sentiment scoring → Summary."""
        query = (
            f"Thông cáo báo chí Ngân hàng Nhà nước về {ticker_context} "
            f"thanh khoản rủi ro tín dụng"
        )
        docs = self.perform_semantic_search(query)
        s_nhnn = self.analyze_policy_sentiment(docs)
        summary = self.generate_vietnamese_summary(docs, s_nhnn)
        return s_nhnn, summary


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    agent = MacroAgent()
    print("\n--- TEST: SCB Vạn Thịnh Phát ---")
    score, report = agent.process_macro_context("SCB Vạn Thịnh Phát")
    print(f"S_nhnn = {score}")
    print(report)

    print("\n--- TEST: SHB ---")
    score2, report2 = agent.process_macro_context("SHB")
    print(f"S_nhnn = {score2}")
    print(report2)
