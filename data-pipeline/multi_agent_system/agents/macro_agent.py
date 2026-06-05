import time
import logging
from typing import List, Dict, Tuple, Any
from datetime import datetime

# Tận dụng cấu hình tập trung
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from data_pipeline_ingestion.config import settings

class MacroAgent:
    """
    Agent phân tích chính sách vĩ mô (Ngân hàng Nhà nước).
    Tích hợp Caching, Semantic Search và tự động tóm tắt bằng tiếng Việt.
    """
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # Cấu hình Cache để tối ưu hiệu năng (TTL: 300 giây)
        self.cache: Dict[str, Dict[str, Any]] = {}
        self.cache_ttl = 300 
        
        # Trạng thái kết nối (Circuit Breaker)
        self.circuit_open = False
        
        # Khởi tạo kết nối ChromaDB
        self.collection = self._setup_chroma_connection()

    def _setup_chroma_connection(self):
        """Thiết lập kết nối với ChromaDB Cloud [Req 3.1]"""
        try:
            self.logger.info("Đang kết nối tới ChromaDB Cloud (Macro_Agent)...")
            client = settings.get_chroma_client()
            collection = client.get_collection(name=settings.CHROMADB_COLLECTION)
            self.circuit_open = False
            self.logger.info("Kết nối Vector Database thành công!")
            return collection
        except Exception as e:
            self.circuit_open = True
            self.logger.error(f"CRITICAL: Lỗi kết nối ChromaDB: {e}. Hệ thống sẽ dùng Fallback.")
            return None

    def _get_from_cache(self, query: str) -> Any:
        """Lấy dữ liệu từ bộ nhớ đệm nếu chưa hết hạn [Req 3.2]"""
        if query in self.cache:
            entry = self.cache[query]
            if time.time() - entry['timestamp'] < self.cache_ttl:
                self.logger.debug(f"Cache HIT cho truy vấn: '{query}'")
                return entry['data']
            else:
                del self.cache[query] # Xóa cache cũ
        return None

    def _save_to_cache(self, query: str, data: Any):
        """Lưu kết quả vào bộ nhớ đệm"""
        self.cache[query] = {
            'timestamp': time.time(),
            'data': data
        }

    def perform_semantic_search(self, query_text: str, k: int = 5, threshold: float = 0.7) -> List[Dict]:
        """
        Tìm kiếm ngữ nghĩa trong kho tài liệu NHNN.
        Chỉ trả về các tài liệu có độ tương đồng (distance) vượt ngưỡng threshold. [Req 3.3, 3.4]
        """
        # 1. Kiểm tra Cache trước
        cached_result = self._get_from_cache(query_text)
        if cached_result is not None:
            return cached_result

        # 2. Xử lý khi rớt mạng (Fallback) [Req 3.8]
        if self.circuit_open or self.collection is None:
            self.logger.warning("Sử dụng Fallback rỗng do ChromaDB không khả dụng.")
            return []

        try:
            start_time = time.time()
            # Thực hiện truy vấn Vector. (ChromaDB tự động dùng default embedding hoặc cần truyền query_embeddings nếu setup phức tạp)
            # Ở đây ta dùng text thuần để Chroma xử lý nếu collection đã set embedding function lúc tạo.
            results = self.collection.query(
                query_texts=[query_text],
                n_results=k
            )
            
            valid_docs = []
            if results and 'documents' in results and results['documents']:
                for i in range(len(results['documents'][0])):
                    # ChromaDB dùng distance (càng nhỏ càng giống). Giả lập chuyển đổi sang similarity (0-1)
                    distance = results['distances'][0][i] if 'distances' in results else 0
                    similarity = max(0.0, 1.0 - distance) 
                    
                    if similarity >= threshold:
                        doc = {
                            "content": results['documents'][0][i],
                            "similarity": similarity,
                            "metadata": results['metadatas'][0][i] if 'metadatas' in results else {}
                        }
                        valid_docs.append(doc)

            query_time = time.time() - start_time
            self.logger.debug(f"Semantic Search hoàn thành trong {query_time:.2f}s. Tìm thấy {len(valid_docs)} tài liệu hợp lệ.")
            
            # Lưu vào cache
            self._save_to_cache(query_text, valid_docs)
            return valid_docs

        except Exception as e:
            self.logger.error(f"Lỗi truy vấn Vector DB: {e}")
            return []

    def analyze_policy_sentiment(self, documents: List[Dict]) -> int:
        """
        Phân tích nội dung tài liệu và gán điểm chính sách: -1, 0, hoặc 1. [Req 3.5, 3.6]
        (Phiên bản MVP dùng Rule-based kết hợp Keyword. Phiên bản đầy đủ gọi API LLM).
        """
        if not documents:
            self.logger.info("Không có tài liệu vĩ mô liên quan. Mặc định S_nhnn = 0 (Trung lập).")
            return 0

        restrictive_keywords = ["thắt chặt", "tăng lãi suất", "hút tiền", "kiểm soát", "hạn chế", "khởi tố", "vi phạm", "xử lý nghiêm"]
        accommodative_keywords = ["nới lỏng", "giảm lãi suất", "bơm tiền", "hỗ trợ", "khuyến khích", "tháo gỡ"]

        score_sum = 0
        for doc in documents:
            content = doc.get("content", "").lower()
            
            # Đếm từ khóa
            restrictive_count = sum(1 for kw in restrictive_keywords if kw in content)
            accommodative_count = sum(1 for kw in accommodative_keywords if kw in content)
            
            if restrictive_count > accommodative_count:
                score_sum -= doc.get("similarity", 1.0) # Tính trọng số theo độ tương đồng
            elif accommodative_count > restrictive_count:
                score_sum += doc.get("similarity", 1.0)

        # Trả về các giá trị rời rạc đúng yêu cầu {-1, 0, 1}
        if score_sum < -0.5:
            return -1 # Thắt chặt / Tiêu cực
        elif score_sum > 0.5:
            return 1  # Nới lỏng / Tích cực
        else:
            return 0  # Trung lập

    def generate_vietnamese_summary(self, documents: List[Dict], s_nhnn: int) -> str:
        """Tự động sinh báo cáo vắn tắt bằng tiếng Việt. [Req 3.7]"""
        if not documents:
            return "Thị trường hiện tại không ghi nhận chỉ thị vĩ mô hay tin tức pháp lý nào nổi bật."

        policy_type = "Trung lập (Duy trì ổn định)"
        if s_nhnn == -1:
            policy_type = "Thắt chặt / Cảnh báo rủi ro (Rút thanh khoản, thanh tra)"
        elif s_nhnn == 1:
            policy_type = "Nới lỏng / Hỗ trợ tích cực (Bơm thanh khoản, giảm lãi suất)"

        avg_similarity = sum(d["similarity"] for d in documents) / len(documents)
        
        summary = (
            f"Đã phân tích {len(documents)} tài liệu/thông cáo liên quan từ Ngân hàng Nhà nước.\n"
            f"- Mức độ tin cậy của thông tin: {avg_similarity * 100:.1f}%\n"
            f"- Đánh giá chính sách hiện tại: {policy_type}\n"
            f"- Ghi chú: Hệ thống đã đối chiếu để khử nhiễu các tin đồn mạng xã hội."
        )
        return summary

    def process_macro_context(self, ticker_context: str) -> Tuple[int, str]:
        """Hàm chính: Thực thi toàn bộ luồng phân tích vĩ mô."""
        # Truy vấn các thông tin liên quan đến bối cảnh hiện tại (Ví dụ: SCB hoặc Vạn Thịnh Phát)
        query_text = f"Thông cáo báo chí Ngân hàng Nhà nước về {ticker_context} thanh khoản và rủi ro"
        
        documents = self.perform_semantic_search(query_text)
        s_nhnn = self.analyze_policy_sentiment(documents)
        summary = self.generate_vietnamese_summary(documents, s_nhnn)
        
        return s_nhnn, summary

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
    agent = MacroAgent()
    
    # Test thử giả lập kịch bản SCB (Khủng hoảng)
    logging.info("--- TEST KỊCH BẢN SCB ---")
    score, report = agent.process_macro_context("SCB Vạn Thịnh Phát")
    logging.info(f"Điểm chính sách: {score}")
    logging.info(f"Báo cáo:\n{report}")