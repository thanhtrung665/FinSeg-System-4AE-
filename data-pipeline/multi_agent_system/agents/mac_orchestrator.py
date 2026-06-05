import time
import logging
from concurrent.futures import ThreadPoolExecutor, TimeoutError

# Import 3 Agent đã xây dựng từ các bước trước
from agents.social_agent import SocialAgent
from agents.macro_agent import MacroAgent
from agents.risk_agent import RiskSynthesisAgent

class MACSystem:
    """
    Nhạc trưởng điều phối (Multi-Agent Controller).
    Kiểm soát luồng thực thi tuần tự, ép Timeout và đảm bảo an toàn toàn hệ thống.
    """
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.info("Đang khởi động Multi-Agent Controller (MAC)...")
        
        # Khởi tạo các Agent cốt lõi
        self.social_agent = SocialAgent()
        self.macro_agent = MacroAgent()
        self.risk_agent = RiskSynthesisAgent()
        
        # Cấu hình giới hạn thời gian chạy (Timeout) [Req 5.2]
        self.execution_timeout = 30.0 # Giới hạn 30 giây cho toàn bộ 1 chu kỳ

    def _execute_with_timeout(self, func, *args, timeout: float):
        """Hàm bọc (wrapper) để ép các Agent phải trả về kết quả trong thời gian quy định."""
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(func, *args)
            try:
                return future.result(timeout=timeout)
            except TimeoutError:
                self.logger.error(f"Timeout: {func.__name__} vượt quá thời gian cho phép ({timeout}s).")
                raise TimeoutError(f"Operation timed out after {timeout} seconds")

    def execute_sequential_workflow(self, ticker_context: str = "SHB") -> dict:
        """
        Thực thi luồng công việc tuần tự cố định: Social -> Macro -> Risk. [Req 5.1, 5.10]
        """
        start_time = time.time()
        self.logger.info(f"--- BẮT ĐẦU CHU KỲ PHÂN TÍCH VMSI (Context: {ticker_context}) ---")
        
        # -------------------------------------------------------------
        # BƯỚC 1: SOCIAL AGENT (Thu thập bình luận)
        # -------------------------------------------------------------
        social_batch = []
        try:
            self.logger.info("[1/3] Đang gọi Social Agent...")
            # Cho phép Social Agent chạy tối đa 10s
            social_batch = self._execute_with_timeout(
                self.social_agent.process_message_batch, 
                timeout=10.0
            )
            self.logger.info(f"Social Agent thu thập được {len(social_batch)} gói tin.")
        except Exception as e:
            self.logger.warning(f"Lỗi Social Agent: {e}. Hệ thống áp dụng Fallback (Bỏ qua Social).")
            social_batch = [] # Graceful degradation: Chạy tiếp với danh sách rỗng

        # -------------------------------------------------------------
        # BƯỚC 2: MACRO AGENT (Phân tích vĩ mô NHNN)
        # -------------------------------------------------------------
        s_nhnn = 0
        macro_summary = "Không thể lấy báo cáo vĩ mô do lỗi kết nối."
        try:
            self.logger.info("[2/3] Đang gọi Macro Agent...")
            # Cho phép Macro Agent chạy tối đa 15s (vì có thể phải gọi ChromaDB)
            s_nhnn, macro_summary = self._execute_with_timeout(
                self.macro_agent.process_macro_context, 
                ticker_context,
                timeout=15.0
            )
        except Exception as e:
            self.logger.warning(f"Lỗi Macro Agent: {e}. Hệ thống áp dụng Fallback (Trung lập).")
            s_nhnn = 0 # Fallback về trạng thái trung lập

        # -------------------------------------------------------------
        # BƯỚC 3: RISK SYNTHESIS AGENT (Tổng hợp & Đánh giá)
        # -------------------------------------------------------------
        result = {}
        try:
            self.logger.info("[3/3] Đang gọi Risk Synthesis Agent...")
            # Truyền toàn bộ dữ liệu vào cho Giám đốc Rủi ro
            result = self.risk_agent.process_cycle(
                social_batch=social_batch,
                s_nhnn=s_nhnn,
                macro_summary=macro_summary
            )
        except Exception as e:
            self.logger.error(f"CRITICAL: Lỗi nghiêm trọng tại Risk Agent: {e}")
            # Nếu Risk Agent chết, hệ thống buộc phải báo lỗi chu kỳ này
            return {"error": "Lỗi tổng hợp VMSI", "details": str(e)}

        total_time = time.time() - start_time
        self.logger.info(f"--- HOÀN THÀNH CHU KỲ ({total_time:.2f}s) ---")
        return result

if __name__ == "__main__":
    # Thiết lập log hiển thị đẹp trên màn hình Terminal
    logging.basicConfig(
        level=logging.INFO, 
        format='%(asctime)s | %(name)-20s | %(levelname)-8s | %(message)s'
    )
    
    mac = MACSystem()
    
    # Giả lập chạy liên tục giống như một service thực thụ
    logging.info("Hệ thống MAC đang chạy (Nhấn Ctrl+C để dừng)...")
    try:
        while True:
            # Ở bản Demo, bạn có thể gọi xen kẽ "SHB" và "SCB" để thấy sự khác biệt
            mac.execute_sequential_workflow(ticker_context="SCB")
            
            # Nghỉ 5 giây trước khi chạy chu kỳ quét tiếp theo
            time.sleep(5)
            
    except KeyboardInterrupt:
        logging.info("Đã nhận lệnh tắt. Đóng MAC System an toàn.")