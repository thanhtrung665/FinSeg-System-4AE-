import time
import logging
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from multi_agent_system.agents.social_agent import SocialAgent
from multi_agent_system.agents.macro_agent import MacroAgent
from multi_agent_system.agents.risk_agent import RiskSynthesisAgent


class MACSystem:
    """
    Multi-Agent Controller (MAC) — điều phối toàn bộ pipeline phân tích VMSI.

    Luồng cố định mỗi chu kỳ:
      1. SocialAgent  → poll Kafka fb_mock_data → scored batch
      2. MacroAgent   → ChromaDB semantic search → S_nhnn + summary
      3. RiskAgent    → tổng hợp VMSI → ghi live_vmsi.json

    Timeout per-step đảm bảo hệ thống không bị treo.
    Graceful degradation: bất kỳ bước nào fail đều fallback, không dừng toàn hệ thống.
    """

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.info("Khởi động Multi-Agent Controller (MAC)...")

        # Timeout (giây) cho từng bước
        # MacroAgent cần >30s lần đầu vì load embedding model (~60s)
        # Sau lần đầu model đã load, chỉ cần ~2s/query
        self.social_timeout = 15.0
        self.macro_timeout = 120.0   # Rộng lần đầu do tải model
        self.risk_timeout = 10.0

        # Khởi tạo agents — MacroAgent load embedding model tại đây (1 lần duy nhất)
        self.logger.info("Đang khởi tạo agents (MacroAgent tải embedding model, có thể mất ~60s)...")
        self.social_agent = SocialAgent()
        self.macro_agent = MacroAgent()
        self.risk_agent = RiskSynthesisAgent()
        self.logger.info("Tất cả agents đã sẵn sàng.")

    def _run_with_timeout(self, func, *args, timeout: float, step_name: str):
        """
        Chạy func(*args) trong thread riêng với timeout.
        Raise TimeoutError nếu vượt quá thời gian.
        """
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(func, *args)
            try:
                return future.result(timeout=timeout)
            except FuturesTimeoutError:
                self.logger.error(
                    f"[{step_name}] TIMEOUT sau {timeout}s — áp dụng fallback."
                )
                raise TimeoutError(f"{step_name} timed out after {timeout}s")

    def execute_sequential_workflow(self, ticker_context: str = "SHB") -> dict:
        """
        Chạy 1 chu kỳ phân tích VMSI đầy đủ.
        Trả về dict kết quả (vmsi_value, status, risk_warning, ...) hoặc dict lỗi.
        """
        t_start = time.time()
        self.logger.info(f"=== BẮT ĐẦU CHU KỲ [{ticker_context}] ===")

        # ── BƯỚC 1: Social Agent ──────────────────────────────────────
        social_batch = []
        try:
            self.logger.info("[1/3] Social Agent đang poll Kafka...")
            # Poll trực tiếp — không dùng thread để tránh Kafka socket fd leak
            social_batch = self.social_agent.process_message_batch(
                batch_size=100, timeout_ms=int(self.social_timeout * 1000)
            )
            self.logger.info(f"[1/3] Social Agent → {len(social_batch)} messages.")
        except Exception as e:
            self.logger.warning(f"[1/3] Social Agent lỗi: {e} — dùng batch rỗng.")
            social_batch = []

        # ── BƯỚC 2: Macro Agent ───────────────────────────────────────
        s_nhnn = 0
        macro_summary = "Không lấy được báo cáo vĩ mô (fallback)."
        try:
            self.logger.info("[2/3] Macro Agent đang query ChromaDB...")
            s_nhnn, macro_summary = self._run_with_timeout(
                self.macro_agent.process_macro_context,
                ticker_context,
                timeout=self.macro_timeout,
                step_name="MacroAgent",
            )
            self.logger.info(f"[2/3] Macro Agent → S_nhnn={s_nhnn}")
        except Exception as e:
            self.logger.warning(f"[2/3] Macro Agent lỗi: {e} — S_nhnn=0 (Trung lập).")
            s_nhnn = 0

        # ── BƯỚC 3: Risk Synthesis ────────────────────────────────────
        try:
            self.logger.info("[3/3] Risk Agent đang tổng hợp VMSI...")
            result = self._run_with_timeout(
                self.risk_agent.process_cycle,
                social_batch,
                s_nhnn,
                macro_summary,
                timeout=self.risk_timeout,
                step_name="RiskAgent",
            )
        except Exception as e:
            self.logger.error(f"[3/3] Risk Agent lỗi NGHIÊM TRỌNG: {e}")
            return {"error": "Lỗi tổng hợp VMSI", "details": str(e)}

        elapsed = time.time() - t_start
        self.logger.info(
            f"=== HOÀN THÀNH CHU KỲ [{ticker_context}] "
            f"| VMSI={result.get('vmsi_value')} "
            f"| {elapsed:.2f}s ==="
        )
        return result

    def shutdown(self):
        """Đóng tất cả kết nối an toàn khi tắt service."""
        try:
            self.social_agent.close()
        except Exception:
            pass
        self.logger.info("MAC System đã shutdown.")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s | %(name)-22s | %(levelname)-8s | %(message)s'
    )

    mac = MACSystem()
    logging.info("MAC System đang chạy (Ctrl+C để dừng)...")

    try:
        while True:
            # Xen kẽ SCB và SHB để thấy sự khác biệt
            for ctx in ["SCB", "SHB"]:
                result = mac.execute_sequential_workflow(ticker_context=ctx)
                logging.info(
                    f"Context={ctx} | VMSI={result.get('vmsi_value', 'N/A')} "
                    f"| Status={result.get('status', 'N/A')}"
                )
                time.sleep(5)
    except KeyboardInterrupt:
        logging.info("Nhận Ctrl+C — đóng MAC System...")
        mac.shutdown()
