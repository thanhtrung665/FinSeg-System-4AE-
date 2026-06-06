import json
import os
import time
import shutil
import logging
import numpy as np
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, Any, List, Tuple

import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from multi_agent_system.engines.vmsi_engine import VMSIEngine

# Luôn ghi live_vmsi.json vào data-pipeline/ bất kể cwd
_PIPELINE_ROOT = Path(__file__).resolve().parent.parent.parent


class RiskSynthesisAgent:
    """
    Giám đốc Rủi ro (CRO) — tổng hợp Social + Macro → VMSI → live_vmsi.json.
    Sử dụng Atomic File Write để tránh file bị corrupt khi ghi.
    """

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.engine = VMSIEngine()

        # Đường dẫn tuyệt đối — an toàn trên cả Windows lẫn Linux server
        self.output_file = str(_PIPELINE_ROOT / "live_vmsi.json")
        self.backup_file = str(_PIPELINE_ROOT / "live_vmsi.json.backup")

        # Trạng thái EMA — khởi tạo ở mức cân bằng (50/100)
        self.previous_vmsi_smoothed = 50.0
        self.logger.info(f"RiskAgent output → {self.output_file}")

    # ------------------------------------------------------------------ Aggregate
    def aggregate_social_batch(self, social_batch: List[Dict]) -> float:
        """
        Tổng hợp batch bình luận → 1 điểm S_social ∈ [-1, 1].
        Trả về 0.0 nếu batch rỗng (không có dữ liệu social).
        """
        if not social_batch:
            return 0.0

        phobert_scores = []
        interaction_weights = []
        credibility_factors = []

        for item in social_batch:
            score = float(item.get("phobert_score", 0.0))
            # Clamp vào [-1, 1] để tránh VMSIEngine raise ValueError
            phobert_scores.append(max(-1.0, min(1.0, score)))

            likes = int(item.get("likes", 0))
            shares = int(item.get("shares", 0))
            comments = int(item.get("comments", 0))
            weight = self.engine.calculate_interaction_weight(
                max(0, likes), max(0, shares), max(0, comments)
            )
            interaction_weights.append(weight)

            cred = float(item.get("credibility", 0.5))
            # Clamp vào [0.1, 1.0] theo yêu cầu VMSIEngine
            credibility_factors.append(max(0.1, min(1.0, cred)))

        return self.engine.calculate_social_score(
            np.array(phobert_scores),
            np.array(interaction_weights),
            np.array(credibility_factors),
        )

    # ------------------------------------------------------------------ Risk
    def assess_risk_levels(self, vmsi: float) -> Tuple[str, str]:
        """5 trạng thái DOD dựa trên thang VMSI 0-100."""
        if vmsi <= 20:
            return (
                "risk_high",
                "CẢNH BÁO ĐỎ (Hoảng loạn): Dấu hiệu bán tháo và rủi ro Bank-run. "
                "Khuyến nghị: Hạ tỷ trọng cổ phiếu, chuẩn bị kịch bản thanh khoản."
            )
        elif vmsi <= 40:
            return (
                "risk_low",
                "Thị trường thận trọng. Không có dòng tiền mới. "
                "Khuyến nghị: Giữ vị thế phòng thủ, rà soát danh mục."
            )
        elif vmsi >= 81:
            return (
                "risk_high",
                "CẢNH BÁO ĐỎ (FOMO): Hưng phấn tột độ — nguy cơ bong bóng. "
                "Khuyến nghị: Phát cảnh báo NĐT, chốt lời tự doanh."
            )
        elif vmsi >= 61:
            return (
                "risk_low",
                "Xu hướng tích cực. Dòng tiền lan tỏa. "
                "Khuyến nghị: Đẩy mạnh tư vấn, mở rộng margin an toàn."
            )
        else:
            return (
                "normal",
                "Thị trường cân bằng. Phản ứng trung lập với tin tức vĩ mô. "
                "Khuyến nghị: Duy trì cơ cấu tài sản hiện tại."
            )

    # ------------------------------------------------------------------ Atomic Write
    def save_atomic_json(self, data: Dict, max_retries: int = 3) -> bool:
        """
        Ghi file JSON an toàn (Atomic Write Pattern):
        1. Ghi ra file .tmp
        2. Backup file cũ
        3. os.replace() — atomic trên cả Linux và Windows
        """
        temp_file = self.output_file + ".tmp"

        for attempt in range(1, max_retries + 1):
            try:
                with open(temp_file, 'w', encoding='utf-8') as f:
                    json.dump(data, f, ensure_ascii=False, indent=4)

                if os.path.exists(self.output_file):
                    shutil.copy2(self.output_file, self.backup_file)

                os.replace(temp_file, self.output_file)
                self.logger.info(f"VMSI Dashboard cập nhật → {data['vmsi_value']}/100")
                return True

            except Exception as e:
                self.logger.warning(f"Lỗi ghi file (lần {attempt}/{max_retries}): {e}")
                time.sleep(0.5)
                if os.path.exists(self.backup_file):
                    try:
                        shutil.copy2(self.backup_file, self.output_file)
                        self.logger.info("Khôi phục từ backup thành công.")
                    except Exception as restore_err:
                        self.logger.error(f"Lỗi restore backup: {restore_err}")

        raise IOError(f"Không thể ghi {self.output_file} sau {max_retries} lần thử.")

    # ------------------------------------------------------------------ Main cycle
    def process_cycle(
        self,
        social_batch: List[Dict],
        s_nhnn: int,
        macro_summary: str = "",
        s_news: float = 0.0,
    ) -> Dict:
        """
        1 chu kỳ tính toán VMSI hoàn chỉnh:
        Social + Macro → I_raw → VMSI → EMA smoothing → Risk assessment → JSON output.
        """
        t0 = time.time()

        s_social = self.aggregate_social_batch(social_batch)
        s_macro = self.engine.calculate_macro_score(s_nhnn, s_news)
        i_raw = self.engine.calculate_raw_index(s_macro, s_social)
        vmsi_current = self.engine.calculate_final_vmsi(i_raw)
        vmsi_smoothed = self.engine.apply_ema_smoothing(vmsi_current, self.previous_vmsi_smoothed)

        self.previous_vmsi_smoothed = vmsi_smoothed
        status, risk_warning = self.assess_risk_levels(vmsi_smoothed)

        payload = {
            "vmsi_value": round(vmsi_smoothed, 2),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "status": status,
            "risk_warning": risk_warning,
            "macro_summary": macro_summary,
            "component_scores": {
                "s_social": round(s_social, 4),
                "s_macro": round(s_macro, 4),
                "s_nhnn": s_nhnn,
                "s_news": round(s_news, 4),
                "vmsi_raw": round(vmsi_current, 2),
            },
            "processing_metadata": {
                "processing_time_seconds": round(time.time() - t0, 4),
                "social_messages_processed": len(social_batch),
            },
        }

        self.save_atomic_json(payload)
        return payload


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    agent = RiskSynthesisAgent()

    # Kịch bản 1: SCB khủng hoảng
    mock_social = [
        {"phobert_score": -0.9, "likes": 500, "shares": 100, "comments": 300, "credibility": 0.8},
        {"phobert_score": -0.8, "likes": 100, "shares": 20,  "comments": 50,  "credibility": 0.5},
    ]
    result = agent.process_cycle(
        social_batch=mock_social,
        s_nhnn=-1,
        macro_summary="NHNN kiểm soát chặt sau vụ SCB.",
    )
    print("\n=== KẾT QUẢ VMSI ===")
    print(json.dumps(result, indent=2, ensure_ascii=False))
