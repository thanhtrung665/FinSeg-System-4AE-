import json
import os
import time
import shutil
import logging
import numpy as np
from datetime import datetime, timezone
from typing import Dict, Any, List, Tuple

# Import VMSI Engine từ thư mục engines
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from multi_agent_system.engines.vmsi_engine import VMSIEngine

class RiskSynthesisAgent:
    """
    Giám đốc Rủi ro (CRO) của hệ thống FinSent-Agent.
    Tổng hợp dữ liệu, đánh giá rủi ro và xuất file JSON báo cáo an toàn (Atomic Write).
    """
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.engine = VMSIEngine()
        
        # Cấu hình đường dẫn xuất báo cáo
        self.output_file = "live_vmsi.json"
        self.backup_file = f"{self.output_file}.backup"
        
        # Biến trạng thái EMA (Exponential Moving Average) [Req 4.9]
        # Khởi tạo thị trường ở mức cân bằng (50)
        self.previous_vmsi_smoothed = 50.0 

    def aggregate_social_batch(self, social_batch: List[Dict]) -> float:
        """Tổng hợp một lô (batch) bình luận mạng xã hội thành 1 điểm số duy nhất."""
        if not social_batch:
            return 0.0
            
        phobert_scores = []
        interaction_weights = []
        credibility_factors = []

        for item in social_batch:
            phobert_scores.append(item.get("phobert_score", 0.0))
            
            # Tính trọng số logarit từ lượt tương tác
            likes = item.get("likes", 0)
            shares = item.get("shares", 0)
            comments = item.get("comments", 0)
            weight = self.engine.calculate_interaction_weight(likes, shares, comments)
            interaction_weights.append(weight)
            
            credibility_factors.append(item.get("credibility", 0.5))

        # Dùng NumPy để tính toán cực nhanh qua VMSI Engine
        return self.engine.calculate_social_score(
            np.array(phobert_scores),
            np.array(interaction_weights),
            np.array(credibility_factors)
        )

    def assess_risk_levels(self, vmsi: float) -> Tuple[str, str]:
        """
        Phân loại rủi ro và sinh cảnh báo tiếng Việt dựa trên 5 trạng thái DOD. [Req 4.4, 4.5]
        """
        if vmsi <= 20:
            return "risk_high", "CẢNH BÁO ĐỎ (Hoảng loạn): Dấu hiệu bán tháo chéo và rủi ro rút tiền hàng loạt (Bank-run) do tin đồn lan truyền. Khuyến nghị: Hạ ngay tỷ trọng cổ phiếu, chuẩn bị kịch bản thanh khoản."
        elif vmsi <= 40:
            return "risk_low", "Thị trường e dè, thận trọng. Không có dòng tiền mới giải ngân. Khuyến nghị: Khối tự doanh giữ vị thế phòng thủ, rà soát danh mục."
        elif vmsi >= 81:
            return "risk_high", "CẢNH BÁO ĐỎ (FOMO): Đám đông hưng phấn tột độ. Dấu hiệu bong bóng tài sản. Khuyến nghị: Phát báo cáo thức tỉnh NĐT, chốt lời danh mục tự doanh."
        elif vmsi >= 61:
            return "risk_low", "Xu hướng Tích cực. Dòng tiền lan tỏa. Khuyến nghị: Đẩy mạnh tư vấn, mở rộng hạn mức margin an toàn."
        else:
            return "normal", "Thị trường Cân bằng, Tin tưởng. Phản ứng trung lập với tin tức vĩ mô. Khuyến nghị: Duy trì cơ cấu tài sản hiện tại."

    def save_atomic_json(self, data: Dict, max_retries: int = 3) -> bool:
        """
        Ghi dữ liệu nguyên tử (Atomic File Operations) [Req 4.6, 4.10]
        Tránh trường hợp đang ghi file thì sập nguồn làm file JSON bị lỗi (corrupted).
        """
        temp_file = f"{self.output_file}.tmp"
        
        for attempt in range(1, max_retries + 1):
            try:
                # 1. Ghi ra file tạm
                with open(temp_file, 'w', encoding='utf-8') as f:
                    json.dump(data, f, ensure_ascii=False, indent=4)
                
                # 2. Backup file cũ nếu tồn tại
                if os.path.exists(self.output_file):
                    shutil.copy2(self.output_file, self.backup_file)
                
                # 3. Đổi tên nguyên tử (Ghi đè cực nhanh lên file chính)
                os.replace(temp_file, self.output_file)
                self.logger.info(f"Đã cập nhật VMSI Dashboard ({data['vmsi_value']}/100).")
                return True
                
            except Exception as e:
                self.logger.warning(f"Lỗi ghi file (Lần {attempt}/{max_retries}): {e}")
                time.sleep(0.5)
                
                # Cứu hộ từ file backup nếu thất bại
                if os.path.exists(self.backup_file):
                    try:
                        shutil.copy2(self.backup_file, self.output_file)
                        self.logger.info("Đã khôi phục file JSON từ bản backup.")
                    except Exception as restore_err:
                        self.logger.error(f"Lỗi khôi phục backup: {restore_err}")
        
        self.logger.error("CRITICAL: Không thể lưu file JSON sau nhiều lần thử.")
        raise IOError("FileOperationError: Thất bại trong việc lưu live_vmsi.json.")

    def process_cycle(self, social_batch: List[Dict], s_nhnn: int, macro_summary: str = "", s_news: float = 0.0) -> Dict:
        """Luồng điều phối tổng hợp 1 chu kỳ tính toán VMSI [Req 4.3]"""
        start_time = time.time()
        
        # 1. Tính toán các thành phần (Chạy qua VMSI Engine bảo mật)
        s_social = self.aggregate_social_batch(social_batch)
        s_macro = self.engine.calculate_macro_score(s_nhnn, s_news)
        
        # 2. Tính Chỉ số Thô & VMSI gốc
        i_raw = self.engine.calculate_raw_index(s_macro, s_social)
        vmsi_current = self.engine.calculate_final_vmsi(i_raw)
        
        # 3. Làm mịn bằng EMA (Triệt tiêu độ nhiễu)
        vmsi_smoothed = self.engine.apply_ema_smoothing(vmsi_current, self.previous_vmsi_smoothed)
        
        # Cập nhật trạng thái cho chu kỳ tiếp theo
        self.previous_vmsi_smoothed = vmsi_smoothed
        
        # 4. Đánh giá rủi ro
        status, risk_warning = self.assess_risk_levels(vmsi_smoothed)
        
        # 5. Đóng gói Standard JSON cho Dashboard [Req 4.7]
        result_payload = {
            "vmsi_value": round(vmsi_smoothed, 2),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "status": status,
            "risk_warning": risk_warning,
            "macro_summary": macro_summary,
            "component_scores": {
                "s_social": round(s_social, 4),
                "s_macro": round(s_macro, 4),
                "s_nhnn": s_nhnn,
                "confidence": 0.85 # Tạm fix, thực tế sẽ lấy từ Macro Agent
            },
            "processing_metadata": {
                "processing_time_seconds": round(time.time() - start_time, 4),
                "social_messages_processed": len(social_batch)
            }
        }
        
        # 6. Ghi file an toàn
        self.save_atomic_json(result_payload)
        
        return result_payload

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    agent = RiskSynthesisAgent()
    
    # Mock data test
    mock_social = [
        {"phobert_score": -0.9, "likes": 500, "shares": 100, "comments": 300, "credibility": 0.8},
        {"phobert_score": -0.8, "likes": 100, "shares": 20, "comments": 50, "credibility": 0.5}
    ]
    logging.info("--- TEST CHU KỲ VMSI (SCB KHỦNG HOẢNG) ---")
    result = agent.process_cycle(social_batch=mock_social, s_nhnn=-1, macro_summary="NHNN kiểm soát chặt.")
    print(json.dumps(result, indent=2, ensure_ascii=False))