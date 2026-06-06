import logging
import numpy as np
from typing import Union

# Custom Exception để kiểm soát lỗi tính toán khắt khe
class VMSICalculationError(Exception):
    """Lỗi phát sinh khi tính toán VMSI gặp giá trị vô cực (Infinity) hoặc NaN."""
    pass

class VMSIEngine:
    """
    Trái tim tính toán của hệ thống FinSent-Agent.
    Áp dụng các công thức lượng hóa tâm lý thị trường (VMSI) chuẩn xác và cực nhanh bằng NumPy.
    """
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)

    def validate_finite_values(self, *values: float) -> None:
        """Đảm bảo không có giá trị nào bị lỗi Toán học (NaN, Infinity) [Req 1.9]"""
        for val in values:
            if not np.isfinite(val):
                self.logger.error(f"Lỗi Toán học nghiêm trọng: Phát hiện giá trị phi tuyến {val}")
                raise VMSICalculationError(f"Giá trị tính toán không hợp lệ: {val}")

    def calculate_interaction_weight(self, likes: int, shares: int, comments: int) -> float:
        """Tính trọng số tương tác bằng hàm Logarit nén: E_i = ln(1 + likes_i + shares_i + comments_i)"""
        if likes < 0 or shares < 0 or comments < 0:
            raise ValueError("Lượt tương tác (likes, shares, comments) không được là số âm.")
        
        weight = np.log(1 + likes + shares + comments)
        self.validate_finite_values(weight)
        return float(weight)

    def validate_inputs(self, phobert_scores: np.ndarray, credibility_factors: np.ndarray) -> None:
        """Kiểm duyệt gắt gao dữ liệu đầu vào [Req 1.2]"""
        if not np.all((phobert_scores >= -1.0) & (phobert_scores <= 1.0)):
            raise ValueError("PhoBERT scores phải nằm trong dải [-1, 1].")
        if not np.all((credibility_factors >= 0.1) & (credibility_factors <= 1.0)):
            raise ValueError("Credibility factors (Độ uy tín) phải nằm trong dải [0.1, 1.0].")

    def calculate_social_score(self, phobert_scores: np.ndarray, interaction_weights: np.ndarray, credibility_factors: np.ndarray) -> float:
        """
        Tính điểm mạng xã hội: S_social(t) = Σ(s_i × E_i × R_i) / Σ(E_i × R_i)
        """
        self.validate_inputs(phobert_scores, credibility_factors)
        
        numerator = np.sum(phobert_scores * interaction_weights * credibility_factors)
        denominator = np.sum(interaction_weights * credibility_factors)
        
        # Xử lý lỗi chia cho 0 nếu không có tương tác nào (tránh crash hệ thống)
        if denominator == 0:
            self.logger.warning("Tổng trọng số tương tác bằng 0, trả về Social Score = 0.0")
            return 0.0
            
        social_score = numerator / denominator
        self.validate_finite_values(social_score)
        
        self.logger.debug(f"Đã tính S_social: {social_score:.4f}")
        return float(social_score)

    def validate_nhnn_score(self, s_nhnn: Union[int, float]) -> None:
        """Điểm chính sách NHNN chỉ được phép là 3 trạng thái rạch ròi [Req 1.5]"""
        if s_nhnn not in [-1, 0, 1]:
            raise ValueError(f"S_nhnn phải thuộc tập hợp {-1, 0, 1}. Nhận được: {s_nhnn}")

    def calculate_macro_score(self, s_nhnn: Union[int, float], s_news: float) -> float:
        """Tính điểm vĩ mô: S_macro(t) = 0.7 × S_nhnn(t) + 0.3 × S_news(t)"""
        self.validate_nhnn_score(s_nhnn)
        
        macro_score = (0.7 * s_nhnn) + (0.3 * s_news)
        self.validate_finite_values(macro_score)
        
        self.logger.debug(f"Đã tính S_macro: {macro_score:.4f}")
        return float(macro_score)

    def calculate_raw_index(self, s_macro: float, s_social: float) -> float:
        """Tính chỉ số thô: I_raw(t) = 0.6 × S_macro(t) + 0.4 × S_social(t)"""
        raw_index = (0.6 * s_macro) + (0.4 * s_social)
        self.validate_finite_values(raw_index)
        return float(raw_index)

    def calculate_final_vmsi(self, i_raw: float) -> float:
        """
        Tính VMSI cuối cùng (thang 0-100): VMSI(t) = 50 × (I_raw(t) + 1)
        Bảo vệ ranh giới (Boundary handling): Nếu I_raw < -1 thì VMSI = 0.
        """
        if i_raw < -1.0:
            self.logger.warning(f"I_raw ({i_raw}) vượt quá giới hạn dưới, ép VMSI về 0.")
            return 0.0
            
        vmsi = 50.0 * (i_raw + 1.0)
        self.validate_finite_values(vmsi)
        
        self.logger.info(f"Đã tính toán thành công VMSI: {vmsi:.2f}/100")
        return float(vmsi)

    def apply_ema_smoothing(self, current_vmsi: float, previous_vmsi_smoothed: float) -> float:
        """
        Làm mịn đồ thị tâm lý bằng hàm EMA (Exponential Moving Average)
        Công thức: VMSI_smoothed(t) = 0.2 × VMSI(t) + 0.8 × VMSI_smoothed(t-1)
        Giúp biểu đồ không bị giật cục bởi các tin rác nhất thời (noise).
        """
        smoothed = (0.2 * current_vmsi) + (0.8 * previous_vmsi_smoothed)
        self.validate_finite_values(smoothed)
        return float(smoothed)