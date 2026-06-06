"""
chatbot_agent.py — Financial Chatbot powered by Qwen2-7B-Instruct.

QUAN TRỌNG: File này YÊU CẦU GPU với VRAM >= 8GB để chạy thực tế.
- load_in_4bit=True yêu cầu thư viện `bitsandbytes` (chỉ hỗ trợ Linux/CUDA đầy đủ).
- Trên Windows local không có GPU, khởi tạo model sẽ bị guard và skip.
- Trên GPU server (Linux + CUDA), model sẽ load bình thường.

Cài đặt trên GPU server:
    pip install bitsandbytes accelerate transformers torch
"""
import logging
import sys

logger = logging.getLogger(__name__)

# Kiểm tra môi trường GPU trước khi import nặng
def _check_gpu_available() -> bool:
    """Kiểm tra có GPU (CUDA) khả dụng không."""
    try:
        import torch
        return torch.cuda.is_available()
    except ImportError:
        return False

def _check_bitsandbytes_available() -> bool:
    """Kiểm tra bitsandbytes đã được cài và hỗ trợ CUDA không."""
    try:
        import bitsandbytes as bnb
        return True
    except (ImportError, RuntimeError):
        return False

# Guard toàn bộ import nặng — chỉ load khi GPU + bitsandbytes sẵn sàng
_GPU_AVAILABLE = _check_gpu_available()
_BNB_AVAILABLE = _check_bitsandbytes_available()
_MODEL_READY = _GPU_AVAILABLE and _BNB_AVAILABLE

if _MODEL_READY:
    import torch
    from transformers import AutoModelForCausalLM, AutoTokenizer, pipeline
    logger.info("GPU + bitsandbytes detected. Model loading enabled.")
else:
    if not _GPU_AVAILABLE:
        logger.warning("ChatbotAgent: CUDA GPU không khả dụng. Model sẽ không được tải.")
    if not _BNB_AVAILABLE:
        logger.warning("ChatbotAgent: bitsandbytes không khả dụng. Cài: pip install bitsandbytes")
    logger.warning("ChatbotAgent chạy ở chế độ STUB (không có model thực).")


class FinancialChatbot:
    """
    Financial Chatbot sử dụng Qwen2-7B-Instruct với quantization 4-bit.
    Yêu cầu GPU server với CUDA + bitsandbytes để chạy thực.
    Trên môi trường local không có GPU, class hoạt động ở chế độ stub.
    """

    def __init__(self, model_name: str = "Qwen/Qwen2-7B-Instruct"):
        self.model_name = model_name
        self.pipe = None

        if not _MODEL_READY:
            reason = []
            if not _GPU_AVAILABLE:
                reason.append("CUDA GPU không khả dụng")
            if not _BNB_AVAILABLE:
                reason.append("bitsandbytes chưa được cài")
            logger.warning(
                f"FinancialChatbot không thể khởi tạo model ({'; '.join(reason)}). "
                "Chạy ở chế độ STUB — generate_advice() sẽ trả về placeholder."
            )
            return

        # Chỉ chạy trên GPU server
        try:
            logger.info(f"Đang tải model {model_name} với 4-bit quantization...")
            tokenizer = AutoTokenizer.from_pretrained(model_name)
            model = AutoModelForCausalLM.from_pretrained(
                model_name,
                torch_dtype=torch.float16,
                device_map="auto",
                load_in_4bit=True,  # Yêu cầu bitsandbytes + CUDA
            )
            self.pipe = pipeline("text-generation", model=model, tokenizer=tokenizer)
            logger.info("Model tải thành công.")
        except Exception as e:
            logger.error(f"Lỗi tải model: {e}")
            self.pipe = None

    @property
    def is_ready(self) -> bool:
        """Trả về True nếu model đã được tải thành công."""
        return self.pipe is not None

    def generate_advice(self, user_query: str, vmsi_context: dict) -> str:
        """
        Sinh lời khuyên tài chính dựa trên VMSI context.
        Trả về placeholder nếu chạy ở môi trường không có GPU.
        """
        if not self.is_ready:
            # Stub response cho môi trường local/test
            vmsi_val = vmsi_context.get("vmsi_value", "N/A")
            status = vmsi_context.get("status", "unknown")
            return (
                f"[STUB - GPU không khả dụng] "
                f"Câu hỏi: {user_query} | "
                f"VMSI={vmsi_val}, Status={status}. "
                f"Vui lòng chạy trên GPU server để nhận tư vấn thực từ mô hình AI."
            )

        # Tiêm ngữ cảnh vào prompt (System Prompt Engineering)
        system_prompt = (
            "Bạn là chuyên gia tư vấn tài chính của FinSent-Agent. "
            f"Dữ liệu hiện tại: VMSI={vmsi_context['vmsi_value']}, "
            f"Trạng thái={vmsi_context['status']}. "
            f"Cảnh báo: {vmsi_context['risk_warning']}. "
            "Hãy tư vấn ngắn gọn, chuyên nghiệp và sử dụng tiếng Việt."
        )

        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_query},
        ]

        result = self.pipe(messages, max_new_tokens=200)
        return result[0]["generated_text"][-1]["content"]


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    print("=== TEST chatbot_agent.py ===")
    bot = FinancialChatbot()
    print(f"Model ready: {bot.is_ready}")
    print(f"GPU available: {_GPU_AVAILABLE}")
    print(f"bitsandbytes available: {_BNB_AVAILABLE}")

    # Test stub response (local không có GPU)
    mock_context = {
        "vmsi_value": 25.0,
        "status": "risk_high",
        "risk_warning": "CANH BAO DO: Hoang loan.",
    }
    response = bot.generate_advice("Tôi có nên bán cổ phiếu SCB ngay bây giờ không?", mock_context)
    print(f"Response: {response}")
    print("=== chatbot_agent.py: GUARD TEST PASSED ===")
