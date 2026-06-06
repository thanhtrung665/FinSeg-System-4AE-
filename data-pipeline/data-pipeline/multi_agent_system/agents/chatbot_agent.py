import torch
from transformers import AutoModelForCausalLM, AutoTokenizer, pipeline

class FinancialChatbot:
    def __init__(self, model_name="Qwen/Qwen2-7B-Instruct"):
        # Load tokenizer và model
        # Dùng device_map="auto" để tự động đẩy lên GPU nếu có
        self.tokenizer = AutoTokenizer.from_pretrained(model_name)
        self.model = AutoModelForCausalLM.from_pretrained(
            model_name,
            torch_dtype=torch.float16,
            device_map="auto",
            load_in_4bit=True  # Nén 4-bit để tiết kiệm RAM
        )
        self.pipe = pipeline("text-generation", model=self.model, tokenizer=self.tokenizer)

    def generate_advice(self, user_query: str, vmsi_context: dict) -> str:
        # Tiêm ngữ cảnh vào prompt (System Prompt Engineering)
        system_prompt = f"""
        Bạn là chuyên gia tư vấn tài chính của FinSent-Agent. 
        Dữ liệu hiện tại: VMSI={vmsi_context['vmsi_value']}, Trạng thái={vmsi_context['status']}.
        Cảnh báo: {vmsi_context['risk_warning']}.
        Hãy tư vấn ngắn gọn, chuyên nghiệp và sử dụng tiếng Việt.
        """
        
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_query}
        ]
        
        # Gọi model
        result = self.pipe(messages, max_new_tokens=200)
        return result[0]['generated_text'][-1]['content']