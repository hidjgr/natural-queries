from langchain.llms.base import LLM
from typing import Optional

# Define the HumanLLM class
class HumanLLM(LLM):
    def _call(self, prompt: str, stop: Optional[str] = None) -> str:
        # Display the prompt to the user and wait for a response
        print(f"Prompt: {prompt}")
        response = input("Your response: ")
        return response
    
    @property
    def _llm_type(self) -> str:
        return "human_llm"
