from langchain.llms.base import LLM
from typing import Optional

class HumanLLM(LLM):
    def _call(self, prompt: str, stop: Optional[str] = None) -> str:
        # Display the prompt to the user and wait for a response
        print(f"Prompt: {prompt}")
        response = input("Your response: ")
        return response
    
    @property
    def _llm_type(self) -> str:
        return "human_llm"

class TestLLM(LLM):
    def _call(self, prompt: str, stop: Optional[str] = None) -> str:
        return 'checkouts.filter(arg_exp=["and", ["in", "Year", 2022, 2023], ["not", ["and", ["gt", "Checkouts", 80], ["lt", "Checkouts", 110]]]]).group(by=["Year"], agg="mean")'
    
    @property
    def _llm_type(self) -> str:
        return "test_llm"
