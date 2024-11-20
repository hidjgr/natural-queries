from langchain_experimental.agents import create_pandas_dataframe_agent
from models import HumanLLM
import pandas as pd
from parser import parser

## Load the Excel data
#df = pd.read_excel("file_example_XLS_1000.xls")
#
## Initialize your custom LLM
#human_llm = HumanLLM()
#
## Create an agent using the human-in-the-loop LLM
#agent = create_pandas_dataframe_agent(human_llm, df, verbose=True)
#
## Run a sample prompt
#response = agent.run("What is the total sales for each product?")
#print(response)

# Sample input for testing
input_string = 'group.group(by="col1", agg="sum").filter(exp=["eq", "col1", 5]).join(group(by="co l2", agg="avg"))'

# Parse the input
result = parser.parse(input_string)
print(result)

