from langchain_experimental.agents import create_pandas_dataframe_agent
from models import HumanLLM, TestLLM
from parser import parser
from data import from_local_csv

# df = pd.read_excel("file_example_XLS_1000.xls")

# Import data as a map
frames = from_local_csv()

# Human LLM and agent
# llm = HumanLLM()
llm = TestLLM()

response = llm.invoke(
        "Yearly average of checkouts between 80 and 110 for 2022 and 2023")

# Parse the input
frame, query = parser.parse(response)

result = query(frames[frame])
