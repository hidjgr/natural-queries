from langchain_experimental.agents import create_pandas_dataframe_agent
from models import HumanLLM
from parser import parser
from data import from_local_csv

# df = pd.read_excel("file_example_XLS_1000.xls")

# Import data as a map
frames = from_local_csv()

# Human LLM and agent
human_llm = HumanLLM()

input_string = 'checkouts.filter(arg_exp=["and", ["in", "Year", 2022, 2023], ["not", ["and", ["gt", "Checkouts", 80], ["lt", "Checkouts", 110]]]]).group(by=["Year"], agg="mean")'

# Parse the input
frame, query = parser.parse(input_string)

result = query(frames[frame])
