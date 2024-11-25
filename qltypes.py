from pandas import DataFrame

class QLFrame(): pass

# argument types
data_t = int|float|str|DataFrame|QLFrame
name_t = str

# column types
## categorical
cat_t = str|bool

## numeric
num_t = int|float
