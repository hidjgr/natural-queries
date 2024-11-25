import pandas as pd
from qltypes import *
from functools import reduce

class FilterExp:

    def eval(self, qf: QLFrame): pass

    @staticmethod
    def _isnode(exp: list):
        return ((len(exp) >= 2) and (exp[0] in ["not", "and", "or"]))

    @staticmethod
    def _isleaf(exp: list):
        return ((len(exp) >= 3) and
                (exp[0] in ["eq", "ne", "in", "gt", "ge", "lt", "le"]))

    @staticmethod
    def parse_exp(exp: list):
        if FilterExp._isnode(exp):
            return FilterNode(exp[0], *map(FilterExp.parse_exp, exp[1:]))
        if FilterExp._isleaf(exp):
            return FilterLeaf(*exp)
        if len(exp) == 0:
            raise IndexError(f"Expression empty.")
        raise NameError(f"Function {exp[0]} does not exist.")


class FilterNode(FilterExp):

    def __init__(self, function: str, *args: tuple[data_t]):
        self.function: str = function
        self.args: tuple[FilterExp] = args

        self.functions = {
                "not": lambda qf: ~self.args.eval(),
                "and": lambda qf: reduce(lambda x,y: x & y, map(lambda x: x.eval(qf), self.args), qf.df[qf.df.columns[0]]==qf.df[qf.df.columns[0]]),
                "or": lambda qf: reduce(lambda x,y: x | y, map(lambda x: x.eval(qf), self.args), qf.df[qf.df.columns[0]]!=qf.df[qf.df.columns[0]]),
                }

    def __repr__(self):
        return f'({(" "+self.function+" ").join(map(str,self.args))})'

    def __str__(self):
        return f'({(" "+self.function+" ").join(map(str,self.args))})'

    def eval(self, qf: QLFrame) -> DataFrame:
        f = self.functions[self.function]
        return f(qf)



class FilterLeaf(FilterExp):

    def __init__(self, function: str, column: str, *args: tuple[data_t]):
        self.function: str = function
        self.column: str = column
        self.args: tuple[data_t] = args

        self.functions = {
                "eq": lambda qf: reduce(lambda x,y: x & (qf.df[column] == y), args, qf.df[column]==qf.df[column]),
                "ne": lambda qf: reduce(lambda x,y: x | (qf.df[column] != y), args, qf.df[column]!=qf.df[column]),
                "in": lambda qf: reduce(lambda x,y: x | (qf.df[column] == y), args, qf.df[column]!=qf.df[column]),
                "gt": lambda qf: reduce(lambda x,y: x & (qf.df[column] >  y), args, qf.df[column]==qf.df[column]),
                "ge": lambda qf: reduce(lambda x,y: x & (qf.df[column] >= y), args, qf.df[column]==qf.df[column]),
                "lt": lambda qf: reduce(lambda x,y: x & (qf.df[column] <  y), args, qf.df[column]==qf.df[column]),
                "le": lambda qf: reduce(lambda x,y: x & (qf.df[column] <= y), args, qf.df[column]==qf.df[column])
                }

    def __repr__(self) -> str:
        return f'({self.column} <{self.function}> {", ".join(map(str,self.args))})'

    def __str__(self) -> str:
        return f'({self.column} <{self.function}> {", ".join(map(str,self.args))})'

    def eval(self, qf: QLFrame) -> DataFrame:
        f = self.functions[self.function]
        return f(qf)


class QLFrame(QLFrame):
    def __init__(self, df: pd.DataFrame, dims=tuple(), keys=tuple()):
        self.df = df
        self.dims = tuple(dims)
        self.keys = tuple(keys)

    def __repr__(self) -> str:
        return repr(self.df)

    def align(self, o_keys):
        if self.keys:
            return self.df.set_index(list(set(self.keys+o_keys)))
        return self.df.set_index(list(o_keys))

    def __eq__(self, o: data_t):
        if isinstance(o, QLFrame):
            return self.align(o.keys) == o.align(self.keys)

    def __gt__(self, o: data_t):
        if isinstance(o, QLFrame):
            x = self.align(o.keys)
            y = o.align(self.keys)
            breakpoint()
            return self.align(o.keys) > o.align(self.keys)

    def __add__(self, o: data_t):
        self.df + o

    def group(self, by:str|list[str]|tuple[str]|dict[str,str], agg:str) -> QLFrame:
        aggtypes = {
            "all": ["count", "all", "any", "cumcount", "first", "last",
                    "bfill", "ffill", "head", "tail", "idxmax", "idxmin",
                    "ngroup", "nunique", "rank", "sample", "shift",
                    "value_counts", "size", "max", "min"],
            "numeric": ["cummax", "cummin", "cumprod", "cumsum", "cov",
                        "std", "sum", "corr", "var", "diff",
                        "mean", "median", "prod", "pct_change",
                        "quantile", "sem", "skew", "ohlc"]
        }

        if isinstance(by, str) or not hasattr(by, '__iter__'):
            by = (by,)
        else:
            by = tuple(by)

        def uniq(seq):
            seen = set()
            seen_add = seen.add
            return [x for x in seq if not (x in seen or seen_add(x))]
        
        groupcols = uniq(self.keys+by)

        dropcols = []
        if agg in aggtypes["numeric"]:
            for k,t in self.df.dtypes.items():
                if (((k not in groupcols) and (t not in ["int", "float"]))
                    or ((k in self.dims) and (k not in by))):
                    dropcols.append(k)

        self.keys = tuple(set(self.keys) | set(by))
        
        return QLFrame(self.df
                       .drop(dropcols, axis=1)
                       .groupby(by=groupcols)
                       .agg(agg)
                       .reset_index(), self.dims, self.keys)

    def filter(self, arg_exp):
        exp = FilterExp.parse_exp(arg_exp)
        return QLFrame(self.df[exp.eval(self)], self.dims, self.keys)

    def op(self, op, other):
        # return op df
        pass

    def join(self, other):
        # return joined
        pass

    def select(self, columns):
        return QLFrame(self.df[columns], self.dims, self.keys)

    def drop(self, columns):

        if isinstance(columns, str) or not hasattr(columns, '__iter__'):
            columns = (columns,)
        else:
            columns = tuple(columns)

        return QLFrame(self.df[self.df.columns.difference(columns)], self.dims, self.keys)


books = QLFrame(pd.read_csv("books.csv", sep="	"),
                dims=["BookID", "Title", "Author", "Genre", "Year"])
checkouts = QLFrame(pd.read_csv("checkouts.csv", sep="	"),
                    dims=["Year", "BookID"])

exp = ["and", ["in", "Year", 2022, 2023], ["and", ["gt", "Checkouts", 80], ["lt", "Checkouts", 110]]]

#books.filter()
#checkouts.filter(exp)

g = checkouts.group(["Year"], "mean")

c = checkouts

c > g
g > c
