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
        def a(x,y):
            return x & (qf.select(self.column) >  y)
        trues = qf.select(self.column)==qf.select(self.column) 
        b = lambda qf: reduce(a, self.args, trues.array)
        b(qf)
        return f(qf)


class QLFrame(QLFrame):
    def __init__(self, df: pd.DataFrame, dims=tuple(), keys=tuple()):
        self.df = df
        self.dims = tuple(dims)
        self.keys = tuple(keys)

    def __repr__(self) -> str:
        return repr(self.df)

    def align(self, o_keys) -> DataFrame:
        if self.keys:
            return self.df.set_index(list(set(self.keys+o_keys)))
        return self.df.set_index(list(o_keys))

    def _comp(self, o, comp):
        comparisons = {
                "eq": lambda x,y: x.eq(y),
                "ne": lambda x,y: x.ne(y),
                "gt": lambda x,y: x.gt(y),
                "ge": lambda x,y: x.ge(y),
                "lt": lambda x,y: x.lt(y),
                "le": lambda x,y: x.le(y),
                }
        if self.df.shape[0] == o.df.shape[0]:
            return comparisons[comp](self.df,o.df)[self.df.columns.difference(self.dims)].all(axis=1)
        return comparisons[comp](self.align(o.keys),o.align(self.keys))[self.df.columns.difference(self.dims)].all(axis=1)

    def _op(self, o, op):
        operations = {
                "add": lambda x,y: x+y,
                "sub": lambda x,y: x-y,
                "mul": lambda x,y: x*y,
                "truediv": lambda x,y: x/y,
                "floordiv": lambda x,y: x//y,
                "pow": lambda x,y: x**y
                }
        return operations[op](self.align(o.keys),o.align(self.keys))[self.df.columns.difference(self.dims)]

    def __eq__(self, o: data_t):
        if isinstance(o, QLFrame):
            return self._comp(o, "eq")

    def __ne__(self, o: data_t):
        if isinstance(o, QLFrame):
            return self._comp(o, "ne")

    def __gt__(self, o: data_t):
        if isinstance(o, QLFrame):
            return self._comp(o, "gt")

    def __ge__(self, o: data_t):
        if isinstance(o, QLFrame):
            return self._comp(o, "ge")

    def __lt__(self, o: data_t):
        if isinstance(o, QLFrame):
            return self._comp(o, "lt")

    def __le__(self, o: data_t):
        if isinstance(o, QLFrame):
            return self._comp(o, "le")

    def __add__(self, o: data_t):
        if isinstance(o, QLFrame):
            return self._op(o, "add")

    def __sub__(self, o: data_t):
        if isinstance(o, QLFrame):
            return self._op(o, "sub")

    def __mul__(self, o: data_t):
        if isinstance(o, QLFrame):
            return self._op(o, "mul")

    def __truediv__(self, o: data_t):
        if isinstance(o, QLFrame):
            return self._op(o, "truediv")

    def __floordiv__(self, o: data_t):
        if isinstance(o, QLFrame):
            return self._op(o, "floordiv")

    def __pow__(self, o: data_t):
        if isinstance(o, QLFrame):
            return self._op(o, "pow")

    def __neg__(self):
        return QLFrame()

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

        def filter_from_set(l, s):
            return [i for i in l if i in s]

        keepcols = self.df.columns
        newdims = uniq(self.dims+by)
        newkeys = uniq(self.keys+by)
        if agg in aggtypes["numeric"]:
            keepcols = filter_from_set(keepcols,
                        set(by) | ((set(self.keys)
                                    | set(i for i,t in self.df.dtypes.items()
                                          if t in ["int", "float"]))
                                   - set(self.dims)))
            newdims = by
            newkeys = by

        return QLFrame(self.df[keepcols].groupby(by=list(by)).agg(agg).reset_index(),
                       newdims, newkeys)

    def filter(self, arg_exp):
        exp = FilterExp.parse_exp(arg_exp)
        return QLFrame(self.df[exp.eval(self)], self.dims, self.keys)

    def op(self, op, other):
        return self._op(other, op)

    def join(self, other):
        # return joined
        pass

    def select(self, columns):
        if isinstance(columns, str) or not hasattr(columns, '__iter__'):
            columns = [columns]
        else:
            columns = list(columns)

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

filt = c.filter(["gt","Checkouts",g,g])

# ┌main 256
# └┬filter 215
#  └┬eval 82
#   └┬lambda 81
#    └┬return 79
#     └┬__gt__ 134
#      └┬_comp 111
#       └align 98
