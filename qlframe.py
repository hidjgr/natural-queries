import pandas as pd
from qltypes import *
from functools import reduce

class FilterExp:

    def eval(self, qf: QLFrame) -> DataFrame: pass

    @staticmethod
    def _isnode(exp: list) -> bool:
        return ((len(exp) >= 2) and (exp[0] in ["not", "and", "or"]))

    @staticmethod
    def _isleaf(exp: list) -> bool:
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

        trues = lambda c: (c==c).values

        self.functions = {
                "not": lambda qf: ~self.args[0].eval(qf),
                "and": lambda qf: reduce(lambda x,y: x & y, map(lambda x: x.eval(qf), self.args), trues(qf.df[qf.df.columns[0]])),
                "or": lambda qf: reduce(lambda x,y: x | y, map(lambda x: x.eval(qf), self.args), ~trues(qf.df[qf.df.columns[0]])),
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
        self.function = function
        self.column = column
        self.args = args

        comp = {
            "eq": lambda x,y: x == y, "ne": lambda x,y: x != y, "in": lambda x,y: x == y,
            "gt": lambda x,y: x > y, "ge": lambda x,y: x >= y,
            "lt": lambda x,y: x < y, "le": lambda x,y: x <= y
                }

        comp_red = {
                "or": lambda qc: reduce(lambda x,y: x | comp[function](qc, y), args, qc!=qc),
                "and": lambda qc: reduce(lambda x,y: x & comp[function](qc, y), args, qc==qc)
                }

        comp_red_t = {c: "or" if c in ("ne", "in") else "and" for c in comp}

        self.filter = lambda qf: comp_red[comp_red_t[function]](qf.df[column])

    def __repr__(self) -> str:
        return f'({self.column} <{self.function}> {", ".join(map(str,self.args))})'

    def __str__(self) -> str:
        return f'({self.column} <{self.function}> {", ".join(map(str,self.args))})'

    def eval(self, qf: QLFrame) -> DataFrame:
        return self.filter(qf)


class QLFrame(QLFrame):
    def __init__(self, df: pd.DataFrame, dims=tuple()):
        self.df = df
        self.dims = tuple(dims)

    def __repr__(self) -> str:
        return repr(self.df)

    def _uniq(self, seq):
        seen = set()
        seen_add = seen.add
        return [x for x in seq if not (x in seen or seen_add(x))]

    def _filter_from_set(self, l, s):
        return [i for i in l if i in s]

    def _align(self, o) -> DataFrame:
        if (not self.dims) or (not o.dims):
            return self, o
        return self.df.set_index(list(self.dims)).align(o.df.set_index(list(o.dims)))

    def _dim_splice(self, fun) -> DataFrame:
        return pd.concat([self.df[list(self.dims)],
                   fun(self.df[self.df.columns.difference(self.dims)])], axis=1)

    def _comp(self, o, comp):
        if isinstance(o, QLFrame):
            return comp(*self._align(o))[self.df.columns.difference(self.dims)].all(axis=1)
        else:
            return self._dim_splice(lambda x: comp(x, o)).set_index(list(self.dims)).all(axis=1)

    def _op(self, o, fun):
        if isinstance(o, QLFrame):
            res = fun(*self._align(o))
            return QLFrame(res.reset_index(), tuple(res.index.names))
        else:
            return QLFrame(self._dim_splice(lambda x: fun(x, o)), self.dims)

    def __eq__(self, o: data_t):
        return self._comp(o, lambda x,y: x.eq(y))

    def __ne__(self, o: data_t):
        return self._comp(o, lambda x,y: x.ne(y))

    def __gt__(self, o: data_t):
        return self._comp(o, lambda x,y: x.gt(y))

    def __ge__(self, o: data_t):
        return self._comp(o, lambda x,y: x.ge(y))

    def __lt__(self, o: data_t):
        return self._comp(o, lambda x,y: x.lt(y))

    def __le__(self, o: data_t):
        return self._comp(o, lambda x,y: x.le(y))

    def __add__(self, o: data_t):
        return self._op(o, lambda x,y: x+y)

    def __radd__(self, o: data_t):
        return self._op(o, lambda x,y: y+x)

    def __sub__(self, o: data_t):
        return self._op(o, lambda x,y: x-y)

    def __rsub__(self, o: data_t):
        return self._op(o, lambda x,y: y-x)

    def __mul__(self, o: data_t):
        return self._op(o, lambda x,y: x*y)

    def __rmul__(self, o: data_t):
        return self._op(o, lambda x,y: y*x)

    def __truediv__(self, o: data_t):
        return self._op(o, lambda x,y: x/y)

    def __rtruediv__(self, o: data_t):
        return self._op(o, lambda x,y: y/x)

    def __mod__(self, o: data_t):
        return self._op(o, lambda x,y: x%y)

    def __rmod__(self, o: data_t):
        return self._op(o, lambda x,y: y%x)

    def __rfloordiv__(self, o: data_t):
        return self._op(o, lambda x,y: x//y)

    def __rfloordiv__(self, o: data_t):
        return self._op(o, lambda x,y: y//x)

    def __pow__(self, o: data_t):
        return self._op(o, lambda x,y: x**y)

    def __rpow__(self, o: data_t):
        return self._op(o, lambda x,y: y**x)

    def __neg__(self):
        return self._dim_splice(lambda x: -x)

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

        keepcols = self.df.columns
        newdims = self._uniq(self.dims+by)
        if agg in aggtypes["numeric"]:
            numcols = set(i for i,t in self.df.dtypes.items() if t in ["int", "float"])
            keepcols = self._filter_from_set(keepcols,
                                       set(by) | (numcols - set(self.dims)) )
            newdims = by

        return QLFrame(self.df[keepcols]
                           .groupby(by=list(by))
                           .agg(agg)
                           .reset_index(),
                       newdims)

    def filter(self, arg_exp) -> QLFrame:
        exp = FilterExp.parse_exp(arg_exp)
        return QLFrame(self.df[exp.eval(self).values], self.dims)

    def op(self, op, other) -> QLFrame:
        return self._op(other, op)

    def join(self, other) -> QLFrame:
        newdims = self._filter_from_set(
                self._uniq(self.dims+other.dims),
                set(self.dims) & set(other.dims))

        return QLFrame(self.df.merge(other.df, on=newdims), newdims)

    def visual_join(self, other):
        return pd.concat([self.df, other.df], axis=1)

    def select(self, columns) -> QLFrame:
        if isinstance(columns, str) or not hasattr(columns, '__iter__'):
            columns = [columns]
        else:
            columns = list(columns)

        return QLFrame(self.df[columns],
                       self._filter_from_set(columns,
                                             set(self.dims) & set(columns)))

    def drop(self, columns) -> QLFrame:
        if isinstance(columns, str) or not hasattr(columns, '__iter__'):
            columns = (columns,)
        else:
            columns = tuple(columns)

        return QLFrame(self.df[self.df.columns.difference(columns)],
                       self._filter_from_set(self.dims,
                                             set(self.df.columns.difference(columns))))

books = QLFrame(pd.read_csv("books.csv", sep="	"),
                dims=["BookID", "Title", "Author", "Genre", "Year"])
checkouts = QLFrame(pd.read_csv("checkouts.csv", sep="	"),
                    dims=["Year", "BookID"])

exp = ["and", ["in", "Year", 2022, 2023], ["not", ["and", ["gt", "Checkouts", 80], ["lt", "Checkouts", 110]]]]

g = checkouts.group(["Year"], "mean")

c = checkouts

filt = c.filter(exp)
