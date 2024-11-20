import pandas as pd

class QLFrame():
    def __init__(self, df: pd.DataFrame, keys=tuple()):
        self.df = df
        self.keys = tuple(keys)

    def __repr__(self):
        return repr(self.df)

    def group(self, by:str|int|list|tuple|dict, agg:str):
        if isinstance(by, str) or not hasattr(by, '__iter__'):
            by = (by,)
        else:
            by = tuple(by)

        def uniq(seq):
            seen = set()
            seen_add = seen.add
            return [x for x in seq if not (x in seen or seen_add(x))]

        return QLFrame(self.df.groupby(by=uniq(self.keys+by)).agg(agg))

    def filter(self, exp):
        {
            "and": lambda: reduce(lambda x,y: x & y, map(self.filter,exp[1:]), True),
            "or": lambda: reduce(lambda x,y: x | y, map(self.filter, exp[1:]), False),
            "not": lambda: ~self.filter(exp[1]),
            "eq": lambda: all(map(lambda x,y: x == y, exp[1:])),
            "ne": lambda: any(map(lambda x,y: x != y, exp[1:])),
            "in": lambda: any(map(lambda x,y: x == y, exp[1:])),
            "gt": lambda: None,
            "ge": lambda: None,
            "lt": lambda: None,
            "le": lambda: None
            }
        if exp[0] in ["eq", "ne", "in", "gt", "ge", "lt", "le"]:
            return None

    def op(self, op, other):
        # return op df
        pass

    def join(self, other):
        # return joined
        pass


books = QLFrame(pd.read_csv("books.csv", sep="	"))
checkouts = QLFrame(pd.read_csv("checkouts.csv", sep="	"), keys=["Year", "BookID"])
test = pd.read_csv("checkouts.csv", sep="	")

#books.filter()
