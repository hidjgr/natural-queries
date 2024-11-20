from functools import reduce
from language import test


def fj():
    print("a")

j = {"test": lambda: fj()}
