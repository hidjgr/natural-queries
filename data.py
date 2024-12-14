import pandas as pd
from qlframe import QLFrame


def from_local_csv():
    books = QLFrame(pd.read_csv("books.csv", sep="	"),
                    dims=["BookID", "Title", "Author", "Genre", "Year"])
    checkouts = QLFrame(pd.read_csv("checkouts.csv", sep="	"),
                        dims=["Year", "BookID"])

    return {
            "books": books,
            "checkouts": checkouts
            }
