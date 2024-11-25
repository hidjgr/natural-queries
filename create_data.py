import pandas as pd
from random import randint
from sklearn.mixture import GaussianMixture

books = pd.read_csv("books.csv", sep="	")
clusters = [randint(0,1) for i in range(books.shape[0])]

checkouts_sample = pd.read_csv("checkouts_sample.csv", sep="	")
sample_mean = checkouts_sample.groupby("Year").mean().mean().Checkouts.round()
sample_data = checkouts_sample.set_index("BookID")[["Checkouts"]]

gm = GaussianMixture(n_components=2)
gm.fit(sample_data)


checkouts = pd.merge(pd.Series(range(2019,2024), name="Year"),
                     pd.Series(range(1,58), name="BookID"),
                     how="cross")

def sample_select(c):
    checkouts, cluster = gm.sample()
    checkouts, cluster = int(checkouts[0][0]), cluster[0]

    while cluster != c:
        checkouts, cluster = gm.sample()
        checkouts, cluster = int(checkouts[0][0]), cluster[0]

    return checkouts

checkouts["Checkouts"] = list(map(sample_select, clusters*5))

checkouts.to_csv("checkouts.csv", sep="	", index=False)
