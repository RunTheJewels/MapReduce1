#!/usr/bin/python
import mincemeat
import csv
import re
import os


def main():
    
    data = {}
    
    directory = "./sherlock"
    allbooks = os.listdir(directory)
    numbooks = len(allbooks)

    for book in allbooks:
        name = os.path.join(directory, book)
        with open(name, "r") as file:
            data[book] = list(filter(lambda x: x != "", re.split(r"\W+", file.read().lower()) ))

    def mapfn(k, v):
        for w in v:
            yield w, (k, 1)

    def reducefn(k, vs):
        res = {}
        for a,i in vs:
            if a in res:
                res[a] += i
            else:
                res[a] = i
        return res

    s = mincemeat.Server()
    s.datasource = data
    s.mapfn = mapfn
    s.reducefn = reducefn
    print("Server is running...")
    res = s.run_server(password="1")
    
    with open("./task2res.csv","wb") as file:
        w = csv.DictWriter(file, fieldnames=["Word"]+allbooks, delimiter=",", restval=0)
        w.writeheader()
        for key in res:
            res[key]["Word"] = key
            w.writerow(res[key])


if __name__ == "__main__":
    main()
