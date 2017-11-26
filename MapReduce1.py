#!/usr/bin/python
import mincemeat
import csv
import re


class gennum():
    def __init__(self):
        self.i = 0
    def next(self):
        self.i += 1
        return self.i

def main():
    
    data = {}
    G = gennum()
    with open("./southpark/All-seasons.csv","r") as file:
        r = csv.DictReader(file,delimiter=",")
        for row in r:
            data[G.next()] = (row["Character"], list(filter(lambda x: x != "", re.split(r"\W+", row["Line"].lower())) )) 
    
    def mapfn(k, v):
        for w in v[1]:
            yield v[0], w

    def reducefn(k, vs):
        res = set()
        for word in vs:
            res.add(word)
        res = len(res)
        return res

    s = mincemeat.Server()
    s.datasource = data
    s.mapfn = mapfn
    s.reducefn = reducefn
    print("Server is running...")
    res = s.run_server(password="1")
    
    with open("./task1res.csv","wb") as file:
        w = csv.writer(file, delimiter=",")
        w.writerow(["Character","Number of words"])
        for key in res:
            w.writerow([key, res[key]])


if __name__ == "__main__":
    main()
