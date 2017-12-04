#!/usr/bin/python
import mincemeat
import csv
import re
import os
import sys


class gennum():
    def __init__(self):
        self.i = 0
    def next(self):
        self.i += 1
        return self.i

def main():
    
    data = {}
    G = gennum()
    
    m,k,n = sys.argv[1:4]

    with open("m1.csv","r") as file:
        r = csv.DictReader(file,delimiter=",")
        for row in r:
            new_key = G.next()
            data[new_key] = row
            data[new_key]["m"] = int(m)
            data[new_key]["k"] = int(k)
            data[new_key]["n"] = int(n)
    

    def mapfn(k, v):
        if v["matrix"] == "a":
            for i in range(v["n"]):
                yield (int(v["row"]),i),(int(v["col"]),int(v["val"]))
        else:
            for i in range(v["m"]):
                yield (i,int(v["col"])),(int(v["row"]),int(v["val"]))

    def reducefn(k, vs):
        d = {}
        for v in vs:
            if v[0] in d:
                d[v[0]].append(v[1])
            else:
                d[v[0]] = [v[1]]
        res = 0
        for key in d:
            res += d[key][0] * d[key][1]
        return res

    s = mincemeat.Server()
    s.datasource = data
    s.mapfn = mapfn
    s.reducefn = reducefn
    print("Server is running...")
    res = s.run_server(password="1")
    
    with open("task3res.csv","wb") as file:
        w = csv.writer(file, delimiter=",")
        w.writerow(["matrix","row","col","val"])
        for key in res:
            w.writerow(["c",key[0],key[1],res[key]])

if __name__ == "__main__":
    main()
