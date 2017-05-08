#!/usr/bin/env python

from math import log
import time

authors = {}
experts = []
stars = []


def preprocess(field):
    f = open("../../data/%s_author_year_ref" % field, "r")
    lines = f.readlines()
    for line in lines:
        line.strip()
        info = line.split(" ")[:-2]
        if (len(info) > 1):
            for i in range(1, len(info)):
                info[i] = float(info[i][5:])
            authors[info[0]] = filter(lambda x: x > 0, info[1:])
    f.close()


def getExpert(field):
    f = open("../../data/%sExpert.txt" % field, "r")
    lines = f.readlines()
    for line in lines:
        line.strip()
        line = line[:-2]
        experts.append(line)
    f.close()


def getStar(field):
    f = open("../../result/%s_ranking.txt" % field, "r")
    lines = f.readlines()
    for line in lines:
        line.strip()
        line = line[:-2]
        star = line.split('\t')
        stars.append(star[0])
    f.close()


def calculate_relent(p, q):
    d1 = authors[p]
    d2 = authors[q]
    map(lambda x: x / sum(d1), d1)
    map(lambda x: x / sum(d2), d2)
    ent = 0.
    for i in range(5):
        ent += d1[i] * log(d2[i] / d1[i])
    print "%s\t%s\t%f\n" % (p, q, ent)
    return ent


def main():
    for field in ["AI"]:
        print "Start process %s at %s" % (field, time.ctime())
        preprocess(field)
        getExpert(field)
        getStar(field)
        f = open("../../result/%s_similar_author.txt" % field, "w")
        for star in stars:
            if star not in authors:
                continue
            else:
                maxent = 10000000.
                sim_author = experts[0]
                for expert in experts:
                    if expert not in authors:
                        print "[WARNING] There is no expert %s\n" % expert
                        continue
                    else:
                        entropy = calculate_relent(star, expert)
                        if extropy < maxent:
                            maxent = extropy
                            sim_author = expert
            f.write("%s\t%s\t%f\n" % (star, expert, maxent))
        f.close()


if __name__ == '__main__':
    main()
