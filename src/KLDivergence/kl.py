#!/usr/bin/env python

from math import log
import time

global authors
global experts
global stars
MAXINT = 10000000.


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
        line = line[:-1]
        # make sure the expert is in the database
        if line in authors:
            experts.append(line)
    f.close()


def getStar(field):
    f = open("../../result/%s_ranking.txt" % field, "r")
    lines = f.readlines()
    for line in lines:
        line.strip()
        line = line[:-1]
        star = line.split('\t')[0]
        # make sure the star is in the database
        if star in authors:
            stars.append(star)
    f.close()


def calculate_relent(p, q):
    d1 = authors[p]
    d2 = authors[q]
    if len(d1) < 5 or len(d2) < 5:
        return MAXINT
    map(lambda x: x / sum(d1), d1)
    map(lambda x: x / sum(d2), d2)
    ent = 0.
    for i in range(5):
        ent += d1[i] * log(d2[i] / d1[i])
    return abs(ent)


def main():
    global authors, experts, stars
    fields = ["AI", "Architecture", "CG", "Database", "HCI", "Network", "PL", "Security", "Theory"]
    for field in fields:
        # init global variables
        authors = {}
        experts = []
        stars = []
        print "Start process %s at %s" % (field, time.ctime())
        preprocess(field)
        getExpert(field)
        getStar(field)
        print "There is %d authors in %s" % (len(authors), field)
        print "There is %d experts in %s" % (len(experts), field)
        print "There is %d stars in %s" % (len(stars), field)
        f = open("../../result/%s_similar_author.txt" % field, "w")
        for star in stars:
            sim_author = experts[0]
            maxent = MAXINT
            for expert in experts:
                entropy = calculate_relent(star, expert)
                if entropy < maxent:
                    maxent = entropy
                    sim_author = expert
            # prevent the star has already become the expert
            if star == sim_author:
                f.write("%s\tEXPERT\n" % star)
            else:
                f.write("%s\t%s\t%f\n" % (star, sim_author, maxent))
        f.close()


if __name__ == '__main__':
    main()
