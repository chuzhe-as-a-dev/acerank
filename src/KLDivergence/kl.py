#!/usr/bin/env python

from math import log
import time

global authors
global experts
global stars
global id2name
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
        expert = line.split('\t')
        # make sure the expert is in the database
        if expert[0] in authors:
            experts.append(expert[0])
            id2name[expert[0]] = expert[1]
    f.close()


def getStar(field):
    f = open("../../result/%s_ranking.txt" % field, "r")
    lines = f.readlines()
    for line in lines:
        line.strip()
        line = line[:-1]
        star = line.split('\t')
        # make sure the star is in the database
        if star[0] in authors:
            stars.append(star[0])
            id2name[star[0]] = star[1]
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
    global authors, experts, stars, id2name
    fields = ["AI", "Architecture", "CG", "Database", "HCI", "Network", "PL", "Security", "Theory"]
    for field in fields:
        # init global variables
        authors = {}
        id2name = {}
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
                f.write("%s\t%s\tEXPERT\n" % (star, id2name[star]))
            else:
                f.write("%s\t%s\t%s\t%s\t%f\n" % (star, id2name[star], sim_author, id2name[sim_author], maxent))
        f.close()


if __name__ == '__main__':
    main()
