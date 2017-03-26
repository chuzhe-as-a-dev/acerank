#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Tue Dec 27 20:35:36 2016

@author: Nausicaasnow
"""

import sys
sys.path.append("swig")
#from time import time
import snap
from time import time

#G1 = snap.TNGraph.New()
#G1.AddNode(4294967294)
#G1.AddNode(2)
#G1.AddNode(3)
#G1.AddNode(4)
#G1.AddNode(5)
#G1.AddNode(6)
#G1.AddNode(7)
#G1.AddEdge(4294967294,2)
#G1.AddEdge(3,2)
#G1.AddEdge(4,2)
#G1.AddEdge(5,2)
#G1.AddEdge(6,2)
#G1.AddEdge(1,7)
#G1.AddEdge(3,4)
#G1.AddEdge(2,4)
#snap.SaveEdgeList(G1, "test.txt", "Save as tab-separated list of edges")

def get_hits_author():
    mapping = snap.TStrIntSH()
    t0 = time()
    file_output_1 = open("paper_author_hits_hub_index.txt",'w')
    file_output_2 = open("paper_author_hits_auth_index.txt",'w')
    G0 = snap.LoadEdgeListStr(snap.PNGraph, "paperid_authorid_ref_index.txt", 0, 1, mapping)
    NIdHubH = snap.TIntFltH()
    NIdAuthH = snap.TIntFltH()
    snap.GetHits(G0, NIdHubH, NIdAuthH,1000)
    print ("HITS time:", round(time()-t0, 3), "s")
    for item in NIdHubH:
        file_output_1.write(str(mapping.GetKey(item))+","+str(NIdHubH[item])+'\n')
    for item in NIdAuthH:
        file_output_2.write(str(mapping.GetKey(item))+","+str(NIdAuthH[item])+'\n')
    # convert input string to node id
    #NodeId = mapping.GetKeyId("814DF491")
    # convert node id to input string
    #NodeName = mapping.GetKey(NodeId)
    #print "name", NodeName
    #print "id  ", NodeId
    print ("finish hits!")
    file_output_1.close()
    file_output_2.close()

def get_hits_venues():
    mapping = snap.TStrIntSH()
    t0 = time()
    file_output_1 = open("paper_venues_hits_hub.txt",'w')
    file_output_2 = open("paper_venues_hits_auth.txt",'w')
    G0 = snap.LoadEdgeListStr(snap.PNGraph, "paperid_venueid_ref.txt", 0, 1, mapping)
    NIdHubH = snap.TIntFltH()
    NIdAuthH = snap.TIntFltH()
    snap.GetHits(G0, NIdHubH, NIdAuthH,1000)
    print ("HITS time:", round(time()-t0, 3), "s")
    for item in NIdHubH:
        file_output_1.write(str(mapping.GetKey(item))+","+str(NIdHubH[item])+'\n')
    for item in NIdAuthH:
        file_output_2.write(str(mapping.GetKey(item))+","+str(NIdAuthH[item])+'\n')
    # convert input string to node id
    # NodeId = mapping.GetKeyId("814DF491")
    # convert node id to input string
    # NodeName = mapping.GetKey(NodeId)
    # print "name", NodeName
    # print "id  ", NodeId
    print ("finish hits!")
    file_output_1.close()
    file_output_2.close()
    
#get_hits_venues()
get_hits_author()