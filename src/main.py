import snap
from time import time, sleep


def hits(graph_filename):
    # create graph
    name_id_map = snap.TStrIntSH()
    graph = snap.LoadEdgeListStr(snap.PNGraph, graph_filename, 0, 1, name_id_map)

    # run HITS algo
    id_hub_map = snap.TIntFltH()
    id_auth_map = snap.TIntFltH()
    snap.GetHits(graph, id_hub_map, id_auth_map, 2)  # iterate 1000 times

    return name_id_map, id_hub_map, id_auth_map


def main():
    field_prefix = ["AI", "Architecture", "CG", "Database", "HCI", "Network", "PL", "Security", "Theory"]
    for prefix in field_prefix:
        # start timer
        start_time = time()

        # run HITS on paper-affil graph
        filename = "../data/%s_paper_affil" % prefix
        name_id_map, id_hub_map, id_auth_map = hits(filename)
        



        print "%s done, consumed time: %.2fs" % (prefix, time() - start_time)


if __name__ == '__main__':
    main()
