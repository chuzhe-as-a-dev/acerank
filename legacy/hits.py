import snap
from time import time


def hits(graph_filename):
    # create graph
    name_id_map = snap.TStrIntSH()
    graph = snap.LoadEdgeListStr(snap.PNGraph, graph_filename, 0, 1, name_id_map)

    # run HITS algo
    id_hub_map = snap.TIntFltH()
    id_auth_map = snap.TIntFltH()
    snap.GetHits(graph, id_hub_map, id_auth_map, 1000)  # iterate 1000 times

    return name_id_map, id_hub_map, id_auth_map


def main():
    field_prefix = ["AI", "Architecture", "CG", "Database", "HCI", "Network", "PL", "Security", "Theory"]
    for prefix in field_prefix:
        # start timer
        start_time = time()

        # run HITS on paper-affil graph
        filename = "../data/%s_paper_affil" % prefix
        name_id_map, id_hub_map, id_auth_map = hits(filename)

        # load affil names
        infile = open("../data/%s_affil" % prefix, "r")
        affil_names = set()
        for affil_name in infile:
            affil_names.add(affil_name.strip())
        infile.close()

        # store only affil's auth
        outfile = open("../data/%s_affil_auth" % prefix, "w")
        max_auth = 0
        for item in id_auth_map:
            if str(name_id_map.GetKey(item)) in affil_names:
                if max_auth < id_auth_map[item]:
                    max_auth = id_auth_map[item]
                outfile.write("%s %f\n" % (name_id_map.GetKey(item), id_auth_map[item]))

        outfile.close()
        print "%s done, consumed time: %.2fs" % (prefix, time() - start_time)
        print "max auth: %.3f" % max_auth


if __name__ == '__main__':
    main()
