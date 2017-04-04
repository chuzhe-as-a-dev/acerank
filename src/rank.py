from operator import itemgetter
from sys import argv


def rank(field_prefix, a, b, c, d):
    # get author - citation
    author_citation = dict()
    infile = open("../data/%s_cita" % field_prefix, "r")
    for line in infile:
        author, citation = line.split()
        author_citation[author] = int(citation)
    infile.close()

    # get author - core citation
    author_core = dict()
    infile = open("../data/%s_core" % field_prefix, "r")
    for line in infile:
        author, core = line.split()
        author_core[author] = int(core)
    infile.close()

    # get author - affiliation auth
    author_affiliation = dict()
    infile = open("../data/%s_author_affil" % field_prefix, "r")
    for line in infile:
        author, affiliation = line.split()
        author_affiliation[author] = float(affiliation)
    infile.close()

    # get author - last factor
    author_last = dict()
    infile = open("../data/%s_last_factor" % field_prefix, "r")
    for line in infile:
        author, last_factor = line.split()
        author_last[author] = float(last_factor)
    infile.close()

    # get author's detail
    author_detail = dict()
    infile = open("../data/%s_author_detail" % field_prefix, "r")
    for line in infile:
        author, affiliation = line.split()
        author_detail[author] = affiliation
    infile.close()

    # get denominator
    citation_average = 1.0 * sum(author_citation.itervalues()) / len(author_citation)
    core_citation_average = 1.0 * sum(author_core.itervalues()) / len(author_core)
    affiliation_average = sum(author_affiliation.itervalues()) / len(author_affiliation)
    last_factor_average = sum(author_last.itervalues()) / len(author_last)

    # get rank scores
    author_scores = dict()
    for author in author_last.iterkeys():
        score = 0.0
        if author in author_citation:
            score += a * author_citation[author] / citation_average
        if author in author_core:
            score += b * author_core[author] / core_citation_average
        if author in author_affiliation:
            score += c * author_affiliation[author] / affiliation_average
        score += d * author_last[author] / last_factor_average

        author_scores[author] = score

    # result
    author_ranking = sorted(author_scores.iteritems(), key=itemgetter(1), reverse=True)
    author_with_low_citation = set([author for author, citation in author_citation.iteritems() if citation < 1000])
    outfile = open("../data/%s_ranking.txt" % field_prefix, "w")
    for author, ranking in author_ranking:
        if author in author_with_low_citation:
            outfile.write("%s\t%f\t%s\n" % (author, ranking, author_detail[author]))
    outfile.close()

    print field_prefix, "down"


def main(a, b, c, d):
    field_prefix = ["AI", "Architecture", "CG", "Database", "HCI", "Network", "PL", "Security", "Theory"]
    for prefix in field_prefix:
        rank(prefix, a, b, c, d)


if __name__ == '__main__':
    usage = "Usage: %s a b c d\n" \
            "    a - factor for author's citation\n" \
            "    b - factor for author's core citation\n" \
            "    c - factor for author's affiliation\n" \
            "    d - factor for the last segment\n" \
            "    Note: All factor will be automatically normalized." % argv[0]

    if len(argv) != 5:
        print usage
        exit(1)
    else:
        try:
            # normalize factors
            a = float(argv[1])
            b = float(argv[2])
            c = float(argv[3])
            d = float(argv[4])
            total = a + b + c + d
            a /= total
            b /= total
            c /= total
            d /= total
        except:
            print usage
            exit(2)
        else:
            main(a, b, c, d)
