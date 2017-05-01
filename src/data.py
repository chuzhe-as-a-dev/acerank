import MySQLdb

# inclusive at both end
time_range_start = 2005
time_range_end = 2010


def get_paper_affil_graph(field_prefix):
    # connect to database
    db = MySQLdb.connect(host="localhost",
                         user="acerank",
                         passwd="0000",
                         db="RisingStar")
    cursor = db.cursor()

    # find papers in time range
    sql = "SELECT DISTINCT PaperID FROM %sPaper" % (field_prefix)
    cursor.execute(sql)
    qualified_papers = set()
    for row in cursor.fetchall():
        qualified_papers.add(row[0])

    # find referenced papers
    sql = "SELECT PaperID, PaperReferenceID FROM %sPaperReference WHERE PaperReferenceID != 'None'" % field_prefix
    cursor.execute(sql)

    # store result to text file
    outfile = open("../data/%s_paper_affil" % field_prefix, "w")
    for row in cursor.fetchall():
        if row[0] in qualified_papers:
            outfile.write("%s %s\n" % (row[0], row[1]))

    sql = "SELECT PaperID, AffiliationID FROM %sPaperAuthor WHERE AffiliationID != 'None'" % (field_prefix)
    cursor.execute(sql)

    # store result to text file
    for row in cursor.fetchall():
        outfile.write("%s %s\n" % (row[0], row[1]))
        outfile.write("%s %s\n" % (row[1], row[0]))


def get_affil(field_prefix):
    outfile = open("../data/%s_affil" % field_prefix, "w")

    # connect to database
    db = MySQLdb.connect(host="localhost",
                         user="acerank",
                         passwd="0000",
                         db="RisingStar")

    # execute query
    cursor = db.cursor()
    sql = "SELECT AffiliationID FROM %sPaperAuthor WHERE AffiliationID != 'None'" % field_prefix
    cursor.execute(sql)

    # store result to text file
    for row in cursor.fetchall():
        outfile.write("%s\n" % (row[0]))


def get_citation(field_prefix):
    # connect to database
    db = MySQLdb.connect(host="localhost",
                         user="acerank",
                         passwd="0000",
                         db="RisingStar")

    # execute query
    cursor = db.cursor()
    sql = """
          SELECT AuthorID, sum(Referenced)
          FROM %sPaperAuthor JOIN (
            SELECT PaperReferenceID AS PaperID, count(*) AS Referenced
            FROM %sPaperReference
            GROUP BY PaperReferenceID) as PaperReferenced USING (PaperID)
          WHERE PaperPublishYear >= %i AND PaperPublishYear <= %i
          GROUP BY AuthorID""" % (field_prefix, field_prefix, time_range_start, time_range_end)
    cursor.execute(sql)

    # store result to text file
    outfile = open("../data/%s_cita" % field_prefix, "w")
    for row in cursor.fetchall():
        outfile.write("%s %s\n" % (row[0], row[1]))

    print field_prefix, "down"


def get_core(field_prefix):
    # get author and their references
    infile = open("../data/%s_cita" % field_prefix, "r")
    author_referenced = dict()
    for line in infile:
        author, referenced = line.split()
        author_referenced[author] = int(referenced)

    # connect to database
    db = MySQLdb.connect(host="localhost",
                         user="acerank",
                         passwd="0000",
                         db="RisingStar")

    # record each paper's author_ids
    paper_authors = dict()
    cursor = db.cursor()
    sql = "SELECT PaperID, AuthorID FROM %sPaperAuthor WHERE PaperPublishYear >= %i AND PaperPublishYear <= %i" % (
    field_prefix, time_range_start, time_range_end)
    cursor.execute(sql)
    for row in cursor.fetchall():
        if row[0] not in paper_authors:
            paper_authors[row[0]] = [row[1]]
        else:
            paper_authors[row[0]].append(row[1])

    # find core author_ids
    author_cores = dict()
    for authors in paper_authors.values():
        for author in authors:
            if author not in author_cores:
                author_cores[author] = set(authors)
            else:
                for coauthor in authors:
                    if coauthor != author:
                        author_cores[author].add(coauthor)

    # calculate core references of each author
    author_core_referenced = dict()
    for author in author_referenced.iterkeys():
        # for those who don't have a coauthor
        if author not in author_cores:
            author_core_referenced[author] = 0
            break

        # accumulate core author_ids' references
        core_referenced = 0
        for coauthor in author_cores[author]:
            if coauthor in author_referenced:
                core_referenced += author_referenced[coauthor]

        author_core_referenced[author] = core_referenced

    # save result to file
    outfile = open("../data/%s_core" % field_prefix, "w")
    for author, core_referenced in author_core_referenced.iteritems():
        outfile.write("%s %s\n" % (author, core_referenced))
    print field_prefix, "down"


def get_author_affil(field_prefix):
    # get prepared affiliation auth value
    infile = open("../data/%s_affil_auth" % field_prefix, "r")
    affil_auth = dict()
    for line in infile:
        affil, auth = line.split()
        affil_auth[affil] = float(auth)

    # find each author's affiliation
    db = MySQLdb.connect(host="localhost",
                         user="acerank",
                         passwd="0000",
                         db="RisingStar")
    cursor = db.cursor()
    sql = """
        SELECT DISTINCT AuthorID, AffiliationID
        FROM %sPaperAuthor
        WHERE AffiliationID != 'None' AND PaperPublishYear >= %i AND PaperPublishYear <= %i""" % (
    field_prefix, time_range_start, time_range_end)
    cursor.execute(sql)

    author_affil = {}
    for row in cursor.fetchall():
        if row[0] not in author_affil:
            if row[1] in affil_auth:
                author_affil[row[0]] = [affil_auth[row[1]], row[1]]
            else:
                author_affil[row[0]] = [0.0, row[1]]
        else:
            if row[1] in affil_auth and affil_auth[row[1]] > author_affil[row[0]]:
                author_affil[row[0]] = [affil_auth[row[1]], row[1]]

    # save to file
    outfile = open("../data/%s_author_affil" % field_prefix, "w")
    for author, detail in author_affil.iteritems():
        outfile.write("%s %f %s\n" % (author, detail[0], detail[1]))
    outfile.close()

    print field_prefix, "down"


def get_last_factor(field_prefix):
    # connect to database
    db = MySQLdb.connect(host="localhost",
                         user="acerank",
                         passwd="0000",
                         db="RisingStar")
    cursor = db.cursor()

    # get author's auth
    sql = """
        SELECT AuthorID, auth
        FROM %sAuthorAuth""" % field_prefix
    cursor.execute(sql)
    author_auth = dict()
    for row in cursor.fetchall():
        author_auth[row[0]] = float(row[1])

    # get author's paper data (paper id and 1/author_seq)
    sql = "SELECT PaperID, AuthorID, 1 / AuthorSequenceNumber FROM %sPaperAuthor WHERE PaperPublishYear >= %i AND PaperPublishYear <= %i" % (
    field_prefix, time_range_start, time_range_end)
    cursor.execute(sql)
    author_papers = dict()
    for row in cursor.fetchall():
        if row[1] not in author_papers:
            author_papers[row[1]] = [(row[0], row[2])]
        else:
            author_papers[row[1]].append((row[0], row[2]))

    # get paper's pagerank
    sql = "SELECT * FROM %sPaperPageRank" % field_prefix
    cursor.execute(sql)
    paper_pagerank = dict()
    for row in cursor.fetchall():
        paper_pagerank[row[0]] = float(row[1])

    # get paper's venue
    sql = "SELECT PaperID, NomAuth FROM %sPaper JOIN %sVenueAuth USING (NormalizedVenueName);" % (
    field_prefix, field_prefix)
    cursor.execute(sql)
    paper_venue_auth = dict()
    for row in cursor.fetchall():
        paper_venue_auth[row[0]] = float(row[1])

    # calculate last factor
    author_last_factor = dict()
    for author in author_auth.keys():
        mul_add_chunk = 0.0
        if author in author_papers:
            for paper_id, con in author_papers[author]:
                if paper_id not in paper_pagerank:
                    continue
                if paper_id not in paper_venue_auth:
                    continue
                mul_add_chunk += float(con) * paper_pagerank[paper_id] * paper_venue_auth[paper_id]
        author_last_factor[author] = mul_add_chunk

    # save to file
    outfile = open("../data/%s_last_factor" % field_prefix, "w")
    for author, last_factor in author_last_factor.iteritems():
        outfile.write("%s %s\n" % (author, last_factor))

    print field_prefix, "down"


def get_author_detail(field_prefix):
    db = MySQLdb.connect(host="localhost",
                         user="acerank",
                         passwd="0000",
                         db="RisingStar")
    cursor = db.cursor()
    sql = "SELECT AuthorID, AffiliationID FROM %sPaperAuthor" % field_prefix
    cursor.execute(sql)

    outfile = open("../data/%s_author_detail" % field_prefix, "w")
    for row in cursor.fetchall():
        outfile.write("%s %s\n" % (row[0], row[1]))


def main():
    field_prefixs = ["AI", "Architecture", "CG", "Database", "HCI", "Network", "PL", "Security", "Theory"]
    for prefix in field_prefixs:
        # a whole procedure must be gone through when time range changes
        get_paper_affil_graph(prefix)
        get_citation(prefix)
        get_core(prefix)
        get_author_affil(prefix)
        get_last_factor(prefix)
        pass


if __name__ == '__main__':
    main()
