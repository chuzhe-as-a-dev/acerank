import MySQLdb


def get_paper_affil_graph(field_prefix):
    outfile = open("../data/%s_paper_affil" % field_prefix, "w")

    # connect to database
    db = MySQLdb.connect(host="localhost",
                         user="acerank",
                         passwd="0000",
                         db="RisingStar")

    # execute query
    cursor = db.cursor()
    sql = "SELECT PaperID, PaperReferenceID FROM %sPaperReference WHERE PaperReferenceID != 'None'" % field_prefix
    cursor.execute(sql)

    # store result to text file
    for row in cursor.fetchall():
        outfile.write("%s %s\n" % (row[0], row[1]))

    sql = "SELECT PaperID, AffiliationID FROM %sPaperAuthor WHERE AffiliationID != 'None'" % field_prefix
    cursor.execute(sql)

    # store result to text file
    for row in cursor.fetchall():
        outfile.write("%s %s\n" % (row[0], row[1]))
        outfile.write("%s %s\n" % (row[1], row[0]))


def main():
    field_prefix = ["AI", "Architecture", "CG", "Database", "HCI", "Network", "PL", "Security", "Theory"]
    for prefix in field_prefix:
        get_paper_affil_graph(prefix)


if __name__ == '__main__':
    main()
