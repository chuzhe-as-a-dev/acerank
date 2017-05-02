import MySQLdb

db = MySQLdb.connect(host="localhost",
                     user="acerank",
                     passwd="0000",
                     db="RisingStar")
cursor = db.cursor()

# find all related author_ids
# affiliation_ids = []
# cursor.execute("SELECT DISTINCT affiliation_id FROM new_PaperAuthor WHERE affiliation_id != 'None';")
# for row in cursor.fetchall():
#     affiliation_ids.append(row[0])
# print "haha"
#
# for affiliation_id in affiliation_ids:
#     cursor.execute("""INSERT INTO new_AffiliationName
#                       SELECT
#                         AffiliationID,
#                         AffiliationName
#                       FROM new_Affiliations
#                       WHERE AffiliationID = %s""", (affiliation_id,))
# db.commit()
# print "haha"

fields = ["AI", "Architecture", "CG", "Database", "HCI", "Network", "PL", "Security", "Theory"]
for field in fields:
    cursor.execute("""INSERT INTO new_AffiliationFieldData
                      SELECT DISTINCT
                        affiliation_id,
                        %s,
                        0
                      FROM new_PaperAuthor
                      WHERE affiliation_id != 'None'""", (field,))
    print "haha", field

db.commit()



# for prefix in fields:
#     cursor = db.cursor()
#     # # add paper of fields into a big table
#     # cursor.execute("""INSERT INTO new_Papers
#     #                   SELECT
#     #                     DISTINCT
#     #                     PaperID,
#     #                     PaperPublishYear,
#     #                     NormalizedVenueName,
#     #                     JournalIDMappedToVenueName,
#     #                     ConferenceSeriesIDMappedToVenueName
#     #                   FROM %sPaper
#     #                   WHERE PaperID NOT IN (SELECT paper_id
#     #                                         FROM new_Papers)""" % (prefix))
#
#     # add paper reference data into database
#     cursor.execute("""INSERT INTO new_PaperReference
#                       SELECT
#                         DISTINCT
#                         PaperID,
#                         PaperReferenceID
#                       FROM %sPaperReference
#                       WHERE (PaperID, PaperReferenceID) NOT IN (SELECT
#                                                                   paper_id,
#                                                                   reference_id
#                                                                 FROM new_PaperReference);""" % (prefix))
#
#     # add paper author to new_PaperAuthor
#     cursor.execute("""INSERT INTO new_PaperAuthor
#                       SELECT
#                         PaperID,
#                         AuthorID,
#                         AuthorSequenceNumber,
#                         AffiliationID
#                       FROM %sPaperAuthor
#                       WHERE (PaperID, AuthorID) NOT IN (SELECT
#                                                           paper_id,
#                                                           author_id
#                                                         FROM new_PaperAuthor);""" % (prefix))
#
#     # add paper's field to new_PaperFieldData
#     cursor.execute("""INSERT INTO new_PaperFieldData
#                       SELECT
#                         DISTINCT
#                         PaperID,
#                         '%s',
#                         0
#                       FROM %sPaper
#                       WHERE (PaperID, '%s') NOT IN (SELECT
#                                                        paper_id,
#                                                        field
#                                                      FROM new_PaperFieldData);""" % (prefix, prefix, prefix))
#
#     # add calculated pagerank to new_PaperFieldData
#     cursor.execute("""UPDATE new_PaperFieldData
#                       SET pagerank = (SELECT PageRank
#                                       FROM %sPaperPageRank
#                                       WHERE PaperID = paper_id)
#                       WHERE (paper_id, field) IN (SELECT
#                                                     PaperID,
#                                                     '%s'
#                                                   FROM %sPaperPageRank)""" % (prefix, prefix, prefix))
#
#     # author field data
#     cursor.execute("""INSERT INTO new_AuthorFieldData
#                       SELECT
#                         authorID,
#                         '%s',
#                         auth,
#                         0,
#                         0,
#                         0
#                       FROM %sAuthorAuth
#                       WHERE (authorID, '%s') NOT IN (SELECT
#                                                        author_id,
#                                                        field
#                                                      FROM new_AuthorFieldData);""" % (prefix, prefix, prefix))
#
#     cursor.execute("""UPDATE new_AuthorFieldData
#                       SET coca_rank = (SELECT cocaRankValue
#                                        FROM %sCocaRank
#                                        WHERE author_id = authorID)
#                       WHERE (author_id, field) IN (SELECT
#                                                      authorID,
#                                                      '%s'
#                                                    FROM %sCocaRank);""" % (prefix, prefix, prefix))
#
#     # big table for venue auth
#     cursor.execute("""INSERT INTO new_VenueFieldData
#                       SELECT
#                         NormalizedVenueName,
#                         '%s',
#                         NomAuth
#                       FROM %sVenueAuth
#                       WHERE (NormalizedVenueName, '%s') NOT IN (SELECT
#                                                                   venue_id,
#                                                                   field
#                                                                 FROM new_VenueFieldData);""" % (prefix, prefix, prefix))
#
#     db.commit()
#
#     print "hahaha", prefix
