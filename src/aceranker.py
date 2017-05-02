import MySQLdb
import snap

from math import pi, atan
from time import time


def timeit(func):
    def timed_func(*args, **kwdargs):
        start_time = time()
        func(*args, **kwdargs)
        end_time = time()
        print func.__name__, "consumed time: %.2fs" % (end_time - start_time)

    return timed_func


class Aceranker:
    def __init__(self, start_year, end_year, self_citation_factor, coauthor_factor, affiliation_factor, paper_factor):
        self.start_year = start_year
        self.end_year = end_year
        self.fields = ["AI", "Architecture", "CG", "Database", "HCI", "Network", "PL", "Security", "Theory"]
        self.connection = MySQLdb.connect(host="localhost",
                                          user="acerank",
                                          passwd="0000",
                                          db="RisingStar")
        factor_sum = self_citation_factor + coauthor_factor + affiliation_factor + paper_factor
        self.self_citation_factor = self_citation_factor / factor_sum
        self.coauthor_factor = coauthor_factor / factor_sum
        self.affiliation_factor = affiliation_factor / factor_sum
        self.paper_factor = paper_factor / factor_sum

    @timeit
    def update_affiliation_hits(self, hits_iteration=1000):
        for field in self.fields:
            # record data id - node id mapping
            data_node_map = {}
            node_data_map = {}
            current_node_id = 0

            # set data structure to deal with duplicate affiliation
            data_ids = set()

            # HITS graph
            graph = snap.TNGraph.New()

            # database cursor
            cursor = self.connection.cursor()

            # find all paper in this field and add nodes to HITS graph
            cursor.execute("""SELECT paper_id
                              FROM new_PaperFieldData
                              WHERE field = %s""", (field,))
            for row in cursor.fetchall():
                paper_id = row[0]
                data_ids.add(paper_id)

                data_node_map[paper_id] = current_node_id
                node_data_map[current_node_id] = paper_id
                graph.AddNode(current_node_id)

                current_node_id += 1

            # find paper references and add edges to HITS graph
            cursor.execute("""SELECT
                                paper_id,
                                reference_id
                              FROM new_PaperReference
                              WHERE paper_id IN (SELECT paper_id
                                                 FROM new_PaperFieldData
                                                 WHERE field = %s) AND
                                    reference_id IN (SELECT paper_id
                                                     FROM new_PaperFieldData
                                                     WHERE field = %s)""", (field, field))
            for row in cursor.fetchall():
                paper_id = row[0]
                reference_id = row[1]
                graph.AddEdge(data_node_map[paper_id], data_node_map[reference_id])

            print "test: all paper references added"

            # find author's affiliation in each paper in this field
            # and add edges to HITS graph
            affiliation_ids = set()
            cursor.execute("""SELECT DISTINCT
                                paper_id,
                                affiliation_id
                              FROM new_PaperAuthor
                              WHERE paper_id IN (SELECT paper_id
                                                 FROM new_PaperFieldData
                                                 WHERE field = %s);""", (field,))
            for row in cursor.fetchall():
                paper_id = row[0]
                affiliation_id = row[1]
                affiliation_ids.add(affiliation_id)

                if affiliation_id not in data_ids:
                    data_ids.add(affiliation_id)

                    # add to map record and add the HITS graph node
                    data_node_map[affiliation_id] = current_node_id
                    node_data_map[current_node_id] = affiliation_id
                    graph.AddNode(current_node_id)

                    current_node_id += 1

                # add new HITS graph edge
                graph.AddEdge(data_node_map[paper_id], data_node_map[affiliation_id])
                graph.AddEdge(data_node_map[affiliation_id], data_node_map[paper_id])

            print "test: all affiliation relationship added"

            # run HITS algorithm
            hub_map = snap.TIntFltH()
            auth_map = snap.TIntFltH()
            snap.GetHits(graph, hub_map, auth_map, hits_iteration)  # iterate 1000 times

            # get result
            for node_id in auth_map:
                if node_data_map[node_id] in affiliation_ids:
                    affiliation_id = node_data_map[node_id]
                    affiliation_auth = auth_map[node_id]
                    cursor.execute("""UPDATE new_AffiliationFieldData
                                      SET hits_auth = %s
                                      WHERE affiliation_id = %s AND field = %s""",
                                   (affiliation_auth, affiliation_id, field))

            # done!
            self.connection.commit()
            print "affiliation's auth in", field, "finished!"

    @timeit
    def update_author_citation(self, paper_reference_changed=False):
        cursor = self.connection.cursor()

        # update reference count of each paper if necessary
        if paper_reference_changed:
            cursor.execute("""UPDATE new_Papers
                              SET reference_count = (SELECT count(*)
                                                     FROM new_PaperReference
                                                     WHERE reference_id = new_Papers.paper_id);""")

        # update citation in each field of each author
        cursor.execute("""UPDATE new_AuthorFieldData
                          SET citation = (SELECT ifnull(sum(reference_count), 0)
                                          FROM new_PaperAuthor
                                            JOIN new_Papers USING (paper_id)
                                            JOIN new_PaperFieldData USING (paper_id)
                                          WHERE new_PaperAuthor.author_id = new_AuthorFieldData.author_id AND
                                                new_PaperFieldData.field = new_AuthorFieldData.field AND
                                                publish_year >= %s AND publish_year <= %s);""",
                       (self.start_year, self.end_year))

        # update total citation of each author
        # pull all paper's citation
        paper_citation = {}
        cursor.execute("""SELECT paper_id, reference_count
                          FROM new_Papers
                          WHERE publish_year >= %s AND publish_year <= %s;""", (self.start_year, self.end_year))
        for row in cursor.fetchall():
            paper_citation[row[0]] = row[1]

        print "info: all papers' citation has been fetched"

        # pull paper of each author
        author_paper = {}
        cursor.execute("""SELECT author_id, paper_id
                          FROM new_PaperAuthor;""")
        for row in cursor.fetchall():
            author_id = row[0]
            paper_id = row[1]
            if author_paper.has_key(author_id):
                author_paper[author_id].append(paper_id)
            else:
                author_paper[author_id] = [paper_id]

        print "info: all authors' paper has been fetched"

        # calculate each author's total citation and upload
        for author_id, paper_ids in author_paper.iteritems():
            total_citation = 0
            for paper_id in paper_ids:
                if paper_citation.has_key(paper_id):
                    total_citation += paper_citation[paper_id]

            cursor.execute("""UPDATE new_AuthorData
                              SET total_citation = %s
                              WHERE author_id = %s;""", (total_citation, author_id))

        # finish!
        print "info: total citation has been updated"
        self.connection.commit()

    @timeit
    def update_author_coauthor(self):
        for field in self.fields:
            cursor = self.connection.cursor()

            # fetch coauthor's h_index of each author
            cursor.execute("""SELECT
                                AuthorPaper.author_id,
                                new_PaperAuthor.author_id AS coauthor_id,
                                h_index
                              FROM new_PaperAuthor
                                JOIN new_AuthorFieldData USING (author_id)
                                JOIN (SELECT
                                        author_id,
                                        paper_id
                                      FROM new_PaperAuthor
                                        JOIN new_PaperFieldData USING (paper_id)
                                      WHERE field = %s) AuthorPaper USING (paper_id);""", (field,))

            author_coauthors = {}
            for row in cursor.fetchall():
                author_id = row[0]
                coauthor_id = row[1]
                h_index = row[2]
                if author_coauthors.has_key(author_id):
                    author_coauthors[author_id].append((coauthor_id, h_index))
                else:
                    author_coauthors[author_id] = [(coauthor_id, h_index)]

            # calculate the coauthor factor
            for author_id, coauthors in author_coauthors.iteritems():
                result = 0.0

                # prepare for normalization
                h_index_factor_sum = sum([self.h_index_factor(h_index) for _, h_index in coauthors])
                if h_index_factor_sum != 0:
                    for coauthor_id, h_index in coauthors:
                        result += 1.0 * h_index * self.h_index_factor(h_index) / h_index_factor_sum

                # update database
                cursor.execute("""UPDATE new_AuthorFieldData
                                  SET coauthor_factor = %s
                                  WHERE author_id = %s AND field = %s;""", (result, author_id, field))

            # finished!
            self.connection.commit()

    @timeit
    def update_author_affiliation(self):
        for field in self.fields:
            cursor = self.connection.cursor()

            # get author - affiliation_auth in each field
            author_affiliation = {}
            cursor.execute("""SELECT
                                author_id,
                                affiliation_id,
                                affiliation_auth
                              FROM new_PaperAuthor
                                JOIN new_Papers USING (paper_id)
                                JOIN new_PaperFieldData USING (paper_id)
                                JOIN new_AffiliationFieldData USING (affiliation_id, field)
                              WHERE field = %s AND publish_year >= %s AND publish_year <= %s;""",
                           (field, self.start_year, self.end_year))
            for row in cursor.fetchall():
                author_id = row[0]
                affiliation_id = row[1]
                affiliation_auth = row[2]
                if author_affiliation.has_key(author_id):
                    if affiliation_auth > author_affiliation[author_id][1]:
                        author_affiliation[author_id] = [affiliation_id, affiliation_auth]
                else:
                    author_affiliation[author_id] = [affiliation_id, affiliation_auth]

            # upload to database
            for author_id, affiliation in author_affiliation.iteritems():
                affiliation_id = affiliation[0]
                affiliation_auth = affiliation[1]
                cursor.execute("""UPDATE new_AuthorFieldData
                                  SET affiliation_factor  = %s,
                                    affiliation_id_chosen = %s
                                  WHERE author_id = %s AND field = %s;""",
                               (affiliation_auth, affiliation_id, author_id, field))

            # finished!
            self.connection.commit()

    @timeit
    def update_author_paper(self):
        for field in self.fields:
            cursor = self.connection.cursor()

            # get author's auth
            author_auth = dict()
            cursor.execute("""SELECT author_id, author_auth
                              FROM new_AuthorFieldData
                              WHERE field = %s;""", (field,))
            for row in cursor.fetchall():
                author_auth[row[0]] = row[1]

            print "info: author - auth has been fetched"

            # get all of author's papers and 1/author_seq)
            author_papers = dict()
            cursor.execute("""SELECT
                                author_id,
                                paper_id,
                                1 / author_sequence_num
                              FROM new_PaperAuthor
                                JOIN new_PaperFieldData USING (paper_id)
                                JOIN new_Papers USING (paper_id)
                              WHERE field = %s AND publish_year >= %s AND publish_year <= %s""",
                           (field, self.start_year, self.end_year))
            for row in cursor.fetchall():
                if author_papers.has_key(row[0]):
                    author_papers[row[0]].append((row[1], row[2]))
                else:
                    author_papers[row[0]] = [(row[1], row[2])]

            print "info: author - paper has been fetched"

            # get paper's venue and pagerank
            paper_pagerank = dict()
            paper_venue_auth = dict()
            cursor.execute("""SELECT
                                paper_id,
                                pagerank,
                                ifnull(venue_auth, 0)
                              FROM new_Papers
                                JOIN new_PaperFieldData USING (paper_id)
                                LEFT JOIN new_VenueFieldData USING (venue_id, field)
                              WHERE field = %s AND 
                                    publish_year >= %s AND publish_year <= %s""",
                           (field, self.start_year, self.end_year))
            for row in cursor.fetchall():
                paper_pagerank[row[0]] = row[1]
                paper_venue_auth[row[0]] = row[2]

            print "info: paper - pagerank, venue_auth has been fetched"

            # calculate the whole paper factor
            for author_id, author_auth in author_auth.iteritems():
                mul_add_chunk = 0.0
                if author_papers.has_key(author_id):
                    for paper_id, author_seq_reciprocal in author_papers[author_id]:
                        if paper_pagerank.has_key(paper_id) and paper_venue_auth.has_key(paper_id):
                            # print type(author_seq_reciprocal), type(paper_pagerank[paper_id]), type(paper_venue_auth[paper_id])
                            mul_add_chunk += float(author_seq_reciprocal) * paper_pagerank[paper_id] * paper_venue_auth[
                                paper_id]

                # upload to database
                cursor.execute("""UPDATE new_AuthorFieldData
                                  SET paper_factor = %s
                                  WHERE author_id = %s AND field = %s;""",
                               (mul_add_chunk * author_auth, author_id, field))

            # finish!
            print "info: paper factor of author has been updated"
            self.connection.commit()

    @timeit
    def update_author_h_index(self):
        for field in self.fields:
            cursor = self.connection.cursor()

            # get author - paper - reference_count
            author_citations = {}
            cursor.execute("""SELECT
                                author_id,
                                reference_count
                              FROM new_PaperAuthor
                                JOIN new_Papers USING (paper_id)
                                JOIN new_PaperFieldData USING (paper_id)
                              WHERE field = %s""", (field,))
            for row in cursor.fetchall():
                author_id = row[0]
                citation = row[1]
                if author_citations.has_key(author_id):
                    author_citations[author_id].append(citation)
                else:
                    author_citations[author_id] = [citation]

            # calculate h-index individually
            for author_id, citations in author_citations.iteritems():
                # sort in descending order
                citations.sort(reverse=True)

                # calculate h-index
                h_index = 0
                for citation in citations:
                    if h_index >= citation:
                        break
                    h_index += 1

                cursor.execute("""UPDATE new_AuthorFieldData
                                  SET h_index = %s 
                                  WHERE author_id = %s AND field = %s""", (h_index, author_id, field))

            # finished!
            self.connection.commit()

    @timeit
    def show_result(self, citation_threshold=1000, list_size=100):
        for field in self.fields:
            cursor = self.connection.cursor()
            cursor.execute("""SELECT
                                author_id,
                                author_name,
                                ace_rank,
                                affiliation_id_chosen,
                                ifnull(affiliation_name, 'None')
                              FROM new_AuthorFieldData
                                JOIN new_AuthorData USING (author_id)
                                LEFT JOIN new_AffiliationData ON (affiliation_id = affiliation_id_chosen)
                              WHERE field = %s AND total_citation <= %s
                              ORDER BY ace_rank DESC
                              LIMIT %s;""", (field, citation_threshold, list_size))

            # diaplay result and write to local file
            outfile = open("../result/%s_ranking.txt" % field, "w")
            for row in cursor.fetchall():
                line = "%s\t%s\t%f\t%s\t%s\n" % row
                print line,
                outfile.write(line)
            print

    @timeit
    def shallow_rank(self):
        for field in self.fields:
            cursor = self.connection.cursor()

            # get all prepared data
            cursor.execute("""SELECT author_id, citation, coauthor_factor, affiliation_factor, paper_factor
                              FROM new_AuthorFieldData
                              WHERE field = %s""", (field,))
            author_info = cursor.fetchall()

            # normalize each item
            cursor.execute("""SELECT
                                avg(citation),
                                avg(coauthor_factor),
                                avg(affiliation_factor),
                                avg(paper_factor)
                              FROM new_AuthorFieldData
                              WHERE field = %s;""", (field,))
            row = cursor.fetchone()
            citation_average = float(row[0])
            coauthor_average = float(row[1])
            affiliation_average = float(row[2])
            paper_average = float(row[3])

            # get rank scores
            for author_id, citation, coauthor_factor, affiliation_factor, paper_factor in author_info:
                score = self.self_citation_factor * citation / citation_average + \
                        self.coauthor_factor * coauthor_factor / coauthor_average + \
                        self.affiliation_factor * affiliation_factor / affiliation_average + \
                        self.paper_factor * paper_factor / paper_average
                cursor.execute("""UPDATE new_AuthorFieldData
                                  SET ace_rank = %s
                                  WHERE author_id = %s AND field = %s;""", (score, author_id, field))

            # finished!
            self.connection.commit()

    @timeit
    def deep_rank(self):
        self.update_affiliation_hits()
        self.update_author_citation(paper_reference_changed=True)
        self.update_author_coauthor()
        self.update_author_affiliation()
        self.update_author_paper()
        self.update_author_h_index()
        self.shallow_rank()

    def h_index_factor(self, h_index):
        return h_index


def main():
    ranker = Aceranker(start_year=2000, end_year=2005,
                       self_citation_factor=0.2, coauthor_factor=0.2, affiliation_factor=0.1, paper_factor=0.5)
    ranker.show_result()


if __name__ == '__main__':
    main()
