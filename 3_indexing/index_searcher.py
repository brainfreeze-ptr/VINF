#!/usr/bin/env python
import lucene
import re
from java.nio.file import Paths
from org.apache.lucene.analysis.en import EnglishAnalyzer
from org.apache.lucene.index import DirectoryReader
from org.apache.lucene.queryparser.classic import QueryParser
from org.apache.lucene.store import NIOFSDirectory
from org.apache.lucene.search import IndexSearcher

INDEX_DIR = "BookIndex"

"""
It will prompt for a search query, then it
will search the Lucene index in the current directory called 'index' for the
search query entered against the 'contents' field.  It will then display the
'path' and 'name' fields for each of the hits it finds in the index.  Note that
search.close() is currently commented out because it causes a stack overflow in
some cases.
"""


def run(searcher, analyzer):
    while True:
        print("\nHit enter with no input to quit.")
        command = input("Query:")
        if command == '':
            return

        print("\nSearching for:", command)
        parser = QueryParser("content", analyzer)
        # parser.setDefaultOperator(QueryParser.Operator.AND)
        query = parser.parse(command)
        scoreDocs = searcher.search(query, 20).scoreDocs
        print("%s total matching documents." % len(scoreDocs))

        for scoreDoc in scoreDocs:
            doc = searcher.doc(scoreDoc.doc)
            print(pretty_print(doc, scoreDoc.score))
            # print(f"{doc.get('id'):6}: {doc.get('title')[:30]:30} ({scoreDoc.score:.3f})")


def pretty_print(doc, score):
    sep = 78 * '-'
    author_book = strip_html(f"{doc.get('author')} - {doc.get('title')}")
    pages = doc.get('pages') if doc.get('pages') is not None else '-'
    description = strip_html(doc.get('description'))
    return f"{author_book[:48]:48} ({doc.get('published')}) | {pages:4} pages | ({score:.3f})\n\t{description[:70]}\n\t{description[70:137]}...\n{sep}"


def strip_html(text):
    text = re.sub('<[^<]+?>', ' ', text)
    text = re.sub(r'[\]\[]', '', text)
    text = re.sub(r"'{2,5}", '', text)
    return text.strip('"')


if __name__ == '__main__':
    lucene.initVM(vmargs=['-Djava.awt.headless=true'])
    print('lucene', lucene.VERSION)
    # base_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
    directory = NIOFSDirectory(Paths.get(INDEX_DIR))
    searcher = IndexSearcher(DirectoryReader.open(directory))
    analyzer = EnglishAnalyzer()
    run(searcher, analyzer)
    del searcher