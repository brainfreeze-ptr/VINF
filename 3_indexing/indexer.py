#!/usr/bin/env python
import sys
import os
import lucene
from datetime import datetime
from java.nio.file import Paths
import inspect
# from org.apache.lucene.analysis.miscellaneous import LimitTokenCountAnalyzer
# from org.apache.lucene.analysis.standard import StandardAnalyzer
from org.apache.lucene.analysis.en import EnglishAnalyzer
from org.apache.lucene.document import Document, Field, FieldType, StoredField
from org.apache.lucene.index import DocValuesType
# from org.apache.lucene.analysis.miscellaneous import PerFieldAnalyzerWrapper
from org.apache.lucene.index import IndexWriter, IndexWriterConfig, IndexOptions
from org.apache.lucene.store import NIOFSDirectory

INDEX_DIR = "BookIndex"

"""
This class is loosely based on the Lucene (java implementation) demo class
org.apache.lucene.demo.IndexFiles.  It will take a directory as an argument
and will index all of the files in that directory and downward recursively.
It will index on the file path, the file name and the file contents.  The
resulting Lucene index will be placed in the current directory and called
'index'.
"""


class Ticker(object):

    def __init__(self):
        self.tick = True


class IndexFiles(object):
    """Usage: python indexer.py <doc_directory>"""

    def __init__(self, tsv, storeDir, analyzer):

        if not os.path.exists(storeDir):
            os.mkdir(storeDir)

        store = NIOFSDirectory(Paths.get(storeDir))
        config = IndexWriterConfig(analyzer)
        config.setOpenMode(IndexWriterConfig.OpenMode.CREATE)
        writer = IndexWriter(store, config)

        print('indexing ...')
        self.indexDocs(tsv, writer)
        print('commit index',)
        writer.commit()
        writer.close()
        print('done')

    def indexDocs(self, tsv, writer):
        id_field_type = FieldType()
        id_field_type.setStored(True)
        id_field_type.setTokenized(False)

        fulltext_field = FieldType()
        fulltext_field.setStored(True)
        fulltext_field.setTokenized(True)
        fulltext_field.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS)
        fulltext_field.setOmitNorms(True)
        fulltext_field.setStoreTermVectors(True)
        fulltext_field.setStoreTermVectorPositions(True)
        fulltext_field.setStoreTermVectorOffsets(True)

        int_field = FieldType()
        int_field.setStored(True)
        int_field.setTokenized(False)
        int_field.setIndexOptions(IndexOptions.DOCS)
        int_field.setDocValuesType(DocValuesType.NUMERIC)

        with open(tsv, 'r') as f:
            headers = f.readlines(1)[0].split('\t')
            cols = {headers[i].strip('\n'): i for i in range(len(headers))}
            books = f.readlines()

        id_ = 0
        for book_data in books:
            id_ += 1
            book = book_data.split('\t')

            # skip books without description?
            if book[cols['description']] == '-':
                print(f"warning: no description in {id_}")
                continue
            
            doc = Document()
            doc.add(StoredField("id", id_))

            for int_col in ['published', 'pages']:
                try:
                    int_value = int(book[cols[int_col]])
                    doc.add(Field(int_col, int_value, int_field))
                except ValueError:
                    continue
            
            for reg_col in ['title', 'author', 'genre', 'country', 'categories', 'description']:
                doc.add(Field(reg_col, book[cols[reg_col]], fulltext_field))

            doc.add(Field("content", book_data, fulltext_field))
            
            writer.addDocument(doc)


if __name__ == '__main__':
    lucene.initVM(vmargs=['-Djava.awt.headless=true'])
    print('lucene', lucene.VERSION)
    start = datetime.now()
    
    base_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
    tsv_file = '/VINF/distributed_processing/wiki_books.csv'
    IndexFiles(tsv_file, os.path.join(base_dir, INDEX_DIR), EnglishAnalyzer())
    
    end = datetime.now()
    print(end - start)