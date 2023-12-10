import unittest
import sys
import pandas as pd
import numpy as np
import re
sys.path.append('../2_distributed_processing')
sys.path.append('../1_crawling_custom_indexing')
from wiki_output_merger import merge_columns
from simple_search import create_tf_idf
from spark_wiki_book_parser import regex_dict


class IRTest(unittest.TestCase):

    def test_merge_columns(self):
        '''Test if the alternative column merging works.'''
        df = pd.DataFrame({
            'main_col': [1, np.nan, 3],
            'alt_col': [26, 2, np.nan]
        })
        df_merged = merge_columns(df, 'main_col', 'alt_col')
        self.assertEqual(len(df_merged), 3)                            # row count has to stay the same
        self.assertEqual(len(df_merged.columns), 1)                    # alt column should be removed
        self.assertListEqual(df_merged.main_col.to_list(), [1, 2, 3])  # values 1 and 3 should stay the same, value 2 should be merged from alt_col
        
    def test_custom_tf_idf(self):
        '''
        Check if the computed tf and idf is correct.
        test file:
        0 header
        1 first random text line
        2 second line text and text
        '''
        tf, idf = create_tf_idf('/Notebooks/VINF/4_unittests_evaluation/test_entities.tsv')
        
        # check if all terms are in tf dictionary 
        for term in ['first', 'random', 'text', 'line', 'second', 'and']:
            self.assertIn(term, tf)

        # check correct assignment wrt document id
        for first_doc_term in ['first', 'random', 'text', 'line']:
            self.assertIn(1, tf[first_doc_term])
        for first_doc_term in ['text', 'line', 'second', 'and']:
            self.assertIn(2, tf[first_doc_term])

        # check correct word count
        for single_occ in ['first', 'random', 'second', 'and']:
            self.assertEqual(1, sum(tf[single_occ].values()))
        for two_occ in ['line']:
            self.assertEqual(2, sum(tf[two_occ].values()))
        for three_occ in ['text']:
            self.assertEqual(3, sum(tf[three_occ].values()))

    def test_regex_extraction(self):
        '''Check if all entity attributes are parsed correctly'''
        with open('witcher_1.txt', 'r') as f:
            book = f.read()

        ground_truth = {
            'title': 'The Last Wish',
            'author': 'Andrzej Sapkowski',
            'published': '1993',
            'pages': '288',
            'genre': 'Fantasy',
            'country': 'Poland',
            'categories': '1993 short story collections',
        }
        
        for col, regex in regex_dict.items():
            self.assertEqual(ground_truth[col], re.search(regex, book).group(1).strip(']['))


if __name__ == '__main__':
    unittest.main()
