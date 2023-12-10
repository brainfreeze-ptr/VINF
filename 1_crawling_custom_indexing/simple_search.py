import nltk
import math
import re
from copy import copy
from nltk.tokenize import word_tokenize
nltk.download('punkt')


# create tf-idf
def create_tf_idf(entity_file, n_entities=None):
    with open(entity_file, 'r') as f:
        entities = f.readlines()

    if n_entities is None:
        N = len(entities) - 1
    else:
        N = n_entities
    
    tf = {}   # {word: {document: num_occurences, document: num_...}}
    df = {}   # {word: num_of_documents}
    doc_id = 0
    for e in entities[1:N + 1]:
        doc_id += 1
        terms = word_tokenize(e.strip('\n'))
        for term in terms:
            term = term.strip(',.?!')
            if term not in tf:
                tf[term] = {}
            tf[term][doc_id] = tf[term].get(doc_id, 0) + 1
        unique_terms = set(terms)
        for t in unique_terms:
            df[t] = df.get(t, 0) + 1

    sorted_df = dict(sorted(df.items(), key=lambda item: item[1], reverse=True))
    idf = {term: math.log((N/freq), 10) for term, freq in sorted_df.items()}
    # display(idf)
    return tf, idf


def parse_args(query):
    search_args = {}
    cmd_args = query.split('--')
    for cmd_arg in cmd_args:
        if not cmd_arg:
            continue 
        arg, params = cmd_arg.split(':')
        search_args[arg] = [param.strip() for param in params.split(',')]
    return search_args


def perform_query(search_args):
    if 'contains' in search_args:
        index_values = {}
        for search_term in search_args['contains']:
            for doc_id, term_freq in tf.get(search_term.strip(), {}).items():
                tf_idf_value = term_freq * idf[search_term]
                index_values[doc_id] = index_values.get(doc_id, 0) + tf_idf_value
    
    if 'pages' in search_args:
        index_values_copy = copy(index_values)
        res = re.search(r'(\d+)([+-])', search_args['pages'][0])
        count = int(res.group(1))
        op = res.group(2)
    
        cmp_fun = lambda x: x >= count if op == '+' else x < count
        
        for doc_id in index_values_copy:
            book_pages = entities[doc_id].split('\t')[3]
            if book_pages.strip() == '-' or not cmp_fun(int(book_pages)):
                index_values.pop(doc_id)

    if 'year' in search_args:
        index_values_copy = copy(index_values)
        res = re.search(r'(\d{4})([+-])', search_args['year'][0])
        year = int(res.group(1))
        op = res.group(2)
    
        cmp_fun = lambda x: x >= year if op == '+' else x < year
        
        for doc_id in index_values_copy:
            published_year = entities[doc_id].split('\t')[2]
            if published_year.strip() == '-' or not cmp_fun(int(published_year)):
                index_values.pop(doc_id)
    
    return index_values


def pretty_print(book_data):
    sep = 78 * '-'
    author_book = f"{book_data[1]} - {book_data[0]}"
    return f"{author_book[:40]:40} {book_data[6]:15} ({book_data[2].strip()}) | {book_data[3]:3} pages | ({tf_idf_value:.3f})\n\t{book_data[10][:70]}\n\t{book_data[10][70:137]}...\n{sep}"


if __name__ == '__main__':
    # create index
    tf, idf = create_tf_idf(entity_file='merged_entities.tsv', n_entities=None)
    
    # load entities
    with open('merged_entities.tsv', 'r') as f:
        entities = f.readlines()
    
    while True:
        query = input('Query:').strip()
        if query == 'exit':
            break

        if query == 'help':
            print('Usage: --contains: space, aliens --pages 200+')
            continue

        search_args = parse_args(query)
        index_values = perform_query(search_args)

        # print results
        top_search_results = sorted(index_values.items(), key=lambda item: item[1], reverse=True)[:10]
        print(f"Top N results for query: {query}")
        for doc_id, tf_idf_value in top_search_results:
            book_data = entities[doc_id].split('\t')
            print(pretty_print(book_data))
        print('\n\n')

