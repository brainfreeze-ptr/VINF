import pandas as pd
import os
# This script merges all parts of spark output into a single tsv file.
# Also merges alternative columns into original (see merge_columns()).
# spark-submit --packages com.databricks:spark-xml_2.12:0.14.0 /VINF/distributed_processing/wiki_book_parser.py 
# hadoop fs -copyToLocal wiki_extracted/part* /VINF/distributed_processing


def merge_columns(df, col1, col2):
    # merge values of col2 (alternate column) into col1 (main column) if col1 is NaN, remove col2
    df[col1] = [main if not pd.isnull(main) else alt for main, alt in zip(df[col1], df[col2])]
    df.drop(columns=[col2], inplace=True)
    return df


def convert_to_int(df, col):
    # convert column into integer values while preserving NaNs
    df[col] = pd.to_numeric(df[col], errors='coerce')
    df[col] = df[col].astype('Int64')
    return df


def postprocess_spark_tsv(df):
    book_count = len(df)
    
    df = merge_columns(df, 'author', 'author_alt')
    df = merge_columns(df, 'author', 'author_alt2')
    df = merge_columns(df, 'published', 'published_alt')
    df = merge_columns(df, 'published', 'published_alt2')
    
    print(f"Books       : {book_count}")
    print(f"Overall:    : {(df.size - df.isna().sum().sum()) / df.size * 100:.2f}%\n")
    for col in df.columns:
        print(f"{col:12}: {(book_count - df[col].isna().sum()) / book_count * 100:.2f}%")

    df = convert_to_int(df, 'pages')
    df = convert_to_int(df, 'published')
    # display(df.head(25))
    
    df.to_csv('distributed_processing/wiki_books.csv', sep='\t', index=False, na_rep='-')


def join_spark_outputs(dir):
    # load all spark outputs into one pandas dataframe
    csv_files = [file for file in os.listdir(dir) if file.endswith('.csv')]
    dfs = []
    for file in csv_files:
        file_path = os.path.join(dir, file)
        df = pd.read_csv(file_path, sep='\t')
        dfs.append(df)
    
    combined_df = pd.concat(dfs, ignore_index=True)
    postprocess_spark_tsv(combined_df)


if __name__ == '__main__':
    join_spark_outputs('distributed_processing/wiki_all_tsv')
