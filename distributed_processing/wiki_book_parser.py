from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_extract, udf, concat_ws, regexp_replace
import re
from pyspark.sql.types import ArrayType, StringType


def extract_categories(text):
    return re.findall(r'\[\[Category:(.*?)\]\]', text)


extract_categories_udf = udf(extract_categories, ArrayType(StringType()))

# create spark session
spark = SparkSession.builder.appName("WikipediaBookParser").getOrCreate()

# load wiki dump
wiki_data = spark.read.format('xml').option('rowTag', 'page').load('enwiki-latest-pages-articles10.xml-p4045403p5399366')

# filter only book articles (either 'book' in title or article contains book infobox)
book_articles = wiki_data.filter(col('title').like('%(novel)%') |
                                 col('title').like('%(book)%') |
                                 col('revision.text._VALUE').rlike(r'\{\{Infobox book'))

# extract values, create columns
books = book_articles \
    .withColumn('title', regexp_extract(col('title'), r'^([^(]*)', 1)) \
    .withColumn('author', regexp_replace(regexp_extract(col('revision.text._VALUE'), r'author\s*=\s*(.*?)\n', 1), r'[\[\]]', '')) \
    .withColumn('author_alt', regexp_extract(col('revision.text._VALUE'), r'is a (\d{4}) .*? by (.*?)[\.,]', 2)) \
    .withColumn('author_alt2', regexp_extract(col('revision.text._VALUE'), r'by.*?[Aa]uthor.*?\[{2}(.*?)\]{2}', 1)) \
    .withColumn('published', regexp_extract(col('revision.text._VALUE'), r'(?:release_date|pub_date)\s*=.*?\b([12]\d{3})\b\n', 1)) \
    .withColumn('published_alt', regexp_extract(col('revision.text._VALUE'), r'is a (\d{4}) .*? by (.*?)[\.,]', 1)) \
    .withColumn('published_alt2', regexp_extract(col('revision.text._VALUE'), r'published in.*?(\d{4})', 1)) \
    .withColumn('pages', regexp_extract(col('revision.text._VALUE'), r'pages\s+=\s*?(\d+).*?\n', 1)) \
    .withColumn('genre', regexp_replace(regexp_extract(col('revision.text._VALUE'), r'genre\s+=\s(.*?)\n', 1), r'[\[\]]', '')) \
    .withColumn('country', regexp_extract(col('revision.text._VALUE'), r'country\s+=\s+\[{0,2}(.*?)\]{0,2}\n', 1)) \
    .withColumn('categories', concat_ws(" | ", extract_categories_udf(book_articles['revision.text._VALUE']))) \
    .withColumn('description', regexp_replace(regexp_extract(col('revision.text._VALUE'), r"'''''([\s\S]*)==", 1), r'[\t\n,\[\]]', '')) \
    .select('title', 'author', 'author_alt', 'author_alt2', 'published', 'published_alt', 'published_alt2', 'pages', 'genre', 'country', 'categories', 'description')

# export into tsv
books.write \
    .mode('overwrite') \
    .option("header", "true") \
    .option("delimiter", "\t") \
    .csv('wiki_books_extracted')

spark.stop()
