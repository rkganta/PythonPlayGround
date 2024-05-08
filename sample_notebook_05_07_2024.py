# boilerplate

from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.window import *

# create spark session
# /Workspace/Users/datakraft867@gmail.com/books.csv
spark = SparkSession.builder.appName("books_query").getOrCreate()
books_df = spark.read.csv("/Volumes/datakraft_batch1/default/datasets/books.csv", header=True, inferSchema=True)

# count the number of rows
books_df.count()

# display to rows
display(books_df)

# create temp value
books_df.createOrReplaceTempView("books_tbl")

# sample SQL
%sql

SELECT * FROM books_tbl;


# How many total books are present in the database?
books_cnt = spark.sql(\
    f"""
    SELECT COUNT(isbn) as books_cnt FROM books_tbl
    """).show()


# what is the total page count of all books published by Bill Bryson?
# cast num_pages to integer
books_df = books_df.withColumn("num_pages", col("num_pages").cast("integer"))
# verify
books_df.printSchema()
# sql
tot_pg_bb = spark.sql(\
    f"""
    SELECT SUM(num_pages)
    FROM books_tbl
    WHERE authors = "Bill Bryson"
    """).show()

# for books published in 2021, what is the average book rating?
# extract year frpm year from publication date
avg_rating = spark.sql(\
  f"""
  WITH CTE AS (
  SELECT 
  CAST(RIGHT(publication_date, 4) AS INT) AS yr,
  ROUND(AVG(average_rating),2) AS avg_rating
  FROM books_tbl
  GROUP BY 1
  ORDER BY 1
  )

  SELECT yr, avg_rating FROM CTE WHERE yr = 1943;
  """).show()
