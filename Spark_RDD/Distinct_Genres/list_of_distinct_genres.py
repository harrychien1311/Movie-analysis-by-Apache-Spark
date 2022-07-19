from pyspark.context import SparkContext
from operator import add
sc=SparkContext("local")

movies_rdd=sc.textFile("../../Movielens/movies.dat")
genres=movies_rdd.map(lambda lines:lines.split("::")[2])
testing=genres.flatMap(lambda line:line.split('|'))
genres_distinct_sorted=testing.distinct().sortBy(lambda x: x[0])
genres_distinct_sorted.saveAsTextFile("result")
