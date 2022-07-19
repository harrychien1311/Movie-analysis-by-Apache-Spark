from pyspark.context import SparkContext
from operator import add
sc=SparkContext("local")
movies_rdd=sc.textFile("../../Movielens/movies.dat")
genre=movies_rdd.map(lambda lines : lines.split("::")[2])
flat_genre=genre.flatMap(lambda lines:lines.split("|"))
genre_kv=flat_genre.map(lambda k : (k,1))
genre_count=genre_kv.reduceByKey(add)
genre_sort= genre_count.sortByKey()
genre_sort.saveAsTextFile("result-csv")
#System.exit(0)
