from pyspark.context import SparkContext

sc=SparkContext("local")
movies_rdd=sc.textFile("../../Movielens/movies.dat")
movie_nm=movies_rdd.map(lambda lines : lines.split("::")[1])

year=movie_nm.map(lambda lines:lines[lines.rfind("(")+1:lines.rfind(")")])
latest=max(year.collect())

latest_movies=movie_nm.filter(lambda lines : '({})'.format(latest) in lines).saveAsTextFile("result")
#System.exit(0)
