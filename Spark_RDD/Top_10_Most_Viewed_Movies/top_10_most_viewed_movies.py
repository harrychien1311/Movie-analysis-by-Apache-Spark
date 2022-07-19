from pyspark.context import SparkContext
from operator import add
sc=SparkContext("local")
ratingsRDD=sc.textFile("../../Movielens/ratings.dat")
movies=ratingsRDD.map(lambda line : str(line.split("::")[1]))
movies_pair=movies.map(lambda mv:(mv,1))

movies_count=movies_pair.reduceByKey(add)
movies_sorted=movies_count.sortBy(lambda x:x[1],False,1)

mv_top10List=movies_sorted.take(10)
mv_top10RDD=sc.parallelize(mv_top10List)

mv_names=sc.textFile("../../Movielens/movies.dat").map(lambda line:(str(line.split("::")[0]),line.split("::")[1]))
join_out=mv_names.join(mv_top10RDD)
join_out.sortBy(lambda x:x[1][1],False).map(lambda x: x[0]+","+x[1][0]+","+str(x[1][1])).repartition(1).saveAsTextFile("Top-10-CSV")
