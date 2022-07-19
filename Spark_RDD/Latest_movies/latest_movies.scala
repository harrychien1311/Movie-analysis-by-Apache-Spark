movies_rdd=sc.textFile("../../Movielens/movies.dat")
movie_nm=movies_rdd.map(lambda lines:lines.split("::")(1))
year=movie_nm.map(lambda lines:lines.substring(lines.lastIndexOf("(")+1,lines.lastIndexOf(")")))
latest=year.max
latest_movies=movie_nm.filter(lambda lines:lines.contains("("+latest+")")).saveAsTextFile("result")
System.exit(0)
