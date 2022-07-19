#!/usr/bin/env bash
# Check if the result directory exsists or not
if [ -d "result-csv" ]
then 
	echo "Removing previous exsisting directory"
	rm -r "result-csv"
else
	echo "Executing script"
fi
spark-submit Movies_in_each_genre.py
echo ""
echo ""
echo "The number of movies in each genre"
echo ""
echo "Genre,Count"
cat "result-csv/part-00000"
echo ""
