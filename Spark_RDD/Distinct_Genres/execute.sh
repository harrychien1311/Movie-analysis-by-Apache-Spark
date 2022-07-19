#!/usr/bin/env bash
# Check if the directory is present or not
if [ -d "result" ]
then
	echo "Removing existing directory"
	rm -r result
else
	echo "Executing File"
fi
spark-submit list_of_distinct_genres.py
echo ""
echo ""
echo "The list of distinct genres are"
echo ""
cat "result/part-00000"
echo ""
