#! /bin/bash
if [ -d "result" ]
then
	echo ""
	echo "Removing existing directory"
	rm -r "result"
else
	echo ""
	echo "Executing script"
	echo ""
fi
spark-submit latest_movies.py
echo ""
echo "The latest released movies are:"
echo ""
cat "result/part-00000"
echo ""
