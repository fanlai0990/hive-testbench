version=${1:-spark}

for filename in sample-queries-tpcds/*.sql; do
	echo $filename
	$SPARK_HOME/bin/spark-sql \
		--conf spark.cores.max=20 \
		--conf spark.sql.shuffle.partitions=4 \
		--database tpcds_text_10 \
		-f $filename \
		1>${filename}.${version}.log 2>${filename}.${version}.err
	break
done 
