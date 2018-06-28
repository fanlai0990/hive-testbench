version=${1:-spark}

SPARK_CORES_MAX=20
SPARK_SQL_SHUFFLE_PARTITIONS=4

for filename in sample-queries-tpcds/*.sql; do
	echo $filename
	$SPARK_HOME/bin/spark-sql \
		--conf spark.cores.max=SPARK_CORES_MAX \
		--conf spark.sql.shuffle.partitions=SPARK_SQL_SHUFFLE_PARTITIONS \
		--database tpcds_text_10 \
		-f $filename \
		1>${filename}.${version}.log 2>${filename}.${version}.err
done 
