version=${1:-spark}

SPARK_CORES_MAX=80
SPARK_SQL_SHUFFLE_PARTITIONS=40

SPARK_DRIVER_MEMORY=4g
SPARK_EXECUTOR_MEMORY=4g

for filename in sample-queries-tpcds/*.sql; do
	echo $filename
	$SPARK_HOME/bin/spark-sql \
		--conf spark.driver.memory=$SPARK_DRIVER_MEMORY \
		--conf spark.executor.memory=$SPARK_EXECUTOR_MEMORY \
		--conf spark.cores.max=$SPARK_CORES_MAX \
		--conf spark.sql.shuffle.partitions=$SPARK_SQL_SHUFFLE_PARTITIONS \
		--database tpcds_text_10 \
		-f $filename \
		1>${filename}.${version}.log 2>${filename}.${version}.err
	#break 
done 
