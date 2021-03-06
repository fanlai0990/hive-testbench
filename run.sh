version=${1:-spark}
seq=${2:-0}

SPARK_CORES_MAX=280
SPARK_SQL_SHUFFLE_PARTITIONS=280

SPARK_DRIVER_MEMORY=300g
SPARK_EXECUTOR_MEMORY=4g

for filename in sample-queries-tpch/*.sql; do
	echo $filename
	$SPARK_HOME/bin/spark-sql \
		--database tpch_text_2 \
        --name $filename \
		-f $filename \
		1>${filename}.${version}.${seq}.log 2>${filename}.${version}.${seq}.err
	#break
done
