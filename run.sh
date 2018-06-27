#export SPARK_HOME=/users/jimmyyou/spark-terra
export SPARK_HOME=/users/jimmyyou/spark-2.3.1-bin-hadoop2.7

for filename in sample-queries-tpcds/*.sql; do
	echo $filename
	$SPARK_HOME/bin/spark-sql \
		--conf spark.cores.max=20 \
		--conf spark.sql.shuffle.partitions=4 \
		--database tpcds_text_10 \
		-f $filename \
		1>${filename}.log 2>${filename}.err
	break
done 

exit 1
for filename in sample-queries-tpch/*.sql; do
	echo $filename
	$SPARK_HOME/bin/spark-sql --database tpch_text_2 -f $filename 1>${filename}.log 2>${filename}.err
done 

