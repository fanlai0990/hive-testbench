SPARK_HOME=/users/wentingt/spark-terra

for filename in sample-queries-tpch/*.sql; do
	echo $filename
	$SPARK_HOME/bin/spark-sql --database tpch_text_2 -f $filename 1>${filename}.log 2>${filename}.err
done 

exit 1

for filename in sample-queries-tpcds/*.sql; do
	echo $filename
	$SPARK_HOME/bin/spark-sql --database tpcds_text_2 -f $filename 1>${filename}.log 2>${filename}.err
done 
