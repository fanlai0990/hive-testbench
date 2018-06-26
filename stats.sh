rm wc.log

for filename in sample-queries-tpch/*.sql; do
	echo $filename
	echo $filename >> wc.log
	grep ${filename}.err -e 'registerShuffle' | wc -l >> wc.log
done 

exit 1

for filename in sample-queries-tpcds/*.sql; do
	echo $filename
	echo $filename >> wc.log
	grep ${filename}.err -e 'registerShuffle' | wc -l >> wc.log
done 
