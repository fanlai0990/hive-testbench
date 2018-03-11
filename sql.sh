database=$1
TESTHOME=/users/wentingt/hive-testbench
INPUT=$TESTHOME/sample-queries-tpcds
OUTPUT=$TESTHOME/trees/${database}
rm -r $OUTPUT
mkdir -p $OUTPUT
HIVELOG=/tmp/wentingt/hive.log

cd $INPUT
for filename in *.sql; do
	echo $filename
	rm $HIVELOG

	filename_=${filename}_
	cp $filename $filename_
	awk '/^$/ {nlstack=nlstack "\n";next;} {printf "%s",nlstack; nlstack=""; print;}' $filename | tac | sed 's/\;/\; explain FORMATTED/g' |  sed '1 s/explain FORMATTED//' | tac > $filename_
	hive --database $database -e "set hive.crossquery.combination=-1; explain FORMATTED $(<$filename_)" 1>$OUTPUT/${filename}.json >>$OUTPUT/log
	rm $filename_             

	cp $HIVELOG $OUTPUT/${filename}.log
done
