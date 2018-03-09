database=$1
cbo=${2:-true}
TESTHOME=/users/wentingt/hive-testbench
INPUT=$TESTHOME/sample-queries-tpcds
OUTPUT=$TESTHOME/trees/${database}-${cbo}
rm -r $OUTPUT
mkdir -p $OUTPUT

cd $INPUT
for filename in *.sql; do
	echo $filename

	filename_=${filename}_
	cp $filename $filename_
	awk '/^$/ {nlstack=nlstack "\n";next;} {printf "%s",nlstack; nlstack=""; print;}' $filename | tac | sed 's/\;/\; explain FORMATTED/g' |  sed '1 s/explain FORMATTED//' | tac > $filename_
	hive --database $database -e "set hive.cbo.enable=$cbo; explain FORMATTED $(<$filename_)" 1>$OUTPUT/${filename}.json 
	rm $filename_             

done
