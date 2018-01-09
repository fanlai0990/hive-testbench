DIR=$1
database=$2
rm $DIR/log $DIR/err
for filename in $DIR/*.sql; do
	echo $filename
        echo "@@@$filename" >>$DIR/log
        echo "@@@$filename" >>$DIR/err
        hive --database $database -f $filename 1>>$DIR/log 2>>$DIR/err
done
