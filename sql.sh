DIR=$1
database=$2
explain=${3:-true}

log_database=log_$database
err_database=err_$database

rm $log_database $err_database

for filename in $DIR/*.sql; do
	echo $filename
        echo "@@@$filename" >>$log_database
        echo "@@@$filename" >>$err_database

	if $explain; then
		filename_=${filename}_
                cp $filename $filename_
		awk '/^$/ {nlstack=nlstack "\n";next;} {printf "%s",nlstack; nlstack=""; print;}' $filename | tac | sed 's/\;/\; explain FORMATTED/g' |  sed '1 s/explain FORMATTED//' | tac > $filename_
    		hive --database $database -e "set hive.execution.engine=tez; explain FORMATTED $(<$filename_)" 1>>$log_database 2>>$err_database
                rm $filename_             
	else 
        	hive --database $database -f $filename 1>>$log_database 2>>$err_database
	fi
done
