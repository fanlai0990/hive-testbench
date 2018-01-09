DIR=$1
database=$2
explain=${3:-true}

log_database=log_$database
err_database=err_$database

rm log_database err_database

for filename in $DIR/*.sql; do
	echo $filename
        echo "@@@$filename" >>$log_database
        echo "@@@$filename" >>$err_database

	if $explain; then
		hive --database $database -e "explain $(<$filename)" 1>>$log_database 2>>$err_database
	else 
        	hive --database $database -f $filename 1>>$log_database 2>>$err_database
	fi

done
