node sql-generator.js
deletionDir=`ls -t deletions`
if [ -z "$deletionDir" ]
then
	printf "NOTHING DELETED %s.\n" $(date +%s) >> deletions.txt
else
	latestFile=`ls -t deletions/* | head -1`
	PGPASSWORD=<DBPASS> psql -h <DBHOST> -U postgres -d <DBNAME> -a -f $latestFile
	mv $latestFile old-$latestFile
	printf "RAN DELETION SCRIPT %s.\n" $latestFile >> deletions.txt
fi
