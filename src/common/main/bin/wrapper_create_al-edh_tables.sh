################################################################################
#                               General Details                                #
################################################################################
# Name        : AngiesList                                                     #
# File        : wrapper_create_al-edh_tables.sh                                #
# Description : This is shell script for all the  hive tables creation.        # 
# Author      : Ashoka Reddy                                                   #
################################################################################


# Check for the input parameters to the shell script. The script only expects global properties as first parameter and local parameter as second parameter.
Show_Usage()
{
    echo "invalid arguments please pass two arguments "
    echo "Usage: "$0" <global properties file with path> <path of local properties file with path>"
    exit 1
}

if [ $# -ne 2 ]
then
    Show_Usage
fi


# Copy the al-edh-global.properties file from S3 to local and load the properties.
aws s3 cp $1 /var/tmp/

if [ $? -eq 0 ]
then
        echo "Global properties file copied successfully from "$1" to "/var/tmp/" "
    global_file=`echo "$(basename $1)"`
        . /var/tmp/${global_file}
        echo "${global_file} loaded successfully "
else
    echo "Load of ${global_file} failed from $1."
    exit 1
fi

# Copy the local properties file from S3 to local and load the properties.
aws s3 cp $2 /var/tmp/

if [ $? -eq 0 ]
then
        echo "Local properties copied successfully from "$2" to "/var/tmp/" "
        local_file=`echo "$(basename $2)"`
        
else
    echo "Load of "${local_file}" failed from "
    exit 1
fi


for i in `cat /var/tmp/${local_file}`
do

	FILE_PATH=`echo $i | cut -f1 -d ','`
		
	FILE_NAME=`echo $i | cut -f2 -d ','`
		
	DB_NAME_VAR=`echo $i | cut -f3 -d ','`
	
	DB_NAME="${!DB_NAME_VAR}"
	echo $DB_NAME
	#Create the database in hive 
	#echo "$DB_NAME database creating...................... "
	hive -e "CREATE DATABASE IF NOT EXISTS $DB_NAME"

		if [ $? -eq 0 ]
		then	
			echo "$DB_NAME Database is avaliable in hive "
		else	
			echo "$DB_NAME Database creation failed "
		exit 1
		fi

	# Hive script to create tables 
	echo "$S3_BUCKET/$FILE_PATH/$FILE_NAME Executing............ "
	hive -f $S3_BUCKET/$FILE_PATH/$FILE_NAME \
			-hivevar DB_NAME="$DB_NAME" \
			-hivevar S3_BUCKET="$S3_BUCKET" 
		if [ $? -eq 0 ]
		then	
			echo "$S3_BUCKET/$FILE_PATH/$FILE_NAME executed successfully"
		else	
			echo "$S3_BUCKET/$FILE_PATH/$FILE_NAME execution failed"
		exit 1
		fi
		

done
