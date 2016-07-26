################################################################################
#                               General Details                                #
################################################################################
# Organisation: AngiesList                                                     #
# File        : wrapper_bkp.sh                                                 #
# Description : This process would take a backup copy of the target table.     #
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
    . /var/tmp/${local_file}
	echo "${local_file} loded successfully from $2"
else
    echo "Load of "${local_file}" failed from "
    exit 1
fi

#Trigger HQL for back up copy of source table .
hive -f ${HIVE_FILE_PATH} \
    --hivevar TARGET_DB="${TARGET_DB}" \
    --hivevar SOURCE_DB="${SOURCE_DB}"


if [ $? -eq 0 ]
then
    echo "The backup completed successfully "
else
    echo "Error while taking backup "
    exit 1
fi