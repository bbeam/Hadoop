################################################################################
#                               General Details                                #
################################################################################
# Name        : AngiesList                                                     #
# File        : Wrapper_Edh_Batch_Audit.sh                                     #
# Description : This script performs auditing								   #
# Author      : Ashoka Reddy                                                   #
################################################################################

# Check for the input parameter to the shell script. The script only expects source name as the first parameter.
Show_Usage()
{
	echo "invalid argument please pass only one argument "
    echo "Usage: $0 <Source Name> <Subject Area>"
    echo "Ex: $0 alweb alwebmetrics"
    exit 1
}

#######################################################
# Function to be used to copy data from s3 to Local
#
# Input
#    source_path - s3 path from where files to be copied
#    destination_path - local  path where files are stored


function fn_copy_to_local()
{
    source_path=$1
    destination_path=$2
	
	aws s3 cp $source_path $destination_path
 	
	put_var=$?
    if [ ${put_var} -eq 0 ]
    then
        echo "Data copied successfully to the location: ${destination_path}"
    else
        echo "Unable to copy data to the location ${destination_path}"
    	exit 100;
    fi     
}

if [ $# -ne 2 ] 
then
    Show_Usage
fi


# Copy the alweb.properties file from S3 to HDFS and load the properties.
fn_copy_to_local s3://al-edh-dm/src/$1/main/conf/$1.properties /var/tmp/

if [ $? -eq 0 ]
then
	. /var/tmp/$1.properties
else
	echo "Copy of $1.properties file failed from S3."
	exit 1
fi


fn_copy_to_local s3://al-edh-dm/src/$1/main/conf/$2.properties /var/tmp/

if [ $? -eq 0 ]
then
	. /var/tmp/$2.properties
else
	echo "Copy of $2.properties file failed from S3."
	exit 1
fi


# Copy the common.properties file from S3 to HDFS and load the properties.
fn_copy_to_local s3://al-edh-dm/src/common/main/conf/common.properties /var/tmp/

if [ $? -ne 0 ]
then
	echo "Copy of common.properties file failed from S3."
	exit 1
fi


hive -f s3://al-edh-dm/src/common/main/hive/Create_${TABLE_EDH_BATCH_AUDIT}.hql \
	--hivevar COMMON_OPERATIONS_DB="${COMMON_OPERATIONS_DB}" \
	--hivevar TABLE_EDH_BATCH_AUDIT="${TABLE_EDH_BATCH_AUDIT}" \
	--hivevar S3_DATA_LOCATION_COMMON_OPERATIONS="${S3_DATA_LOCATION_COMMON_OPERATIONS}"
	
if [ $? -eq 0 ]
then
	echo "Audit table created successfully"
else
	echo "error in creation of audt table "
	exit 1
fi
