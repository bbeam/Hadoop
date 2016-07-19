################################################################################
#                               General Details                                #
################################################################################
# Name        : AngiesList                                                     #
# File        : wrapper_dq_t_skuitem.sh                                        #
# Description : This script performs data quality and cleansing on the data,   #
#				and finally creates a new table with the elements according to #
#				the incoming schema							 				   #
# Author      : Varun Rauthan                                                  #
################################################################################

# Check for the input parameter to the shell script. The script only expects source name as the first parameter.
Show_Usage()
{
	echo "invalid argument please pass only one argument "
    echo "Usage: $0 <Source Name>"
    echo "Ex: $0 alweb"
    exit 1
}

if [ $# -ne 1 ] 
then
    Show_Usage
fi


# Copy the alweb.properties file from S3 to HDFS and load the properties.
aws s3 cp s3://al-edh-dm/src/$1/main/conf/$1.properties /var/tmp/

if [ $? -eq 0 ]
then
	. /var/tmp/$1.properties
else
	echo "Load of $1.properties file failed from S3."
	exit 1
fi


# Copy the error.properties file from S3 to HDFS and load the properties.
aws s3 cp s3://al-edh-dm/src/$1/main/conf/error.properties /var/tmp/

if [ $? -ne 0 ]
then
	echo "Copy of error.properties file failed from S3."
	exit 1
fi


# Business date(Current Date-1) and current timestamp to be used in logs as well as pig, hive scripts.
BUSDATE=`date --date='-'1' day' +"%Y-%m-%d"`
TIMESTAMP=`date +"%Y-%m-%d_%H:%M:%S"`

#Hive Metastore refresh for partitioned tables.
hive -e "msck repair table ${ALWEB_INCOMING_DB}.inc_t_skuitem"
hive -e "msck repair table ${ALWEB_OPERATIONS_DB}.err_dq_t_skuitem"


#Pig Script to be triggered for data checking and cleansing.
pig \
	-param BUSDATE=$BUSDATE \
	-param TIMESTAMP=$TIMESTAMP \
	-param_file /var/tmp/$1.properties \
	-param_file /var/tmp/error.properties \
	-file s3://al-edh-dm/src/$1/main/pig/dq_t_skuitem.pig \
	-useHCatalog


#Hive Metastore refresh for manual partitioned tables.
hive -e "msck repair table ${ALWEB_OPERATIONS_DB}.err_dq_t_skuitem"

