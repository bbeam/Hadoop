################################################################################
#                               General Details                                #
################################################################################
# Name        : AngiesList                                                     #
# File        : wrapper_tf_dim_product.sh                                      #
# Description : This script performs data transformation, performs             #
#				transformation cleaning										   #
# Author      : Varun Rauthan                                                  #
################################################################################

# Check for the input parameter to the shell script. The script only expects source name as the first parameter.
Show_Usage()
{
	echo "invalid argument please pass only one argument "
    echo "Usage: $0 <Source Name> <Subject Area>"
    echo "Ex: $0 alweb alwebmetrics"
    exit 1
}

if [ $# -ne 2 ] 
then
    Show_Usage
fi


# Copy the alweb.properties file from S3 to HDFS and load the properties.
aws s3 cp s3://al-edh-dm/src/$1/main/conf/$1.properties /var/tmp/

if [ $? -eq 0 ]
then
	. /var/tmp/$1.properties
else
	echo "Copy of $1.properties file failed from S3."
	exit 1
fi


aws s3 cp s3://al-edh-dm/src/$1/main/conf/$2.properties /var/tmp/

if [ $? -eq 0 ]
then
	. /var/tmp/$2.properties
else
	echo "Copy of $2.properties file failed from S3."
	exit 1
fi


# Copy the error.properties file from S3 to HDFS and load the properties.
aws s3 cp s3://al-edh-dm/src/$1/main/conf/error.properties /var/tmp/

if [ $? -ne 0 ]
then
	echo "Copy of error.properties file failed from S3."
	exit 1
fi


# Current Date to be used in logs as well as pig, hive scripts.
BUSDATE=`date --date='-'1' day' +"%Y-%m-%d"`

	
#Pig Script to be triggered for data checking and cleansing.
pig \
	-param BUSDATE=$BUSDATE \
	-param_file /var/tmp/$1.properties \
	-param_file /var/tmp/$2.properties \
	-param_file /var/tmp/error.properties \
	-file s3://al-edh-dm/src/$1/main/pig/tf_dim_product.pig \
	-useHCatalog


#Hive Metastore refresh for manual partitioned tables.
hive -e "msck repair table ${ALWEB_OPERATIONS_DB}.err_tf_dim_product"


