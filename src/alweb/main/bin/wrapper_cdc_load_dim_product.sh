################################################################################
#                               General Details                                #
################################################################################
# Name        : AngiesList                                                     #
# File        : wrapper_cdc_load_dim_product.sh                                #
# Description : This script performs CDC, adds Surrogate Key and performs SDC. #
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

# Copy the alweb.properties file from S3 to HDFS and load the properties.
aws s3 cp s3://al-edh-dm/src/alweb/main/conf/cdc_dim_product.properties /var/tmp/

if [ $? -ne 0 ]
then
	echo "Copy of cdc_dim_product.properties file failed from S3."
	exit 1
fi


aws s3 cp s3://al-edh-dm/src/common/main/lib/datafu-1.2.0.jar /var/tmp/

if [ $? -ne 0 ]
then
	echo "Copy of datafu-1.2.0.jar failed from S3."
	exit 1
fi


#Pig Script to be triggered for data checking and cleansing.
pig \
	-file s3://al-edh-dm/src/$1/main/pig/cdc_dim_product.pig \
	-useHCatalog


#Load data into the dimention table using this HQL.
hive -f s3://al-edh-dm/src/$1/main/hive/load_dim_product.hql \
	--hivevar WORK_DB="${WORK_DB}" \
	--hivevar ALWEB_GOLD_DB="${ALWEB_GOLD_DB}" \
	--hivevar SK_MAP="${SK_MAP}"

