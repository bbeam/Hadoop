################################################################################
#                               General Details                                #
################################################################################
# Name        : AngiesList                                                     #
# File        : Wrapper_Load_bkp_dim_product.sh                                #
# Description : This script performs data quality and cleansing on the data,   #
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
	echo "Load of $1.properties file failed from S3."
	exit 1
fi
aws s3 cp s3://al-edh-dm/src/$1/main/conf/$2.properties /var/tmp/

if [ $? -eq 0 ]
then
	. /var/tmp/$2.properties
else
	echo "Load of $2.properties file failed from S3."
	exit 1
fi



# Current Date to be used in logs as well as pig, hive scripts.
DATE=`date +%d-%m-%Y`


#Trigger HQL for load  into back up table (BKP_DIM_PRODUCT) from DIM_PRODUCT table .
hive -f s3://al-edh-dm/src/$1/main/hive/Load_${TABLE_BKP_DIM_PRODUCT}.hql \
	--hivevar ALWEBMETRICS_OPERATIONS="${ALWEBMETRICS_OPERATIONS}" \
	--hivevar ALWEB_GOLD_DB="${ALWEB_GOLD_DB}" \
	--hivevar TABLE_DIM_PRODUCT="${TABLE_DIM_PRODUCT}" \
	--hivevar TABLE_BKP_DIM_PRODUCT="${TABLE_BKP_DIM_PRODUCT}"
