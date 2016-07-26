################################################################################
#                               General Details                                #
################################################################################
# Name        : AngiesList                                                     #
# File        : Create_table_alweb_Store_Page_Loaded.sh                                          #
# Description : This is shell script for all the alweb hive table creation.    # 
# Author      : Abhinav Mehar                                                  #
################################################################################









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


#Trigger HQL for hive incoming table(INC_Store_Page_Loaded) creation.
hive -f s3://al-edh-dm/src/$1/main/hive/Create_${TABLE_INC_STORE_PAGE_LOADED}.hql \
	--hivevar ALWEB_INCOMING_DB="${ALWEB_INCOMING_DB}" \
	--hivevar SOURCE_SCHEMA_SEGMENT="${SOURCE_SCHEMA_SEGMENT}" \
	--hivevar TABLE_INC_STORE_PAGE_LOADED="${TABLE_INC_STORE_PAGE_LOADED}" \
	--hivevar S3_LOCATION_INCOMING_DATA="${S3_LOCATION_INCOMING_DATA}" \
	--hivevar SOURCE_ALWEB="${SOURCE_ALWEB}" \
	--hivevar EXTRACTIONTYPE_INCREMENTAL="${EXTRACTIONTYPE_INCREMENTAL}" \
	--hivevar FREQUENCY_DAILY="${FREQUENCY_DAILY}"


#Trigger HQL for hive data quality table(DQ_Store_Page_Loaded) creation.
hive -f s3://al-edh-dm/src/$1/main/hive/Create_${TABLE_DQ_STORE_PAGE_LOADED}.hql \
	--hivevar WORK_DB="${WORK_DB}" \
	--hivevar TABLE_DQ_STORE_PAGE_LOADED="${TABLE_DQ_STORE_PAGE_LOADED}" \
	--hivevar HDFS_LOCATION="${HDFS_LOCATION}" \
	--hivevar SOURCE_ALWEB="${SOURCE_ALWEB}"
	
	
#Trigger HQL for hive Error table(ERR_DQ_Store_Page_Loaded) creation.
hive -f s3://al-edh-dm/src/$1/main/hive/Create_${TABLE_ERR_DQ_STORE_PAGE_LOADED}.hql \
	--hivevar WORK_DB="${WORK_DB}" \
	--hivevar TABLE_ERR_DQ_STORE_PAGE_LOADED="${TABLE_ERR_DQ_STORE_PAGE_LOADED}" \
	--hivevar S3_LOCATION_OPERATIONS_DATA="${S3_LOCATION_OPERATIONS_DATA}" \
	--hivevar SOURCE_ALWEB="${SOURCE_ALWEB}" \
	--hivevar ALWEB_OPERATIONS_DB="${ALWEB_OPERATIONS_DB}" 


	

##Trigger HQL for hive transformation table(TF_Store_Page_Loaded) creation.
#hive -f s3://al-edh-dm/src/alweb/main/hive/Create_${TABLE_TF_STORE_PAGE_LOADED}.hql \
#	--hivevar SOURCE_SCHEMA_SEGMENT="${SOURCE_SCHEMA_SEGMENT}" \
#	--hivevar WORK_DB="${WORK_DB}" \
#	--hivevar TABLE_TF_STORE_PAGE_LOADED="${TABLE_TF_STORE_PAGE_LOADED}" \
#	--hivevar HDFS_LOCATION="${HDFS_LOCATION}" \
#	--hivevar SUBJECT_ALWEBMETRICS="${SUBJECT_ALWEBMETRICS}"
#	
#	
#
#
##Trigger HQL for hive Error table(ERR_TF_Store_Page_Loaded) creation.
#hive -f s3://al-edh-dm/src/$1/main/hive/Create_${TABLE_ERR_TF_STORE_PAGE_LOADED}.hql \
#	--hivevar TABLE_ERR_TF_STORE_PAGE_LOADED="${TABLE_ERR_TF_STORE_PAGE_LOADED}" \
#	--hivevar S3_LOCATION_OPERATIONS_DATA="${S3_LOCATION_OPERATIONS_DATA}" \
#	--hivevar SUBJECT_ALWEBMETRICS="${SUBJECT_ALWEBMETRICS}" \
#	--hivevar ALWEB_OPERATIONS_DB="${ALWEB_OPERATIONS_DB}" 
#

	#Trigger HQL for hive Error table(ERR_TF_Store_Page_Loaded) creation.
hive -f s3://al-edh-dm/src/$1/main/hive/Create_${TABLE_BR_STORE_PAGE_LOADED}.hql \
	--hivevar WORK_DB="${WORK_DB}" \
	--hivevar TABLE_BR_STORE_PAGE_LOADED="${TABLE_BR_STORE_PAGE_LOADED}" \
	--hivevar SUBJECT_ALWEBMETRICS="${SUBJECT_ALWEBMETRICS}" \
	--hivevar HDFS_LOCATION="${HDFS_LOCATION}" \

