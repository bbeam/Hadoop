################################################################################
#                               General Details                                #
################################################################################
# Name        : AngiesList                                                     #
# File        : wrapper_dq_t_sku.sh                                            #
# Description : This script performs data quality and cleansing on the data,   #
#				and finally creates a new table with the elements according to #
#				the incoming schema							 				   #
# Author      : Varun Rauthan                                                  #
################################################################################

# Copy the al-edh-global.properties file from S3 to HDFS and load the properties.
aws s3 cp s3://al-edh-dm/src/common/main/conf/al-edh-global.properties /var/tmp/

if [ $? -eq 0 ]
then
	. /var/tmp/al-edh-global.properties
else
	echo "Load of al-edh-global.properties file failed from S3."
	exit 1
fi


# Copy the dq_t_sku.properties file from S3 to HDFS and load the properties.
aws s3 cp s3://al-edh-dm/src/alweb/main/conf/dq_t_sku.properties /var/tmp/

if [ $? -eq 0 ]
then
	. /var/tmp/dq_t_sku.properties
else
	echo "Load of dq_t_sku.properties file failed from S3."
	exit 1
fi


# Business date(Current Date-1) and current timestamp to be used in logs as well as pig, hive scripts.
BUSDATE=`date --date='-'1' day' +"%Y-%m-%d"`
TIMESTAMP=`date +"%Y-%m-%d_%H:%M:%S"`

#Hive Metastore refresh for manual partitioned tables.
hive -e "msck repair table ${ALWEB_INCOMING_DB}.inc_t_sku"
hive -e "msck repair table ${COMMON_OPERATIONS_DB}.edh_batch_audit"

	
#Pig Script to be triggered for data checking and cleansing.
pig \
	-param BUSDATE=$BUSDATE \
	-param TIMESTAMP=$TIMESTAMP \
	-param_file /var/tmp/al-edh-global.properties \
	-param_file /var/tmp/dq_t_sku.properties \
	-file s3://al-edh-dm/src/alweb/main/pig/dq_t_sku.pig \
	-useHCatalog


#Hive Metastore refresh for manual partitioned tables.
hive -e "msck repair table ${COMMON_OPERATIONS_DB}.edh_batch_audit"


