################################################################################
#                               General Details                                #
################################################################################
# Name        : AngiesList                                                     #
# File        : create_table_alweb.sh                                          #
# Description : This is shell script for all the alweb hive table creation.    # 
# Author      : Varun Rauthan                                                  #
################################################################################

# Copy the alweb.properties file from S3 to HDFS and load the properties.
aws s3 cp s3://al-edh-dev/src/common/main/conf/al-edh-global.properties /var/tmp/

if [ $? -eq 0 ]
then
	. /var/tmp/al-edh-global.properties
else
	echo "Load of al-edh-global.properties file failed from S3."
	exit 1
fi


##Trigger HQL for hive incoming table(inc_t_sku) creation. 
hive -f s3://al-edh-dev/src/alweb/main/hive/create_inc_t_sku.hql \
	--hivevar SOURCE_SCHEMA="${SOURCE_SCHEMA}" \
	--hivevar S3_LOCATION_INCOMING_DATA="${S3_LOCATION_INCOMING_DATA}" \
	--hivevar SOURCE_ALWEB="${SOURCE_ALWEB}" \
	--hivevar EXTRACTIONTYPE_FULL="${EXTRACTIONTYPE_FULL}" \
	--hivevar FREQUENCY_DAILY="${FREQUENCY_DAILY}" \
	--hivevar ALWEB_INCOMING_DB="${ALWEB_INCOMING_DB}"


##Trigger HQL for hive data quality table(dq_t_sku) creation.
hive -f s3://al-edh-dev/src/alweb/main/hive/create_dq_t_sku.hql \
	--hivevar ALWEB_WORK_DB="${ALWEB_WORK_DB}" \
	--hivevar HDFS_LOCATION="${HDFS_LOCATION}" \
	--hivevar SOURCE_ALWEB="${SOURCE_ALWEB}" \
	--hivevar SOURCE_SCHEMA="${SOURCE_SCHEMA}"
	

##Trigger HQL for hive table(inc_t_skuitem) creation.
hive -f s3://al-edh-dev/src/alweb/main/hive/create_inc_t_skuitem.hql \
	--hivevar SOURCE_SCHEMA="${SOURCE_SCHEMA}" \
	--hivevar S3_LOCATION_INCOMING_DATA="${S3_LOCATION_INCOMING_DATA}" \
	--hivevar SOURCE_ALWEB="${SOURCE_ALWEB}" \
	--hivevar EXTRACTIONTYPE_FULL="${EXTRACTIONTYPE_FULL}" \
	--hivevar FREQUENCY_DAILY="${FREQUENCY_DAILY}" \
	--hivevar ALWEB_INCOMING_DB="${ALWEB_INCOMING_DB}"


##Trigger HQL for hive data quality table(dq_t_skuitem) creation. 
hive -f s3://al-edh-dev/src/alweb/main/hive/create_dq_t_skuitem.hql \
	--hivevar ALWEB_WORK_DB="${ALWEB_WORK_DB}" \
	--hivevar HDFS_LOCATION="${HDFS_LOCATION}" \
	--hivevar SOURCE_ALWEB="${SOURCE_ALWEB}" \
	--hivevar SOURCE_SCHEMA="${SOURCE_SCHEMA}"
	

##Trigger HQL for hive Error table(create_edh_batch_error.hql) creation.
hive -f s3://al-edh-dev/src/common/main/hive/create_edh_batch_error.hql \
	--hivevar COMMON_OPERATIONS_DB="${COMMON_OPERATIONS_DB}" \
	--hivevar S3_LOCATION_OPERATIONS_DATA="${S3_LOCATION_OPERATIONS_DATA}"