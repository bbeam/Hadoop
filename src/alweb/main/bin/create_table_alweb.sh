################################################################################
#                               General Details                                #
################################################################################
# Name        : AngiesList                                                     #
# File        : create_table_alweb.sh                                          #
# Description : This is shell script for all the alweb hive table creation.    # 
# Author      : Varun Rauthan                                                  #
################################################################################

# Copy the alweb.properties file from S3 to HDFS and load the properties.
aws s3 cp s3://al-edh-dm/src/common/main/conf/al-edh-global.properties /var/tmp/

if [ $? -eq 0 ]
then
	. /var/tmp/al-edh-global.properties
else
	echo "Load of al-edh-global.properties file failed from S3."
	exit 1
fi


##Trigger HQL for hive incoming table(inc_t_sku) creation. 
hive -f s3://al-edh-dm/src/alweb/main/hive/create_inc_t_sku.hql \
	--hivevar SOURCE_SCHEMA="${SOURCE_SCHEMA}" \
	--hivevar S3_LOCATION_INCOMING_DATA="${S3_LOCATION_INCOMING_DATA}" \
	--hivevar SOURCE_ALWEB="${SOURCE_ALWEB}" \
	--hivevar EXTRACTIONTYPE_FULL="${EXTRACTIONTYPE_FULL}" \
	--hivevar FREQUENCY_DAILY="${FREQUENCY_DAILY}" \
	--hivevar ALWEB_INCOMING_DB="${ALWEB_INCOMING_DB}"


##Trigger HQL for hive data quality table(dq_t_sku) creation.
hive -f s3://al-edh-dm/src/alweb/main/hive/create_dq_t_sku.hql \
	--hivevar ALWEB_WORK_DB="${ALWEB_WORK_DB}" \
	--hivevar HDFS_LOCATION="${HDFS_LOCATION}" \
	--hivevar SOURCE_ALWEB="${SOURCE_ALWEB}" \
	--hivevar SOURCE_SCHEMA="${SOURCE_SCHEMA}"
	

##Trigger HQL for hive table(inc_t_skuitem) creation.
hive -f s3://al-edh-dm/src/alweb/main/hive/create_inc_t_skuitem.hql \
	--hivevar SOURCE_SCHEMA="${SOURCE_SCHEMA}" \
	--hivevar S3_LOCATION_INCOMING_DATA="${S3_LOCATION_INCOMING_DATA}" \
	--hivevar SOURCE_ALWEB="${SOURCE_ALWEB}" \
	--hivevar EXTRACTIONTYPE_FULL="${EXTRACTIONTYPE_FULL}" \
	--hivevar FREQUENCY_DAILY="${FREQUENCY_DAILY}" \
	--hivevar ALWEB_INCOMING_DB="${ALWEB_INCOMING_DB}"


##Trigger HQL for hive data quality table(dq_t_skuitem) creation. 
hive -f s3://al-edh-dm/src/alweb/main/hive/create_dq_t_skuitem.hql \
	--hivevar ALWEB_WORK_DB="${ALWEB_WORK_DB}" \
	--hivevar HDFS_LOCATION="${HDFS_LOCATION}" \
	--hivevar SOURCE_ALWEB="${SOURCE_ALWEB}" \
	--hivevar SOURCE_SCHEMA="${SOURCE_SCHEMA}"
	

#Trigger HQL for hive Error table(err_dq_t_skuitem) creation.
hive -f s3://al-edh-dm/src/alweb/main/hive/create_err_dq_t_skuitem.hql \
	--hivevar ALWEB_GOLD_DB="${ALWEB_GOLD_DB}" \
	--hivevar S3_LOCATION_OPERATIONS_DATA="${S3_LOCATION_OPERATIONS_DATA}" \
	--hivevar SOURCE_ALWEB="${SOURCE_ALWEB}" \
	--hivevar ALWEB_OPERATIONS_DB="${ALWEB_OPERATIONS_DB}" 
	

#Trigger HQL for hive transformation table(tf_dim_product) creation.
hive -f s3://al-edh-dm/src/alweb/main/hive/create_tf_dim_product.hql \
#	--hivevar SOURCE_SCHEMA="${SOURCE_SCHEMA}" \
	--hivevar HDFS_LOCATION="${HDFS_LOCATION}" \
	--hivevar SUBJECT_ALWEBMETRICS="${SUBJECT_ALWEBMETRICS}"
	

#Trigger HQL for hive Error table(err_tf_dim_product) creation.
hive -f s3://al-edh-dm/src/alweb/main/hive/create_err_tf_dim_product.hql \
	--hivevar S3_LOCATION_OPERATIONS_DATA="${S3_LOCATION_OPERATIONS_DATA}" \
	--hivevar SUBJECT_ALWEBMETRICS="${SUBJECT_ALWEBMETRICS}" \
	--hivevar ALWEB_OPERATIONS_DB="${ALWEB_OPERATIONS_DB}" 


#Trigger HQL for temporary dimention table(dim_product_tmp) creation, to be used in SDC.
hive -f s3://al-edh-dm/src/alweb/main/hive/create_dim_product_tmp.hql \
	--hivevar HDFS_LOCATION="${HDFS_LOCATION}" \
	--hivevar SUBJECT_ALWEBMETRICS="${SUBJECT_ALWEBMETRICS}" \
	--hivevar WORK_DB="${WORK_DB}" 

	
#Trigger HQL for dimention table(dim_product) creation.
hive -f s3://al-edh-dm/src/alweb/main/hive/create_dim_product.hql \
	--hivevar S3_LOCATION_GOLD_DATA="${S3_LOCATION_GOLD_DATA}" \
	--hivevar SUBJECT_ALWEBMETRICS="${SUBJECT_ALWEBMETRICS}" \
	--hivevar ALWEB_GOLD_DB="${ALWEB_GOLD_DB}" 
	
