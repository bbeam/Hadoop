################################################################################
#                               General Details                                #
################################################################################
# Name        : AngiesList                                                     #
# File        : Create_table_alweb.sh                                          #
# Description : This is shell script for all the alweb hive table creation.    # 
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


#Trigger HQL for hive incoming table(INC_t_Sku) creation.
hive -f s3://al-edh-dm/src/$1/main/hive/Create_${TABLE_INC_T_SKU}.hql \
	--hivevar SOURCE_SCHEMA="${SOURCE_SCHEMA}" \
	--hivevar TABLE_INC_T_SKU="${TABLE_INC_T_SKU}" \
	--hivevar S3_LOCATION_INCOMING_DATA="${S3_LOCATION_INCOMING_DATA}" \
	--hivevar SOURCE_ALWEB="${SOURCE_ALWEB}" \
	--hivevar EXTRACTIONTYPE_FULL="${EXTRACTIONTYPE_FULL}" \
	--hivevar FREQUENCY_DAILY="${FREQUENCY_DAILY}"


#Trigger HQL for hive data quality table(DQ_t_Sku) creation.
hive -f s3://al-edh-dm/src/$1/main/hive/Create_${TABLE_DQ_T_SKU}.hql \
	--hivevar WORK_DB="${WORK_DB}" \
	--hivevar TABLE_DQ_T_SKU="${TABLE_DQ_T_SKU}" \
	--hivevar HDFS_LOCATION="${HDFS_LOCATION}" \
	--hivevar SOURCE_ALWEB="${SOURCE_ALWEB}"
	
	
#Trigger HQL for hive Error table(ERR_DQ_t_Sku) creation.
hive -f s3://al-edh-dm/src/$1/main/hive/Create_${TABLE_ERR_DQ_T_SKU}.hql \
	--hivevar TABLE_ERR_DQ_T_SKU="${TABLE_ERR_DQ_T_SKU}" \
	--hivevar S3_LOCATION_OPERATIONS_DATA="${S3_LOCATION_OPERATIONS_DATA}" \
	--hivevar SOURCE_ALWEB="${SOURCE_ALWEB}" \
	--hivevar ALWEB_OPERATIONS_DB="${ALWEB_OPERATIONS_DB}" 


#Trigger HQL for hive table(INC_t_SkuItem) creation.
hive -f s3://al-edh-dm/src/$1/main/hive/Create_${TABLE_INC_T_SKUITEM}.hql \
	--hivevar SOURCE_SCHEMA="${SOURCE_SCHEMA}" \
	--hivevar TABLE_INC_T_SKUITEM="${TABLE_INC_T_SKUITEM}" \
	--hivevar S3_LOCATION_INCOMING_DATA="${S3_LOCATION_INCOMING_DATA}" \
	--hivevar SOURCE_ALWEB="${SOURCE_ALWEB}" \
	--hivevar EXTRACTIONTYPE_FULL="${EXTRACTIONTYPE_FULL}" \
	--hivevar FREQUENCY_DAILY="${FREQUENCY_DAILY}"


#Trigger HQL for hive data quality table(DQ_t_SkuItem) creation.
hive -f s3://al-edh-dm/src/$1/main/hive/Create_${TABLE_DQ_T_SKUITEM}.hql \
	--hivevar WORK_DB="${WORK_DB}" \
	--hivevar TABLE_DQ_T_SKUITEM="${TABLE_DQ_T_SKUITEM}" \
	--hivevar HDFS_LOCATION="${HDFS_LOCATION}"\
	--hivevar SOURCE_ALWEB="${SOURCE_ALWEB}"
	

#Trigger HQL for hive Error table(ERR_DQ_t_SkuItem) creation.
hive -f s3://al-edh-dm/src/$1/main/hive/Create_${TABLE_ERR_DQ_T_SKUITEM}.hql \
	--hivevar ALWEB_GOLD_DB="${ALWEB_GOLD_DB}" \
	--hivevar TABLE_ERR_DQ_T_SKUITEM="${TABLE_ERR_DQ_T_SKUITEM}" \
	--hivevar S3_LOCATION_OPERATIONS_DATA="${S3_LOCATION_OPERATIONS_DATA}" \
	--hivevar SOURCE_ALWEB="${SOURCE_ALWEB}" \
	--hivevar ALWEB_OPERATIONS_DB="${ALWEB_OPERATIONS_DB}" 
	

#Trigger HQL for hive transformation table(TF_dim_product) creation.
hive -f s3://al-edh-dm/src/alweb/main/hive/Create_${TABLE_TF_DIM_PRODUCT}.hql \
#	--hivevar SOURCE_SCHEMA="${SOURCE_SCHEMA}" \
	--hivevar TABLE_TF_DIM_PRODUCT="${TABLE_TF_DIM_PRODUCT}" \
	--hivevar HDFS_LOCATION="${HDFS_LOCATION}" \
	--hivevar SUBJECT_ALWEBMETRICS="${SUBJECT_ALWEBMETRICS}"
	

#Trigger HQL for hive Error table(ERR_TF_dim_product) creation.
hive -f s3://al-edh-dm/src/$1/main/hive/Create_${TABLE_ERR_TF_DIM_PRODUCT}.hql \
	--hivevar TABLE_ERR_TF_DIM_PRODUCT="${TABLE_ERR_TF_DIM_PRODUCT}" \
	--hivevar S3_LOCATION_OPERATIONS_DATA="${S3_LOCATION_OPERATIONS_DATA}" \
	--hivevar SUBJECT_ALWEBMETRICS="${SUBJECT_ALWEBMETRICS}" \
	--hivevar ALWEB_OPERATIONS_DB="${ALWEB_OPERATIONS_DB}" 


#Trigger HQL for temporary dimention table(dim_product) creation, to be used in SDC.
hive -f s3://al-edh-dm/src/$1/main/hive/Create_${TABLE_DIM_PRODUCT_TMP}.hql \
	--hivevar TABLE_DIM_PRODUCT_TMP="${TABLE_DIM_PRODUCT_TMP}" \
	--hivevar HDFS_LOCATION="${HDFS_LOCATION}" \
	--hivevar SUBJECT_ALWEBMETRICS="${SUBJECT_ALWEBMETRICS}" \
	--hivevar WORK_DB="${WORK_DB}" 

	
#Trigger HQL for dimention table(dim_product) creation.
hive -f s3://al-edh-dm/src/$1/main/hive/Create_${TABLE_DIM_PRODUCT}.hql \
	--hivevar TABLE_DIM_PRODUCT="${TABLE_DIM_PRODUCT}" \
	--hivevar S3_LOCATION_GOLD_DATA="${S3_LOCATION_GOLD_DATA}" \
	--hivevar SUBJECT_ALWEBMETRICS="${SUBJECT_ALWEBMETRICS}" \
	--hivevar ALWEB_GOLD_DB="${ALWEB_GOLD_DB}" 
	