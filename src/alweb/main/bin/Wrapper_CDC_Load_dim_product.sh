################################################################################
#                               General Details                                #
################################################################################
# Name        : AngiesList                                                     #
# File        : Wrapper_CDC_Load_dim_product                                   #
# Description : This script performs CDC, adds Surrogate Key and performs SDC. #
# Author      : Varun Rauthan                                                  #
################################################################################

# Copy the alweb.properties file from S3 to HDFS and load the properties.
aws s3 cp s3://al-edh-dm/src/alweb/main/conf/CDC_${TABLE_DIM_PRODUCT}.properties /var/tmp/

if [ $? -ne 0 ]
then
	echo "Copy of CDC_product.properties file failed from S3."
	exit 1
fi


aws s3 cp s3://al-edh-dm/src/common/main/bin/util_generate_pig_SCD2.sh /var/tmp/

if [ $? -ne 0 ]
then
	echo "Copy of util_generate_pig_SCD2.sh file failed from S3."
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
	-file s3://al-edh-dm/src/$1/main/pig/CDC_${TABLE_DIM_PRODUCT}.pig \
	-useHCatalog


#Load data into the dimention table using this HQL.
hive -f s3://al-edh-dm/src/$1/main/hive/Load_${TABLE_DIM_PRODUCT}.hql \
	--hivevar TABLE_DIM_PRODUCT="${TABLE_DIM_PRODUCT}" \
	--hivevar WORK_DB="${WORK_DB}" \
	--hivevar TABLE_DIM_PRODUCT_TMP="${TABLE_DIM_PRODUCT_TMP}" \
	--hivevar ALWEB_GOLD_DB="${ALWEB_GOLD_DB}" \
	--hivevar SK_MAP="${SK_MAP}"

