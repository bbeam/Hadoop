################################################################################
#                               General Details                                #
################################################################################
# Name        : AngiesList                                                     #
# File        : bkp_dim_product.sh                                             #
# Description : This process would take a backup copy of the target table.     #
# Author      : Ashoka Reddy                                                   #
################################################################################

# Check for the input parameter to the shell script. The script only expects source name as the first parameter.
Show_Usage()
{
    echo "invalid argument please pass only one argument "
    echo "Usage: $0 <global properties file>"
    echo "Ex: $0 al-edh-global"
    exit 1
}

if [ $# -ne 1 ] 
then
    Show_Usage
fi


# Copy the alweb.properties file from S3 to HDFS and load the properties.
aws s3 cp s3://al-edh-dm/src/common/main/conf/$1.properties /var/tmp/

if [ $? -eq 0 ]
then
    . /var/tmp/$1.properties
else
    echo "Load of $1.properties file failed from S3."
    exit 1
fi

#Create a backup table for dimension product target table 
hive -f s3://al-edh-dm/src/shareddim/main/hive/create_bkp_dim_product.hql \
    --hivevar SHAREDDIM_OPERATIONS_DB="${SHAREDDIM_OPERATIONS_DB}" \
    --hivevar S3_LOCATION_OPERATIONS_DATA="${S3_LOCATION_OPERATIONS_DATA}" \
    --hivevar SUBJECT_SHAREDDIM="${SUBJECT_SHAREDDIM}"


#Trigger HQL for load  into back up table (BKP_DIM_PRODUCT) from DIM_PRODUCT table .
hive -f s3://al-edh-dm/src/shareddim/main/hive/load_bkp_dim_product.hql \
    --hivevar SHAREDDIM_OPERATIONS_DB="${SHAREDDIM_OPERATIONS_DB}" \
    --hivevar SHAREDDIM_GOLD_DB="${SHAREDDIM_GOLD_DB}" 
