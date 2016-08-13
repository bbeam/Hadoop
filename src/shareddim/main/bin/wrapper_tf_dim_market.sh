################################################################################
#                               General Details                                #
################################################################################
# Name        : AngiesList                                                     #
# File        : wrapper_tf_dim_market.sh                                       #
# Description : This Script performs the SCD process for dim market table      #
################################################################################


# Usage descriptor for invalid input arguments to the wrapper
Show_Usage()
{
    	echo "invalid arguments please pass exactly three arguments "
    	echo "Usage: "$0" <global properties file with path> <path of local properties file with path> <business date(YYYY-MM-DD)>"
    	exit 1
}

if [ $# -ne 3 ]
then
    	Show_Usage
fi

# Copy the al-edh-global.properties file from S3 to local and load the properties.
global_file=`echo "$(basename $1)"`
aws s3 cp $1 /var/tmp/

if [ $? -eq 0 ]
then
        echo "${global_file} file copied successfully from "$1" to "/var/tmp/" "
else
   		echo "Copy of ${global_file} failed from $1."
    	exit 1
fi

. /var/tmp/${global_file}
if [ $? -eq 0 ]
then
        echo "Load of ${global_file} successful"
else
		echo "Load of ${global_file} failed"
		exit 1
fi

# Copy the local properties file from S3 to local and load the properties.
local_file=`echo "$(basename $2)"`
if [ $? -eq 0 ]
then
		aws s3 cp $2 /var/tmp/
else
   		echo "Copy of ${local_file} failed from $1."
    	exit 1
fi

. /var/tmp/${local_file}
if [ $? -eq 0 ]
then
        echo "Load of ${local_file} successful"
else
		echo "Load of ${local_file} failed"
		exit 1
fi

echo "****************BUSINESS DATE/MONTH*****************"
EDH_BUS_DATE=$3
echo "Business Date : $EDH_BUS_DATE"
EDH_BUS_MONTH=$(date -d "$EDH_BUS_DATE" '+%Y%m')
echo "Business Month :$EDH_BUS_MONTH"


echo "*****************CDC PROCESS STARTS*******************"
TF_PIG_FILE_NAME=$(basename $TF_PIG_FILE_PATH)

# Pig Script to be triggered for data checking and cleansing.
pig  \
	-param_file /var/tmp/$global_file \
	-param_file /var/tmp/$local_file \
	-useHCatalog
	-file $TF_PIG_FILE_PATH

if [ $? -eq 0 ]
then
		echo "$TF_PIG_FILE_NAME executed without any error."
else
		echo "$TF_PIG_FILE_NAME execution failed."
		exit 1
fi

# Hive Metastore refresh for error table .
hive -e "msck repair table ${COMMON_OPERATIONS_DB}.${AUDIT_TABLE_NAME}"

# Hive Metastore refresh status check
if [ $? -eq 0 ]
then
		echo "Hive Metastore refresh successful for error table."
else
  		echo "Hive Metastore refresh failed for error table" >&2
  		exit 1
fi

# Hive Metastore refresh for TF table .
hive -e "msck repair table ${WORK_SHARED_DIM_DB}.tf_dim_market"

# Hive Metastore refresh status check
if [ $? -eq 0 ]
then
		echo "Hive Metastore refresh successful for TF table ${WORK_SHARED_DIM_DB}.tf_dim_market"
else
  		echo "Hive Metastore refresh failed for TF table ${WORK_SHARED_DIM_DB}.tf_dim_market" >&2
  		exit 1
fi

# Hive script to insert extraction audit record
hive -f $TF_AUDIT_HQL_PATH \
	-hivevar ENTITY_NAME=$SUBJECT_SHAREDDIM \
	-hivevar OPERATIONS_COMMON_DB=$OPERATIONS_COMMON_DB \
	-hivevar AUDIT_TABLE_NAME=$AUDIT_TABLE_NAME \
	-hivevar USER_NAME=$USER_NAME \
	-hivevar EDH_BUS_DATE=$EDH_BUS_DATE \
	-hivevar TF_DB=$GOLD_SHARED_DIM_DB \
	-hivevar TF_TABLE=tf_dim_market

# Hive Status check
if [ $? -eq 0 ]
then
		echo "$TF_AUDIT_HQL_PATH  executed without any error."
else
		echo "$TF_AUDIT_HQL_PATH  execution failed."
		exit 1
fi
