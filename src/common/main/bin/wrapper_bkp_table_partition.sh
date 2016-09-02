################################################################################
#   General Details                                                            #
################################################################################
# Organisation: AngiesList                                                     #
# File        : wrapper_bkp_table_partition.sh                                 #
# Description : This process would take a backup copy of the target table.     #
# Author      : Ashoka Reddy                                                   #
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

echo "****************BUSINESS DATE/MONTH*****************"
EDH_BUS_DATE=$3
echo "Business Date : $EDH_BUS_DATE"
EDH_BUS_MONTH=$(date -d "$EDH_BUS_DATE" '+%Y%m')
echo "Business Month :$EDH_BUS_MONTH"

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


hive -f $LOAD_BKP_HIVE_PATH \
    -hivevar BKP_DB_NAME=$BKP_DB_NAME \
    -hivevar BKP_TABLE_NAME=$BKP_TABLE_NAME \
    -hivevar SOURCE_DB_NAME=$SOURCE_DB_NAME \
    -hivevar SOURCE_TABLE_NAME=$SOURCE_TABLE_NAME \
    -hivevar PARTITION_COLUMNS=$PARTITION_COLUMNS \
    -hivevar EDH_BUS_DATE=$EDH_BUS_DATE
    
# Hive Status check
if [ $? -eq 0 ]
then
      echo "$LOAD_BKP_HIVE_PATH executed without any error."
      echo "The backup of $SOURCE_DB_NAME.$SOURCE_TABLE_NAME table completed successfully "
else
    echo "The backup of $SOURCE_DB_NAME.$SOURCE_TABLE_NAME table failed"
exit 1
fi