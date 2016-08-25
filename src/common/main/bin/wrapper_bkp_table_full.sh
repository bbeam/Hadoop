################################################################################
#   General Details                                                            #
################################################################################
# Organisation: AngiesList                                                     #
# File        : wrapper_bkp_table_full.sh                                      #
# Description : This process would take a backup copy of the target table.     #
# Author      : Ashoka Reddy                                                   #
################################################################################

# Check for the input parameters to the shell script. The script only expects global properties as first parameter and local parameter as second parameter.
Show_Usage()
{
echo "invalid arguments please pass two arguments "
echo "Usage: "$0" <global properties file with path> <path of local properties file with path>"
exit 1
}

if [ $# -ne 2 ] 
then
Show_Usage
fi


# Copy the al-edh-global.properties file from S3 to local and load the properties.
aws s3 cp $1 /var/tmp/

if [ $? -eq 0 ]
then
    echo "Global properties file copied successfully from "$1" to "/var/tmp/" "
global_file=`echo "$(basename $1)"`
    . /var/tmp/${global_file}
    echo "${global_file} loaded successfully "
else
echo "Load of ${global_file} failed from $1."
exit 1
fi

# Copy the local properties file from S3 to local and load the properties.
aws s3 cp $2 /var/tmp/

if [ $? -eq 0 ]
then
    echo "Local properties copied successfully from "$2" to "/var/tmp/" "
    local_file=`echo "$(basename $2)"`
. /var/tmp/${local_file}
    echo "${local_file} loded successfully from $2"
else
echo "Load of "${local_file}" failed from "
exit 1
fi

# Hive script to load backup of target table 

hive -f $LOAD_BKP_HIVE_PATH \
    -hivevar BKP_DB_NAME=$BKP_DB_NAME \
    -hivevar BKP_TABLE_NAME=$BKP_TABLE_NAME \
    -hivevar SOURCE_DB_NAME=$SOURCE_DB_NAME \
    -hivevar SOURCE_TABLE_NAME=$SOURCE_TABLE_NAME 
# Hive Status check
if [ $? -eq 0 ]
then
      echo "$LOAD_BKP_HIVE_PATH executed without any error."
      echo "The backup of $SOURCE_DB_NAME.$SOURCE_TABLE_NAME table completed successfully "
else
    echo "The backup of $SOURCE_DB_NAME.$SOURCE_TABLE_NAME table failed"
exit 1
fi