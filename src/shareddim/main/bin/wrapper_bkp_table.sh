################################################################################
#                               General Details                                #
################################################################################
# Organisation: AngiesList                                                     #
# File        : wrapper_bkp_table.sh                                           #
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
if [ $TARGET_TABLE_TYPE == "DIMENSION" ]
then
    hive -f $LOAD_BKP_DIMENSION_TABLE_HQL_PATH \
    -hivevar BKP_DB_NAME=$OPERATIONS_SHARED_DIM_DB \
    -hivevar BKP_TABLE_NAME=$BKP_TABLE_NAME \
    -hivevar SOURCE_DB_NAME=$GOLD_DIM_DB \
    -hivevar SOURCE_TABLE_NAME=$TRGT_DIM_TABLE_NAME 
    # Hive Status check
    if [ $? -eq 0 ]
    then
        echo "$LOAD_BKP_DIMENSION_TABLE_HQL_PATH executed without any error."
    else
        echo "$LOAD_BKP_DIMENSION_TABLE_HQL_PATH execution failed."
        exit 1
    fi
elif [ $TARGET_TABLE_TYPE == "FACT" ]
then
    hive -f $LOAD_BKP_FACT_TABLE_HQL_PATH \
    -hivevar BKP_DB_NAME=$OPERATIONS_AL_WEBMETRICS_DB \
    -hivevar BKP_TABLE_NAME=$BKP_TABLE_NAME \
    -hivevar SOURCE_DB_NAME=$GOLD_AL_WEBMETRICS_DB \
    -hivevar SOURCE_TABLE_NAME=$TRGT_FACT_TABLE_NAME \
    -hivevar PARTITION_COLUMNS=$FACT_TABLE_PARTITION_COLUMNS 
    
    # Hive Status check
    if [ $? -eq 0 ]
    then
        echo "$LOAD_BKP_FACT_TABLE_HQL_PATH executed without any error."
    else
        echo "$LOAD_BKP_FACT_TABLE_HQL_PATH execution failed."
        exit 1
    fi
fi
