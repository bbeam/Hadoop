################################################################################
#                               General Details                                #
################################################################################
# Name        : AngiesList                                                     #
# File        : wrapper_ingestion_sqoop.sh                                            #
# Description : This script performs data quality and cleansing on the data,   #
#				and finally creates a new table with the elements according to #
#				the incoming schema							 				   #
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

# Copy the options file from S3
OPTIONS_FILE_NAME=$(basename $OPTIONS_FILE_PATH)
aws s3 cp $OPTIONS_FILE_PATH /var/tmp/
if [ $? -eq 0 ]
then
		echo "Copy of ${OPTIONS_FILE_NAME} successful"
else
   		echo "Copy of ${OPTIONS_FILE_NAME} failed from S3"
    	exit 1
fi


echo "****************BUSINESS DATE/MONTH*****************"
EDH_BUS_DATE=$3
echo "Business Date : $EDH_BUS_DATE"
EDH_BUS_MONTH=$(date -d "$EDH_BUS_DATE" '+%Y%m')
echo "Business Month :$EDH_BUS_MONTH"

echo "*************SQOOP IMPORT JOB UTILITY*******************"
# deleting the sqoop target location, if it already exists.
echo "executing : aws s3 rm $S3_BUCKET/$DATA_DIRECTORY=$EDH_BUS_DATE --recursive"
aws s3 rm $S3_BUCKET/$DATA_DIRECTORY=$EDH_BUS_DATE --recursive
# replace the extract_date with the edh_bus_date generated in this shell in case of incremental load in the options file.
sed -ie "s/EXTRACT_DATE/$EDH_BUS_DATE/g" /var/tmp/$OPTIONS_FILE_NAME 
echo -e "Sqoop Command running is :\nsqoop import <DB CONNECTION_URL> --target-dir $S3_BUCKET/$DATA_DIRECTORY=$EDH_BUS_DATE --options-file /var/tmp/$OPTIONS_FILE_NAME"
echo -e "Options file used is:\n"
cat /var/tmp/$OPTIONS_FILE_NAME
echo -e "\n"
sqoop import $CONNECTION_URL --target-dir $S3_BUCKET/$DATA_DIRECTORY=$EDH_BUS_DATE --options-file /var/tmp/$OPTIONS_FILE_NAME

if [ $? -eq 0 ]
then
  		echo "Successfully Ingested the data into s3 directory $S3_BUCKET/$DATA_DIRECTORY=$EDH_BUS_DATE"
else
  		echo "Sqoop Ingestion Job is Failed" >&2
  		exit 1
fi

# Hive Metastore refresh for incoming table as it is a manual partitioned table .
hive -e "msck repair table $INCOMING_DB.$TABLE_NAME_INC"

# Hive Metastore refresh status check
if [ $? -eq 0 ]
then
		echo "Hive Metastore refresh successful for incoming table."
else
  		echo "Hive Metastore refresh failed for incoming table." >&2
  		exit 1
fi


echo "*****************DQ PROCESS STARTS*******************"
JAR_FILE_NAME=$(basename $DQ_GENERATOR_JAR_FILE_PATH)
INPUT_JSON_FILE_NAME=$(basename $INPUT_JSON_FILE_PATH)
SCHEMA_FILE_NAME=$(basename $VALIDATION_SCHEMA_FOR_DQ_JSON)
OUTPUT_PIG_FILE_NAME=$(basename $OUTPUT_PIG_FILE_PATH)
JAR_FILE_NAME=$(basename $DQ_GENERATOR_JAR_FILE_PATH)


# JSON Schema copy to hdfs
aws s3 cp $VALIDATION_SCHEMA_FOR_DQ_JSON /var/tmp/
if [ $? -eq 0 ]
then
		echo "JSON schema file $SCHEMA_FILE_NAME for validation copied successfully"
else
		echo "copy of JSON schema file $SCHEMA_FILE_NAME for validation failed"
		exit 1
fi

# JSON copy to hdfs
aws s3 cp $INPUT_JSON_FILE_PATH /var/tmp/
if [ $? -eq 0 ]
then
		echo "JSON input file $INPUT_JSON_FILE_NAME copied successfully"
else
		echo "copy of JSON input file $INPUT_JSON_FILE_NAME failed"
		exit 1
fi

# Copy JAR file to hdfs
aws s3 cp $DQ_GENERATOR_JAR_FILE_PATH /var/tmp/
if [ $? -eq 0 ]
then
		echo "JSON input file $JAR_FILE_NAME copied successfully"
else
		echo " copy of JSON input file $JAR_FILE_NAME failed"
		exit 1
fi

# Run java jar program for to generate DQ pig script
java -cp /var/tmp/EDH_JAVA-1.0-jar-with-dependencies.jar com.angieslist.edh.dq.dqgenerator.DQParser  /var/tmp/$INPUT_JSON_FILE_NAME /var/tmp/$SCHEMA_FILE_NAME

if [ $? -eq 0 ]
then
  		echo "$TABLE_NAME_DQ".pig" for performing DQ activity created successfully"
else
  		echo "$TABLE_NAME_DQ".pig" generation failed"
  		exit 1
fi

# Copy output pig file to s3
aws s3 cp $TABLE_NAME_DQ".pig" $OUTPUT_PIG_FILE_PATH
if [ $? -eq 0 ]
then
  		echo "$TABLE_NAME_DQ".pig" copied to s3"
else
  		echo "copying $TABLE_NAME_DQ".pig" to s3 failed"
  		exit 1
fi

# Pig Script to be triggered for data checking and cleansing.
pig \
	-param EDHBUSDATE=$EDH_BUS_DATE \
	-param_file /var/tmp/$global_file \
	-param_file /var/tmp/$local_file \
	-file $OUTPUT_PIG_FILE_PATH \
	-useHCatalog

if [ $? -eq 0 ]
then
		echo "$OUTPUT_PIG_FILE_NAME executed without any error."
else
		echo "$OUTPUT_PIG_FILE_NAME execution failed."
		exit 1
fi

# Hive Metastore refresh for error table .
hive -e "msck repair table ${OPERATIONS_COMMON_DB}.${ERROR_TABLE_NAME}"

# Hive Metastore refresh status check
if [ $? -eq 0 ]
then
		echo "Hive Metastore refresh successful for error table."
else
  		echo "Hive Metastore refresh failed for error table" >&2
  		exit 1
fi

# Hive Metastore refresh for DQ table .
hive -e "msck repair table ${GOLD_DB}.${TABLE_NAME_DQ}"

# Hive Metastore refresh status check
if [ $? -eq 0 ]
then
		echo "Hive Metastore refresh successful for DQ table."
else
  		echo "Hive Metastore refresh failed for DQ table" >&2
  		exit 1
fi

# Hive script to insert extraction audit record
hive -f $INCOMING_AUDIT_HQL_PATH \
	-hivevar ENTITY_NAME=$SOURCE \
	-hivevar INCOMING_DB=$INCOMING_DB \
	-hivevar INCOMING_TABLE=$TABLE_NAME_INC \
	-hivevar USER_NAME=$USER_NAME \
	-hivevar EDH_BUS_DATE=$EDH_BUS_DATE
		
# Hive Status check
if [ $? -eq 0 ]
then
		echo "$INCOMING_AUDIT_HQL_PATH executed without any error."
else
		echo "$INCOMING_AUDIT_HQL_PATH execution failed."
		exit 1
fi

# Hive script to insert DQ audit record
hive -f $DQ_AUDIT_HQL_PATH \
	-hivevar ENTITY_NAME=$SOURCE \
	-hivevar GOLD_DB=$GOLD_DB \
	-hivevar INCOMING_DB=$INCOMING_DB \
	-hivevar INCOMING_TABLE=$TABLE_NAME_INC \
	-hivevar DQ_TABLE=$TABLE_NAME_DQ \
	-hivevar USER_NAME=$USER_NAME \
	-hivevar EDH_BUS_MONTH=$EDH_BUS_MONTH \
	-hivevar EDH_BUS_DATE=$EDH_BUS_DATE
		
# Hive Status check
if [ $? -eq 0 ]
then
		echo "$DQ_AUDIT_HQL_PATH executed without any error."
else
		echo "$DQ_AUDIT_HQL_PATH execution failed."
		exit 1
fi
