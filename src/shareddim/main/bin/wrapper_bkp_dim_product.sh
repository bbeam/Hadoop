################################################################################
#                               General Details                                #
################################################################################
# Organisation: AngiesList                                                     #
# File        : wrapper_bkp_dim_product.sh                                     #
# Description : This process would take a backup copy of the target table.     #
# Author      : Ashoka Reddy                                                   #
################################################################################

# Check for the input parameter to the shell script. The script only expects  environment as first parameter and global properties as second parameter.
Show_Usage()
{
    echo "invalid arguments please pass two arguments "
    echo "Usage: $0 <environment> <global properties file>"
    echo "Ex: $0 al-edh-dev al-edh-global.properties"
    exit 1
}


#######################################################
# Function to be used to copy data from s3 to Local
#
# Input
#    source_path - s3 path from where files to be copied
#    destination_path - local  path where files are stored


function fn_copy_to_local()
{
    source_path=$1
    destination_path=$2
	
	aws s3 cp $source_path $destination_path
 	
	put_var=$?
    if [ ${put_var} -eq 0 ]
    then
        echo "Data copied successfully to the location: ${destination_path}"
    else
        echo "Unable to copy data to the location ${destination_path}"
    	exit 100;
    fi     
}




if [ $# -ne 2 ] 
then
    Show_Usage
fi


# Copy the al-edh-global.properties file from S3 to local and load the properties.
fn_copy_to_local s3://$1/src/common/main/conf/$2 /var/tmp/

if [ $? -eq 0 ]
then
    . /var/tmp/$2
else
    echo "Load of $2 file failed from s3://$1/src/common/main/conf/$2."
    exit 1
fi


#Trigger HQL for load  into back up table (BKP_DIM_PRODUCT) from DIM_PRODUCT table .
hive -f s3://$1/src/shareddim/main/hive/load_bkp_dim_product.hql \
    --hivevar SHAREDDIM_OPERATIONS_DB="${SHAREDDIM_OPERATIONS_DB}" \
    --hivevar SHAREDDIM_GOLD_DB="${SHAREDDIM_GOLD_DB}" 

if [ $? -eq 0 ]
then
    echo "The target table backup completed successfully "
else
    echo "Error while taking backup of target table "
    exit 1
fi