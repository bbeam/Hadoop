################################################################################
#                               General Details                                #
################################################################################
# Organisation: AngiesList                                                     #
# File        : wrapper_br_dim_product .sh                                     #
# Description : This script for applying bussiness rules in product dimension  #
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

	
#Pig Script to be triggered for applying business rules to the dimension product.
pig -param_file /var/tmp/$2 \
	-file s3://al-edh-dm/src/shareddim/main/pig/br_dim_product.pig \
	-useHCatalog
	
if [ $? -eq 0 ]
then
    echo "Bussiness Rules applied successfully"
else
    echo "Error while applying bussiness rules for dimension product"
    exit 1
fi

