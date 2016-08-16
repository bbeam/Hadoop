#!/bin/bash
#####################################################################################
#                                                                                   #
# Name         :- s3_to_linux_jdbc_jars_copy.sh                                          #
#                                                                                   #
# Description  :- This script will perform copy operation                           #
#                 from s3 to master node    .                                       #
#                                                                                   #
# Author       :- GAURAV MAHESHWARI                                                 #
#                                                                                   #
# Company      :- Datametica Solutions                                              #
#####################################################################################
a=`sudo aws s3 cp s3://al-edh-uat/src/common/main/lib/RedshiftJDBC4-1.1.10.1010.jar /usr/lib/sqoop/lib/`
b=`sudo aws s3 cp s3://al-edh-uat/src/common/main/lib/mysql-connector-java-bin.jar /usr/lib/sqoop/lib/`
c=`sudo aws s3 cp s3://al-edh-uat/src/common/main/lib/sqljdbc4.jar /usr/lib/sqoop/lib/`
