#!/bin/bash
###############################################################################
#                               General Details                               #
###############################################################################
#                                                                             #
# Name                                                                        #
#     : scd_script_generator.sh                                               #
#                                                                             #
# Description                                                                 #
#     : This script will generate corresponding scd pig and hive script for a #
#       given dimension table as the input.                                   #
#                                                                             #
# Author                                                                      #
#     : Abhinav Mehar                                                         #
#                                                                             #
# Note                                                                        #
#     : 1) key column,non key column,non key date column,timestamp column.                                         #
#       2) If function argument ends with * then it means that required       #
#          argument.                                                          #
#       3) If function argument ends with ? then it means that optional       #
#          argument.                                                          #
#                                                                             #
###############################################################################
#                     Global Environment Base Properties                      #
###############################################################################

############################################################################################################################################################
##Setup SCRIPT_HOME##
############################################################################################################################################################

USER_PATH="${BASH_SOURCE[0]}";
SCRIPT_HOME=`dirname "$USER_PATH"`

############################################################################################################################################################
##Initialize file path based on the table name passed as input parameter##
############################################################################################################################################################

table_name=$1

properties_file=`echo "$SCRIPT_HOME"/../resources/"$table_name".properties`
key_file=`echo "$SCRIPT_HOME"/../tmp/"$table_name"_key.txt`
non_key_file=`echo "$SCRIPT_HOME"/../tmp/"$table_name"_non_key.txt`
non_key_dt_file=`echo "$SCRIPT_HOME"/../tmp/"$table_name"_non_key_dt.txt`
non_key_non_dt_file=`echo "$SCRIPT_HOME"/../tmp/"$table_name"_non_key_non_dt.txt`
pig_file=`echo "$SCRIPT_HOME"/../pig/"$table_name".pig`
hive_file=`echo "$SCRIPT_HOME"/../hive/"$table_name".hql`

############################################################################################################################################################
##Fetch information from the table.properties file and create files and variables to be used in the script##
############################################################################################################################################################

cat "$properties_file"|grep ^key|cut -d":" -f2>"$key_file"
cat "$properties_file"|grep ^non-key|cut -d":" -f2>"$non_key_file"
cat "$properties_file"|grep ^non-key-datetime|cut -d":" -f2>"$non_key_dt_file"
cat "$properties_file"|grep ^non-key|grep -v non-key-datetime|cut -d":" -f2>"$non_key_non_dt_file"
partition_col=`cat "$properties_file"|grep ^partition-col|cut -d":" -f2|tr -d ' '`
partition_logic=`cat "$properties_file"|grep ^partition-logic|cut -d":" -f2|tr -d ' '`
timestamp_col=`cat "$properties_file"|grep ^timestamp-col|cut -d":" -f2|tr -d ' '`
source_tab=`cat "$properties_file"|grep ^source|cut -d":" -f2|tr -d ' '`
target_tab=`cat "$properties_file"|grep ^target|cut -d":" -f2|tr -d ' '`
max_sk_tab=`cat "$properties_file"|grep ^max-sk-tab|cut -d":" -f2|tr -d ' '`
hive_db=`cat "$properties_file"|grep ^hive-db|cut -d":" -f2|tr -d ' '`
target_wrk_tab=`cat "$properties_file"|grep ^wrk-tab|cut -d":" -f2|tr -d ' '`
##target_wrk_loc=`cat "$properties_file"|grep ^wrk-loc|cut -d":" -f2`
loading_logic=`cat "$properties_file"|grep ^loading-logic|cut -d":" -f2|tr -d ' '`

############################################################################################################################################################
##Exception Handling for vales entered in corresponding Properties files##
############################################################################################################################################################

if [ `cat "$key_file"|wc -l` == 0 ]
then
   echo "ERROR:Please enter key column information in $properties_file"
   exit 1
fi

if [ `cat "$non_key_file"|wc -l` == 0 ]
then
   echo "ERROR:Please enter non-key information in $properties_file"
      exit 1
fi

if [  "$partition_col" == ""  ] || [ "$partition_col" == "non" ]
then
   echo "ERROR:Please enter partition-col information in $properties_file.This field could not be non"
      exit 1
fi

if [  "$partition_logic" == "" ]
then
   echo "ERROR:Please enter partition-logic information in $properties_file.If nothing then keep it non"
      exit 1
fi



if [  "$timestamp_col" == "" ]
then
   echo "ERROR:Please enter timestamp-col information in ""$properties_file,If nothing then keep it non"
      exit 1
fi



if  [  "$loading_logic" == "incremental" ]
then 
    if  [  "$timestamp_col" == "non"  ]
	   then 
          echo "ERROR:The timestamp-col value couldn't be non as loading_logic is incremental .Please enter  a valid timestamp-col information in $properties_file or change the loading_logic to complete"
       exit 1
	fi
elif [  "$loading_logic" != "complete" ]
then 
   echo "ERROR:loading_logic could be either incremental or complete"
   exit 1
fi

if [  "$source_tab" == "" ]
then
   echo "ERROR:Please enter source information in $properties_file"
      exit 1
elif [[  "$source_tab" != *"."* ]]
then
     echo "ERROR:Please enter a valid entry for source information in $properties_file.It should be <db_name>.<table.name>"
	 exit 1
fi


if [  "$target_tab" == "" ]
then
   echo "ERROR:Please enter target information in $properties_file"
      exit 1
elif [[  "$source_tab" != *"."* ]]
then
     echo "ERROR:Please enter a valid entry for target information in $properties_file.It should be <db_name>.<table.name>"
	 exit 1
fi


if [  "$max_sk_tab" == "" ]
then
   echo "ERROR:Please enter max-sk-tab information in $properties_file"
      exit 1
elif [[  "$source_tab" != *"."* ]]
then
     echo "ERROR:Please enter a valid entry for max-sk-tab information in $properties_file.It should be <db_name>.<table.name>"
	 exit 1
fi

############################################################################################################################################################
##SCD Logic Starts   ##
############################################################################################################################################################

echo "--##############################################################################"$'\n'>"$pig_file"
echo "--#                              General Details                               #"$'\n'>>"$pig_file"
echo "--##############################################################################"$'\n'>>"$pig_file"
echo "--#                                                                            #"$'\n'>>"$pig_file"
echo "--# Name                                                                       #"$'\n'>>"$pig_file"
echo "--#     : $table_name                                                          #"$'\n'>>"$pig_file"
echo "--# File                                                                       #"$'\n'>>"$pig_file"
echo "--#     : $table_name.pig                                                      #"$'\n'>>"$pig_file"
echo "--# Description                                                                #"$'\n'>>"$pig_file"
echo "--#     :                                                                      #"$'\n'>>"$pig_file"
echo "--#                                                                            #"$'\n'>>"$pig_file"
echo "--#                                                                            #"$'\n'>>"$pig_file"
echo "--#                                                                            #"$'\n'>>"$pig_file"
echo "--# Author                                                                     #"$'\n'>>"$pig_file"
echo "--#     : Angieslist-Dev-Team                                                  #"$'\n'>>"$pig_file"
echo "--#                                                                            #"$'\n'>>"$pig_file"
echo "--##############################################################################"$'\n'>>"$pig_file"
echo "--#                                   Load                                     #"$'\n'>>"$pig_file"
echo "--##############################################################################"$'\n'>>"$pig_file"

############################################################################################################################################################
echo "/*-----Register datafu jar for md5 calulation------*/"$'\n'>>"$pig_file"
############################################################################################################################################################


echo "register /home/hadoop/jars/datafu-1.2.0.jar;"$'\n'>>"$pig_file"

echo "define MD5 datafu.pig.hash.MD5();"$'\n'>>"$pig_file"

############################################################################################################################################################
echo "/*-----Pull base and scd tables from into pig using HCatalog------*/"$'\n'>>"$pig_file"
############################################################################################################################################################
#####START##############

echo "load_source = LOAD  '$source_tab' USING org.apache.hive.hcatalog.pig.HCatLoader();"$'\n'>>"$pig_file";

echo "load_master = LOAD  '$target_tab' USING org.apache.hive.hcatalog.pig.HCatLoader();"$'\n'>>"$pig_file";

#####END###############


############################################################################################################################################################
echo "/*-----Pull max s_key processed for the table to start from max + 1------*/"$'\n'>>"$pig_file"
############################################################################################################################################################
#####START##############

echo "load_max_sk = LOAD  '$max_sk_tab' USING org.apache.hive.hcatalog.pig.HCatLoader();"$'\n'>>"$pig_file";

echo "filter_load_max_sk = FILTER load_max_sk BY table_name =='"$table_name"';"$'\n'>>"$pig_file";
#####END###############



##Convert the records into "," seperated values##

key_columns=`sed ':a;N;$!ba;s/\n/,/g' "$key_file"`

non_key_columns=`sed ':a;N;$!ba;s/\n/,/g' "$non_key_file"`

############################################################################################################################################################
##Split non_key columns into non_key_date columns and non_key_non_date columns beacuse while generating    ##
##md5 value columns needs to be concatenaed and there are seperate proceures to type-cast date and non_date##
##columns (chararray) for non_date columns and ToString for date colums                                    ##
############################################################################################################################################################
non_key_non_dt_columns=`sed ':a;N;$!ba;s/\n/,/g' "$non_key_non_dt_file"`

non_key_dt_columns=`sed ':a;N;$!ba;s/\n/,/g' "$non_key_dt_file"`


char_key_columns=`echo "(chararray)$key_columns"|sed 's/,/,\(chararray\)/g'`

char_non_key_non_dt_columns=`echo "(chararray)$non_key_non_dt_columns"|sed 's/,/,\(chararray\)/g'`

char_non_key_dt_columns=`echo "ToString($non_key_dt_columns)"|sed 's/,/\),ToString\(/g'`

############################################################################################################################################################
##Concatenation will only be applied if multiple values are available   ##
############################################################################################################################################################

if [ `cat "$key_file"|wc -l` -gt 1 ]
then
   concat_char_key_columns=`echo "(chararray)CONCAT($char_key_columns)"`
else
   concat_char_key_columns=`echo "$char_key_columns"`
fi


if [ `cat "$non_key_dt_file"|wc -l` -gt 0 ]
then
   concat_char_non_key_dt_columns=`echo "$char_non_key_non_dt_columns","$char_non_key_dt_columns"`
else
   concat_char_non_key_dt_columns=`echo "$char_non_key_non_dt_columns"`
fi

############################################################################################################################################################
##Check if timestamp_col is available or not if available then incule it as well for md5 generation  ##
############################################################################################################################################################

if [[ "$timestamp_col" != "non" ]]
then
   concat_char_non_key_dt_columns_uts=`echo "$concat_char_non_key_dt_columns",ToString"($timestamp_col)"`
else
   concat_char_non_key_dt_columns_uts=`echo "$concat_char_non_key_dt_columns"`
fi

echo "$concat_char_non_key_dt_columns"

 if [ "$partition_logic" == "non" ]
  then
   concat_char_non_key_dt_columns_uts_pt=`echo "$concat_char_non_key_dt_columns_uts",(chararray)"($partition_col)"`
  else
   concat_char_non_key_dt_columns_uts_pt=`echo "$concat_char_non_key_dt_columns_uts"`
  fi

############################################################################################################################################################
##Concatenation will only be applied if multiple values are available   ##
############################################################################################################################################################


if [ `cat "$non_key_file"|wc -l` -gt 1 ]
then
   concat_char_non_key_columns=`echo "(chararray)CONCAT($concat_char_non_key_dt_columns_uts_pt)"`
else
   concat_char_non_key_columns=`echo "$concat_char_non_key_dt_columns_uts_pt"`
fi



echo "$concat_char_non_key_columns"

############################################################################################################################################################
echo "/*-----Generate md5 values for key and non key values along with other columns------*/"$'\n'>>"$pig_file"
############################################################################################################################################################

echo "md5_source = FOREACH load_source GENERATE *,MD5($concat_char_key_columns) as (md5_key_value:chararray),MD5($concat_char_non_key_columns) as (md5_non_key_value:chararray);"$'\n'>>"$pig_file";



############################################################################################################################################################
##Check if loading logic is incemental or complete . Based on these different process will be followed ##
############################################################################################################################################################

if [ "$loading_logic" == "complete" ]

then

        ############################################################################################################################################################
        echo "/*-----Join new base table with the current active records based on key columns to split data same,new(insert) or changed(update or delete records)------*/"$'\n'>>"$pig_file"
        ############################################################################################################################################################

        echo "join_source_master = JOIN md5_source BY ("$key_columns") FULL,load_master BY ("$key_columns");"$'\n'>>"$pig_file";
        
        echo "SPLIT join_source_master INTO"$'\n'"       same_record IF (md5_source::md5_non_key_value==load_master::md5_non_key_value),"$'\n'"       update_delete_record IF (((NOT(md5_source::md5_non_key_value==load_master::md5_non_key_value)) AND load_master::md5_key_value IS NOT NULL) OR md5_source::md5_key_value IS NULL),"$'\n'"       insert_record IF (load_master::md5_key_value is null);"$'\n'>>"$pig_file";
        
        
        ############################################################################################################################################################
        echo "/*-----Fetch all the partitions from the scd table associated with new and changed records------*/"$'\n'>>"$pig_file"
        ############################################################################################################################################################
        echo "--Start"$'\n'>>"$pig_file";
	    ############################################################################################################################################################
        
        echo "union_insert_upt_del = UNION update_delete_record,insert_record ;"$'\n'>>"$pig_file";

        echo "sel_partitoned_val = FOREACH union_insert_upt_del GENERATE $partition_col as $partition_col;"$'\n'>>"$pig_file";
        echo "dist_partition= DISTINCT sel_partitoned_val;"$'\n'>>"$pig_file";
        
        echo "join_dist_partition= JOIN dist_partition by $partition_col,load_master by $partition_col;"$'\n'>>"$pig_file";
		
        ############################################################################################################################################################
        echo "--End"$'\n'>>"$pig_file";
	    ############################################################################################################################################################
        
        load_master_key_columns=`echo "load_master::$key_columns"|sed 's/,/,load_master::/g'`
		
		
  
        ############################################################################################################################################################
        echo "/*-----In Section will create below kind of records for pulled partition                                                                            -------------*/"$'\n'>>"$pig_file";		
        echo "/*-----1.Keep all the unaltered records as it is                                                                                                    -------------*/"$'\n'>>"$pig_file";
        echo "/*-----2.For Deleted records set the delete_record_ind to 'Y'   -------------*/"$'\n'>>"$pig_file";
 	    ############################################################################################################################################################
        ############################################################################################################################################################
        echo "/***************************************Section 1 starts ****************************************************************/"$'\n'>>"$pig_file";		
	    ############################################################################################################################################################
 
        echo "join_cust_id = JOIN join_dist_partition by ($load_master_key_columns) LEFT , update_delete_record by ($load_master_key_columns);"$'\n'>>"$pig_file";
        
        
        echo "SPLIT join_cust_id INTO"$'\n'" unaltered_record IF (update_delete_record::load_master::md5_key_value is null AND md5_source::md5_key_value IS NULL),"$'\n'" altered_delete_record IF (NOT(update_delete_record::load_master::md5_key_value is null) AND md5_source::md5_key_value IS NULL),"$'\n'" altered_update_record IF (NOT(update_delete_record::load_master::md5_key_value is null) AND md5_source::md5_key_value IS NOT NULL);"$'\n'>>"$pig_file";
        
        ############################################################################################################################################################
        echo "/***************************************Section 1.a ****************************************************************/"$'\n'>>"$pig_file";		
	    ############################################################################################################################################################

        
        echo "partiton_unaltered_records= FOREACH unaltered_record GENERATE">>"$pig_file";
        echo "join_dist_partition::load_master::s_key AS s_key,">>"$pig_file";
		
############################################################################################################################################################
## Generate Foreach column reference for key columns   ##
############################################################################################################################################################
  
  
         while read line
         do
         echo "join_dist_partition::load_master::$line AS $line,"
         done <"$key_file">>"$pig_file";

############################################################################################################################################################
## Generate Foreach column reference for nonkey columns   ##
############################################################################################################################################################

          while read line
         do
         echo "join_dist_partition::load_master::$line AS $line,"
         done <"$non_key_file">>"$pig_file";
		 
############################################################################################################################################################
       
        echo "join_dist_partition::load_master::delete_record_ind AS delete_record_ind,">>"$pig_file"
        echo "join_dist_partition::load_master::md5_non_key_value AS md5_non_key_value,">>"$pig_file"
        echo "join_dist_partition::load_master::md5_key_value AS md5_key_value,">>"$pig_file"

############################################################################################################################################################
## Check if  timestamp_col is non,if not then include that column ##
############################################################################################################################################################
        
        if [[ "$timestamp_col" != "non" ]]
        then
        echo "join_dist_partition::load_master::$timestamp_col AS $timestamp_col,">>"$pig_file"
        fi
        
        echo "join_dist_partition::load_master::$partition_col AS $partition_col;"$'\n'>>"$pig_file"
  
         
        ############################################################################################################################################################
        echo "/***************************************Section 1.c ****************************************************************/"$'\n'>>"$pig_file";		
	    ############################################################################################################################################################
        
        
        echo "partiton_altered_delete_records= FOREACH altered_delete_record GENERATE">>"$pig_file";
        echo "join_dist_partition::load_master::s_key AS s_key,">>"$pig_file";
        
         while read line
         do
         echo "join_dist_partition::load_master::$line AS $line,"
         done <"$key_file">>"$pig_file";
        
          while read line
         do
         echo "join_dist_partition::load_master::$line AS $line,"
         done <"$non_key_file">>"$pig_file";
        
        echo "'Y' AS delete_record_ind ,">>"$pig_file"
        echo "join_dist_partition::load_master::md5_non_key_value AS md5_non_key_value,">>"$pig_file"
        echo "join_dist_partition::load_master::md5_key_value AS md5_key_value,">>"$pig_file"
        
        if [[ "$timestamp_col" != "non" ]]
        then
        echo "join_dist_partition::load_master::$timestamp_col AS $timestamp_col,">>"$pig_file"
        fi
        
        echo "join_dist_partition::load_master::$partition_col AS $partition_col;"$'\n'>>"$pig_file"
        
        ############################################################################################################################################################
        echo "/***************************************Section 1.d ****************************************************************/"$'\n'>>"$pig_file";		
	    ############################################################################################################################################################
 
 
        echo "union_partition= UNION partiton_unaltered_records,partiton_altered_delete_records;"$'\n'>>"$pig_file"
        
        ############################################################################################################################################################
        echo "/***************************************Section 1 ends ****************************************************************/"$'\n'>>"$pig_file";		
	    ############################################################################################################################################################

		
	    ############################################################################################################################################################
        echo "/*-----This Section will generate below kind of records for pulled partition                                                                                  -------------*/"$'\n'>>"$pig_file";		
        echo "/*-----1.Close the efff_end_dt and update current_active_ind status of the current active records                                                             -------------*/"$'\n'>>"$pig_file";
        echo "/*-----2.Generate a new record as current active from the record received from base table with  current_active_ind as 'Y' and efff_end_dt as 9999-12-31       -------------*/"$'\n'>>"$pig_file";
 	    ############################################################################################################################################################
        ############################################################################################################################################################
        echo "/***************************************Section 2 starts ****************************************************************/"$'\n'>>"$pig_file";		
	    ############################################################################################################################################################

                
        echo "filter_update_record = FILTER update_delete_record  BY  NOT(md5_source::md5_key_value IS NULL);"$'\n'>>"$pig_file"

        ############################################################################################################################################################
        echo "/***************************************Section 2.a ****************************************************************/"$'\n'>>"$pig_file";		
	    ############################################################################################################################################################

        
        echo "update_record_source = FOREACH filter_update_record GENERATE">>"$pig_file"
        echo "load_master::s_key AS s_key,">>"$pig_file"
############################################################################################################################################################
## Generate Foreach column reference for key columns   ##
############################################################################################################################################################
        
         while read line
         do
         echo "md5_source::$line AS $line,"
         done <"$key_file">>"$pig_file";
############################################################################################################################################################
## Generate Foreach column reference for non key columns   ##
############################################################################################################################################################
       
          while read line
         do
         echo "md5_source::$line AS $line,"
         done <"$non_key_file">>"$pig_file";
		 
############################################################################################################################################################
## Check if  timestamp_col is non,if not then include timestamp_col as eff_start_dt otherwise CurrentTime() ##
############################################################################################################################################################

          echo "'N' as delete_record_ind,">>"$pig_file";
         echo "md5_source::md5_non_key_value as md5_non_key_value,">>"$pig_file";
         echo "md5_source::md5_key_value as md5_key_value,">>"$pig_file";

############################################################################################################################################################
## Check if  timestamp_col is non,if not then include that column ##
############################################################################################################################################################
		 
         if [[ "$timestamp_col" != "non" ]]
        then
         echo "md5_source::$timestamp_col AS $timestamp_col,">>"$pig_file";
        fi


############################################################################################################################################################
## Check if  partition_logic is non,if not then include that logic as partition_col logic  ##
############################################################################################################################################################
        
        if [ "$partition_logic" == "non" ]
        then
        echo "md5_source::$partition_col AS $partition_col;"$'\n'>>"$pig_file";
        else
        echo "$partition_logic AS $partition_col;"$'\n'>>"$pig_file";
        fi


        ############################################################################################################################################################
        echo "/***************************************Section 2 ends ****************************************************************/"$'\n'>>"$pig_file";		
	    ############################################################################################################################################################

	    ############################################################################################################################################################
        echo "/*-----This Section will generate below kind of records                                                                                   -------------*/"$'\n'>>"$pig_file";		
        echo "/*-----1.Generate the surregate key for the newly inserted records                                                                        -------------*/"$'\n'>>"$pig_file";
        echo "/*-----2.Generate a record  for newly inserts as current active from the record received from base table withcurrent_active_ind as 'Y' and efff_end_dt as 9999-12-31-------------*/"$'\n'>>"$pig_file";
 	    ############################################################################################################################################################
        ############################################################################################################################################################
        echo "/***************************************Section 3 starts ****************************************************************/"$'\n'>>"$pig_file";		
	    ############################################################################################################################################################
 
        
        
        echo "insert_record_sk = RANK insert_record;"$'\n'>>"$pig_file";
        
        echo "join_insert_record_sk_max = CROSS insert_record_sk,filter_load_max_sk;"$'\n'>>"$pig_file";
        
        
        echo "insert_record_source= FOREACH join_insert_record_sk_max GENERATE ">>"$pig_file";
        echo "insert_record_sk::rank_insert_record + filter_load_max_sk::max_sk AS s_key,">>"$pig_file";
         while read line
         do
         echo "insert_record_sk::md5_source::$line AS $line,"
         done <"$key_file">>"$pig_file";
        
          while read line
         do
         echo "insert_record_sk::md5_source::$line AS $line,"
         done <"$non_key_file">>"$pig_file";
        
        echo "'N' as delete_record_ind,">>"$pig_file";
        echo "insert_record_sk::md5_source::md5_non_key_value AS md5_non_key_value,">>"$pig_file";
        echo "insert_record_sk::md5_source::md5_key_value AS md5_key_value,">>"$pig_file";
        
         if [[ "$timestamp_col" != "non" ]]
        then
        echo "insert_record_sk::md5_source::$timestamp_col AS $timestamp_col,">>"$pig_file";
        fi
 
############################################################################################################################################################
## Check if  partition_logic is non,if not then include that logic as partition_col logic  ##
############################################################################################################################################################
 
        if [ "$partition_logic" == "non" ]
        then
        echo "insert_record_sk::md5_source::$partition_col AS $partition_col;"$'\n'>>"$pig_file";
        else
        echo "$partition_logic AS $partition_col;"$'\n'>>"$pig_file";
        fi
  
        ############################################################################################################################################################
        echo "/***************************************Section 3 ends ****************************************************************/"$'\n'>>"$pig_file";		
	    ############################################################################################################################################################

  
 	    ############################################################################################################################################################
        echo "/*-----This Section will union all the generated records and make final set of records     -------------*/"$'\n'>>"$pig_file";		
 	    ############################################################################################################################################################
        ############################################################################################################################################################
        echo "/***************************************Section 4 starts ****************************************************************/"$'\n'>>"$pig_file";		
	    ############################################################################################################################################################
 
        echo "union_all= UNION insert_record_source,update_record_source,union_partition;"$'\n'>>"$pig_file";
        
        
        echo "final_records = FOREACH union_all GENERATE ">>"$pig_file";
        echo "s_key As s_key,">>"$pig_file";
        
         while read line
         do
         echo "$line AS $line,"
         done <"$key_file">>"$pig_file";
        
         while read line
         do
         echo "$line AS $line,"
         done <"$non_key_file">>"$pig_file";
		 
        if [[ "$timestamp_col" != "non" ]]
        then
        ##echo "ToString($timestamp_col,'yyyy-MM-dd HH:mm:ss') AS $timestamp_col,">>"$pig_file";
        echo "$timestamp_col AS $timestamp_col,">>"$pig_file";
		fi
		
         echo "delete_record_ind AS delete_record_ind,">>"$pig_file";
        echo "md5_non_key_value AS md5_non_key_value,">>"$pig_file";
        echo "md5_key_value AS md5_key_value,">>"$pig_file";
        echo "$partition_col AS $partition_col;"$'\n'>>"$pig_file";

        ############################################################################################################################################################
        echo "/***************************************Section 4 ends ****************************************************************/"$'\n'>>"$pig_file";		
	    ############################################################################################################################################################


else
		############################################################################################################################################################
        echo "/*-----.Identify update and insert records                                                                                         -------------*/"$'\n'>>"$pig_file";
 	    ############################################################################################################################################################
        ############################################################################################################################################################
        echo "/***************************************Section 1 starts ****************************************************************/"$'\n'>>"$pig_file";		
	    ############################################################################################################################################################         
        
         echo "SPLIT join_source_master INTO "$'\n'" same_record IF (md5_source::md5_non_key_value==load_master::md5_non_key_value), "$'\n'" update_record IF ((NOT(md5_source::md5_non_key_value==load_master::md5_non_key_value)) AND load_master::md5_key_value IS NOT NULL) ,"$'\n'" insert_record IF (load_master::md5_key_value is null);"$'\n'>>"$pig_file";
        ############################################################################################################################################################
        echo "/***************************************Section 1 ends ****************************************************************/"$'\n'>>"$pig_file";		
	    ############################################################################################################################################################

		
		############################################################################################################################################################
        echo "/*-----This Section will generate below kind of records                                                                                   -------------*/"$'\n'>>"$pig_file";		
        echo "/*-----1.Generate the surregate key for the newly inserted records                                                                        -------------*/"$'\n'>>"$pig_file";
        echo "/*-----2.Generate a record  for newly inserts as current active from the record received from base table with current_active_ind as 'Y' and eff_end_dt as 9999-12-31-------------*/"$'\n'>>"$pig_file";
 	    ############################################################################################################################################################
        ############################################################################################################################################################
        echo "/***************************************Section 2 starts ****************************************************************/"$'\n'>>"$pig_file";		
	    ############################################################################################################################################################

    	 echo "insert_record_sk = RANK insert_record;"$'\n'>>"$pig_file";
         
         echo "join_insert_record_sk_max = CROSS insert_record_sk,filter_load_max_sk;"$'\n'>>"$pig_file";
         
         
         
         echo "insert_record_source= FOREACH join_insert_record_sk_max GENERATE ">>"$pig_file";
         echo "insert_record_sk::rank_insert_record + filter_load_max_sk::max_sk AS s_key,">>"$pig_file";
          while read line
          do
          echo "insert_record_sk::md5_source::$line AS $line,"
          done <"$key_file">>"$pig_file";
         
           while read line
          do
          echo "insert_record_sk::md5_source::$line AS $line,"
          done <"$non_key_file">>"$pig_file";
         
		
		 
         echo "insert_record_sk::md5_source::md5_non_key_value AS md5_non_key_value,">>"$pig_file";
         echo "insert_record_sk::md5_source::md5_key_value AS md5_key_value,">>"$pig_file";
         
          if [[ "$timestamp_col" != "non" ]]
         then
         echo "insert_record_sk::md5_source::$timestamp_col AS $timestamp_col,">>"$pig_file";
         fi
         
         if [ "$partition_logic" == "non" ]
         then
         echo "insert_record_sk::md5_source::$partition_col AS $partition_col;"$'\n'>>"$pig_file";
         else
         echo "$partition_logic AS $partition_col;"$'\n'>>"$pig_file";
         fi

        ############################################################################################################################################################
        echo "/***************************************Section 2 ends ****************************************************************/"$'\n'>>"$pig_file";		
	    ############################################################################################################################################################

        ############################################################################################################################################################
        echo "/*-----In Section will create below kind of records for pulled partition                                                                            -------------*/"$'\n'>>"$pig_file";
        echo "/*-----1.Identify the corresponding partitions of inserted and updated records                                                                      -------------*/"$'\n'>>"$pig_file";
        echo "/*-----2.Fectch all the corresponding partition records to rebuild it again                                                                         -------------*/"$'\n'>>"$pig_file";
		echo "/*-----3.Keep all the unaltered records as it is                                                                                                    -------------*/"$'\n'>>"$pig_file";
        echo "/*-----4.For updates only process the history records as the current active records will closed using timestamp column present in the update        -------------*/"$'\n'>>"$pig_file";
	    echo "/*-----  records present in the base table.This is covered in the next section                                                                       -------------*/"$'\n'>>"$pig_file";		
	    ############################################################################################################################################################
        ############################################################################################################################################################
        echo "/***************************************Section 3 starts ****************************************************************/"$'\n'>>"$pig_file";		
	    ############################################################################################################################################################

		 
		 
         echo "sel_partitioned_val_update = FOREACH update_record GENERATE $partition_col as $partition_col;"$'\n'>>"$pig_file";
		 
		 echo "sel_partitioned_val_insert = FOREACH insert_record_source GENERATE $partition_col as $partition_col;"$'\n'>>"$pig_file";
		 
		 echo "union_partitioned_val = union sel_partitioned_val_update,sel_partitioned_val_insert;"$'\n'>>"$pig_file";


         echo "dist_partition= DISTINCT union_partitioned_val;"$'\n'>>"$pig_file";
         
         echo "join_dist_partition= JOIN dist_partition by $partition_col,load_master by $partition_col;"$'\n'>>"$pig_file";
         
         load_master_key_columns=`echo "load_master::$key_columns"|sed 's/,/,load_master::/g'`
         load_master_key_columns=`echo "load_master::$key_columns"|sed 's/,/,load_master::/g'`
         
         echo "join_cust_id = JOIN join_dist_partition by ($load_master_key_columns) LEFT , update_record by ($load_master_key_columns);"$'\n'>>"$pig_file";
         
         
         
         
         echo "SPLIT join_cust_id INTO"$'\n' "unaltered_record IF (update_record::load_master::md5_key_value is null),"$'\n' "altered_record IF (NOT(update_record::load_master::md5_key_value is null));"$'\n'>>"$pig_file";
  
        ############################################################################################################################################################
        echo "/***************************************Section 3.a ****************************************************************/"$'\n'>>"$pig_file";		
	    ############################################################################################################################################################
  
         
         echo "partiton_unaltered_records= FOREACH unaltered_record GENERATE">>"$pig_file";
         echo "join_dist_partition::load_master::s_key AS s_key,">>"$pig_file";
         
          while read line
          do
          echo "join_dist_partition::load_master::$line AS $line,"
          done <"$key_file">>"$pig_file";
         
           while read line
          do
          echo "join_dist_partition::load_master::$line AS $line,"
          done <"$non_key_file">>"$pig_file";
         
         echo "join_dist_partition::load_master::md5_non_key_value AS md5_non_key_value,">>"$pig_file"
         echo "join_dist_partition::load_master::md5_key_value AS md5_key_value,">>"$pig_file"
         
         if [[ "$timestamp_col" != "non" ]]
         then
         echo "join_dist_partition::load_master::$timestamp_col AS $timestamp_col,">>"$pig_file"
         fi
         
         echo "join_dist_partition::load_master::$partition_col AS $partition_col;"$'\n'>>"$pig_file"
         
         
          
         ############################################################################################################################################################
        echo "/***************************************Section 3 ends ****************************************************************/"$'\n'>>"$pig_file";		
	    ############################################################################################################################################################
        
 	    ############################################################################################################################################################
        echo "/*-----This Section will generate below kind of records for pulled partition                                                                                  -------------*/"$'\n'>>"$pig_file";		
        echo "/*-----1.Close the efff_end_dt and change current_active_ind status of the current active records of scd table                                                -------------*/"$'\n'>>"$pig_file";
        echo "/*-----2.Generate a new record as current active from the record received from base table with  current_active_ind as 'Y' and efff_end_dt as 9999-12-31       -------------*/"$'\n'>>"$pig_file";
 	    ############################################################################################################################################################
        ############################################################################################################################################################
        echo "/***************************************Section 4 starts ****************************************************************/"$'\n'>>"$pig_file";		
	    ############################################################################################################################################################
        	   
         
		############################################################################################################################################################
        echo "/***************************************Section 4.a ****************************************************************/"$'\n'>>"$pig_file";		
	    ############################################################################################################################################################

         echo "update_record_source = FOREACH update_record GENERATE">>"$pig_file"
         echo "load_master::s_key AS s_key,">>"$pig_file"
         
          while read line
          do
          echo "md5_source::$line AS $line,"
          done <"$key_file">>"$pig_file";
         
           while read line
          do
          echo "md5_source::$line AS $line,"
          done <"$non_key_file">>"$pig_file";
         
          echo "md5_source::md5_non_key_value as md5_non_key_value,">>"$pig_file";
          echo "md5_source::md5_key_value as md5_key_value,">>"$pig_file";
         
          if [[ "$timestamp_col" != "non" ]]
         then
          echo "md5_source::$timestamp_col AS $timestamp_col,">>"$pig_file";
         fi
         
         if [ "$partition_logic" == "non" ]
         then
         echo "md5_source::$partition_col AS $partition_col;"$'\n'>>"$pig_file";
         else
         echo "$partition_logic AS $partition_col;"$'\n'>>"$pig_file";
         fi
         
         
      
        ############################################################################################################################################################
        echo "/***************************************Section 4 ends ****************************************************************/"$'\n'>>"$pig_file";		
	    ############################################################################################################################################################
	  
        

         
	    ############################################################################################################################################################
        echo "/*-----This Section will union all the generated records and make final set of records     -------------*/"$'\n'>>"$pig_file";		
 	    ############################################################################################################################################################
        ############################################################################################################################################################
        echo "/***************************************Section 5 starts ****************************************************************/"$'\n'>>"$pig_file";		
	    ############################################################################################################################################################
         
         
         echo "union_all= UNION insert_record_source,update_record_source,partiton_unaltered_records;"$'\n'>>"$pig_file";
        
         echo "final_records = FOREACH union_all GENERATE ">>"$pig_file";
         echo "s_key As s_key,">>"$pig_file";
         
          while read line
          do
          echo "$line AS $line,"
          done <"$key_file">>"$pig_file";
         
          while read line
          do
          echo "$line AS $line,"
          done <"$non_key_file">>"$pig_file";
		  
         if [[ "$timestamp_col" != "non" ]]
         then
         ##echo "ToString($timestamp_col,'yyyy-MM-dd HH:mm:ss') AS $timestamp_col,">>"$pig_file";
		 echo "$timestamp_col AS $timestamp_col,">>"$pig_file";
         fi
		 
         echo "md5_non_key_value AS md5_non_key_value,">>"$pig_file";
         echo "md5_key_value AS md5_key_value,">>"$pig_file";
         echo "$partition_col AS $partition_col;"$'\n'>>"$pig_file";
        ############################################################################################################################################################
        echo "/***************************************Section 5 ends ****************************************************************/"$'\n'>>"$pig_file";		
	    ############################################################################################################################################################

fi

############################################################################################################################################################
echo "/*-----Load the final recors into the intermediate table     -------------*/"$'\n'>>"$pig_file";		
############################################################################################################################################################
############################################################################################################################################################
echo "/***************************************Section 6 starts ****************************************************************/"$'\n'>>"$pig_file";		
############################################################################################################################################################

##echo "STORE final_records INTO 's3://"$target_wrk_loc"' USING PigStorage(',');"$'\n'>>"$pig_file";##
echo "STORE final_records INTO '$target_wrk_tab' USING org.apache.hive.hcatalog.pig.HCatStorer();"$'\n'>>"$pig_file";
############################################################################################################################################################
echo "/***************************************Section 6 ends ****************************************************************/"$'\n'>>"$pig_file";		
############################################################################################################################################################


echo "SET hive.exec.dynamic.partition.mode=non-strict;">"$hive_file";

echo "use "$hive_db";"$'\n'>>"$hive_file";

if [[ "$timestamp_col" != "non" ]]
then
echo "insert overwrite table $target_tab PARTITION($partition_col) select s_key,$key_columns,$non_key_columns,$timestamp_col,eff_start_dt,eff_end_dt,current_active_ind,md5_non_key_value,md5_key_value,$partition_col from $target_wrk_tab ; "$'\n'>>"$hive_file";
else
echo "insert overwrite table $target_tab PARTITION($partition_col) select s_key,$key_columns,$non_key_columns,eff_start_dt,eff_end_dt,current_active_ind,md5_non_key_value,md5_key_value,$partition_col from $target_wrk_tab ; "$'\n'>>"$hive_file";
fi

echo "insert overwrite table "$hive_db".table_sk_map PARTITION(table_name) SELECT MAX(s_key) AS s_key,'"$table_name"' FROM $target_tab;"$'\n'>>"$hive_file";

rm -f "$key_file"
rm -f "$non_key_file"