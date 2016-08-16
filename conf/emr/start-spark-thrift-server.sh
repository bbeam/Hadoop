#!/bin/bash 

while true 

do

	echo "checking process"; 
        
	process=`ps -ef|grep HiveThriftServer2|grep -v grep|wc -l`
		
        
	if [ $process -eq 0 ]
	then
	
	     echo "staring spark thrift server";
				
                
	    
	     sudo -u spark /usr/lib/spark/sbin/start-thriftserver.sh  --hiveconf hive.server2.thrift.port=10001
		       
            
             echo "spark-thrift server is up and exiting the loop";
				
                
            
             exit 0
      
        fi
 
        
        sleep 10
        
done