a=`aws s3 cp  s3://al-edh-uat/conf/emr/task-runner /home/hadoop/TaskRunner --recursive`
nohup java -jar /home/hadoop/TaskRunner/TaskRunner-1.0.jar --workerGroup=ALWorkerGroupUAT --region=us-east-1 --logUri=s3://al-edh-uat/logs/emr/workergroup &
