a=`aws s3 cp  s3://al-edh-sandbox-dm/conf/emr/task-runner /home/hadoop/TaskRunner --recursive`
nohup java -jar /home/hadoop/TaskRunner/TaskRunner-1.0.jar --workerGroup=ALWorkerGroupSandbox-DM --region=us-east-1 --logUri=s3://al-edh-sandbox-dm/logs/emr/workergroup &
