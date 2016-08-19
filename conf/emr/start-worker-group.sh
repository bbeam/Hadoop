a=`aws s3 cp  s3://al-edh-prod/conf/emr/task-runner /home/hadoop/TaskRunner --recursive`
nohup java -jar /home/hadoop/TaskRunner/TaskRunner-1.0.jar --workerGroup=ALWorkerGroupProd --region=us-east-1 --logUri=s3://al-edh-prod/logs/emr/workergroup &
