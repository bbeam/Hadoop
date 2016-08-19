a=`aws s3 cp  s3://al-edh-stage/conf/emr/task-runner /home/hadoop/TaskRunner --recursive`
java -jar /home/hadoop/TaskRunner/TaskRunner-1.0.jar  --workerGroup=ALWorkerGroupStage --region=us-east-1 --logUri=s3://al-edh-stage/logs/emr/workergroup
