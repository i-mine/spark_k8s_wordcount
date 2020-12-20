# SPARK Word Count

spark-submit demo
```bash
spark-submit \
--master k8s://https:master-url \
--deploy-mode cluster \
--name com.mobvista.dataplatform.WordCount \
--conf spark.kubernetes.container.image=818539432014.dkr.ecr.us-east-1.amazonaws.com/engineplus/spark-py:3.0.1-v1.0.0 \
--class com.mobvista.dataplatform.WordCount \
--executor-memory 512m --driver-memory 512m  --executor-cores 2 \
/data/mobdev/spark_k8s_wordcount-2.0-SNAPSHOT.jar \
s3://mob-emr-test/lei.du/word.txt \
s3://mob-emr-test/lei.du/word_count_result

echo "status is $?"
```