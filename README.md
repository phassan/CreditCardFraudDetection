# CreditCardFraudDetection
- This project aims to provide a Real-Time Credit Card Fraud Detection system using Apache Spark's Machine Learning Library (MLlib) and Kafka. 
- Implemented and evaluated classification techniques, including logistic regression and random forest on a dataset consisting of 284,807 transactions. The [dataset](https://www.kaggle.com/mlg-ulb/creditcardfraud) is available on Kaggle which has features including Time, Amount, Class, and V1 to V28.
- Python script simulate_trans.py is used to simulate credit card transactions in order to feed into the Kafka topic using Logstash.
- Predictions are stored in Elasticsearch for visualization using Kibana. 

# Requirements
-	[Hadoop](https://hadoop.apache.org/)
-	[Spark](https://spark.apache.org/) 2.4+
-	[Kafka](https://kafka.apache.org/) 2.0+

#  How to use it?
   This is a maven-scala project.
 
Installation:
```
cd CreditCardFraudDetection
mvn package
```

EXECUTION:
```
./bin/spark-submit \
 --class com.sparkml.CreditCardFraudDetection.Driver \
 --master <master-url> \
 --deploy-mode <deploy-mode> \
 --conf <key>=<value> \
 ccfd-0.1.jar \
 arg1(m/k) arg2(l/r)
 ```
 
 Argument is one of the following:
 - m for generating classification model
 - k for fraud detection and saving the result to Elasticsearch
 - l for Logistic regression
 - r for Random forest
