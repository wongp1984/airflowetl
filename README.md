### Airflow ETL Pipeline 
This repository contains the code to implement an ETL pipeline which extracts data from AWS S3, transforms them to a analytic database and load them back into the AWS Redshift. The whole pipeline is automated as a DAG in Airflow. It simulates a fictious scenario set out in the UDacity Data Engineering with AWS Nanodegree. 

### Project Background: STEDI S3 and Glue ETL
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app. In this project, an ETL pipeline is built which extracts Sparkify data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for analytics team to continue finding insights into what songs their users are listening to.

### Implementations
All the implementations are run in Airflow Glue as a DAG. The codes in this project only show the DAG and the customised operators. The airflow DAG is illustrated in below graph.
![dag_graph](https://github.com/wongp1984/airflowetl/blob/main/airflow_dag.png)

### Prerequisite
#### AWS
The IAM roles and users ID should have been configured to S3 and Redshift access from the Internet. 

#### Airflow
The Airflow should have been installed and configured with connections to AWS Redshift and AWS S3 for data retrieval and data loading.
