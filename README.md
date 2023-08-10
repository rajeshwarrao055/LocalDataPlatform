# Description

Spark-Submit Command 
```commandline
spark-submit \
    --master local \
    --packages "org.apache.hadoop:hadoop-aws:3.3.2,com.amazonaws:aws-java-sdk-bundle:1.11.1026" \
    main.py
```

## Setup
* Minio installation
```commandline
brew install minio/stable/minio
MINIO_ROOT_USER=admin MINIO_ROOT_PASSWORD=password minio server /Users/rajeshwarrao/data --console-address ":9001"
```

* Spark installation 
  * https://spark.apache.org/downloads.html - Download zip from here 
  * tar xzvf spark-3.3.2-bin-hadoop3.tgz - extract to directory
  * sudo mv spark-3.3.2-bin-hadoop3 /usr/local/spark - move to /usr/local/spark

* Airflow Installation 
  * pip install apache-airflow
  * pip install apache-airflow-providers-amazon (Needed in dag)
  * airflow db init 
  * airflow users create --username admin --firstname Admin --lastname User --role Admin --email admin@example.com
  * airflow webserver --port 8080 
  * airflow scheduler
  * Change `dags_folder` in airflow.cfg to relevant folder with dags