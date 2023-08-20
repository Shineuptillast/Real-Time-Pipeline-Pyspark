import spark_manager
import os
from pyspark.sql.functions import *
from pyspark.sql.types import *
from dotenv import load_dotenv
load_dotenv()

def read_from_cassandra_table(spark, key_space, tbl ):
       df = spark.read.format('org.apache.spark.sql.cassandra').options(table=tbl, keyspace=key_space).load()
       return df

def write_to_kafka(df_emp,topic_name):
       
       df_emp = df_emp.select(col('emp_id').cast(StringType()).alias('key'),to_json(struct("emp_id","emp_name","city","state")).alias("value"))
       # Preparing connection to kafka
       try:
              (df_emp.write.format("kafka")
             .option("kafka.bootstrap.servers", os.getenv('BOOTSTRAP_SERVER'))
             .option("kafka.security.protocol", "SASL_SSL")
             .option("kafka.sasl.jaas.config",
                     "org.apache.kafka.common.security.plain.PlainLoginModule  required username='{}' password='{}';".format(
                         os.getenv('API_KEY'),  os.getenv('API_SECRET_KEY')))
             .option("kafka.ssl.endpoint.identification.algorithm", "https")
             .option("kafka.sasl.mechanism", "PLAIN")
             .option("checkpointLocation", os.path.join("kafka_checkpoint"))
             .option("topic", topic_name).save())

              print(f"DataFrame has been pushed to the Kafka Topic {topic_name}")
       except Exception as err:
              print("the error is ", err)




if __name__ == "__main__":
       df = read_from_cassandra_table(spark_manager.spark,os.getenv('KEYSPACE'),os.getenv('TABLE_NAME'))
       df.printSchema()
       df.show()

       nRows = df.count()
       print(nRows)
       Columns=df.columns
       nColumns=len(Columns)

       if nRows>0:
              try:
                     print(os.getenv('TOPIC_NAME'))
                     write_to_kafka(df, os.getenv('TOPIC_NAME'))
              except Exception as err:
                     print(err)
       elif nRows==0:
              print("Don't write to Kafka")