from spark_manager import spark
from pyspark.sql.types import *
from pyspark.sql.functions import *
import data_schema
import os
from dotenv import load_dotenv
load_dotenv()

sch = data_schema.Schema_Kafka()

def read_from_kafka():
    df = spark.readStream.format('kafka').option("kafka.bootstrap.servers", os.getenv('BOOTSTRAP_SERVER')).option("kafka.security.protocol", os.getenv('SECURITY_PROTOCOL')).option("kafka.sasl.jaas.config",
                  "org.apache.kafka.common.security.plain.PlainLoginModule  required username='{}' password='{}';".format(os.getenv('API_KEY'), os.getenv('API_SECRET_KEY'))).option("kafka.ssl.endpoint.identification.algorithm", os.getenv('ALGORITHM')).option("kafka.sasl.mechanism", os.getenv('SSL_MECHANISM')).option("kafka.group.id", os.getenv('GROUP_ID')).option("subscribe", os.getenv('TOPIC_NAME')).option("startingOffsets", os.getenv('AUTO_OFFSET_RESET')).option("failOnDataLoss", "false").load()
    df.printSchema()
    return df
def processEachInterval(df,epoch_id):
    
    df.withColumn('value', from_json(decode("value",charset='UTF-8'),schema =sch.schema_employee()).alias('value')).select("value.*")
    nRows=df.count()
    if nRows>0:
        print("storing in local folder")
        df.printSchema()
        df.show(truncate=False)
        print(os.getenv("DATA_SINK"))
        df.write.mode("overwrite").parquet(path=os.getenv("DATA_SINK"))


if __name__=="__main__" :
    df=read_from_kafka()

    query=(df.writeStream.trigger(processingTime=os.getenv('PROCESSING_INTERVAL')).foreachBatch(processEachInterval).start())
    query.awaitTermination()



    
