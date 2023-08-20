from pyspark.sql import SparkSession

#SparkContext.addPyFile(path=os.getenv('BUNDLE_ZIP'))


"""with open(os.getenv('TOKEN_JSON')) as f:
    secrets = json.load(f)

CLIENT_ID = secrets["clientId"]
CLIENT_SECRET = secrets["secret"]"""

spark = SparkSession.builder.appName('Real_time_pipeline').getOrCreate()

#spark.sparkContext.addPyFile(path=os.getenv('BUNDLE_ZIP'))

#path = os.getenv('BUNDLE_NAME')
#path=path.startswith(SparkFiles.getRootDirectory())

#spark.conf.set('spark.cassandra.connection.config.cloud.path',path)
#spark.conf.set('spark.cassandra.auth.username',CLIENT_ID)
#spark.conf.set('spark.cassandra.auth.password',CLIENT_SECRET)
