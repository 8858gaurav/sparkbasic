import pyspark, pandas as pd
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
import getpass, time
username = getpass.getuser()
print(username)

# demo for managed spark table.

spark = SparkSession \
           .builder \
           .appName("basic application") \
           .config("spark.sql.warehouse.dir", f"/user/{username}/warehouse") \
           .enableHiveSupport() \
           .master("yarn") \
           .getOrCreate()
spark.sparkContext.setLogLevel('WARN')

for k, v in spark.sparkContext.getConf().getAll():
    print(f"{k} = {v}")
# spark.eventLog.enabled = true
# spark.sql.repl.eagerEval.enabled = true
# spark.eventLog.dir = hdfs:///spark-logs
# spark.driver.appUIAddress = http://g01.itversity.com:4040
# spark.dynamicAllocation.maxExecutors = 10
# spark.driver.port = 42577
# spark.app.startTime = 1763308489327
# spark.sql.warehouse.dir = /user/itv020752/warehouse
# spark.yarn.historyServer.address = m02.itversity.com:18080
# spark.executorEnv.PYTHONPATH = /opt/spark-3.1.2-bin-hadoop3.2/python/lib/py4j-0.10.9-src.zip:/opt/spark-3.1.2-bin-hadoop3.2/python/<CPS>{{PWD}}/pyspark.zip<CPS>{{PWD}}/py4j-0.10.9-src.zip
# spark.yarn.jars = 
# spark.history.provider = org.apache.spark.deploy.history.FsHistoryProvider
# spark.serializer.objectStreamReset = 100
# spark.history.fs.logDirectory = hdfs:///spark-logs
# spark.app.name = basic application
# spark.submit.deployMode = client
# spark.history.fs.update.interval = 10s
# spark.driver.extraJavaOptions = -Dderby.system.home=/tmp/derby/
# spark.org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter.param.PROXY_HOSTS = m02.itversity.com
# spark.ui.filters = org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter
# spark.app.id = application_1762730537643_3253
# spark.executor.extraLibraryPath = /opt/hadoop/lib/native
# spark.history.ui.port = 18080
# spark.shuffle.service.enabled = true
# spark.dynamicAllocation.minExecutors = 2
# spark.executor.id = driver
# spark.driver.host = g01.itversity.com
# spark.ui.proxyBase = /proxy/application_1762730537643_3237
# spark.history.fs.cleaner.enabled = true
# spark.master = yarn
# spark.sql.catalogImplementation = hive
# spark.rdd.compress = True
# spark.org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter.param.PROXY_URI_BASES = http://m02.itversity.com:19088/proxy/application_1762730537643_3253
# spark.submit.pyFiles = 
# spark.yarn.isPython = true
# spark.dynamicAllocation.enabled = true
# spark.ui.showConsoleProgress = true

data = [("Maths", 12, '2020-08-01'), ("Pyshics", 15, '2020-01-01'), ("Chemistry", 18, '2020-02-01'), ("English", 20, '2020-03-01'), ("Arts", 25, '2020-04-01'), ("Economics", 30, '2020-05-01'), ("History", 45, '2020-06-01'), ("Computer", 50, '2020-07-01')]

df = spark.createDataFrame(data, 'Sub string, Marks integer, score_date string')
df.show()
# +---------+-----+----------+
# |      Sub|Marks|score_date|
# +---------+-----+----------+
# |    Maths|   12|2020-08-01|
# |  Pyshics|   15|2020-01-01|
# |Chemistry|   18|2020-02-01|
# |  English|   20|2020-03-01|
# |     Arts|   25|2020-04-01|
# |Economics|   30|2020-05-01|
# |  History|   45|2020-06-01|
# | Computer|   50|2020-07-01|
# +---------+-----+----------+

df = spark.createDataFrame(data).toDF('Sub', 'Marks', 'score_date')
df.show()
# +---------+-----+----------+
# |      Sub|Marks|score_date|
# +---------+-----+----------+
# |    Maths|   12|2020-08-01|
# |  Pyshics|   15|2020-01-01|
# |Chemistry|   18|2020-02-01|
# |  English|   20|2020-03-01|
# |     Arts|   25|2020-04-01|
# |Economics|   30|2020-05-01|
# |  History|   45|2020-06-01|
# | Computer|   50|2020-07-01|
# +---------+-----+----------+

from pyspark.sql.functions import *
df = df.withColumn('Grade', expr("""
case when Marks < 20 THEN 'FAIL' WHEN Marks <= 30 THEN 'PASS' WHEN Marks >30 THEN 'DISTINCTION'  ELSE NULL END """))
df.show()
# +---------+-----+----------+-----------+
# |      Sub|Marks|score_date|      Grade|
# +---------+-----+----------+-----------+
# |    Maths|   12|2020-08-01|       FAIL|
# |  Pyshics|   15|2020-01-01|       FAIL|
# |Chemistry|   18|2020-02-01|       FAIL|
# |  English|   20|2020-03-01|       PASS|
# |     Arts|   25|2020-04-01|       PASS|
# |Economics|   30|2020-05-01|       PASS|
# |  History|   45|2020-06-01|DISTINCTION|
# | Computer|   50|2020-07-01|DISTINCTION|
# +---------+-----+----------+-----------+

# df = df.withColumn('score_date', to_date(col("score_date"), "yyyy/MM/dd"))
# df.show()

# df = df.withColumn('score_date_1', date_format(to_timestamp(col("score_date")), "yyyy-MM-dd"))
# df.show()

# df = df.withColumn('score_date_1', df["score_date"].cast('date'))
# df.show()


# df = df.select(df.Sub.cast('string'),
#               df.Marks.cast('integer'),
#               df.score_date.cast('date'),
#               df.Grade.cast('string'))
# df.show()


df.groupBy('Grade').agg({'Marks':'sum', 'Marks': 'mean', 'Marks':'std'}).show()
# will print only last
# +-----------+------------------+
# |      Grade|     stddev(Marks)|
# +-----------+------------------+
# |DISTINCTION|3.5355339059327378|
# |       FAIL|               3.0|
# |       PASS|               5.0|
# +-----------+------------------+


df.groupBy('Grade').agg({'Marks':'sum', 'Marks': 'mean'}).show()
# will print only last
# +-----------+----------+
# |      Grade|avg(Marks)|
# +-----------+----------+
# |DISTINCTION|      47.5|
# |       FAIL|      15.0|
# |       PASS|      25.0|
# +-----------+----------+


df.groupBy('Grade').agg({'Marks':'sum'}).show()
# will print only last
# +-----------+----------+
# |      Grade|sum(Marks)|
# +-----------+----------+
# |DISTINCTION|        95|
# |       FAIL|        45|
# |       PASS|        75|
# +-----------+----------+

df.groupBy('Grade').agg(sum('Marks'), mean('Marks'), stddev('Marks')).show()
# use this for one go
# +-----------+----------+----------+------------------+
# |      Grade|sum(Marks)|avg(Marks)|stddev_samp(Marks)|
# +-----------+----------+----------+------------------+
# |DISTINCTION|        95|      47.5|3.5355339059327378|
# |       FAIL|        45|      15.0|               3.0|
# |       PASS|        75|      25.0|               5.0|
# +-----------+----------+----------+------------------+

df.select(df['Marks'] == 30, df.Marks).show()
# +------------+-----+
# |(Marks = 30)|Marks|
# +------------+-----+
# |       false|   12|
# |       false|   15|
# |       false|   18|
# |       false|   20|
# |       false|   25|
# |        true|   30|
# |       false|   45|
# |       false|   50|
# +------------+-----+

df[df.Marks == 30, df.Marks].show()
# +------------+-----+
# |(Marks = 30)|Marks|
# +------------+-----+
# |       false|   12|
# |       false|   15|
# |       false|   18|
# |       false|   20|
# |       false|   25|
# |        true|   30|
# |       false|   45|
# |       false|   50|
# +------------+-----+

df.selectExpr("*", "IF(Marks % 2 = 0, 1, 0) AS flag").show()
# +---------+-----+----------+-----------+----+
# |      Sub|Marks|score_date|      Grade|flag|
# +---------+-----+----------+-----------+----+
# |    Maths|   12|2020-08-01|       FAIL|   1|
# |  Pyshics|   15|2020-01-01|       FAIL|   0|
# |Chemistry|   18|2020-02-01|       FAIL|   1|
# |  English|   20|2020-03-01|       PASS|   1|
# |     Arts|   25|2020-04-01|       PASS|   0|
# |Economics|   30|2020-05-01|       PASS|   1|
# |  History|   45|2020-06-01|DISTINCTION|   0|
# | Computer|   50|2020-07-01|DISTINCTION|   1|
# +---------+-----+----------+-----------+----+