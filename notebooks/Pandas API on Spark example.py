# Databricks notebook source
# MAGIC %md # Pandas API on Spark
# MAGIC **U**ser **D**efined **F**unctions are a convenient way to reuse logic that needs to be be executed on datasets.
# MAGIC UDFs are also used to wrap complex logic, such as ML (or even DL) models, and make it accessible for downstream consumers in SQL.
# MAGIC <p>
# MAGIC In this notebook, we'll only focus on **pandas_udf** (but there are other types of UDFs).
# MAGIC <p>pandas_udfs use [Arrow](https://arrow.apache.org/) to cut down on serialization between the JVM and python. Some operations can also benefit from Pandas' vectorized operations to gain an additional performance boost.
# MAGIC 
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2017/10/image1-4.png" width="400" height="200" display="block" margin-left="auto" margin-right="auto">

# COMMAND ----------

# MAGIC %md ### Pandas code example

# COMMAND ----------

# DBTITLE 1,Find number of calls to the Fire Department by incident zip code - using Pandas
import pandas as pd
import time

# Record the start time
start = time.time()

# Read the CSV file with the header
pandasDF = pd.read_csv('/dbfs/databricks-datasets/timeseries/Fires/Fire_Department_Calls_for_Service.csv', header=0)

# Compute the total number of calls per zip code 
pandasDF.groupby('Zipcode of Incident')['Call Number'].count()

# Record the end time
end = time.time()

print('Command took ', end - start, ' seconds')

# COMMAND ----------

# MAGIC %md ### Pandas API on Spark code example

# COMMAND ----------

# DBTITLE 1,Find number of calls to the Fire Department by incident zip code - using Pandas API on Spark
import pyspark.pandas as ps
import time

# Record the start time
start = time.time()

# Read the CSV file with the header
pysparkDF = ps.read_csv('dbfs:/databricks-datasets/timeseries/Fires/Fire_Department_Calls_for_Service.csv', header=0)

# Compute the total number of calls per zip code 
pysparkDF.groupby('Zipcode of Incident')['Call Number'].count()

# Record the end time
end = time.time()

print('Command took ', end - start, ' seconds')

# COMMAND ----------

# MAGIC %md ### pandas-on-Spark DataFrame example

# COMMAND ----------

# DBTITLE 1,Find number of calls to the Fire Department by incident zip code - using pandas-on-Spark DataFrame
import time

# Record the start time
start = time.time()

# Read the CSV file with the header
pysparkDF = spark.read.option("header", "true").option("inferSchema", "true").csv('dbfs:/databricks-datasets/timeseries/Fires/Fire_Department_Calls_for_Service.csv')

pandasOnSparkDF = pysparkDF.to_pandas_on_spark()

# Compute the total number of calls per zip code 
pandasOnSparkDF.groupby('Zipcode of Incident')['Call Number'].count()

# Record the end time
end = time.time()

print('Command took ', end - start, ' seconds')

# COMMAND ----------

# MAGIC %md ### Plotting with Pandas API on Spark example

# COMMAND ----------

# DBTITLE 1,Find number of calls to the Fire Department by incident zip code - using pandas-on-Spark DataFrame
import pyspark.pandas as ps

# Read the CSV file with the header
pandasOnSparkDF = ps.read_csv('dbfs:/databricks-datasets/timeseries/Fires/Fire_Department_Calls_for_Service.csv', header=0)

# Generate a pie plot of the number of calls by incident zip code
pandasOnSparkDF['Zipcode of Incident'].value_counts().plot.pie()

# COMMAND ----------

# MAGIC %md ### Read More
# MAGIC * [Databricks blog post](https://databricks.com/blog/2020/05/20/new-pandas-udfs-and-python-type-hints-in-the-upcoming-release-of-apache-spark-3-0.html)
# MAGIC * [PySpark documentation](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.pandas_udf.html)
# MAGIC * [Benchmark notebook](https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/1281142885375883/2174302049319883/7729323681064935/latest.html)
# MAGIC * ["Old UDFs" vs pandas_udfs](https://databricks.com/blog/2017/10/30/introducing-vectorized-udfs-for-pyspark.html)
