# Databricks notebook source
# MAGIC %md # Pandas API on Spark
# MAGIC [Koalas](https://koalas.readthedocs.io/en/latest/) is a project aiming to provide a pandas DataFrame API on top of [Apache Spark](https://spark.apache.org/).
# MAGIC <br>
# MAGIC As of Spark 3.2, this project was merged into the Spark code base, and is now referred to as Pandas API on Spark.
# MAGIC <br>
# MAGIC The Pandas API on Spark is a Pandas' API compatible drop-in replacement which provides Pandas' users the benefits of Spark, with minimal code changes.
# MAGIC <br>
# MAGIC It is also useful for PySpark users by supporting tasks that are easier to accomplish using Pandas, like plotting an Apache Spark DataFrame.

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
# MAGIC `to_pandas_on_spark()` allows you to convert an existing PySpark DataFrame into a pandas-on-Spark DataFrame.
# MAGIC <br>
# MAGIC We suggest you use this method, rather than `to_pandas_on_spark()` (which loads all the data into the driver's memory).

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
# MAGIC * [Databricks blog post - Pandas API on Upcoming Apache Spark 3.2](https://databricks.com/blog/2021/10/04/pandas-api-on-upcoming-apache-spark-3-2.html)
# MAGIC * [Open Data Science Conference blog post - Supercharge Your Pandas Code with Apache Spark](https://odsc.com/blog/supercharge-your-pandas-code-with-apache-spark/)
# MAGIC * [PySpark documentation - Pandas API on Spark](https://spark.apache.org/docs/latest/api/python/user_guide/pandas_on_spark/index.html)
# MAGIC * [PySpark documentation - `to_pandas_on_spark()`](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.to_pandas_on_spark.html)
# MAGIC * [PySpark documentation - `toPandas()`](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.toPandas.html)
