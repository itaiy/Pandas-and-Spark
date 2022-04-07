# Databricks notebook source
# MAGIC %md # pandas_udf
# MAGIC **U**ser **D**efined **F**unctions are a convenient way to reuse logic that needs to be be executed on datasets.
# MAGIC UDFs are also used to wrap complex logic, such as ML (or even DL) models, and make it accessible for downstream consumers in SQL.
# MAGIC <p>
# MAGIC In this notebook, we'll only focus on **pandas_udf** (but there are other types of UDFs).
# MAGIC <p>pandas_udfs use [Arrow](https://arrow.apache.org/) to cut down on serialization between the JVM and python. Some operations can also benefit from Pandas' vectorized operations to gain an additional performance boost.
# MAGIC 
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2017/10/image1-4.png" width="400" height="200" display="block" margin-left="auto" margin-right="auto">

# COMMAND ----------

# DBTITLE 1,The data we'll be working with
spark.read.format("delta").load("dbfs:/databricks-datasets/learning-spark-v2/people/people-10m.delta").createOrReplaceTempView("10m")
df = spark.table("10m") 
display(df)

# COMMAND ----------

# MAGIC %md ### Types of UDFs
# MAGIC Generaly speaking, all types of pandas_udfs accept a `pandas.Series` and differ by return type

# COMMAND ----------

# MAGIC %md ### Series to Series
# MAGIC **Input**: `pd.Series` <p>
# MAGIC **Output**: `pd.Series` <p>
# MAGIC **Description**: For each item in the input series, we will apply the logic and return an item in the output series

# COMMAND ----------

import pandas as pd
from pyspark.sql.functions import pandas_udf       

# This UDF accepts a series of items of type "long", and returns a series of items of the same type
@pandas_udf('long')
def pandas_plus_one_vectorized(s: pd.Series) -> pd.Series:
    return s + 1 

# This UDF accepts a series of items of type "string", and returns a series of items of the same type
@pandas_udf('string')
def pandas_uppercase(s: pd.Series) -> pd.Series:
    return s.apply(lambda x: x.upper())


# For each record, we will apply 2 UDFs (on "id" column and on "firstName" column)
df.select(pandas_plus_one_vectorized("id").alias("id_plus_one"), \
          pandas_uppercase("firstName").alias("uc_name")).display()


# COMMAND ----------

# DBTITLE 1,Once registered, UDFs can be called directly from SQL
# Register the UDFs (so they can be called from SQL)
# The first argument is how we will call the UDF from SQL, and the second argument is the name of the function itself
spark.udf.register("p_plus1", pandas_plus_one_vectorized)
spark.udf.register("p_uppercase", pandas_uppercase)

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT p_plus1(id) as p1, p_uppercase(firstName) as uc_name
# MAGIC FROM 10m

# COMMAND ----------

# MAGIC %md ### Iterator of Series to Iterator Series
# MAGIC Returns an `Iterator` instead of the actual `Series`. <p> Can also be used to perform an expensive initialization operation **once**, before operating on a batch of rows. As before, output size should equate to input size.

# COMMAND ----------

import pandas as pd
from pyspark.sql.functions import pandas_udf       
from typing import Iterator

def very_expensive_initialization():
  # Do something expensive (external database fetch, REST calls, load a file, etc.)
  return " Add me to all strings"


@pandas_udf("string")
def pd_ucase_iterator(iterator: Iterator[pd.Series]) -> Iterator[pd.Series]:
    # Do some expensive initialization with a state
    state = very_expensive_initialization()
    for curr_series in iterator:
        # Use that state for the whole iterator.
        yield curr_series.apply(lambda x: x.upper() + state)
  
df.select(pd_ucase_iterator("firstName").alias("uc_name")).display()


# COMMAND ----------

# MAGIC %md ### Iterator of multiple Series to Iterator Series
# MAGIC We'll use this method to apply a single UDF over **multiple columns**. Output size should equate to input size.

# COMMAND ----------

from typing import Iterator, Tuple
import pandas as pd
from pyspark.sql.functions import pandas_udf       

@pandas_udf("string")
def full_name(
        iterator: Iterator[Tuple[pd.Series, pd.Series]]) -> Iterator[pd.Series]:
    return (a + " " + b for a, b in iterator)

df.select("firstName", "lastName", full_name("firstName", "lastName").alias("full_name")).display()


# COMMAND ----------

# MAGIC %md ### Aggregate UDF (Series to Scalar)
# MAGIC Receives a `Series` as input, returns an aggregated scalar (e.g `long`, `string`, etc.)

# COMMAND ----------

import pandas as pd
from pyspark.sql.functions import pandas_udf
from pyspark.sql import Window

# Let's return the average salary by last name

@pandas_udf("double")
def avg_salary(v: pd.Series) -> float:
    return v.mean()

df.groupby("lastName").agg(avg_salary("salary").alias("average_salary")).display()



# COMMAND ----------

# MAGIC %md ### Working with Structs - pandas.DataFrame to pandas.DataFrame
# MAGIC Receives a `struct` as an input, return value can by `DataFrame`, `Series`, `Tuple` or scalar

# COMMAND ----------

import pandas as pd
from pyspark.sql.functions import pandas_udf, struct
from pyspark.sql import Window

@pandas_udf("firstName string, lastName string, uc_firstName string , uc_lastName string")
def uppercase_names(data: pd.DataFrame) -> pd.DataFrame:
    data['uc_firstName'] = data['firstName'].str.upper()
    data['uc_lastName'] = data['lastName'].str.upper()
    return data

df.withColumn("nameStruct", struct("firstName", "lastName")).select(uppercase_names("nameStruct")).display()

# COMMAND ----------

# MAGIC %md ### Read More
# MAGIC * [Databricks blog post - New Pandas UDFs and Python Type Hints in the Upcoming Release of Apache Spark 3.0](https://databricks.com/blog/2020/05/20/new-pandas-udfs-and-python-type-hints-in-the-upcoming-release-of-apache-spark-3-0.html)
# MAGIC * [PySpark documentation - pandas_udf](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.pandas_udf.html)
# MAGIC * [Pandas UDFs Benchmark notebook](https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/1281142885375883/2174302049319883/7729323681064935/latest.html)
# MAGIC * ["Old UDFs" vs pandas_udfs](https://databricks.com/blog/2017/10/30/introducing-vectorized-udfs-for-pyspark.html)

# COMMAND ----------

TODO: Debugging locally  
