# Databricks notebook source
# MAGIC %md 
# MAGIC #Get table information
# MAGIC This will get the information about all the tables from all the databases. The key information for us in this instance is created date.

# COMMAND ----------

### Get information about all the tables
### Improvement could be made to make this more pyspark native, but it is never going to be a massive amount of data so may not be an issue

from functools import reduce
import pyspark.sql.functions as f
from pyspark.sql import Window

getTablesInDatabase = lambda db: spark.sql("SHOW TABLE EXTENDED FROM " + db + " LIKE '*'").collect() # For a database get a list of all the tables with the details

databases = spark.sql("SHOW databases") # get all the database names from the environment

databases_as_list = databases.rdd.map(lambda x: x.databaseName).collect() # convert to a python list

all_tables = list(map(getTablesInDatabase, databases_as_list)) # get all the tables details

all_tables_filtered = list(filter(lambda x: len(x) > 0, all_tables))# remove any empty databases

all_tables = reduce(lambda x, y: x + y, all_tables_filtered) # join all the database information data together

# convert to dataframe
columns=["database","tableName","isTemporary","information"]
df_all_tables = spark.createDataFrame(data=all_tables,schema=columns)

# COMMAND ----------

# MAGIC %md #Get created dates
# MAGIC This will process the table information, extracting the created date for each table, and then joining all the dates available for a given table against a table, and then indicating on what date the table was created. The format of this information is useful for the purposes of calculating cumulative information.

# COMMAND ----------

# Create a count of the number of tables by created date.

DATE_TBL = DATABASE.TABLE # <-- Name of your date table

## Extract the created date from the tables, provide a count, and create the prefix of the table which identifies the subject area associated with the tables
df_created_dt = df_all_tables \
                    .withColumn("createdDate", f.split(f.regexp_extract(f.col('information'), '(?<=Created Time: ).*', 0), ' ')) \
                    .select(
                        f.col('database'),
                        f.col('tableName'),
                        f.split(f.col('tableName'), '_')[0].alias('prefix'),
                        f.to_date(f.concat_ws(' ', f.col('createdDate')[2], f.col('createdDate')[1], f.col('createdDate')[5]), 'dd MMM yyyy').alias('createdDate'),
                        f.lit(1).alias('n'))

min_date, max_date = df_created_dt.agg(
                        f.date_format(f.min('createdDate'), "yyyy-MM-dd"), 
                        f.date_format(f.max('createdDate'), "yyyy-MM-dd")) \
                    .first()

# Get a list of of dates from a date table. You will need to modify for your environment
df_dates = spark.sql("SELECT TO_DATE(DAY_DATE) AS dt FROM " + DATE_TBL + " WHERE DAY_DATE >= " + "\'" + min_date + "\'" + " AND DAY_DATE <= " + "\'" + max_date + "\'" + " ORDER BY DAY_DATE")

# get all the dates possible merged with all the tables
df_all_dates_with_tbl = df_dates \
                        .crossJoin(df_created_dt) \
                        .select(
                            df_dates.dt,
                            df_created_dt.database,
                            df_created_dt.createdDate,
                            df_created_dt.prefix, 
                            df_created_dt.tableName)
                     
df_all_dates_with_count = df_all_dates_with_tbl.withColumn("table_count", f.when(f.col('dt') == f.col('createdDate'), 1).otherwise(0))

# COMMAND ----------

# MAGIC %md #Analysis
# MAGIC Cummulative count of tables by date grouped by database and prefix

# COMMAND ----------

# Analysis of the above cell output to calculate the cummulative count
# Filter the data as required if you are only interested in looking at a single database

# Create the windowing function that we will use
w = Window.partitionBy("tableName") \
  .orderBy("dt") \
  .rowsBetween(Window.unboundedPreceding, Window.currentRow)

# Create the cumulative count
df_cummulative = df_all_dates_with_count.withColumn("cummulative_count", f.sum(df_all_dates_with_count.table_count).over(w))

# Filter the data if required
df_filtered = df_cummulative.filter(f.col('database') == 'curated_zone')

# Aggregate the data by databases and prefix (subject area)
aggregated_data = df_filtered.groupBy(f.col('dt'), f.col('database'), f.col('prefix')).agg(f.sum(f.col('cummulative_count'))).orderBy(f.col('database'), f.col('prefix'), f.col('dt')).collect()

display(aggregated_data)

# COMMAND ----------
