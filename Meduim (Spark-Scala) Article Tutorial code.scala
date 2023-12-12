// Databricks notebook source
// MAGIC %md
// MAGIC
// MAGIC ## Overview
// MAGIC
// MAGIC The input for our Spark ETL process consists of two files stored in DBFS. The resulting output Dataframes will be persisted as Apache Hive tables. We utilized Databricks Community Edition to create this example. 
// MAGIC
// MAGIC This notebook has three sections. In the first section, we extract the input files from DBFS and load it into Spark distributed memory, in the second section we visualise the datasets charecteristics to acquire a better understanding and in the final section we execute a series of transformations and store the result. 

// COMMAND ----------

// MAGIC %md
// MAGIC ## Data Extraction

// COMMAND ----------

// MAGIC %scala 
// MAGIC val file_location = "/FileStore/tables/gdp_decomposition_pop_weighted_average.csv"
// MAGIC val file2_location = "/FileStore/tables/gdp_over_hours_worked.csv"
// MAGIC val file_type = "csv"
// MAGIC val infer_schema = "true"
// MAGIC val first_row_is_header = "true"
// MAGIC val delimiter = ","
// MAGIC
// MAGIC
// MAGIC var df = spark.read.format(file_type) 
// MAGIC .option("inferSchema", infer_schema) 
// MAGIC .option("header", first_row_is_header) 
// MAGIC .option("sep", delimiter) 
// MAGIC .load(file_location)
// MAGIC
// MAGIC var df2 = spark.read.format(file_type) 
// MAGIC .option("inferSchema", infer_schema) 
// MAGIC .option("header", first_row_is_header) 
// MAGIC .option("sep", delimiter) 
// MAGIC .load(file2_location)
// MAGIC
// MAGIC display(df)
// MAGIC
// MAGIC val temp_table_name = "gdp_decomposition_pop"
// MAGIC
// MAGIC df.createOrReplaceTempView(temp_table_name)
// MAGIC
// MAGIC val temp_table2_name = "gdp_over_hours_worked"
// MAGIC
// MAGIC df2.createOrReplaceTempView(temp_table2_name)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Data Visualisation
// MAGIC

// COMMAND ----------

// MAGIC %sql
// MAGIC
// MAGIC /* Query the created temp table in a SQL cell */
// MAGIC
// MAGIC select * from gdp_over_hours_worked

// COMMAND ----------

df2.printSchema()

// COMMAND ----------

// MAGIC %md
// MAGIC ### Data Pipeline
// MAGIC
// MAGIC 1- Cast 'unemploment_r' to the correct numerical data type.
// MAGIC
// MAGIC 2- Enrich dataframe with unemployment rate category:
// MAGIC We create a new column 'unemployment_category' that represente a particular category of country that verify the filter using i the query. 

// COMMAND ----------

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.when

val transformedDf = df2.withColumn("unemployment_r", col("unemployment_r").cast("float"))
display(transformedDf)

// COMMAND ----------



var enrichedDf = transformedDf.withColumn("unemployment_category", 
    when(col("unemployment_r") <= 4, "Low")
    .when((col("unemployment_r") > 4) && (col("unemployment_r") <= 11), "Moderate")
    .otherwise("High")
)

// COMMAND ----------

display(enrichedDf)

// COMMAND ----------

// MAGIC %md
// MAGIC We view the physical plan and the logical plan associated to the DSL code that created the Dataframe enrichedDf from transformedDf.

// COMMAND ----------

enrichedDf.explain()

// COMMAND ----------

enrichedDf.queryExecution

// COMMAND ----------

val joinDf= enrichedDf.join(df, enrichedDf("gdp_over_pop") < df("gdp_over_pop_adjust_prices_pop_weighted_average"))
display(joinDf)

// COMMAND ----------

// MAGIC %md
// MAGIC 3- We calculate the average gdp per decate and then the average gdp per decate and per country

// COMMAND ----------

import org.apache.spark.sql.functions._
var transformedEnrichedDf = enrichedDf.withColumn("decade", expr("cast(year / 10 as int) * 10"))
transformedEnrichedDf= transformedEnrichedDf.withColumn("gdp_ppp_c", col("gdp_ppp_c").cast("float"))
val averageGdpByDecadeDf = transformedEnrichedDf.groupBy("decade").avg("gdp_ppp_c")
display(averageGdpByDecadeDf)


// COMMAND ----------

display(transformedEnrichedDf)

// COMMAND ----------

val averageGdpByDecadeCountryDf = transformedEnrichedDf.groupBy("decade","country").avg("gdp_ppp_c")
display(averageGdpByDecadeCountryDf)

// COMMAND ----------

// MAGIC %md
// MAGIC 4- We concatenate 'country' and 'year' columns into a new 'country_year' column and sort the data based on that resulting column

// COMMAND ----------


transformedEnrichedDf = transformedEnrichedDf.withColumn("country_year", concat_ws("_", col("country"), col("year")))


// COMMAND ----------

transformedEnrichedDf = transformedEnrichedDf.orderBy(col("country_year").desc)
display(transformedEnrichedDf)

// COMMAND ----------

// MAGIC %md
// MAGIC 5- We filter the data based on the year column
// MAGIC

// COMMAND ----------

val filteredDf = transformedEnrichedDf.filter(col("year") === 2020)

// COMMAND ----------

display(filteredDf)

// COMMAND ----------

// MAGIC %md
// MAGIC 6- We calculate the number of unemployment categorie in each country , then in each country / year

// COMMAND ----------

val countryCountByCatDf = transformedEnrichedDf.groupBy("unemployment_category").agg(count("*").alias("count"))
display(countryCountByCatDf)

// COMMAND ----------


// We create a table in Apache Hive from the DataFrame.
// Once saved, this table will persist across cluster restarts as well as allow various users in databricks across different notebooks to query this data.


var permanent_table_name = "averageGdpByDecadeCountry"
averageGdpByDecadeCountryDf.write.format("parquet").saveAsTable(permanent_table_name)


permanent_table_name = "transformedEnriched"
transformedEnrichedDf.write.format("parquet").saveAsTable(permanent_table_name)

permanent_table_name = "countryCountByCat"
countryCountByCatDf.write.format("parquet").saveAsTable(permanent_table_name)
