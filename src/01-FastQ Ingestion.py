# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # 01 - FastQ Ingestion
# MAGIC 
# MAGIC Ingest Fastq files into a Bronze table

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Setup some parameterse

# COMMAND ----------

# DBTITLE 1,Setup
# MAGIC %python
# MAGIC 
# MAGIC """
# MAGIC Setup some defaults
# MAGIC """
# MAGIC 
# MAGIC _bronze_table : str = "bronze_fastq"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Download FastQ Files

# COMMAND ----------

# MAGIC %sh
# MAGIC 
# MAGIC # download some files
# MAGIC 
# MAGIC wget https://resources.qiagenbioinformatics.com/testdata/paeruginosa-reads.zip

# COMMAND ----------

# MAGIC %sh
# MAGIC 
# MAGIC # unzip the downloaded files
# MAGIC 
# MAGIC unzip paeruginosa-reads.zip

# COMMAND ----------

# DBTITLE 1,Distribute files onto DBFS
# MAGIC %fs
# MAGIC 
# MAGIC cp -r file:/databricks/driver/paeruginosa-reads/ dbfs:/tmp/paeruginosa-reads

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Ingest into Bronze Tables

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Load Data

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC """
# MAGIC Load fastq files into a dataframe
# MAGIC """
# MAGIC 
# MAGIC from pyspark.sql import DataFrame
# MAGIC from pyspark.sql.functions import current_timestamp
# MAGIC 
# MAGIC _df : DataFrame = (sc.wholeTextFiles("dbfs:/tmp/paeruginosa-reads/*.fastq")
# MAGIC                      .toDF(["ingestion_path", "fastq"])
# MAGIC                      .withColumn("timestamp", current_timestamp())
# MAGIC                      .withColumnRenamed("ingestion_path", "fastq_name")
# MAGIC                    )

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Ingest to Bronze Table

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC """
# MAGIC Ingest fastq files into bronze tables
# MAGIC """
# MAGIC   
# MAGIC try:
# MAGIC   
# MAGIC   # write to dataframe
# MAGIC   (_df.write
# MAGIC       .format("delta")
# MAGIC       .mode("append")
# MAGIC       .saveAsTable(_bronze_table)
# MAGIC   )
# MAGIC    
# MAGIC except Exception as err:
# MAGIC    raise ValueError(f"Failed to write to table {_bronze_table}, errror : {err}")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Test Query

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC --
# MAGIC -- See some fastq files
# MAGIC --
# MAGIC 
# MAGIC SELECT fastq_name
# MAGIC FROM bronze_fastq;
