# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Process Fastq files with kallisto

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Setup

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC """
# MAGIC Setup some parameters
# MAGIC """
# MAGIC 
# MAGIC _silver_table = "silver_fastq_kallisto"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Install Trimmomatic

# COMMAND ----------

# MAGIC %conda install -c bioconda kallisto

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Kallisto UDF

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Index Function

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC import os, subprocess
# MAGIC import numpy as np
# MAGIC from pyspark.sql.functions import udf
# MAGIC from pyspark.sql.types import StringType
# MAGIC 
# MAGIC @udf('string')
# MAGIC def run_kallisto_index(fastq : str,
# MAGIC                        fastq_name : str,
# MAGIC                        params : str) -> str:
# MAGIC   """
# MAGIC   Run Kallisto
# MAGIC   
# MAGIC   @param fastq        | fastq string
# MAGIC   @param fastq_name   | fastq name
# MAGIC   @param params       | kallisto parameters
# MAGIC   """
# MAGIC   
# MAGIC   # create a temporary file on the worker
# MAGIC   _temp_file = f"/tmp/{fastq_name}.fastq"
# MAGIC   _temp_out_file = _temp_file.replace(".fastq", ".trimmed.fastq")
# MAGIC   
# MAGIC   with open(_temp_file, "w") as _fastq:
# MAGIC     _fastq.write(fastq)
# MAGIC     try:
# MAGIC       _res = subprocess.run(["kallisto", params,
# MAGIC                                          "index",
# MAGIC                                          "-i",
# MAGIC                                          _temp_out_file,
# MAGIC                                          _temp_file],
# MAGIC                             stderr = subprocess.STDOUT,
# MAGIC                             check=True)
# MAGIC     except Exception as err:
# MAGIC         return err
# MAGIC       
# MAGIC   # Clean up
# MAGIC   os.remove(_temp_file)
# MAGIC   
# MAGIC   # grab the output file results
# MAGIC   with open(_temp_out_file, 'r') as _out_file:
# MAGIC     _out_str = _out_file.read()
# MAGIC     _out_file.close()
# MAGIC     
# MAGIC   # remove the output fule
# MAGIC   os.remove(_temp_out_file)
# MAGIC   
# MAGIC   # return the output
# MAGIC   return _out_str
# MAGIC 
# MAGIC spark.udf.register("run_kallisto_index", run_kallisto_index)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Quantize reads

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC import os, subprocess
# MAGIC from shutil import rmtree
# MAGIC import numpy as np
# MAGIC from pyspark.sql.functions import udf
# MAGIC from pyspark.sql.types import StringType
# MAGIC import tempfile
# MAGIC 
# MAGIC @udf('string')
# MAGIC def run_kallisto_quant(fastq : str,
# MAGIC                        fastq_name : str,
# MAGIC                        index : str
# MAGIC                        params : str) -> str:
# MAGIC   """
# MAGIC   Run Kallisto
# MAGIC   
# MAGIC   @param fastq        | fastq string
# MAGIC   @param fastq_name   | fastq name
# MAGIC   @param params       | kallisto parameters
# MAGIC   """
# MAGIC   
# MAGIC   # create a temporary file on the worker
# MAGIC   _temp_file = f"/tmp/{fastq_name}.fastq"
# MAGIC   _temp_out_dir = tempfile.TemporaryDirectory()
# MAGIC   _temp_index = f"/tmp/{fastq_name}.index"
# MAGIC   
# MAGIC   with open(_temp_file, "w") as _fastq:
# MAGIC     _fastq.write(fastq)
# MAGIC     with open(_temp_index, "w") as _index:
# MAGIC       try:
# MAGIC         _res = subprocess.run(["kallisto", params,
# MAGIC                                            "quant",
# MAGIC                                            "-i",
# MAGIC                                            _temp_index,
# MAGIC                                            "--plaintext",
# MAGIC                                            "--output-dir",
# MAGIC                                            _temp_out_dir,
# MAGIC                                            _temp_file],
# MAGIC                               stderr = subprocess.STDOUT,
# MAGIC                               check=True)
# MAGIC       except Exception as err:
# MAGIC           return err
# MAGIC       
# MAGIC   # Clean up
# MAGIC   os.remove(_temp_file)
# MAGIC   os.remove(_temp_index)
# MAGIC   
# MAGIC   # grab the output file results
# MAGIC   with open(_temp_out_dir+"/abundance.tsv", 'r') as _out_file:
# MAGIC     _out_str = _out_file.read()
# MAGIC     _out_file.close()
# MAGIC     
# MAGIC   # remove the output fule
# MAGIC   rmtree(_temp_out_dir)
# MAGIC   
# MAGIC   # return the output
# MAGIC   return _out_str
# MAGIC 
# MAGIC spark.udf.register("run_kallisto_quant", run_kallisto_quant)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Run Kallisto

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Load Data

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC """
# MAGIC Load fastq table
# MAGIC """
# MAGIC 
# MAGIC from pyspark.sql import DataFrame
# MAGIC 
# MAGIC # load the dataframe
# MAGIC _df : DataFrame = (spark.read
# MAGIC                         .format("delta")
# MAGIC                         .table("bronze_fastq"))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Run Kallisto

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC """
# MAGIC Run Kallisto
# MAGIC """
# MAGIC 
# MAGIC from pyspark.sql import DataFrame
# MAGIC from pyspark.sql.functions import col, lit
# MAGIC 
# MAGIC # set some parameters for kallisto
# MAGIC _params = ""
# MAGIC 
# MAGIC # run the kallisto udf
# MAGIC _df_index : DataFrame = (_df.withColumn("params", lit(_params))
# MAGIC                             .withColumn("kallisto_index", 
# MAGIC                                          run_kallisto_index(col("params"), 
# MAGIC                                                             col("fastq"), 
# MAGIC                                                             col("fastq_name")))
# MAGIC                           )

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Ingest to Silver Table

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC """
# MAGIC Ingest Kallisto index results to silver table
# MAGIC """
# MAGIC 
# MAGIC try:
# MAGIC   
# MAGIC   # write to dataframe
# MAGIC   (_df_index.write
# MAGIC             .format("delta")
# MAGIC             .mode("append")
# MAGIC             .saveAsTable(_silver_table)
# MAGIC   )
# MAGIC    
# MAGIC except Exception as err:
# MAGIC    raise ValueError(f"Failed to write to table {_silver_table}, errror : {err}")
