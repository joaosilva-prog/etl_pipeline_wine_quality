# Databricks notebook source
# MAGIC %md
# MAGIC ### Primeiro Passo: Duplicatas removidas e filtros aplicados às colunas exigidas.

# COMMAND ----------

from pyspark.sql.functions import when, col

df = spark.read.table("workspace.wine_db.df_bronze")

df.dropDuplicates()

df_wine = df.withColumn("quality_label", when(df.quality >= 7, "good"). \
when((df.quality >= 5) & (df.quality <= 6), "average"). \
otherwise("bad")).filter((df.alcohol >= 8) & (df.alcohol <= 15)). \
filter((df.pH >= 2.5) & (df.pH <= 4.0)). \
filter((df.density >= 0.98) & (df.density <= 1.05)). \
filter((df.residual_sugar >= 0) & (df.chlorides >= 0))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Segundo Passo: Salvar como Delta Table no DataBase criado previamente.

# COMMAND ----------

df_wine.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable("wine_db.df_silver")