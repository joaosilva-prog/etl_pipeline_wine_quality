# Databricks notebook source
# MAGIC %md
# MAGIC ### Primeiro Passo: Leitura do arquivo CSV para DF Spark, normalizacão dos tipos de Dados recebidos e remapeamento dos nomes das tabelas.

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, DoubleType

schema = StructType([
  StructField("fixed_acidity", DoubleType(), True),
  StructField("volatile_acidity", DoubleType(), True),
  StructField("citric_acid", DoubleType(), True),
  StructField("residual_sugar", DoubleType(), True),
  StructField("chlorides", DoubleType(), True),
  StructField("free_sulfur_dioxide", DoubleType(), True),
  StructField("total_sulfur_dioxide", DoubleType(), True),
  StructField("density", DoubleType(), True),
  StructField("pH", DoubleType(), True),
  StructField("sulphates", DoubleType(), True),
  StructField("alcohol", DoubleType(), True),
  StructField("quality", DoubleType(), True)
])

# COMMAND ----------

df_test = spark.read.format("csv"). \
option("sep", ";"). \
option("header", "true"). \
schema(schema). \
load("dbfs:/databricks-datasets/wine-quality/winequality-white.csv")

df = df_test.withColumnRenamed("fixed acidity", "fixed_acidity"). \
withColumnRenamed("volatile acidity", "volatile_acidity"). \
withColumnRenamed("citric acid", "citric_acid"). \
withColumnRenamed("residual sugar", "residual_sugar"). \
withColumnRenamed("free sulfur dioxide", "free_sulfur_dioxide"). \
withColumnRenamed("total sulfur dioxide", "total_sulfur_dioxide")

# Esqueci que no material de apoio havia a sugestão de como fazer este código acima de forma resumida, com:
# cols = [c.replace(' ', '_') for c in cols]
# Decidi manter o código que eu fiz nesse caso, mesmo que mais verboso ou que não seja a forma mais correta de se realizar.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Segundo Passo: Salvando os dados como Delta Table na DataBase previamente criada.

# COMMAND ----------

df.write.format("delta").mode("overwrite").saveAsTable("wine_db.df_bronze")