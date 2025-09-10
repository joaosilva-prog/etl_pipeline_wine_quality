# Databricks notebook source
df_gold = spark.read.table("workspace.wine_db.df_gold")
display(df_gold)

# COMMAND ----------

# MAGIC %md
# MAGIC O primeiro passo é conferirmos abaixo como ficou nosso esquema da tabela Gold final depois de todos os processos serem rodados através das etapas Medallion:

# COMMAND ----------

df_gold.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Vamos comparar com o schema inicial do arquivo:
# MAGIC

# COMMAND ----------

df_inicial = spark.read.format("csv"). \
option("sep", ";"). \
option("header", "true"). \
option("inferSchema", "true"). \
load("dbfs:/databricks-datasets/wine-quality/winequality-white.csv")

df_inicial.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Podemos ver que diversas colunas não chegaram a este processo final de etapa Gold, passaram por transformacões como média, agrupamento, e mudanca de nome de coluna.
# MAGIC Vamos visualizar abaixo a título de visualizacão como era a tabela na camada Silver:

# COMMAND ----------

df_silver = spark.read.table("workspace.wine_db.df_silver")
df_silver.display()
df_silver.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Interessante notar aqui que a coluna pH na tabela Silver apesar de possuir valores bem próximos, foram arredondados para 3 na tabela Gold, representando desta forma a média de valores próximos que tende ao 3.
# MAGIC Outro dado relevante de percebermos é como a densidade também ficam com valores próximos a 1, com uma pequena variância entre um e outro.
# MAGIC Vamos discutir agora sobre a coluna stddev_alcohol:

# COMMAND ----------

stddev = df_gold.select("stddev_alcohol")
stddev.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Podemos ver que em quadro geral a variacão de álcool é baixa em um contexto geral quando comparamos todos os vinhos, o que fica evidenciado quando olhamos para a baixa discrepância na coluna avg_alcohol, que mostra a média de álcool dentre todos os vinhos.
# MAGIC Vamos avaliar as quantidades:

# COMMAND ----------

total = df_gold.select("quality_label", "total")
total.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Podemos averiguar a quantidade amplamente maior de vinhos "average" do que os outros, o que nos mostra um certo padrão das coisas no nosso mundo de tenderem sempre a média, mantendo um certo equilíbrio na diferenca.
# MAGIC Por fim, vejamos algumas estatísticas gerais interessantes:

# COMMAND ----------

df_gold.summary().show()

# COMMAND ----------

# MAGIC %md
# MAGIC