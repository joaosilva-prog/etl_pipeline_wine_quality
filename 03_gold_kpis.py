# Databricks notebook source
# MAGIC %md
# MAGIC ### Filtros e exibicão das métricas solicitadas.

# COMMAND ----------

df_gold = spark.sql("""SELECT 
    quality_label, 
    ROUND(AVG(alcohol), 2) AS avg_alcohol,
    ROUND(AVG(residual_sugar), 2) AS avg_residual_sugar,
    ROUND(AVG(density), 4) AS avg_density,
    ROUND(AVG(pH), 0) AS rounded_pH,
    ROUND(STDDEV_SAMP(alcohol), 2) AS stddev_alcohol,
    COUNT(*) AS total
FROM 
    workspace.wine_db.df_silver
GROUP BY 
    quality_label
ORDER BY
    quality_label DESC""")
display(df_gold)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Segundo Passo: Salvando como Delta Table no DataBase criado previamente.

# COMMAND ----------

df_gold.write.format("delta").option("mergeSchema", "true").mode("overwrite").saveAsTable("wine_db.df_gold")