# Databricks notebook source
# MAGIC %md
# MAGIC ### Etapa inicial, criacão do Database.

# COMMAND ----------

spark.sql("CREATE DATABASE IF NOT EXISTS wine_db")