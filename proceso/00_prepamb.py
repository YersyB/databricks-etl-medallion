# Databricks notebook source
dbutils.widgets.text("catalog", "dbw_etl_medallion")
catalog = dbutils.widgets.get("catalog")

# Los schemas de dev ya existen con su ubicación; IF NOT EXISTS no los modifica.
# Los de prod heredan la ubicación administrada del catálogo prod_medallion.
for capa in ["bronze", "silver", "gold"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{capa}")
    print(f"Schema listo: {catalog}.{capa}")

display(spark.sql(f"SHOW SCHEMAS IN {catalog}"))