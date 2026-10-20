# Databricks notebook source
dbutils.widgets.text("catalog", "dbw_etl_medallion")
catalog = dbutils.widgets.get("catalog")


for capa in ["bronze", "silver", "gold"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{capa}")
    print(f"Schema listo: {catalog}.{capa}")

display(spark.sql(f"SHOW SCHEMAS IN {catalog}"))