# Databricks notebook source
dbutils.widgets.text("catalog", "dbw_etl_medallion")
dbutils.widgets.text("storage", "stetlmedallionybu")

catalog = dbutils.widgets.get("catalog")
storage = dbutils.widgets.get("storage")

# Schemas con ubicación administrada en su contenedor
for capa in ["bronze", "silver", "gold"]:
    spark.sql(f"""
        CREATE SCHEMA IF NOT EXISTS {catalog}.{capa}
        MANAGED LOCATION 'abfss://{capa}@{storage}.dfs.core.windows.net/{capa}_managed/'
    """)
    print(f"Schema listo: {catalog}.{capa}")

# Verificación
display(spark.sql(f"SHOW SCHEMAS IN {catalog}"))