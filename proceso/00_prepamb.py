# Databricks notebook source
# Crea los schemas bronze, silver y gold en el catálogo del ambiente.
# Los catálogos los crea un administrador (ver PrepAmb/00_admin_permisos.py).

dbutils.widgets.text("catalog", "dbw_etl_medallion")
catalog = dbutils.widgets.get("catalog")

print(f"Creando schemas en {catalog}")

for capa in ["bronze", "silver", "gold"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{capa}`")
    print(f"  -> Schema listo: {catalog}.{capa}")

display(spark.sql(f"SHOW SCHEMAS IN `{catalog}`"))
