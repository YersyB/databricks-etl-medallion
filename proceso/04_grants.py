# Databricks notebook source
# MAGIC %sql
# MAGIC -- seguridad/grants.sql
# MAGIC -- Permisos de lectura sobre las tablas Gold creadas por el ETL.
# MAGIC -- Reemplaza `account users` por tu grupo si quieres restringirlo.
# MAGIC
# MAGIC GRANT USE CATALOG ON CATALOG dbw_etl_medallion TO `account users`;
# MAGIC GRANT USE SCHEMA ON SCHEMA dbw_etl_medallion.gold TO `account users`;
# MAGIC
# MAGIC GRANT SELECT ON TABLE dbw_etl_medallion.gold.desempeno_producto TO `account users`;
# MAGIC GRANT SELECT ON TABLE dbw_etl_medallion.gold.patron_compra_tiempo TO `account users`;
# MAGIC GRANT SELECT ON TABLE dbw_etl_medallion.gold.resumen_clientes TO `account users`;

# COMMAND ----------

dbutils.widgets.text("catalog", "dbw_etl_medallion")
catalog = dbutils.widgets.get("catalog")

grupo = "account users"  # reemplaza por tu grupo si lo necesitas

permisos = [
    f"GRANT USE CATALOG ON CATALOG {catalog} TO `{grupo}`",
    f"GRANT USE SCHEMA ON SCHEMA {catalog}.gold TO `{grupo}`",
    f"GRANT SELECT ON TABLE {catalog}.gold.desempeno_producto TO `{grupo}`",
    f"GRANT SELECT ON TABLE {catalog}.gold.patron_compra_tiempo TO `{grupo}`",
    f"GRANT SELECT ON TABLE {catalog}.gold.resumen_clientes TO `{grupo}`",
]

for sentencia in permisos:
    spark.sql(sentencia)
    print(f"OK: {sentencia}")

# Verificación
display(spark.sql(f"SHOW GRANTS ON TABLE {catalog}.gold.desempeno_producto"))