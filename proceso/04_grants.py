# Databricks notebook source
# MAGIC %sql
# MAGIC -- seguridad/grants.sql
# MAGIC -- Permisos de lectura sobre las tablas Gold creadas por el ETL.
# MAGIC
# MAGIC GRANT USE CATALOG ON CATALOG dbw_etl_medallion TO `account users`;
# MAGIC GRANT USE SCHEMA ON SCHEMA dbw_etl_medallion.gold TO `account users`;
# MAGIC
# MAGIC GRANT SELECT ON TABLE dbw_etl_medallion.gold.desempeno_producto TO `account users`;
# MAGIC GRANT SELECT ON TABLE dbw_etl_medallion.gold.patron_compra_tiempo TO `account users`;
# MAGIC GRANT SELECT ON TABLE dbw_etl_medallion.gold.resumen_clientes TO `account users`;

# COMMAND ----------

# Databricks notebook source
# Grants de lectura sobre las tablas Gold.

dbutils.widgets.text("catalog", "dbw_etl_medallion")
catalog = dbutils.widgets.get("catalog")

sp = "dce59739-ba76-4c1e-a04b-0dd3dfd0e586"
grupo = "account users"  

# 1. MANAGE para el service principal.
try:
    spark.sql(f"GRANT MANAGE ON CATALOG {catalog} TO `{sp}`")
    print(f"OK: MANAGE otorgado en {catalog}")
except Exception as e:
    print(f"AVISO: no se pudo otorgar MANAGE en {catalog}. Debe otorgarlo un administrador. Detalle: {e}")

# 2. Permisos de lectura para el grupo
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

# 3. Verificación
display(spark.sql(f"SHOW GRANTS ON TABLE {catalog}.gold.desempeno_producto"))