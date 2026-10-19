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
