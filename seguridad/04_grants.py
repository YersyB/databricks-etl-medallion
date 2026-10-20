# Databricks notebook source


dbutils.widgets.text("catalog", "dbw_etl_medallion")
catalog = dbutils.widgets.get("catalog")

sp = "dce59739-ba76-4c1e-a04b-0dd3dfd0e586"   # ID de aplicación de sp-github-cicd
grupo = "account users"                        


def ejecutar_no_bloqueante(sentencia):
    """Permisos que solo un administrador puede otorgar. Si fallan, el pipeline continúa."""
    try:
        spark.sql(sentencia)
        print(f"OK: {sentencia}")
    except Exception as e:
        print(f"AVISO (no bloqueante): {sentencia}\n{e}")


def ejecutar_obligatorio(sentencia):
    """Permisos de lectura. Si fallan, el pipeline se detiene."""
    spark.sql(sentencia)
    print(f"OK: {sentencia}")


# 1. Administración del catálogo para el service principal
ejecutar_no_bloqueante(f"GRANT MANAGE ON CATALOG {catalog} TO `{sp}`")


if catalog == "prod_medallion":
    ejecutar_no_bloqueante(f"ALTER CATALOG {catalog} OWNER TO `{sp}`")

# 2. Permisos de lectura sobre Gold (obligatorios)
permisos_lectura = [
    f"GRANT USE CATALOG ON CATALOG {catalog} TO `{grupo}`",
    f"GRANT USE SCHEMA ON SCHEMA {catalog}.gold TO `{grupo}`",
    f"GRANT SELECT ON TABLE {catalog}.gold.desempeno_producto TO `{grupo}`",
    f"GRANT SELECT ON TABLE {catalog}.gold.patron_compra_tiempo TO `{grupo}`",
    f"GRANT SELECT ON TABLE {catalog}.gold.resumen_clientes TO `{grupo}`",
]

for sentencia in permisos_lectura:
    ejecutar_obligatorio(sentencia)

# 3. Verificación
display(spark.sql(f"SHOW GRANTS ON CATALOG {catalog}"))