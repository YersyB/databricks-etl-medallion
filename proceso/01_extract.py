# Databricks notebook source
for f in ["orders.csv", "order_products__prior.csv", "order_products__train.csv"]:
    df = spark.read.option("sep", ";").option("header", True).csv(
        f"abfss://raw@stetlmedallionybu.dfs.core.windows.net/instacart_ordenes/{f}")
    print(f, df.count())

# COMMAND ----------

from pyspark.sql import functions as F

# Parámetros (widgets) 
dbutils.widgets.text("catalog", "dbw_etl_medallion")
dbutils.widgets.text("raw_base", "abfss://raw@stetlmedallionybu.dfs.core.windows.net")

catalog = dbutils.widgets.get("catalog")
raw_base = dbutils.widgets.get("raw_base")

SEP = ";"

fuentes = {
    "orders":               f"{raw_base}/instacart_ordenes/orders.csv",
    "order_products_prior": f"{raw_base}/instacart_ordenes/order_products__prior.csv",
    "order_products_train": f"{raw_base}/instacart_ordenes/order_products__train.csv",
    "products":             f"{raw_base}/instacart_catalogo/products.csv",
    "aisles":               f"{raw_base}/instacart_catalogo/aisles.csv",
    "departments":          f"{raw_base}/instacart_catalogo/departments.csv",
}

def leer_raw(ruta):
    return (spark.read
            .option("header", True)
            .option("sep", SEP)
            .option("inferSchema", True)
            .csv(ruta))

for nombre, ruta in fuentes.items():
    df = leer_raw(ruta)

    # Quitar columnas vacías generadas por el ";" final de algunas líneas
    columnas_validas = [c for c in df.columns if c.strip() and not c.startswith("_c")]
    df = df.select(*columnas_validas)

    # Columnas de auditoría
    df = (df
          .withColumn("ingestion_ts", F.current_timestamp())
          .withColumn("source_file", F.col("_metadata.file_path")))

    destino = f"{catalog}.bronze.{nombre}"
    (df.write
       .format("delta")
       .mode("overwrite")
       .option("overwriteSchema", "true")
       .saveAsTable(destino))

    filas_origen = leer_raw(ruta).count()
    filas_destino = spark.table(destino).count()
    print(f"{destino}: origen={filas_origen} destino={filas_destino}")

    # Validación: el conteo debe coincidir
    assert filas_origen == filas_destino, f"Conteo distinto en {destino}"

# COMMAND ----------

display(spark.table("dbw_etl_medallion.bronze.orders").limit(5))