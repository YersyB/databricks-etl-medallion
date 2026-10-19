# Databricks notebook source
from pyspark.sql import functions as F
from delta.tables import DeltaTable

dbutils.widgets.text("catalog", "dbw_etl_medallion")
catalog = dbutils.widgets.get("catalog")

def leer_silver(tabla):
    return spark.table(f"{catalog}.silver.{tabla}")

def guardar_gold(df, tabla):
    destino = f"{catalog}.gold.{tabla}"
    (df.write.format("delta")
       .mode("overwrite")
       .option("overwriteSchema", "true")
       .saveAsTable(destino))
    print(f"{destino}: {spark.table(destino).count()} filas")

od = leer_silver("order_detail")
orders = leer_silver("orders")

# 1. Desempeño por producto (se actualiza con MERGE para que sea reejecutable)
desempeno_producto = (od
    .groupBy("product_id", "product_name", "aisle", "department")
    .agg(
        F.count("*").alias("total_compras"),
        F.sum("reordered").alias("total_recompras"),
        F.countDistinct("user_id").alias("clientes_unicos"),
    )
    .withColumn("tasa_recompra", F.round(F.col("total_recompras") / F.col("total_compras"), 4))
    .withColumn("ts_carga", F.current_timestamp()))

destino_producto = f"{catalog}.gold.desempeno_producto"
if spark.catalog.tableExists(destino_producto):
    (DeltaTable.forName(spark, destino_producto).alias("t")
        .merge(desempeno_producto.alias("s"), "t.product_id = s.product_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())
    print(f"{destino_producto}: MERGE aplicado")
else:
    guardar_gold(desempeno_producto, "desempeno_producto")

# 2. Patrón de compra por día de la semana y hora
patron_tiempo = (od
    .select("order_id", "order_dow", "order_hour_of_day")
    .dropDuplicates(["order_id"])
    .groupBy("order_dow", "order_hour_of_day")
    .agg(F.count("*").alias("total_ordenes"))
    .orderBy("order_dow", "order_hour_of_day"))
guardar_gold(patron_tiempo, "patron_compra_tiempo")

# 3. Resumen por cliente
resumen_clientes = (orders
    .groupBy("user_id")
    .agg(
        F.count("*").alias("total_ordenes"),
        F.avg("days_since_prior_order").alias("promedio_dias_entre_ordenes"),
        F.max("order_number").alias("max_numero_orden"),
    ))
guardar_gold(resumen_clientes, "resumen_clientes")

# 4. Validaciones de calidad
gold_compras = spark.table(destino_producto).agg(F.sum("total_compras")).first()[0]
silver_detalle = od.count()
print(f"Compras en gold: {gold_compras} | Filas en silver.order_detail: {silver_detalle}")
assert gold_compras == silver_detalle, "Las compras de gold no cuadran con silver"

ordenes_detalle = od.select("order_id").distinct().count()
ordenes_patron = spark.table(f"{catalog}.gold.patron_compra_tiempo").agg(F.sum("total_ordenes")).first()[0]
print(f"Órdenes en patrón: {ordenes_patron} | Órdenes distintas en detalle: {ordenes_detalle}")
assert ordenes_patron == ordenes_detalle, "Las órdenes del patrón no cuadran"