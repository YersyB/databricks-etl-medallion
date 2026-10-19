# Databricks notebook source
from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "dbw_etl_medallion")
catalog = dbutils.widgets.get("catalog")

# Alcance del análisis: el archivo de órdenes termina en el usuario 63100,
# así que solo los usuarios anteriores tienen historial completo.
USUARIO_MAX_COMPLETO = 63100

def leer_bronze(tabla):
    return spark.table(f"{catalog}.bronze.{tabla}")

def guardar_silver(df, tabla):
    destino = f"{catalog}.silver.{tabla}"
    (df.write.format("delta")
       .mode("overwrite")
       .option("overwriteSchema", "true")
       .saveAsTable(destino))
    print(f"{destino}: {spark.table(destino).count()} filas")

# 1. Órdenes: tipos, duplicados, nulos clave y alcance de usuarios completos
orders = (leer_bronze("orders")
    .dropDuplicates(["order_id"])
    .filter(F.col("order_id").isNotNull() & F.col("user_id").isNotNull())
    .select(
        F.col("order_id").cast("long"),
        F.col("user_id").cast("long"),
        F.trim("eval_set").alias("eval_set"),
        F.col("order_number").cast("int"),
        F.col("order_dow").cast("int"),
        F.col("order_hour_of_day").cast("int"),
        F.col("days_since_prior_order").cast("int"),
        F.col("days_since_prior_order").isNull().alias("es_primera_orden"),
        F.col("ingestion_ts"),
    )
    .filter(F.col("user_id") < USUARIO_MAX_COMPLETO))
guardar_silver(orders, "orders")

# 2. Productos enriquecidos con pasillo y departamento
products = (leer_bronze("products")
    .dropDuplicates(["product_id"])
    .select(
        F.col("product_id").cast("long"),
        F.trim("product_name").alias("product_name"),
        F.col("aisle_id").cast("long"),
        F.col("department_id").cast("long"),
    ))

aisles = leer_bronze("aisles").select(
    F.col("aisle_id").cast("long"), F.trim("aisle").alias("aisle"))
departments = leer_bronze("departments").select(
    F.col("department_id").cast("long"), F.trim("department").alias("department"))

products_silver = (products
    .join(aisles, "aisle_id", "left")
    .join(departments, "department_id", "left"))
guardar_silver(products_silver, "products")

# 3. Detalle de órdenes: prior + train, limitado a las órdenes del alcance
op_prior = leer_bronze("order_products_prior").withColumn("origen", F.lit("prior"))
op_train = leer_bronze("order_products_train").withColumn("origen", F.lit("train"))

order_products = (op_prior.unionByName(op_train)
    .select(
        F.col("order_id").cast("long"),
        F.col("product_id").cast("long"),
        F.col("add_to_cart_order").cast("int"),
        F.col("reordered").cast("int"),
        F.col("origen"),
    )
    .filter(F.col("order_id").isNotNull() & F.col("product_id").isNotNull())
    .dropDuplicates(["order_id", "product_id"]))

antes = order_products.count()
order_products = order_products.join(orders.select("order_id"), "order_id", "left_semi")
despues = order_products.count()
print(f"order_products: {antes - despues} filas fuera del alcance (órdenes de usuarios incompletos)")
guardar_silver(order_products, "order_products")

order_detail = (order_products
    .join(orders.select("order_id", "user_id", "order_number", "order_dow",
                        "order_hour_of_day", "days_since_prior_order"),
          "order_id", "inner")
    .join(products_silver, "product_id", "left"))
guardar_silver(order_detail, "order_detail")

# 4. Validaciones de calidad
sin_producto = order_detail.filter(F.col("product_name").isNull()).count()
print(f"Filas de detalle sin producto en catálogo: {sin_producto}")
assert sin_producto == 0, "Hay product_id que no existen en products"

sin_orden = order_products.join(orders, "order_id", "left_anti").count()
print(f"Filas de order_products sin orden: {sin_orden}")
assert sin_orden == 0, "Hay order_id que no existen en orders"

usuarios_max = orders.agg(F.max("user_id")).first()[0]
print(f"Usuario máximo en orders: {usuarios_max} (debe ser menor a {USUARIO_MAX_COMPLETO})")
assert usuarios_max < USUARIO_MAX_COMPLETO, "orders contiene usuarios fuera del alcance"