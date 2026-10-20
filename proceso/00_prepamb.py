# Databricks notebook source
# Databricks notebook source
#dbutils.widgets.dropdown("entorno", "dev", ["dev", "prod", "ambos"])
#entorno = dbutils.widgets.get("entorno")
entorno = "ambos"
print(f"{entorno}:")

# Definir 2 catálogos
catalogos = {
    "dev": "dbw_etl_medallion",
    "prod": "prod_medallion"
}

# Crear ambos
if entorno == "ambos":
    catalogos_a_crear = list(catalogos.values())
else:
    catalogos_a_crear = [catalogos[entorno]]

for catalog in catalogos_a_crear:
    print(f"\n--- Creando estructura en CATÁLOGO: {catalog} ---")
    
    # 1. Crear el catálogo si no existe (solo si eres Metastore Admin)
    try:
        spark.sql(f"CREATE CATALOG IF NOT EXISTS `{catalog}`")
        print(f"Catálogo listo: {catalog}")
    except Exception as e:
        print(f"No pude crear catálogo {catalog} (ya existe o no eres admin): {e}")

    # 2. Crear las 3 capas
    for capa in ["bronze", "silver", "gold"]:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{capa}`")
        print(f"  -> Schema listo: {catalog}.{capa}")

# 3. Verificar todo
for catalog in catalogos_a_crear:
    print(f"\nSchemas en {catalog}:")
    display(spark.sql(f"SHOW SCHEMAS IN `{catalog}`"))