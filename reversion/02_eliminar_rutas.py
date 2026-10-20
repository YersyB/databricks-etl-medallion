# Elimina las rutas físicas de las capas Bronze, Silver y Gold.
# NUNCA toca raw: ahí están los datos de origen.

dbutils.widgets.text("confirmar", "")
dbutils.widgets.text("storage", "stetlmedallionybu")

# Protección: evita ejecuciones accidentales
if dbutils.widgets.get("confirmar") != "SI":
    raise Exception("Reversión cancelada. Escribe SI en el widget 'confirmar' para ejecutar.")

storage = dbutils.widgets.get("storage")

rutas = [
    f"abfss://bronze@{storage}.dfs.core.windows.net/bronze_managed/",
    f"abfss://silver@{storage}.dfs.core.windows.net/silver_managed/",
    f"abfss://gold@{storage}.dfs.core.windows.net/gold_managed/",
]

for ruta in rutas:
    if dbutils.fs.exists(ruta):
        dbutils.fs.rm(ruta, True)
        print(f"Eliminada: {ruta}")
    else:
        print(f"No existe: {ruta}")

# Verificación: las rutas de Gold, Silver y Bronze ya no deben existir
for ruta in rutas:
    assert not dbutils.fs.exists(ruta), f"Todavía existe: {ruta}"
print("Reversión completa. raw no fue modificado.")