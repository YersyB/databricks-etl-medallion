-- Elimina las tablas lógicas creadas por el ETL.
-- Orden: primero Gold, luego Silver y al final Bronze, por dependencias.

DROP TABLE IF EXISTS dbw_etl_medallion.gold.desempeno_producto;
DROP TABLE IF EXISTS dbw_etl_medallion.gold.patron_compra_tiempo;
DROP TABLE IF EXISTS dbw_etl_medallion.gold.resumen_clientes;

DROP TABLE IF EXISTS dbw_etl_medallion.silver.order_detail;
DROP TABLE IF EXISTS dbw_etl_medallion.silver.order_products;
DROP TABLE IF EXISTS dbw_etl_medallion.silver.orders;
DROP TABLE IF EXISTS dbw_etl_medallion.silver.products;

DROP TABLE IF EXISTS dbw_etl_medallion.bronze.order_products_train;
DROP TABLE IF EXISTS dbw_etl_medallion.bronze.order_products_prior;
DROP TABLE IF EXISTS dbw_etl_medallion.bronze.orders;
DROP TABLE IF EXISTS dbw_etl_medallion.bronze.products;
DROP TABLE IF EXISTS dbw_etl_medallion.bronze.aisles;
DROP TABLE IF EXISTS dbw_etl_medallion.bronze.departments;