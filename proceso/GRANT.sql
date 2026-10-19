-- seguridad/grants.sql
-- Permisos de lectura sobre las tablas Gold creadas por el ETL.
-- Reemplaza `account users` por tu grupo si quieres restringirlo.

GRANT USE CATALOG ON CATALOG dbw_etl_medallion TO `account users`;
GRANT USE SCHEMA ON SCHEMA dbw_etl_medallion.gold TO `account users`;

GRANT SELECT ON TABLE dbw_etl_medallion.gold.desempeno_producto TO `account users`;
GRANT SELECT ON TABLE dbw_etl_medallion.gold.patron_compra_tiempo TO `account users`;
GRANT SELECT ON TABLE dbw_etl_medallion.gold.resumen_clientes TO `account users`;
