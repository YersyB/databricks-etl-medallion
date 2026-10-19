CREATE SCHEMA IF NOT EXISTS dbw_etl_medallion.bronze
  MANAGED LOCATION 'abfss://bronze@stetlmedallionybu.dfs.core.windows.net/bronze_managed/';

CREATE SCHEMA IF NOT EXISTS dbw_etl_medallion.silver
  MANAGED LOCATION 'abfss://silver@stetlmedallionybu.dfs.core.windows.net/silver_managed/';

CREATE SCHEMA IF NOT EXISTS dbw_etl_medallion.gold
  MANAGED LOCATION 'abfss://gold@stetlmedallionybu.dfs.core.windows.net/gold_managed/';
