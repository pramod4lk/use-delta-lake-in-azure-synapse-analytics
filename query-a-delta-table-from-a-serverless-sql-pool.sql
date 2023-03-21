-- Query a delta table from a serverless SQL pool

 -- This is auto-generated code
 SELECT
     TOP 100 *
 FROM
     OPENROWSET(
         BULK 'https://datalakexxxxxxx.dfs.core.windows.net/files/delta/products-delta/',
         FORMAT = 'DELTA'
     ) AS [result]