# mtech-daas-product-pdata.daas_internal.ddl_metadata_for_null --> dump from TD dbc.columns

CREATE TEMP FUNCTION MakePartitionByExpression(
  column_name STRING, data_type STRING
) AS (
  IF(
    column_name = '_PARTITIONTIME',
    'DATE(_PARTITIONTIME)',
    IF(
      data_type = 'TIMESTAMP',
      CONCAT('DATE(', column_name, ')'),
      column_name
    )
  )
);

CREATE TEMP FUNCTION MakePartitionByClause(
  columns ARRAY<STRUCT<column_name STRING, data_type STRING, is_nullable STRING,  is_partitioning_column STRING, clustering_ordinal_position INT64>>
) AS (
  IFNULL(
    CONCAT(
      'PARTITION BY ',
      (SELECT MakePartitionByExpression(column_name, data_type)
       FROM UNNEST(columns) WHERE is_partitioning_column = 'YES'),
      '\n'),
    ''
  )
);

CREATE TEMP FUNCTION MakeClusterByClause(
  columns ARRAY<STRUCT<column_name STRING, data_type STRING, is_nullable STRING,  is_partitioning_column STRING, clustering_ordinal_position INT64>>
) AS (
  IFNULL(
    CONCAT(
      'CLUSTER BY ',
      (SELECT STRING_AGG(column_name, ', ' ORDER BY clustering_ordinal_position)
        FROM UNNEST(columns) WHERE clustering_ordinal_position IS NOT NULL),
      '\n'
    ),
    ''
  )
);

CREATE TEMP FUNCTION MakeNullable(data_type STRING, is_nullableN STRING)
AS (
  IF(not STARTS_WITH(data_type, 'ARRAY<') and is_nullableN = 'NO', ' NOT NULL', '')
);

CREATE TEMP FUNCTION MakeColumnList(
  columns ARRAY<STRUCT<column_name STRING, data_type STRING, is_nullable STRING,  is_partitioning_column STRING, clustering_ordinal_position INT64>>
) AS (
  IFNULL(
    CONCAT(
      '(\n',
      (SELECT STRING_AGG(CONCAT('  ', column_name, ' ', data_type,  MakeNullable(data_type, is_nullable)), ',\n')
       FROM UNNEST(columns)),
      '\n)\n'
    ),
    ''
  )
);

CREATE TEMP FUNCTION MakeOptionList(
  options ARRAY<STRUCT<option_name STRING, option_value STRING>>
) AS (
  IFNULL(
    CONCAT(
      'OPTIONS (\n',
      (SELECT STRING_AGG(CONCAT('  ', option_name, '=', option_value), ',\n') FROM UNNEST(options)),
      '\n)\n'),
    ''
  )
);
WITH is_null  AS (
select bq.*, case when td.NULLABLE='Y' THEN 'YES'
WHEN td.NULLABLE='N'  THEN 'NO'
ELSE bq.is_nullable
END is_nullableN
from  `mtech-daas-transact-pdata.trst_sls`.INFORMATION_SCHEMA.COLUMNS bq
Join `mtech-daas-product-pdata.daas_internal.ddl_metadata_for_null`td
on upper(td.tableName)=upper(BQ.Table_name)
and upper(td.ColumnName)=upper(bq.column_name)),
Components AS (
  SELECT
    CONCAT('`', table_catalog, '.', table_schema, '.', table_name, '`') AS table_name,
    ARRAY_AGG(
      STRUCT(column_name, data_type, is_nullableN, is_partitioning_column, clustering_ordinal_position)
      ORDER BY ordinal_position
    ) AS columns,
     CONCAT('\`', table_catalog, '.', 'daas_internal', '.', table_name, '\`') AS daas_table_name,
    (SELECT ARRAY_AGG(STRUCT(option_name, option_value))
     FROM   `mtech-daas-transact-pdata.trst_sls`.INFORMATION_SCHEMA.TABLE_OPTIONS AS t2
     WHERE t.table_name = t2.table_name) AS options
  FROM `mtech-daas-transact-pdata.trst_sls`.INFORMATION_SCHEMA.TABLES AS t
  LEFT JOIN is_null t2
  USING (table_catalog, table_schema, table_name)
  WHERE table_type = 'BASE TABLE'
  AND  table_name in ('tran_item_summary','tran_item','tran','tran_item_return')
  GROUP BY table_catalog, table_schema, t.table_name
)
SELECT
  CONCAT(
  'bq query --use_legacy_sql=false "CREATE OR REPLACE TABLE ',
    daas_table_name,
     '\n',
     'AS',
    '\n',
    'SELECT * FROM ',table_name,';',
    '\n',
    'CREATE OR REPLACE TABLE ',
    table_name,
    '\n',
    MakeColumnList(columns),
    MakePartitionByClause(columns),
    MakeClusterByClause(columns),
    MakeOptionList(options),';',
    '\n',
    'INSERT INTO',table_name,
    '\n',
    'SELECT * FROM',daas_table_name,';"'
    )
FROM Components

