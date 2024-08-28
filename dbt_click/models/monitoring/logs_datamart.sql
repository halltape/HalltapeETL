
WITH tmp AS
    (
        SELECT *
        FROM file('{{ var("clickhouse_data_lake_logs") }}', 'CSV',
            'datamart_name String,
            load_date String,
            total_rows Int64,
            created_at String')
    )
SELECT
    datamart_name,
    load_date, 
    total_rows,
    MAX(created_at) AS created_at
FROM tmp
GROUP BY datamart_name, load_date, total_rows