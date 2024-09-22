{{ config(materialized='incremental', unique_key='load_date') }}

WITH source_data AS (
    SELECT *
    FROM file('{{ var("clickhouse_data_lake_mart") }}', 'CSVWithNames',
        'card_order_dt String,
        card_num String,
        cookie String,
        url String,
        transaction_level Boolean,
        status_flag Boolean,
        load_date String')
),
existing_data AS (
    SELECT *
    FROM {{ this }}  -- Существующая таблица
)
SELECT *
FROM source_data
WHERE load_date NOT IN (SELECT load_date FROM existing_data)  -- Добавляем только новые записи