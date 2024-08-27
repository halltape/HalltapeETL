
SELECT *
FROM file('{{ var("clickhouse_data_lake_mart") }}', 'CSVWithNames',
    'card_order_dt String,
    card_num String,
    cookie String,
    url String,
    transaction_level Boolean,
    status_flag Boolean,
    load_date String')