SELECT
    TICKER,
    PRIMARY_EXCHANGE_NAME,
    DATE,
    VALUE,
    data_source_name,
    {{dbt_utils.pivot(
        column = 'VARIABLE_NAME',
        values = dbt_utils.get_column_values(ref('stg_stock_price_time_series'), 'VARIABLE_NAME'),
        then_value = 'value'
    )}}
FROM {{ref('stg_stock_price_time_series')}}
GROUP BY TICKER,PRIMARY_EXCHANGE_NAME,DATE,VALUE,data_source_name