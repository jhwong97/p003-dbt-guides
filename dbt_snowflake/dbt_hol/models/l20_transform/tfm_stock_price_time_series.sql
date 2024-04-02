WITH cte AS(
    SELECT 
        TICKER,
        PRIMARY_EXCHANGE_NAME,
        VARIABLE_NAME,
        DATE,
        VALUE,
        data_source_name
    FROM 
        {{ref('stg_stock_price_time_series')}} src
    WHERE
        VARIABLE_NAME IN ('Post-Market Close', 'Pre-Market Open', 'All-Day High', 'All-Day Low', 'Nasdaq Volume')
)
SELECT *
FROM cte
PIVOT(SUM(Value) FOR VARIABLE_NAME IN ('Post-Market Close', 'Pre-Market Open', 'All-Day High', 'All-Day Low', 'Nasdaq Volume'))
AS p(TICKER,PRIMARY_EXCHANGE_NAME,DATE,data_source_name,close, open, high, low, volume)
