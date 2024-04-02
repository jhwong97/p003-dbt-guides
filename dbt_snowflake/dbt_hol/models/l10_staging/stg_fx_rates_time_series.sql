SELECT
    VARIABLE,
    BASE_CURRENCY_ID,
    QUOTE_CURRENCY_ID,
    BASE_CURRENCY_NAME,
    QUOTE_CURRENCY_NAME,
    DATE,
    VALUE,
    'CYBERSYN.FX_RATES_TIMESERIES' data_source_name
FROM {{source('FINANCIAL__ECONOMIC_ESSENTIALS', 'FX_RATES_TIMESERIES')}} src