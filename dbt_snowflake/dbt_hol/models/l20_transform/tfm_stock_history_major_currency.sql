{{ 
config(
	  materialized='view', 
      tags=["Reference Data"]
	  ) 
}}

SELECT
    tsh.*,
    fx_gbp.value * open     AS gbp_open,
    fx_gbp.value * high	    AS gbp_high,
    fx_gbp.value * low      AS gbp_low,
    fx_gbp.value * close    AS gbp_close,
    fx_eur.value * open     AS eur_open,
    fx_eur.value * high	    AS eur_high,
    fx_eur.value * low      AS eur_low,
    fx_eur.value * close    AS eur_close
FROM
    {{ref('tfm_stock_history')}} tsh,
    {{ref('tfm_fx_rates')}} fx_gbp,
    {{ref('tfm_fx_rates')}} fx_eur
WHERE 
    fx_gbp.QUOTE_CURRENCY_ID = 'GBP' AND
    fx_eur.QUOTE_CURRENCY_ID = 'EUR' AND
    tsh.date = fx_gbp.date AND
    tsh.date = fx_eur.date
