-- Test singular: el porcentaje de canceladas debe estar entre 0 y 100.
select transaction_date, country_code, cancelled_pct
from {{ ref('gold_daily_country_metrics') }}
where cancelled_pct < 0 or cancelled_pct > 100
