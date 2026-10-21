-- Test singular: una sola fila por dia y pais en Gold.
select transaction_date, country_code, count(*) as n
from {{ ref('gold_daily_country_metrics') }}
group by transaction_date, country_code
having count(*) > 1
