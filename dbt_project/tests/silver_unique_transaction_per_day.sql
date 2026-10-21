-- Test singular: (transaction_id, transaction_date) es unica en Silver (la regla de duplicado).
select transaction_id, transaction_date, count(*) as n
from {{ ref('silver_transactions') }}
group by transaction_id, transaction_date
having count(*) > 1
