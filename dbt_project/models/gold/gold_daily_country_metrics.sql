-- Gold: metricas diarias por pais para Producto y Finanzas (definidas por Laura):
--   * monto total facturado: solo COMPLETED
--   * ticket promedio: promedio del monto de las COMPLETED (supuesto)
--   * transacciones unicas: transacciones procesadas (validas y deduplicadas)
--   * porcentaje de canceladas: CANCELLED / total procesado (incluye PENDING; supuesto)
select
    transaction_date,
    country_code,
    coalesce(sum(amount) filter (where status = 'COMPLETED'), 0)       as total_billed,
    round(avg(amount) filter (where status = 'COMPLETED'), 2)          as avg_ticket,
    count(distinct transaction_id)                                     as unique_transactions,
    round(
        100.0 * count(*) filter (where status = 'CANCELLED') / count(*), 2
    )                                                                  as cancelled_pct
from {{ ref('silver_transactions') }}
group by transaction_date, country_code
