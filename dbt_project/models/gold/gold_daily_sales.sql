-- Gold: ventas diarias por moneda y producto para el reporte de Producto y Finanzas.
-- Supuesto: solo cuentan ventas COMPLETED (canceladas y reembolsadas quedan afuera).
select
    order_date,
    currency,
    product,
    count(*)            as orders_count,
    sum(quantity)       as units_sold,
    sum(total_amount)   as revenue,
    round(avg(total_amount), 2) as avg_order_value
from {{ ref('silver_sales') }}
where status = 'COMPLETED'
group by order_date, currency, product
