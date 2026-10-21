-- Silver: tipado, limpieza y deduplicacion de Bronze.
-- Supuesto: la clave de negocio es order_id; si se repite, gana la carga mas reciente.
with source as (

    select * from {{ source('bronze', 'raw_sales') }}

),

cleaned as (

    select
        trim(order_id)                                  as order_id,
        case
            when trim(order_date) ~ '^\d{4}-\d{2}-\d{2}$' then to_date(trim(order_date), 'YYYY-MM-DD')
            when trim(order_date) ~ '^\d{2}/\d{2}/\d{4}$' then to_date(trim(order_date), 'DD/MM/YYYY')
        end                                             as order_date,
        nullif(trim(customer_id), '')                   as customer_id,
        initcap(trim(product))                          as product,
        nullif(trim(quantity), '')::integer             as quantity,
        -- algunos precios llegan con coma decimal ("12,50")
        nullif(replace(trim(unit_price), ',', '.'), '')::numeric(12, 2) as unit_price,
        upper(trim(currency))                           as currency,
        upper(trim(status))                             as status,
        _source_file,
        _ingested_at
    from source

),

deduplicated as (

    select
        *,
        row_number() over (partition by order_id order by _ingested_at desc) as rn
    from cleaned
    where order_id is not null and order_id <> ''

)

select
    order_id,
    order_date,
    customer_id,
    product,
    quantity,
    unit_price,
    quantity * unit_price as total_amount,
    currency,
    status,
    _source_file,
    _ingested_at
from deduplicated
where rn = 1
