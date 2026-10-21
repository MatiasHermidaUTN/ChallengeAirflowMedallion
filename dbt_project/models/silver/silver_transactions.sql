-- Silver (paso 2): transacciones validas y deduplicadas.
-- Duplicado = mismo transaction_id y transaction_date (regla de Laura). Se conserva la fila
-- COMPLETED si existe, y si no la de ingesta mas reciente.
with valid as (

    select *
    from {{ ref('silver_typed_transactions') }}
    where coalesce(status, '') <> 'TEST'
      and amount is not null
      and amount >= 0
      and transaction_id is not null
      and transaction_date is not null

),

ranked as (

    select
        *,
        row_number() over (
            partition by transaction_id, transaction_date
            order by (status = 'COMPLETED') desc, _ingested_at desc
        ) as rn
    from valid

)

select
    transaction_id,
    customer_id,
    transaction_date,
    amount,
    status,
    -- country_code sin lista oficial (duda abierta): nulo -> 'UNKNOWN', no se descarta
    coalesce(country_code, 'UNKNOWN') as country_code,
    _source_file,
    _ingestion_date,
    _ingested_at
from ranked
where rn = 1
