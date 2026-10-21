-- Silver (paso 1): tipado y normalizacion de TODAS las filas de Bronze, sin descartar nada.
-- Lo que no se puede parsear queda en NULL y lo detecta la validacion del paso siguiente.
select
    nullif(trim(transaction_id), '')            as transaction_id,
    nullif(trim(customer_id), '')               as customer_id,
    case
        when trim(transaction_date) ~ '^\d{4}-\d{2}-\d{2}$' then to_date(trim(transaction_date), 'YYYY-MM-DD')
        when trim(transaction_date) ~ '^\d{4}/\d{2}/\d{2}$' then to_date(trim(transaction_date), 'YYYY/MM/DD')
    end                                         as transaction_date,
    -- amount puede traer simbolos de moneda ("$1,250.50"): se dejan solo digitos, punto y signo
    case
        when regexp_replace(coalesce(amount, ''), '[^0-9.-]', '', 'g') ~ '^-?\d+(\.\d+)?$'
            then regexp_replace(amount, '[^0-9.-]', '', 'g')::numeric(14, 2)
    end                                         as amount,
    upper(nullif(trim(status), ''))             as status,
    upper(nullif(trim(country_code), ''))       as country_code,
    _source_file,
    _ingestion_date,
    _ingested_at
from {{ source('bronze', 'raw_transactions') }}
