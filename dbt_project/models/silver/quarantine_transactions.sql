-- Cuarentena: filas descartadas con el motivo. Reglas confirmadas por Laura:
-- status TEST, monto negativo o nulo. Sumo (supuesto): sin id o fecha invalida, porque no se pueden deduplicar.
select
    *,
    case
        when status = 'TEST'                  then 'test_status'
        when amount is null                   then 'null_or_invalid_amount'
        when amount < 0                       then 'negative_amount'
        when transaction_id is null           then 'missing_transaction_id'
        when transaction_date is null         then 'invalid_transaction_date'
    end as quarantine_reason
from {{ ref('silver_typed_transactions') }}
where status = 'TEST'
   or amount is null
   or amount < 0
   or transaction_id is null
   or transaction_date is null
