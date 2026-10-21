-- Test singular: ninguna fila de Silver puede tener monto negativo (las negativas van a cuarentena).
select transaction_id, amount
from {{ ref('silver_transactions') }}
where amount < 0
