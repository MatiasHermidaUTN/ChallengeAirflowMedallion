{% test dbt_utils_free_positive(model, column_name) %}

-- Falla si hay valores <= 0 (evita depender del paquete dbt_utils en la v1).
select {{ column_name }} from {{ model }} where {{ column_name }} <= 0

{% endtest %}
