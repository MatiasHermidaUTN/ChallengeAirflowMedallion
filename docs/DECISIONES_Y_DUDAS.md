# Decisiones y dudas — pipeline Medallion de ventas (Airflow + dbt)

> **v1**: escrita *antes* de hablar con Laura (Data Engineering Lead). Todo lo marcado como supuesto está por confirmar.

## 1. Qué construí
DAG diario `sales_medallion`: `ingest_bronze` -> `dbt_run_silver` -> `dbt_test_silver` -> `dbt_run_gold` -> `dbt_test_gold`.
- **Bronze** (Python): carga el CSV del día tal cual (todo texto) en `bronze.raw_sales`, con `_source_file` y `_ingested_at`. Idempotente por archivo.
- **Silver** (dbt): tipado, limpieza (fechas en 2 formatos, coma decimal, mayúsculas) y deduplicación por `order_id`.
- **Gold** (dbt): ventas diarias por moneda y producto (órdenes, unidades, revenue, ticket promedio) solo de ventas COMPLETED.
- **Calidad**: tests de dbt (unique, not_null, accepted_values, positivo) entre Silver y Gold: si fallan, Gold no corre.
- Entorno local: `docker-compose` con Postgres (warehouse) + Airflow standalone con dbt-postgres.

## 2. Supuestos (a confirmar)
| # | Supuesto |
|---|---|
| S1 | Un archivo por día, `sales_YYYY-MM-DD.csv`, en una carpeta local. |
| S2 | Columnas: order_id, order_date, customer_id, product, quantity, unit_price, currency, status. |
| S3 | Warehouse PostgreSQL (el enunciado no lo dice). |
| S4 | `order_id` es la clave de negocio; ante duplicados gana la carga más reciente. |
| S5 | Fechas `YYYY-MM-DD` o `DD/MM/YYYY`; precios pueden traer coma decimal. |
| S6 | Revenue solo de ventas COMPLETED. |
| S7 | Monedas ARS/USD/EUR; no se convierte entre monedas. |

## 3. Decisiones
- D1 Bronze en Python y no en dbt: dbt no carga archivos; así se conserva el dato crudo.
- D2 Tests entre capas como compuerta de calidad.
- D3 `catchup=False` y `max_active_runs=1` para no pisar cargas.
- D4 Test genérico propio en lugar de `dbt_utils`, para no sumar dependencias.

## 4. Dudas para Laura
1. ¿Cuál es el warehouse real (BigQuery, Snowflake, Postgres, Redshift)? ¿Hay entorno de Airflow existente?
2. ¿De dónde llegan los CSV (S3, GCS, SFTP, carpeta) y con qué frecuencia/nombre? ¿Llegan tarde o reprocesos?
3. ¿Cuál es el esquema exacto del CSV y qué problemas de calidad conocen?
4. ¿Qué hacer con filas inválidas: rechazar, cuarentena o fallar el pipeline?
5. ¿Cómo se define "venta" para Finanzas (cancelados, reembolsos, impuestos, descuentos)?
6. ¿Qué métricas y granularidad necesita el reporte Gold? ¿Hay SLA de entrega?
7. ¿Cómo se manejan las monedas (conversión, tipo de cambio)?
8. ¿Carga incremental o full refresh? ¿Hay histórico a cargar (backfill)?
9. ¿Convenciones del equipo: naming de capas, dbt tests mínimos, CI y Git?
10. ¿Alertas/monitoreo esperados ante fallas?
