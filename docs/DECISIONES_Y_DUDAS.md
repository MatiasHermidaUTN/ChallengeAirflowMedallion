# Decisiones y dudas — pipeline Medallion de transacciones (Airflow + dbt)

Historial: la **v1** (commit en `main` antes de este merge) se escribió *antes* de hablar con Laura
(Data Engineering Lead), con un esquema inventado (`sales_*`). La **v2** (esta) se ajusta a sus respuestas.

## 1. Qué hace el pipeline
DAG diario `sales_medallion`: `ingest_bronze` -> `dbt_run_silver` -> `dbt_test_silver` -> `dbt_run_gold` -> `dbt_test_gold`.
Gold no corre si fallan los tests de Silver.

| Capa | Qué contiene |
|---|---|
| Bronze | `bronze.raw_transactions`: el CSV tal cual (todo string), **append-only y particionada por fecha de ingesta**. |
| Silver | `silver_typed_transactions` (tipado), `quarantine_transactions` (rechazadas con motivo) y `silver_transactions` (válidas y deduplicadas). |
| Gold | `gold_daily_country_metrics`: por día y país, facturación, ticket promedio, transacciones únicas y % de canceladas. |

## 2. Respuestas de Laura
| Tema | Respuesta |
|---|---|
| Warehouse y Airflow | PostgreSQL o Snowflake compatible con ANSI SQL; ya existe un entorno de Airflow. |
| Archivo | Nombre tipo `transactions_raw.csv`, diario. |
| Esquema | `transaction_id`, `customer_id`, `transaction_date`, `amount`, `status`, `country_code`, todo string. |
| Status | `COMPLETED`, `PENDING`, `CANCELLED`, `TEST`. |
| Duplicado | Mismo `transaction_id` y `transaction_date`; se conserva COMPLETED o, si no, la ingesta más reciente. |
| Formatos | Fechas `YYYY-MM-DD` y `YYYY/MM/DD`; `amount` puede traer símbolos de moneda. |
| Inválidos | Descartar `TEST`, montos negativos o nulos, enviándolos a una **tabla de cuarentena**. |
| Gold | Por día y país: monto facturado (solo COMPLETED), ticket promedio, transacciones únicas, % canceladas sobre el total procesado. |
| Bronze | Append-only, particionada por fecha de ingesta. |
| Sin información | Lista de países, SLAs, reintentos, convenciones de CI/tests dbt: "tendría que consultarlo". |

## 3. Qué cambié respecto de la v1
- Esquema real (`transaction_*`, `country_code`) en lugar de `order_*`/`product`/`currency`.
- Bronze pasó de "borrar y recargar por archivo" a **append-only con partición diaria**; reintentar un día reemplaza solo su partición.
- Silver ahora separa tipado, cuarentena y validación/deduplicación con la regla de duplicados de Laura.
- Gold dejó de ser por producto/moneda: ahora es por país con las 4 métricas pedidas.
- Tests dbt nuevos: motivos de cuarentena, sin negativos en Silver, unicidad `(id, fecha)`, % de canceladas entre 0 y 100.

## 4. Decisiones y supuestos (míos, a confirmar)
| # | Decisión / supuesto |
|---|---|
| D1 | Bronze se carga con Python (dbt no ingesta archivos); el dato crudo no se toca. |
| D2 | Se agregan a cuarentena filas sin `transaction_id` o con fecha no parseable (no se pueden deduplicar). Laura solo confirmó TEST, negativos y nulos. |
| D3 | Monto `0` es válido (solo se rechazan nulos y negativos, como dijo Laura). |
| D4 | `country_code` nulo se reemplaza por `UNKNOWN` y la fila no se descarta (no tengo lista de países). |
| D5 | "Total procesado" para el % de canceladas = filas válidas deduplicadas de todos los status, incluido PENDING. |
| D6 | Ticket promedio = promedio del monto de las COMPLETED. |
| D7 | "Transacciones únicas" = ids distintos entre las válidas, de cualquier status. |
| D8 | `amount` se limpia dejando dígitos, punto y signo (`$1,250.50` -> `1250.50`); asume punto decimal. |
| D9 | Silver y Gold son tablas recalculadas completas en cada corrida (full refresh) sobre todo Bronze. |
| D10 | Credenciales por variables de entorno; sin secretos en el repo. |

## 5. Dudas abiertas
1. ¿`amount` puede venir con coma decimal (`1.250,50`)? Hoy se asume punto.
2. Lista válida de `country_code` (ISO-2) y qué hacer con inválidos.
3. ¿PENDING debe entrar en el % de canceladas? Asumí que sí.
4. ¿Silver/Gold deberían ser incrementales? Con mucho histórico, el full refresh no escala.
5. SLA/horario, alertas y reintentos del DAG (puse 2 reintentos de 5 min).
6. Convención de Git/CI y tests dbt mínimos del equipo.
7. Si el archivo se pisa cada día con el mismo nombre, ¿cómo se detecta un archivo repetido o atrasado?
8. Zona horaria de `transaction_date` y de la fecha de ingesta (usa la fecha lógica de Airflow).

## 6. Estrategia de Git
Sin convención confirmada por el equipo, uso: `main` protegida, ramas `feature/<descripcion>`, PR con revisión y CI verde, merge `--no-ff` (conserva el historial).
Para este cambio: `feature/transactions_schema_and_quarantine`. Rollback con `git revert -m 1` del merge; no hay migraciones de datos destructivas (Bronze solo se agrega).

## 7. Qué NO verifiqué
Esta máquina no tenía Postgres ni Docker en ejecución, así que no corrí dbt ni el DAG: verifiqué solo que el DAG compila y revisé el SQL a mano. El siguiente paso sería `docker compose up` y `dbt build` con el CSV de ejemplo.
