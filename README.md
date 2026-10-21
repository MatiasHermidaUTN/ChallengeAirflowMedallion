# Pipeline Medallion de transacciones (Airflow + dbt)

DAG `sales_medallion`: `transactions_raw.csv` -> Bronze -> Silver (+ cuarentena) -> Gold, con tests de calidad en dbt entre capas.

- `dags/sales_medallion_dag.py` orquestación; Bronze append-only particionada por fecha de ingesta
- `dbt_project/models/silver/` tipado, cuarentena y deduplicación
- `dbt_project/models/gold/` métricas diarias por país
- `dbt_project/tests/` tests singulares; los genéricos están en los `schema.yml`
- `data/raw/transactions_raw.csv` ejemplo con casos borde (TEST, negativos, nulos, duplicados, ambos formatos de fecha)
- `docker-compose.yml` + `Dockerfile` entorno local (Postgres + Airflow)
- `tests/` integridad del DAG
- `docs/DECISIONES_Y_DUDAS.md` decisiones, respuestas de Laura, dudas abiertas y estrategia de Git

Local: `docker compose up --build`, UI en http://localhost:8080 (usuario y clave en los logs de `standalone`), activar el DAG `sales_medallion`.
La conexión `warehouse` llega por la variable `AIRFLOW_CONN_WAREHOUSE`.
