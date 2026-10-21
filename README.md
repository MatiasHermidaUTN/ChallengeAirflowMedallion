# Pipeline Medallion de ventas (Airflow + dbt)

DAG `sales_medallion`: CSV de ventas -> Bronze -> Silver -> Gold, con tests de calidad en dbt.

- `dags/sales_medallion_dag.py` orquestación
- `dbt_project/` modelos Silver y Gold, tests y profile
- `data/raw/` CSV de ejemplo
- `docker-compose.yml` + `Dockerfile` entorno local (Postgres + Airflow)
- `tests/` integridad del DAG
- `docs/DECISIONES_Y_DUDAS.md` decisiones, supuestos y dudas

Local: `docker compose up --build`, UI en http://localhost:8080 (usuario/clave en los logs de `standalone`),
crear la conexión `warehouse` si no se usa la variable `AIRFLOW_CONN_WAREHOUSE` y activar el DAG.
