# Spark Data Pipeline — NYC Taxi

End-to-end пайплайн обработки данных поездок такси Нью-Йорка (NYC Yellow Taxi) с использованием Apache Spark, MinIO (S3) и ClickHouse.

## Архитектура

```
Ingestion (Python) → MinIO (Bronze/S3)
     ↓
Spark (чтение из S3, чистка, расчёт метрик)
     ↓
MinIO (Silver/Gold — агрегаты)
     ↓
ClickHouse (аналитика/дашборды)
```

## Компоненты

- **MinIO** — S3-совместимое объектное хранилище. Сырые parquet-файлы и результаты агрегации.
- **Ingestion** — загрузка данных из открытого API NYC TLC в MinIO.
- **Spark** — читает `s3a://nyc-taxi/yellow_tripdata/`, чистит `NaN`, считает метрики (длительность поездки, выручка, tip_ratio, стоимость мили) и пишет агрегаты по дням, зонам и типам оплаты.
- **ClickHouse** — колоночная БД для быстрых аналитических запросов и визуализации.
- **Jupyter** — PySpark ноутбуки для ad-hoc анализа.
- **Kubernetes RBAC** — манифест `spark-rbac.yaml` для запуска Spark на K8s.

## Запуск

```bash
docker compose up -d
```

1. Утилита `main.py` загружает данные за 2025–2026 в MinIO.
2. Spark-приложение `spark/app.py` запускается в кластере, читает из MinIO, трансформирует и пишет результат обратно.
3. ClickHouse доступен на `localhost:8123` (HTTP) и `localhost:9004` (native).

Команда по запуску расчета:
```bash
 spark-submit \
  --master k8s://https://127.0.0.1:6443 \
  --deploy-mode cluster \
  --name spark-s3-job \
  --conf spark.kubernetes.namespace=spark \
  --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
  --conf spark.kubernetes.container.image=apache/spark:3.5.0 \
  --conf spark.kubernetes.file.upload.path=/tmp \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --conf spark.hadoop.fs.s3a.endpoint=http://host.docker.internal:9000 \
  --conf spark.hadoop.fs.s3a.access.key=minioadmin \
  --conf spark.hadoop.fs.s3a.secret.key=minioadmin \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
  --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  --py-files spark/app.py \
  spark/app.py
```

Пример запроса в Clickhouse:
```sql
SELECT *
FROM s3(
    'http://minio:9000/nyc-taxi/yellow_tripdata/daily/*.parquet',
    'minioadmin',
    'minioadmin',
    'Parquet'
);
```