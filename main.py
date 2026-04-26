import logging
from pendulum import datetime
from utils.s3.s3_utils import (
    s3_create_client,
    s3_delete_bucket,
    s3_create_bucket,
    s3_load_file,
)
from utils.ingestion.ingestion_utils import (
    ingestion_get_urls,
    ingestion_get_metainfo,
)

# Конфигурация логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

BUCKET = 'nyc-taxi'
DATE_FROM = datetime(2026, 1, 1)
DATE_TO = datetime(2026, 12, 31)


def load_data_to_s3():
    # Подготовка S3
    s3_client = s3_create_client()
    s3_delete_bucket(s3_client, BUCKET)
    s3_create_bucket(s3_client, BUCKET)

    # Подготовка metainfo
    urls = ingestion_get_urls(DATE_FROM, DATE_TO)
    metainfo = ingestion_get_metainfo(urls)

    # Загрузка в S3
    s3_load_file(s3_client, BUCKET, metainfo)


if __name__ == '__main__':
    load_data_to_s3()
