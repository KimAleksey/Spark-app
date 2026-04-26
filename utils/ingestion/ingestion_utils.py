import logging
from pendulum import datetime, formatting

DATE_FROM = datetime(2026, 1, 1)
DATE_TO = datetime(2026, 12, 31)
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
FILE_PREFIX = "yellow_tripdata"

# Конфигурация логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

def ingestion_get_urls(date_from: datetime, date_to: datetime) -> list[str]:
    """
    Получаем список url для загрузки

    :param month_from: Дата с
    :param month_to: Дата по
    :return: Список URL
    """
    diff_in_months = date_to.diff(date_from).in_months() + 1

    urls = []
    for i in range(diff_in_months):
        file_name = FILE_PREFIX + f"_{date_from.add(months=i).format("YYYY-MM")}.parquet"
        url = BASE_URL + file_name
        urls.append(url)
    logging.info(f"URLS: {urls}")
    return urls


def ingestion_get_metainfo(urls: list[str]) -> list[dict[str, str]]:
    """
    Получаем список словарей (путь в s3, url, имя файла).

    :param files: Список url для загрузки
    :return: Список словарей {url: str, key: str}
    """
    metainfo = []
    for url in urls:
        file_name = url.split("/")[-1]
        key = FILE_PREFIX + "/" + file_name.split(".")[0][16:20] + "/" + file_name.split(".")[0][21:23] + "/data.parquet"
        buff = {
            "url": url,
            "key": key,
        }
        metainfo.append(buff)
    return metainfo


if __name__ == '__main__':
    urls = ingestion_get_urls(DATE_FROM, DATE_TO)
    dicts = ingestion_get_metainfo(urls)