import boto3
import logging
from dotenv import load_dotenv
from os import getenv
from botocore.exceptions import ClientError
from requests import get
from requests.exceptions import HTTPError

# Конфигурация логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)


def s3_create_client() -> boto3.client:
    """
    Client для работы с S3

    :return: S3 Client
    """
    try:
        load_dotenv()
        logging.info("Получены секреты.")
    except FileNotFoundError as e:
        raise e

    try:
        client = boto3.client(
            's3',
            endpoint_url="http://localhost:9000",
            aws_access_key_id=getenv('S3_ACCESS_KEY'),
            aws_secret_access_key=getenv('S3_SECRET_KEY'),
        )
        logging.info("Успешно подключено к S3.")
        return client
    except Exception as e:
        raise e


def s3_check_bucket_exists(s3_client: boto3.client, bucket_name: str) -> bool:
    """
    Проверка, что bucket создан

    :param s3_client: S3 client
    :param bucket_name: Bucket name
    :return: True/False - Bucket exists
    """
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        return True
    except ClientError as e:
        error_code = int(e.response["Error"]["Code"])
        if error_code == 404:
            return False
        else:
            raise


def s3_create_bucket(s3_client: boto3.client, bucket_name: str) -> None:
    """
    Создаем bucket

    :param s3_client: S3 client
    :param bucket_name: Bucket name
    :return: None
    """
    if not s3_check_bucket_exists(s3_client, bucket_name):
        try:
            s3_client.create_bucket(Bucket=bucket_name)
            logging.info(f"Bucket {bucket_name} создан.")
        except Exception as e:
            raise e
    else:
        logging.info(f"Bucket {bucket_name} уже существует.")


def s3_delete_bucket(s3_client: boto3.client, bucket_name: str) -> None:
    """
    Удаляем bucket

    :param s3_client: S3 client
    :param bucket_name: Bucket name
    :return: None
    """
    if s3_check_bucket_exists(s3_client, bucket_name):
        try:
            s3_clear_path(s3_client, bucket_name, "")
            s3_client.delete_bucket(Bucket=bucket_name)
            logging.info(f"Bucket {bucket_name} удален")
        except Exception as e:
            raise e
    else:
        logging.info(f"Bucket {bucket_name} не существует")


def s3_clear_path(s3_client, bucket_name: str, path: str) -> None:
    """
    Очищает все объекты по prefix (path) в bucket.

    :param s3_client: S3 client
    :param bucket_name: Bucket name
    :return: None
    """
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        paginator = s3_client.get_paginator("list_objects_v2")

        for page in paginator.paginate(Bucket=bucket_name, Prefix=path):
            if "Contents" in page:
                for obj in page["Contents"]:
                    s3_client.delete_object(
                        Bucket=bucket_name,
                        Key=obj["Key"]
                    )
                    logging.info(f"Deleted: {obj['Key']}")
        logging.info(f"Path '{path}' cleared")
    except Exception as e:
        logging.error(f"Error clearing path: {e}")
        raise


def s3_load_file(
        s3_client: boto3.client,
        bucket: str,
        metainfo: list[dict[str, str]],
    ) -> None:
        """
        Скачивает файл по URL и загружает его в S3 (через память).

        :param s3_client: boto3 S3 client
        :param bucket: имя bucket
        :param metainfo: metainfo - url, key
        """
        for file_info in metainfo:
            try:
                response = get(file_info['url'])
                response.raise_for_status()
            except HTTPError as e:
                if response.status_code == 403:
                    logging.warning(f"Skip 403 Forbidden: {file_info['url']}")
                    continue
                else:
                    raise e
            try:
                s3_client.put_object(
                    Bucket=bucket,
                    Key=file_info['key'],
                    Body=response.content
                )
                logging.info(f"Uploaded: {file_info['key']}")
            except Exception as e:
                raise e



if __name__ == '__main__':
    s3_client = s3_create_client()
    s3_delete_bucket(s3_client, 'nyc-taxi')
    # s3_create_bucket(s3_client, 'test1')