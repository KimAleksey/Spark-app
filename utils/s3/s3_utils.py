import boto3
import logging
from dotenv import load_dotenv
from os import getenv
from botocore.exceptions import ClientError


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
    try:
        if not s3_check_bucket_exists(s3_client, bucket_name):
            s3_client.create_bucket(Bucket=bucket_name)
            logging.info(f"Bucket {bucket_name} создан.")
        else:
            logging.info(f"Bucket {bucket_name} уже существует.")
    except Exception as e:
        raise e


def s3_delete_bucket(s3_client: boto3.client, bucket_name: str) -> None:
    """
    Удаляем bucket

    :param s3_client: S3 client
    :param bucket_name: Bucket name
    :return: None
    """
    try:
        if s3_check_bucket_exists(s3_client, bucket_name):
            s3_client.delete_bucket(Bucket=bucket_name)
            logging.info(f"Bucket {bucket_name} удален")
        else:
            logging.info(f"Bucket {bucket_name} не существует")
    except Exception as e:
        raise e


# def s3_load_file(s3_client: boto3.client, bucket_name: str, file_name: str) -> str:


if __name__ == '__main__':
    s3_client = s3_create_client()
    s3_delete_bucket(s3_client, 'test')
    s3_create_bucket(s3_client, 'test')