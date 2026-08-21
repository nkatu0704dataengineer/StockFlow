from airflow.hooks.base import BaseHook
from minio import Minio
from io import BytesIO
from airflow.exceptions import AirflowNotFoundException
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd

BUCKET_NAME = "stock-market"

def get_minio_client():
    minio = BaseHook.get_connection("minio")
    client = Minio(
        endpoint=minio.extra_dejson["endpoint_url"].split("//")[1],
        access_key=minio.login,
        secret_key=minio.password,
        secure=False
    )
    return client

def get_stock_prices(url):
    import requests
    import json
    api = BaseHook.get_connection("stock_api")
    response = requests.get(
        url,
        headers=api.extra_dejson["headers"]
    )
    response.raise_for_status()
    return json.dumps(response.json()["chart"]["result"][0])


def _store_prices(stock):
    import json
    minio = BaseHook.get_connection("minio")
    client = get_minio_client()
    if not client.bucket_exists(BUCKET_NAME):
        client.make_bucket(BUCKET_NAME)

    stock = json.loads(stock)
    symbol = stock["meta"]["symbol"]
    data = json.dumps(stock, ensure_ascii=False).encode("utf-8")
    objw = client.put_object(
        bucket_name=BUCKET_NAME,
        object_name=f"{symbol}/prices.json",
        data=BytesIO(data),
        length=len(data)
    )
    return f"{objw.bucket_name}/{symbol}"

def _get_formatted_csv(path):
    client = get_minio_client()
    prefix_name=f"{path.split('/')[1]}/formatted_prices/"
    objects = client.list_objects(BUCKET_NAME, prefix=prefix_name, recursive=True)
    for obj in objects:
        if obj.object_name.endswith(".csv"):
            return obj.object_name
    raise AirflowNotFoundException(f"No CSV file found in {prefix_name} in bucket {BUCKET_NAME}")
    
def _load_to_postgres(path):
    """
    Download formatted CSV from MinIO
    and load it into PostgreSQL.
    """
    client = get_minio_client()
    # Download csv from MinIO
    response = client.get_object(
        BUCKET_NAME,
        path
    )
    df = pd.read_csv(response)
    # Create SQLAlchemy engine
    hook = PostgresHook(postgres_conn_id="postgres")
    engine = hook.get_sqlalchemy_engine()
    # Load dataframe into PostgreSQL
    df.to_sql(
        name="stock_market",
        con=engine,
        schema="public",
        if_exists="replace",
        index=False
    )
    response.close()
    response.release_conn()