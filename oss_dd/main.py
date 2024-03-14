import json
import os
from opensearchpy import OpenSearch
from opensearch_dsl import Search
from typing import Generator

CONDITION = json.loads(os.getenv("CONDITION"))
OSS_HOST = os.getenv("OSS_HOST", "localhost")
OSS_PORT = int(os.getenv("OSS_PORT", 9200))
INDEX = os.getenv("INDEX")

client = OpenSearch(
    hosts=[{'host': OSS_HOST, 'port': OSS_PORT}],
    http_compress=True,
    use_ssl=True,
    ssl_assert_hostname=False,
    ssl_show_warn=False,
)


def get_id_list(condition: dict) -> Generator[dict, None, None]:
    s = Search(using=client, index=INDEX).query("match", **condition)

    response = s.execute()
    print("Got %d Hits:" % response['hits']['total']['value'])

    return s.scan()


def delete_data(customer: dict):
    # 削除対象確認用に標準出力に出力する。
    print(f'{customer["customer_id"]},{customer["tenant_id"]},{customer["last_name"]}')
    client.delete(index=INDEX, id=str(customer["customer_id"]))


def main():
    deleting_customer_list = get_id_list(CONDITION)
    for customer in deleting_customer_list:
        delete_data(customer)

    print('Data deleted')


if __name__ == '__main__':
    main()
