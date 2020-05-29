from bigquery import BQClient

def run_main():
    bq_client = BQClient.from_config()

    query_str = f'SELECT * FROM {bq_client.dataset_id}.{bq_client.table_id} LIMIT 1000'
    bq_client.query(query_str)

    bq_client.export_csv()

    for row in bq_client.results:
        print(f'{row[0]} | {row[1]}')


if __name__ == '__main__':
    run_main()

