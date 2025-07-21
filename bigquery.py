from enum import Enum
from typing import List, Optional

import pandas as pd
from google.cloud import bigquery

BQ_USD_PER_TB = 5


class Bigquery:
    class FileType(Enum):
        CSV = bigquery.SourceFormat.CSV
        PARQUET = bigquery.SourceFormat.PARQUET

    class WriteMode(Enum):
        LOAD_TRUNCATE = bigquery.job.WriteDisposition.WRITE_TRUNCATE
        APPEND = bigquery.job.WriteDisposition.WRITE_APPEND

    class LoadJobConfig(Enum):
        SKIP_LEADING_ROWS = 'skip_leading_rows'

    def __init__(self) -> None:
        self._client = bigquery.Client()

    @staticmethod
    def _read_path(path: str) -> str:
        with open(path, 'r') as f:
            return f.read()

    def run_dml_script_from_path(self, sql_path: str) -> None:
        self.run_dml_script(self._read_path(sql_path))

    def run_dml_script(self, sql: str) -> None:
        script_job = self._client.query(sql)
        script_job.result()
        print(f'dml script executed successfully. {script_job.num_dml_affected_rows} records were affected')

    def run_query_from_path(self, sql_path: str) -> List[dict]:
        return self.run_query(self._read_path(sql_path))

    def run_query(self, sql: str) -> List[dict]:
        results = self._client.query(sql)
        results_dict = list(map(dict, [r for r in results]))
        print(
            f'{len(results_dict)} records returned from bigquery. {results.total_bytes_billed / 1024 / 1024} MB billed '
            f'({round((results.total_bytes_billed / 1024 / 1024 / 1024 / 1024), 4) * BQ_USD_PER_TB} USD)'
        )
        return results_dict

    def load_from_local(self, file_path: str, file_type, write_mode, table_path: str, conf=None) -> None:
        if conf is None:
            conf = {}

        job_config = bigquery.LoadJobConfig(source_format=file_type.value,
                                            write_disposition=write_mode.value,
                                            autodetect=True
                                            )
        # additional load configs
        job_config.skip_leading_rows = conf.get(bigquery.LoadJobConfig.skip_leading_rows, 1)
        job_config.allow_jagged_rows = conf.get(bigquery.LoadJobConfig.allow_jagged_rows, 1)
        job_config.allow_quoted_newlines = conf.get(bigquery.LoadJobConfig.allow_quoted_newlines, 1)
        job_config.max_bad_records = conf.get('max_bad_records', 10)

        with open(file_path, 'rb') as f:
            job = self._client.load_table_from_file(f, table_path, job_config=job_config)
        job.result()
        table = self._client.get_table(table_path)
        print(f'load data from  {file_path} to {table_path} completed successfully!')

    def load_from_dataframe(self, df: pd.DataFrame, write_mode, table_path: str, schema: list) -> None:
        job_config = bigquery.LoadJobConfig(schema=schema, write_disposition=write_mode.value)
        job = self._client.load_table_from_dataframe(df, table_path, job_config=job_config)
        job.result()
        table = self._client.get_table(table_path)
        print(f'loaded {len(df)} rows and {len(df.columns)} columns to {table_path}')

    def load_from_local_parquet(self, file_path: str, write_mode, table_path: str) -> None:
        job_config = bigquery.LoadJobConfig(source_format=self.FileType.PARQUET.value,
                                            write_disposition=write_mode.value)

        with open(file_path, 'rb') as f:
            job = self._client.load_table_from_file(f, table_path, job_config=job_config)
        job.result()
        table = self._client.get_table(table_path)
        print(f'loaded {table.num_rows} rows and {len(table.schema)} columns to {table_path}')

    def insert_rows_json(self, records: List[dict], table_path: str) -> None:
        table = self._client.get_table(table_path)
        if table:
            insert_response = self._client.insert_rows_json(json_rows=records, table=table_path)
            if insert_response:
                print(insert_response)
            else:
                print(f'{len(records)} rows inserted successfully to {table_path}')
        else:
            print(f'table {table_path} was not found')

    def verify_table(self, table_path: str, schema: List[bigquery.SchemaField],
                     partitioning_type: Optional[str] = 'date', clustering_fields=Optional[None]) -> None:
        table = self._client.get_table(table_path)

        if not table:
            table = bigquery.Table(table_path, schema=schema)
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field=partitioning_type
            )
            if clustering_fields is not None:
                table.clustering_fields = clustering_fields
            table = self._client.create_table(table)  # Make an API request.
            print("created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id))

    def run_append_script(self, sql: str, destination_table: str) -> None:
        job_config = bigquery.QueryJobConfig(allow_large_results=True,
                                             destination=destination_table,
                                             write_disposition=bigquery.job.WriteDisposition.WRITE_APPEND)
        script_job = self._client.query(sql, job_config=job_config)
        script_job.result()
        print(f'append script executed successfully on table {destination_table}.')
