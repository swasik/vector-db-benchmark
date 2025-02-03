import time
from typing import List

import numpy as np
from cassandra.cluster import Cluster
from cassandra.concurrent import execute_concurrent_with_args

from dataset_reader.base_reader import Record
from engine.base_client import IncompatibilityError
from engine.base_client.distances import Distance
from engine.base_client.upload import BaseUploader
from engine.clients.cassandra.config import get_db_config

from time import sleep


class CassandraUploader(BaseUploader):
    conn = None
    upload_params = {}


    @classmethod
    def init_client(cls, host, distance, connection_params, upload_params):
        cls.config = get_db_config(host, connection_params)
        cls.keyspace_name = cls.config["keyspace_name"]
        cls.data_table_name = cls.config["data_table_name"]

        cls.cluster = Cluster([cls.config["host"]])
        cls.conn = cls.cluster.connect()

        cls.conn.set_keyspace(cls.keyspace_name)

        cls.insert_query = cls.conn.prepare(f"""
            INSERT INTO {cls.data_table_name} (id, embedding) VALUES (?, ?)
        """)

        cls.upload_params = upload_params


    @classmethod
    def upload_batch(cls, batch: List[Record]):
        try:
            data = []
            for record in batch:
                data.append((record.id, record.vector))

            execute_concurrent_with_args(cls.conn, cls.insert_query, data, concurrency=100)
        except Exception as e:
            print(e)


    @classmethod
    def post_upload(cls, distance):
        try:
            while True:
                try:
                    cls.conn.execute(f"""
                        SELECT * FROM {cls.keyspace_name}.{cls.data_table_name}
                        ORDER BY embedding ANN OF {np.ones(100).tolist()}
                        LIMIT 1
                    """)
                    break
                except Exception as e:
                    # TODO: Catch only INDEX_NOT_AVAILABLE error
                    print(e)
                    time.sleep(10)
            print(f"Index '{cls.index_name}' created (if not exists).")
        except Exception as e:
            print(e)

        return {}


    @classmethod
    def delete_client(cls):
        cls.cluster.shutdown()
