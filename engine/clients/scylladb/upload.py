from typing import List

import numpy as np
from cassandra.cluster import Cluster
from cassandra.concurrent import execute_concurrent_with_args

from dataset_reader.base_reader import Record
from engine.base_client import IncompatibilityError
from engine.base_client.distances import Distance
from engine.base_client.upload import BaseUploader
from engine.clients.scylladb.config import get_db_config


class ScyllaDbUploader(BaseUploader):
    DISTANCE_MAPPING = {
        # TODO: Add support for this in CQL when adding support for vector type
        Distance.L2: "vector_l2_ops",
        Distance.COSINE: "vector_cosine_ops",
    }
    conn = None
    upload_params = {}


    @classmethod
    def init_client(cls, host, distance, connection_params, upload_params):
        cls.config = get_db_config(host, connection_params)
        cls.keyspace_name = cls.config["keyspace_name"]
        cls.data_table_name = cls.config["data_table_name"]
        cls.indexes_table_name = cls.config["indexes_table_name"]
        cls.param_m = upload_params["hnsw_config"]["m"]
        cls.param_ef_construct = upload_params["hnsw_config"]["ef_construct"]

        cls.cluster = Cluster([cls.config["host"]])
        cls.conn = cls.cluster.connect()
        print("ScyllaDB connection created")

        cls.conn.set_keyspace(cls.keyspace_name)
        cls.conn.execute(f"""
            INSERT INTO {cls.indexes_table_name} 
                (id, indexed_elements_count, param_m, param_ef_construct, dimension, canceled)
            VALUES (1, 0, {cls.param_m}, {cls.param_ef_construct}, 96, false);
        """)
        cls.insert_query = cls.conn.prepare(f"""
            INSERT INTO {cls.data_table_name} (id, description, embedding) VALUES (?, ?, ?)
        """)

        cls.upload_params = upload_params


    @classmethod
    def upload_batch(cls, batch: List[Record]):
        try:
            data = []
            for record in batch:
                data.append((record.id, "", record.vector))

            insert_query = cls.conn.prepare(f"""
                INSERT INTO {cls.data_table_name} (id, description, embedding) VALUES (?, ?, ?)
            """)
            execute_concurrent_with_args(cls.conn, insert_query, data, concurrency=100)
        except Exception as e:
            print(e)


    @classmethod
    def post_upload(cls, distance):
        try:
            hnsw_distance_type = cls.DISTANCE_MAPPING[distance]
        except KeyError:
            raise IncompatibilityError(f"Unsupported distance metric: {distance}")

        # TODO: Schedule creating the index
        # cls.conn.execute(
        #     f"CREATE INDEX ON items USING hnsw (embedding {hnsw_distance_type}) WITH (m = {cls.upload_params['hnsw_config']['m']}, ef_construction = {cls.upload_params['hnsw_config']['ef_construct']})"
        # )

        return {}


    @classmethod
    def delete_client(cls):
        cls.cluster.shutdown()
