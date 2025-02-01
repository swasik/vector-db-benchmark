import itertools
import threading
import time
from typing import List, Tuple

import numpy as np
from cassandra.cluster import Cluster

from dataset_reader.base_reader import Query
from engine.base_client.distances import Distance
from engine.base_client.search import BaseSearcher
from engine.clients.cassandra.config import get_db_config
from engine.clients.cassandra.parser import CassandraConditionParser


class CassandraSearcher(BaseSearcher):
    DISTANCE_MAPPING = {
        Distance.L2: "similarity_euclidean",
        Distance.COSINE: "similarity_cosine",
        Distance.DOT: "similarity_dot_product",
    }

    conn = None
    distance = None
    search_params = {}
    parser = CassandraConditionParser()

    @classmethod
    def init_client(cls, host, distance, connection_params: dict, search_params: dict):
        cls.config = get_db_config(host, connection_params)
        cls.keyspace_name = cls.config["keyspace_name"]
        cls.data_table_name = cls.config["data_table_name"]

        cls.cluster = Cluster([cls.config["host"]])
        cls.conn = cls.cluster.connect()
        cls.conn.set_keyspace(cls.keyspace_name)

        cls.select_query = cls.conn.prepare(f"""
            SELECT * FROM {cls.keyspace_name}.{cls.data_table_name}
            ORDER BY embedding ANN OF ?
            LIMIT ?;
        """)

    @classmethod
    def search_one(cls, query: Query, top) -> List[Tuple[int, float]]:
        # TODO: Use query.metaconditions for datasets with filtering
        result = cls.conn.execute(cls.select_query.bind([query.vector, top]))
        
        return result

    @classmethod
    def delete_client(cls):
        cls.cluster.shutdown()
