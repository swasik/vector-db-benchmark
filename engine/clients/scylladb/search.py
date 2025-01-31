import itertools
import threading
import time
from typing import List, Tuple

from multiprocessing import Queue

import numpy as np
from cassandra.cluster import Cluster

from dataset_reader.base_reader import Query
from engine.base_client.distances import Distance
from engine.base_client.search import BaseSearcher
from engine.clients.scylladb.config import get_db_config
from engine.clients.scylladb.parser import ScyllaDbConditionParser

MAX_PROCESSES = 1000

class ScyllaDbSearcher(BaseSearcher):
    conn = None
    distance = None
    search_params = {}
    parser = ScyllaDbConditionParser()
    scylladb_id = 0
    counter = itertools.count()


    @classmethod
    def next(cls):
        return next(cls.counter) * MAX_PROCESSES + cls.scylladb_id

    @classmethod
    def init_client(cls, host, distance, connection_params: dict, search_params: dict):
        if "scylladb_ids" not in search_params:
            queue = Queue()
            for i in range(MAX_PROCESSES):
                queue.put(i)
            search_params["scylladb_ids"] = queue
        cls.scylladb_id = search_params["scylladb_ids"].get()
        cls.config = get_db_config(host, connection_params)
        cls.keyspace_name = cls.config["keyspace_name"]
        cls.queries_table_name = cls.config["queries_table_name"]

        cls.cluster = Cluster([cls.config["host"]])
        cls.conn = cls.cluster.connect()
        cls.conn.set_keyspace(cls.keyspace_name)

        ef = search_params["config"]["hnsw_ef"]
        if distance == Distance.COSINE:
            cls.insert_query = cls.conn.prepare(f"""
                INSERT INTO {cls.queries_table_name} 
                    (id, vector_index_id, embedding, param_ef_search, top_results_limit, result_computed, result_keys, result_scores) 
                VALUES (?, 1, ?, {ef}, ?, false, NULL, NULL);
            """)
        else:
            raise NotImplementedError(f"Unsupported distance metric {cls.distance}")
        
        cls.status_query = cls.conn.prepare(f"""
            SELECT id FROM {cls.queries_table_name} 
                WHERE id = ? AND result_computed = true
            ALLOW FILTERING;
        """)
        cls.results_query = cls.conn.prepare(f"""
            SELECT result_keys, result_scores FROM {cls.queries_table_name} 
            WHERE id = ?
        """)

    @classmethod
    def search_one(cls, query: Query, top) -> List[Tuple[int, float]]:
        # TODO: Use query.metaconditions for datasets with filtering
        id = cls.next()
        cls.conn.execute(cls.insert_query.bind([id, query.vector, top]))
        while True:
            time.sleep(0.001)
            if any(cls.conn.execute(cls.status_query.bind([id]))):
                break

        result = cls.conn.execute(cls.results_query.bind([id])).one()
        if not any(result):
            return []
        return zip(result.result_keys, result.result_scores)

    @classmethod
    def delete_client(cls):
        cls.cluster.shutdown()
