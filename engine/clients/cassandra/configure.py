import time
from cassandra.cluster import Cluster

from benchmark.dataset import Dataset
from engine.base_client import IncompatibilityError
from engine.base_client.configure import BaseConfigurator
from engine.base_client.distances import Distance
from engine.clients.cassandra.config import get_db_config


class CassandraConfigurator(BaseConfigurator):
    def __init__(self, host, collection_params: dict, connection_params: dict):
        super().__init__(host, collection_params, connection_params)
        self.config = get_db_config(host, connection_params)
        self.keyspace_name = self.config["keyspace_name"]
        self.data_table_name = self.config["data_table_name"]

        self.cluster = Cluster([self.config["host"]])
        self.conn = self.cluster.connect()


    def clean(self):
        self.conn.execute(f"DROP TABLE IF EXISTS {self.keyspace_name}.{self.data_table_name};")


    def recreate(self, dataset: Dataset, collection_params):
        self.conn.execute(f"""
            CREATE KEYSPACE IF NOT EXISTS {self.keyspace_name}
            WITH replication = {{ 'class': 'SimpleStrategy', 'replication_factor': 1 }};
        """)
        print(f"Keyspace '{self.keyspace_name}' created (if not exists).")

        self.conn.set_keyspace(self.keyspace_name)
        self.conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {self.data_table_name} (
                id BIGINT PRIMARY KEY,
                embedding VECTOR<FLOAT, 100>,
            );
        """)
        print(f"Table '{self.data_table_name}' created (if not exists).")


    def delete_client(self):
        self.cluster.shutdown()
