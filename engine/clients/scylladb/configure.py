from cassandra.cluster import Cluster

from benchmark.dataset import Dataset
from engine.base_client import IncompatibilityError
from engine.base_client.configure import BaseConfigurator
from engine.base_client.distances import Distance
from engine.clients.scylladb.config import get_db_config


class ScyllaDbConfigurator(BaseConfigurator):
    def __init__(self, host, collection_params: dict, connection_params: dict):
        super().__init__(host, collection_params, connection_params)
        self.config = get_db_config(host, connection_params)
        self.keyspace_name = self.config["keyspace_name"]
        self.table_name = self.config["table_name"]

        self.cluster = Cluster([self.config["host"]])
        self.conn = self.cluster.connect()
        print("ScyllaDB connection created")


    def clean(self):      
        self.conn.execute(f"DROP TABLE IF EXISTS {self.keyspace_name}.{self.table_name};")
        self.conn.execute(f"DROP KEYSPACE IF EXISTS {self.keyspace_name};")


    def recreate(self, dataset: Dataset, collection_params):
        if dataset.config.distance in [Distance.DOT, Distance.L2]:
            raise IncompatibilityError

        self.conn.execute(f"""
        CREATE KEYSPACE IF NOT EXISTS {self.keyspace_name}
        WITH replication = {{ 'class': 'SimpleStrategy', 'replication_factor': 1 }};
        """)
        print(f"Keyspace '{self.keyspace_name}' created (if not exists).")

        self.conn.set_keyspace(self.keyspace_name)
        self.conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            id BIGINT PRIMARY KEY,
            description TEXT,
            embedding LIST<FLOAT>
        );
        """)
        print(f"Table '{self.table_name}' created (if not exists) in keyspace '{self.keyspace_name}'.")


    def delete_client(self):
        self.cluster.shutdown()
