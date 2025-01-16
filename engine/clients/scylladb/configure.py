import time
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
        self.data_table_name = self.config["data_table_name"]
        self.indexes_table_name = self.config["indexes_table_name"]

        self.cluster = Cluster([self.config["host"]])
        self.conn = self.cluster.connect()
        print("ScyllaDB connection created")


    def has_any_rows(self, table_name):
        rows = self.conn.execute(f"""
            SELECT * FROM {table_name}
        """)
        return any(rows)


    def indexes_table_exists(self):
        rows = self.conn.execute(f"""
            SELECT table_name FROM system_schema.tables
            WHERE keyspace_name = '{self.keyspace_name}' AND table_name = '{self.indexes_table_name}';
        """)
        return any(rows)


    def clean(self):
        self.conn.execute(f"DROP TABLE IF EXISTS {self.keyspace_name}.{self.data_table_name};")
        # TODO: uncommend after proper handling of vector types and indexes is implemented in CQL
        # As for now we cannot remove keyspace as it keeps information about indexes created in the past
        # self.conn.execute(f"DROP KEYSPACE IF EXISTS {self.keyspace_name};")
        if self.indexes_table_exists():
            rows = self.conn.execute(f"SELECT id FROM {self.keyspace_name}.{self.indexes_table_name}")
            if any(rows):
                for row in rows:
                    self.conn.execute(f"""
                        UPDATE {self.keyspace_name}.{self.indexes_table_name} 
                        SET canceled = true
                        WHERE id = {row.id};
                    """)

                counter = 0
                while self.has_any_rows(f"{self.keyspace_name}.{self.indexes_table_name}"):
                    print(f"Waiting for indexes to be cleaned ({counter}s)", end="\r")
                    time.sleep(1)
                    counter += 1


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
            CREATE TABLE IF NOT EXISTS {self.data_table_name} (
                id BIGINT PRIMARY KEY,
                description TEXT,
                embedding LIST<FLOAT>
            );
        """)
        print(f"Table '{self.data_table_name}' created (if not exists) in keyspace '{self.keyspace_name}'.")
        self.conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {self.indexes_table_name} (
                id INT PRIMARY KEY,
                indexed_elements_reserved INT,
                indexed_elements_count INT,
                canceled BOOLEAN
            );
        """)
        print(f"Table '{self.indexes_table_name}' created (if not exists) in keyspace '{self.keyspace_name}'.")


    def delete_client(self):
        self.cluster.shutdown()
