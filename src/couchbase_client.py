from couchbase.cluster import Cluster, ClusterOptions
from couchbase_core.cluster import PasswordAuthenticator
from couchbase.exceptions import CouchbaseException


class CouchbaseClusterFactory:
    def __init__(self):
        pass

    def create_cluster(self):
        return Cluster(
            "couchbase://localhost",
            ClusterOptions(PasswordAuthenticator("username", "password")),
        )


class CouchbaseBucketManager:
    def __init__(self):
        self._cluster_factory = CouchbaseClusterFactory()
        self._cluster = self._cluster_factory.create_cluster()
        pass

    def get_bucket(self, bucket_name):
        return self._cluster.bucket(bucket_name)

    def upsert(self, bucket_name, document_id, json_payload):
        bucket = self.get_bucket(bucket_name)
        bucket.upsert(document_id, json_payload)

    def query(self, bucket_name, query, params):
        bucket = self.get_bucket(bucket_name)
        return self._cluster.query(query, parameters=params)

    def disconnect(self):
        self._cluster.disconnect()
