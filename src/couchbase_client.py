from couchbase.cluster import Cluster
from couchbase.options import ClusterOptions
from couchbase.auth import PasswordAuthenticator
from couchbase.exceptions import CouchbaseException
from secret_provider import SecretProvider


class CouchbaseClusterFactory:
    def __init__(self):
        self._secret_provider = SecretProvider()

    def create_cluster(self):
        username = self._secret_provider.get_secret("COUCHBASE_USERNAME")
        password = self._secret_provider.get_secret("COUCHBASE_PASSWORD")
        return Cluster(
            "couchbase://localhost",
            ClusterOptions(PasswordAuthenticator(username, password)),
        )


class CouchbaseBucketManager:
    def __init__(self):
        self._cluster_factory = CouchbaseClusterFactory()
        self._cluster = self._cluster_factory.create_cluster()

    def get_bucket(self, bucket_name):
        return self._cluster.bucket(bucket_name)

    def upsert(self, bucket_name, document_id, json_payload):
        bucket = self.get_bucket(bucket_name)
        scope = bucket.scope("scope_tmp")
        collection = scope.collection("collection_tmp")
        collection.upsert(document_id, json_payload)

    def query(self, bucket_name, query, params):
        return self._cluster.query(query, params=params)

    def close(self):
        self._cluster.close()
