from couchbase.exceptions import CouchbaseException
from couchbase_client import CouchbaseCmdManager, CouchbaseQryManager, Cmd, Qry
import random
import asyncio

bucket_name = "cb_bucket_tmp"
scope_name = "scope_tmp"
collection_name = "collection_tmp"
# document_id = str(random.randint(1, 1000000))
document_id = "882318"

cmd_manager = CouchbaseCmdManager(bucket_name, scope_name, collection_name)
qry_manager = CouchbaseQryManager()


async def upsert():
    try:
        json_payload = {
            "id": document_id,
            "name": "Simon Stipcich",
            "age": 33,
            "email": "simon.stipcich@gmail.com",
        }
        await cmd_manager.command(Cmd(document_id, json_payload))
    except CouchbaseException as e:
        print(f"An error occurred: {e}")


async def query():
    try:
        #'CREATE INDEX idx_example ON `{bucket._name}`.`your_scope_name`.`your_collection_name`(property_name)'
        query = f"SELECT * FROM {bucket_name}.{scope_name}.{collection_name} WHERE id = '{document_id}'"
        params = []

        result = qry_manager.query(Qry(query, params))
        for row in result:
            print(row)

    except CouchbaseException as e:
        print(f"An error occurred: {e}")


# asyncio.run(upsert())
asyncio.run(query())
