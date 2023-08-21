from couchbase.exceptions import CouchbaseException
from couchbase_client import CouchbaseBucketManager

manager = CouchbaseBucketManager()

bucket_name = "<<name>>"
document_id = "example_doc"

try:
    json_payload = {
        "id": document_id,
        "name": "John Doe",
        "age": 30,
        "email": "john.doe@example.com",
    }
    manager.upsert(bucket_name, document_id, json_payload)
except CouchbaseException as e:
    print(f"An error occurred: {e}")


try:
    query = f"SELECT * FROM {bucket_name} WHERE id = $1"
    params = [document_id]

    result = manager.query(bucket_name, query, parameters=params)
    for row in result:
        print(row)
except CouchbaseException as e:
    print(f"An error occurred: {e}")

manager.disconnect()
