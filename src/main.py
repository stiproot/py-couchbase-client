from couchbase.exceptions import CouchbaseException
from couchbase_client import CouchbaseBucketManager

manager = CouchbaseBucketManager()

bucket_name = "cb_bucket_tmp"
scope_name = "scope_tmp"
collection_name = "collection_tmp"
document_id = "5"

# try:
#     json_payload = {
#         "id": document_id,
#         "name": "Simon Stipcich",
#         "age": 33,
#         "email": "simon.stipcich@gmail.com",
#     }
#     manager.upsert(bucket_name, document_id, json_payload)
# except CouchbaseException as e:
#     print(f"An error occurred: {e}")


try:
    #'CREATE INDEX idx_example ON `{bucket._name}`.`your_scope_name`.`your_collection_name`(property_name)'
    query = f"SELECT * FROM {bucket_name}.{scope_name}.{collection_name} WHERE id = '{document_id}'"
    params = []

    result = manager.query(bucket_name, query, params=params)
    for row in result:
        print(row)

except CouchbaseException as e:
    print(f"An error occurred: {e}")

manager.close()
