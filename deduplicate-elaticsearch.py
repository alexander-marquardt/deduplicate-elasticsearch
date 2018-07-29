#!/usr/local/bin/python3
import hashlib
from elasticsearch import Elasticsearch
es = Elasticsearch(["localhost:9200"])
dict_of_duplicate_docs = {}

# The following line defines the fields that will be
# used to determine if a document is a duplicate
keys_to_include_in_hash = ["CAC", "FTSE", "SMI"]


# Process documents returned by the current search/scroll
def populate_dict_of_duplicate_docs(hits):
    for item in hits:
        combined_key = ""
        for mykey in keys_to_include_in_hash:
            combined_key += str(item['_source'][mykey])

        _id = item["_id"]

        hashval = hashlib.md5(combined_key.encode('utf-8')).digest()

        # If the hashval is new, then we will create a new key
        # in the dict_of_duplicate_docs, which will be
        # assigned a value of an empty array.
        # We then immediately push the _id onto the array.
        # If hashval already exists, then
        # we will just push the new _id onto the existing array
        dict_of_duplicate_docs.setdefault(hashval, []).append(_id)


# Loop over all documents in the index, and populate the
# dict_of_duplicate_docs data structure.
def scroll_over_all_docs():
    data = es.search(index="stocks", scroll='1m',  body={"query": {"match_all": {}}})

    # Get the scroll ID
    sid = data['_scroll_id']
    scroll_size = len(data['hits']['hits'])

    # Before scroll, process current batch of hits
    populate_dict_of_duplicate_docs(data['hits']['hits'])

    while scroll_size > 0:
        data = es.scroll(scroll_id=sid, scroll='2m')

        # Process current batch of hits
        populate_dict_of_duplicate_docs(data['hits']['hits'])

        # Update the scroll ID
        sid = data['_scroll_id']

        # Get the number of results that returned in the last scroll
        scroll_size = len(data['hits']['hits'])


def loop_over_hashes_and_remove_duplicates():
    # Search through the hash of doc values to see if any
    # duplicate hashes have been found
    for hashval, array_of_ids in dict_of_duplicate_docs.items():
      if len(array_of_ids) > 1:
        print("********** Duplicate docs hash=%s **********" % hashval)
        # Get the documents that have mapped to the current hasval
        matching_docs = es.mget(index="stocks", doc_type="doc", body={"ids": array_of_ids})
        for doc in matching_docs['docs']:
            # In order to remove the possibility of hash collisions,
            # write code here to check all fields in the docs to
            # see if they are truly identical - if so, then execute a
            # DELETE operation on all except one.
            # In this example, we just print the docs.
            print("doc=%s\n" % doc)



def main():
    scroll_over_all_docs()
    loop_over_hashes_and_remove_duplicates()


main()
