#!/usr/local/bin/python3

# A description and analysis of this code can be found at 
# https://alexmarquardt.com/2018/07/23/deduplicating-documents-in-elasticsearch/

# Delete duplicate documents in es

from loguru import logger
import hashlib
from elasticsearch import Elasticsearch, helpers

ES_HOST = 'localhost:9200'
ES_USER = 'elastic'
ES_PASSWORD = 'elastic'

es = Elasticsearch([ES_HOST], http_auth=(ES_USER, ES_PASSWORD))
dict_of_duplicate_docs = {}

# The following line defines the fields that will be
# used to determine if a document is a duplicate
# Filtered Repeated Fields
keys_to_include_in_hash = ["CAC", "FTSE", "SMI"]

# Find the index of repeated fields, you can add wildcard *
scan_index = "stocks*"

# Delete the index of the document, wildcards cannot be added
mget_index = "stock"

# Get according to the content of the specific es
doc_type = "_doc"


# Process documents returned by the current search/scroll
def populate_dict_of_duplicate_docs(hit):

    combined_key = ""
    for mykey in keys_to_include_in_hash:
        combined_key += str(hit['_source'][mykey])

    _id = hit["_id"]

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
    for hit in helpers.scan(es, index=scan_index):
        populate_dict_of_duplicate_docs(hit)


def loop_over_hashes_and_remove_duplicates():
    # Search through the hash of doc values to see if any
    # duplicate hashes have been found
    for hashval, array_of_ids in dict_of_duplicate_docs.items():
        if len(array_of_ids) > 1:
            logger.info("********** Duplicate docs hash=%s **********" % hashval)
            # Get the documents that have mapped to the current hasval
            matching_docs = es.mget(index=mget_index, doc_type=doc_type, body={"ids": array_of_ids})
            count = len(matching_docs['docs'])
            for doc in matching_docs['docs']:
                # popup after deleting a duplicate document
                logger.info("doc=%s\n" % doc)
                es.delete(index=doc['_index'], id=doc['_id'])
                count -= 1
                logger.info(f"delete index: {doc['_index']} _id: {doc['_id']}")
                if count <= 1: break


def main():
    logger.info("scroll over all docs ...")
    scroll_over_all_docs()
    logger.info("remove duplicates ...")
    loop_over_hashes_and_remove_duplicates()


main()
