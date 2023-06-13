#!/usr/local/bin/python3

# A description and analysis of this code can be found at 
# https://alexmarquardt.com/2018/07/23/deduplicating-documents-in-elasticsearch/

# 删除es里重复的文档

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
# 筛选的重复字段
keys_to_include_in_hash = ["CAC", "FTSE", "SMI"]

# 查找重复字段的索引，可以加通配符*
scan_index = "stocks*"

# 删除文档的索引，不可以加通配符
mget_index = "stock"

# 根据具体es的内容得到
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
                # 删除一个重复文档后弹出
                logger.info("doc=%s\n" % doc)
                es.delete(index=doc['_index'], id=doc['_id'])
                count -= 1
                logger.info(f"delete index: {doc['_index']} _id: {doc['_id']}")
                if count <= 1: break


def main():
    logger.info("查找全部文档...")
    scroll_over_all_docs()
    logger.info("删除重复文档...")
    loop_over_hashes_and_remove_duplicates()


main()
