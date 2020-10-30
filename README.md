# deduplicate-elasticsearch
A python script to detect duplicate documents in Elasticsearch. Once duplicates have been detected, it is straightforward to call a delete operation to remove duplicates.

For a full description on how this script works including an analysis of the memory requirements, see: https://alexmarquardt.com/2018/07/23/deduplicating-documents-in-elasticsearch/

The following files are expected to exist in the directory from which this module is executed:

* secrets.py  
    - A collection of Elasticsearch vars and Authentication credentials with expected defintions:
        - ES_HOST = "URL_WITHOUT_SCHEMA"
        - ES_USER = "elastic"
        - ES_PASSWORD = "elastic"
        - ES_PORT = "9200"
        - ES_INDEX = "source-YYYY."
