# deduplicate-elasticsearch
A python script to detect duplicate documents in Elasticsearch. Once duplicates have been detected, it is straightforward to call a delete operation to remove duplicates.

For a full description on how this script works including an analysis of the memory requirements, see: https://alexmarquardt.com/2018/07/23/deduplicating-documents-in-elasticsearch/

在原始脚本基础上，添加删除重复文档的实现逻辑，且要留下一个文档
